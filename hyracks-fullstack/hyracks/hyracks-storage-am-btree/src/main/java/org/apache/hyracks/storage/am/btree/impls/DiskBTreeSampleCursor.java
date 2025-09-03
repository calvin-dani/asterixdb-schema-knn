/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.btree.impls;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.common.api.ILSMIndexBatchPointCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

// todo: copies parts from DiskBTreePointSearchCursor and BTreeRangeSearchCursor
//  unify the code and remove duplication
// Also extract abstract out the common parts between DiskBTreeSampleCursor and ColumnBTreeSampleCursor
public final class DiskBTreeSampleCursor extends EnforcedIndexCursor implements ITreeIndexCursor {

    private static final Logger LOGGER = LogManager.getLogger();
    private final DiskBTree bTree;
    private final BTreeOpContext bTreeOpCtx;
    private final IBufferCacheReadContext bufferCacheOpCtx;
    private final IBTreeLeafFrame leafFrame;
    private final ITreeIndexTupleReference frameTuple;
    private long totalTimeTakenToFindRandomLeaf = 0;
    private long totalTimeTakenToFindRandomTuples = 0;

    // todo: need a map to keep track of keys already seen, to avoid duplicates.
    // u64: (pageId << 32) | tupleIndex
    private final LongSet seenTupleIndexes;

    // Cardinality variables
    private static final int MAX_LEAF_FINDING_ATTEMPTS = 500; // Setting just a random value for now
    private boolean endedPreemptively = false;
    private final int componentSampleCardinality;
    private final Random randomNumGen;
    // Number of LIVE tuples sampled from the component so far.
    private int sampledCount;
    private boolean continueCurrentLeaf = false;
    private int hasNextAttemptCount = 0;
    private int totalAccessCount;

    private ICachedPage page = null;
    private int pageId = -1;
    private int rootPageId = -1;

    private IBufferCache bufferCache;
    private int fileId = -1;

    // search predicate
    private final ILSMIndexBatchPointCursor searchCursor;
    private final BatchPredicateWithKeys batchPredicate;
    private final List<ITupleReference> searchKeys;
    private final BitSet foundIndexes;

    public DiskBTreeSampleCursor(DiskBTree diskBTree, IBTreeLeafFrame leafFrame, int componentSampleCardinality,
            long sampleSeed, BTreeOpContext ctx, IBufferCacheReadContext bufferCacheOpCtx,
            ILSMIndexBatchPointCursor searchCursor) {
        this.bTree = diskBTree;
        this.leafFrame = leafFrame;
        this.randomNumGen = new Random(sampleSeed);
        this.bTreeOpCtx = ctx;
        this.bufferCacheOpCtx = bufferCacheOpCtx;
        this.componentSampleCardinality = componentSampleCardinality;
        this.frameTuple = leafFrame.createTupleReference();
        this.searchCursor = searchCursor;
        this.seenTupleIndexes = new LongOpenHashSet();
        this.foundIndexes = new BitSet();
        this.searchKeys = new ArrayList<>();
        this.batchPredicate = new BatchPredicateWithKeys();
        this.totalAccessCount = 0;
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        if (page != null) {
            releasePage();
        }
        pageId = ((BTreeCursorInitialState) initialState).getPageId();
        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();
        page = initialState.getPage();
        leafFrame.setPage(page);
        if (leafFrame.getTupleCount() > 0) {
            continueCurrentLeaf = true;
        }
    }

    // todo: please convert it into an iterative function
    @Override
    protected boolean doHasNext() throws HyracksDataException {
        if (sampledCount >= componentSampleCardinality || hasNextAttemptCount >= MAX_LEAF_FINDING_ATTEMPTS) {
            endedPreemptively = true;
            return false; // No more samples to take from this component.
        }

        totalAccessCount++;
        // todo: continueCurrentLeaf if true, picks elements from the same leaf page as the last call to doHasNext()
        // currently, we are picking one item from each leaf page, and then randomly picking another leaf page
        // and picking one item from there, and so on.
        // we can batch this to pick multiple items from the same leaf page.
        if (!continueCurrentLeaf) {
            ICachedPage nextLeaf = findNextRandomLeafPage();
        }

        // todo: can we avoid the variable continueCurrentLeaf altogether? -> ig so
        continueCurrentLeaf = false;
        int foundTupleIndex = findRandomTuple();
        long pageTupleKey = getPageTupleKey(pageId, foundTupleIndex);
        if (foundTupleIndex == -1 || seenTupleIndexes.contains(pageTupleKey)) {
            hasNextAttemptCount++;
            return doHasNext();
        }

        searchKeys.clear();
        foundIndexes.clear();
        searchKeys.add(frameTuple);
        batchPredicate.reset(searchKeys);
        searchCursor.setPredicate(batchPredicate);

        searchCursor.doHasNextWithPredicate(foundIndexes);
        if (foundIndexes.isEmpty()) {
            hasNextAttemptCount = 0;
            sampledCount++;
            seenTupleIndexes.add(pageTupleKey);
            return true;
        }

        hasNextAttemptCount++;
        return doHasNext();
    }

    @Override
    public void print() {
        LOGGER.info(
                "StatsLogging: Sampled tree {} with {} tuples from BTree, target={} endedPreemptively={} totalAccessCount={} totalTimeToFindRandomLeaf={} totalTimeToFindRandomTuples={}",
                bTree, sampledCount, componentSampleCardinality, endedPreemptively, totalAccessCount,
                totalTimeTakenToFindRandomLeaf, totalTimeTakenToFindRandomTuples);
    }

    private long getPageTupleKey(int pageId, int tupleIndex) {
        return (((long) pageId) << 32) | (tupleIndex & 0xffffffffL);
    }

    private ICachedPage findNextRandomLeafPage() throws HyracksDataException {
        long nanos = System.nanoTime();
        int numberOfAttempts = 0;
        while (numberOfAttempts < MAX_LEAF_FINDING_ATTEMPTS) {
            releasePage();
            ICachedPage rootPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPageId));
            ICachedPage randomLeafPage = bTree.getRandomLeafPage(rootPage, bTreeOpCtx, bufferCacheOpCtx);
            // page is already pinned in the above call
            // a bad side effect?
            long leafPageDiskPageId = randomLeafPage.getDiskPageId();
            page = randomLeafPage;
            leafFrame.setPage(page);
            pageId = BufferedFileHandle.getPageId(leafPageDiskPageId);
            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                numberOfAttempts++;
                continue;
            }
            totalTimeTakenToFindRandomLeaf += (System.nanoTime() - nanos);
            return leafFrame.getPage();
        }
        // todo: replace with proper exception
        throw new HyracksDataException("Attempt cycle exhausted while trying to find a random leaf page with tuples.");
    }

    private int findRandomTuple() {
        long nanos = System.nanoTime();
        totalAccessCount--; // compensate for the increment in doHasNext
        int numberOfTuples = leafFrame.getTupleCount();
        for (int i = 0; i < numberOfTuples; i++) {
            totalAccessCount++;
            //            int randomTupleIndex = (int) (randomNumGen.nextDouble() * numberOfTuples);
            int randomTupleIndex = ThreadLocalRandom.current().nextInt(numberOfTuples);
            frameTuple.resetByTupleIndex(leafFrame, randomTupleIndex);
            if (!frameTuple.isAntimatter()) {
                totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
                return randomTupleIndex;
            }
        }
        totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
        return -1;
    }

    @Override
    protected void doNext() throws HyracksDataException {
        // NoOp
    }

    @Override
    protected void doDestroy() throws HyracksDataException {
        // No Op all resources are released in the close call
    }

    @Override
    protected void doClose() throws HyracksDataException {
        if (page != null) {
            releasePage();
        }
        // todo: haven't given a thought about making the cursor reusable after close
        sampledCount = 0;
        page = null;
        seenTupleIndexes.clear();
        pageId = -1;
    }

    @Override
    protected ITupleReference doGetTuple() {
        return frameTuple;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    private void releasePage() {
        bufferCache.unpin(page);
        page = null;
    }
}
