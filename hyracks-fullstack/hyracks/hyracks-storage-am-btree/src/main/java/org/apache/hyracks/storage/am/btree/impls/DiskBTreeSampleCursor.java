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

import org.apache.hyracks.api.exceptions.ErrorCode;
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

public final class DiskBTreeSampleCursor extends EnforcedIndexCursor implements ITreeIndexCursor {

    private static final Logger LOGGER = LogManager.getLogger();
    private final DiskBTree bTree;
    private final BTreeOpContext bTreeOpCtx;
    private final IBufferCacheReadContext bufferCacheOpCtx;
    private final IBTreeLeafFrame leafFrame;
    private final ITreeIndexTupleReference frameTuple;

    // u64: (pageId << 32) | tupleIndex
    private final LongSet seenTupleIndexes;

    // Sampling related fields
    private final int maxLeafFindingAttempts;
    private final long componentSampleCardinality;
    private final Random randomNumGen;

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

    // Number of LIVE tuples sampled from the component so far.
    private int sampledCount;
    private int hasNextAttemptCount = 0;
    private int totalAccessCount;

    // Debug and traceability
    private long totalTimeTakenToFindRandomLeaf = 0;
    private long totalTimeTakenToFindRandomTuples = 0;
    private boolean endedPreemptively = false;

    public DiskBTreeSampleCursor(DiskBTree diskBTree, IBTreeLeafFrame leafFrame, long componentSampleCardinality,
            long sampleSeed, BTreeOpContext ctx, IBufferCacheReadContext bufferCacheOpCtx,
            ILSMIndexBatchPointCursor searchCursor, int maxLeafFindingAttempts) {
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
        this.maxLeafFindingAttempts = maxLeafFindingAttempts;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        if (page != null) {
            releasePage();
        }
        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();
    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        while (sampledCount < componentSampleCardinality && hasNextAttemptCount < maxLeafFindingAttempts) {
            totalAccessCount++;

            findNextRandomLeafPage();

            int foundTupleIndex = findRandomTuple();
            long pageTupleKey = getPageTupleKey(pageId, foundTupleIndex);

            // Skip if no valid tuple found or already seen
            if (foundTupleIndex == -1 || seenTupleIndexes.contains(pageTupleKey)) {
                hasNextAttemptCount++;
                continue;
            }

            // Check if tuple exists in newer LSM components
            searchKeys.clear();
            foundIndexes.clear();
            searchKeys.add(frameTuple);
            batchPredicate.reset(searchKeys);
            searchCursor.setPredicate(batchPredicate);
            searchCursor.doHasNextWithPredicate(foundIndexes);

            if (foundIndexes.isEmpty()) {
                // Tuple is unique and not found in newer components - valid sample
                hasNextAttemptCount = 0;
                sampledCount++;
                seenTupleIndexes.add(pageTupleKey);
                return true;
            }

            // Tuple exists in newer component, try again
            hasNextAttemptCount++;
        }

        endedPreemptively = (sampledCount < componentSampleCardinality);
        return false;
    }

    private long getPageTupleKey(int pageId, int tupleIndex) {
        return (((long) pageId) << 32) | (tupleIndex & 0xffffffffL);
    }

    /**
     * Navigates from the root to a random leaf page by uniformly selecting a child at each interior level.
     * <p>
     * <b>Sampling bias note:</b> This gives each leaf a probability proportional to
     * {@code ∏(1/fan_out_of_ancestor)}. If interior nodes at the same level have different fan-outs
     * (e.g. the rightmost node is partially full), leaves under low-fan-out ancestors are slightly
     * over-represented. For well-balanced B-trees the bias is negligible.
     * </p>
     */
    private void findNextRandomLeafPage() throws HyracksDataException {
        long nanos = System.nanoTime();
        int numberOfAttempts = 0;
        while (numberOfAttempts < maxLeafFindingAttempts) {
            releasePage();
            // Pass the random generator to ensure reproducible sampling
            ICachedPage randomLeafPage =
                    bTree.getRandomLeafPage(rootPageId, bTreeOpCtx, bufferCacheOpCtx, randomNumGen);
            // page is already pinned in the above call
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
            return;
        }

        throw HyracksDataException.create(ErrorCode.RANDOM_SAMPLE_LEAF_NOT_FOUND, maxLeafFindingAttempts);
    }

    /**
     * Picks a random non-antimatter tuple from the current leaf page.
     * <p>
     * Chooses a random starting index and scans sequentially (wrapping around),
     * guaranteeing every slot is visited exactly once. Returns the first
     * non-antimatter tuple found, or -1 if the entire leaf is antimatter.
     * </p>
     */
    private int findRandomTuple() {
        long nanos = System.nanoTime();
        totalAccessCount--; // compensate for the increment in doHasNext
        int numberOfTuples = leafFrame.getTupleCount();
        int idx = randomNumGen.nextInt(numberOfTuples);
        for (int i = 0; i < numberOfTuples; i++) {
            totalAccessCount++;
            frameTuple.resetByTupleIndex(leafFrame, idx);
            if (!frameTuple.isAntimatter()) {
                totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
                return idx;
            }
            idx++;
            if (idx == numberOfTuples) {
                idx = 0;
            }
        }
        totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
        return -1;
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "{} stats - sampledCount: {}, totalAccessCount: {}, "
                            + "totalTimeTakenToFindRandomLeaf: {} ns, totalTimeTakenToFindRandomTuples: {} ns, "
                            + "endedPreemptively: {}",
                    this.getClass().getName(), sampledCount, totalAccessCount, totalTimeTakenToFindRandomLeaf,
                    totalTimeTakenToFindRandomTuples, endedPreemptively);
        }
        if (page != null) {
            releasePage();
        }
        sampledCount = 0;
        page = null;
        seenTupleIndexes.clear();
        pageId = -1;
        // Reset debug counters
        totalAccessCount = 0;
        totalTimeTakenToFindRandomLeaf = 0;
        totalTimeTakenToFindRandomTuples = 0;
        endedPreemptively = false;
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
        if (page != null) {
            // first page of the cursor.
            bufferCache.unpin(page);
        }
        page = null;
    }
}
