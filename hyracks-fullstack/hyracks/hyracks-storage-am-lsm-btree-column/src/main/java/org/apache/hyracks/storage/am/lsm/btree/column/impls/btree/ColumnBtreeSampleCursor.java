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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTreeCursorInitialState;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.btree.impls.BatchPredicateWithKeys;
import org.apache.hyracks.storage.am.common.api.ILSMIndexBatchPointCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public final class ColumnBtreeSampleCursor extends EnforcedIndexCursor
        implements ITreeIndexCursor, IColumnReadMultiPageOp {

    private final ColumnBTree bTree;
    private final BTreeOpContext opCtx;
    private final ColumnBTreeReadLeafFrame leafFrame;
    private final IColumnReadContext context;
    private final IColumnTupleIterator frameTuple;

    // todo: need a map to keep track of keys already seen, to avoid duplicates.
    // u64: (pageId << 32) | tupleIndex
    private final LongSet seenTupleIndexes;

    // Cardinality variables
    private static final int MAX_LEAF_FINDING_ATTEMPTS = 20; // Setting just a random value for now
    private final int componentSampleCardinality;
    private final long sampleSeed;
    // Number of LIVE tuples sampled from the component so far.
    private int sampledCount;
    private boolean continueCurrentLeaf = false;
    private int hasNextAttemptCount = 0;

    private IBufferCache bufferCache;
    private int fileId = -1;
    // Need a batch point cursor here, in order to verify the liveness of the tuple based on the key.
    // something like private BatchedPointSearchCursor batchPointCursor;

    private int rootPageId;
    private ICachedPage page0 = null;
    private int page0Id = -1;
    private int tupleIndex = -1;

    // search predicate
    private final ILSMIndexBatchPointCursor searchCursor;
    private final BatchPredicateWithKeys batchPredicate;
    private final List<ITupleReference> searchKeys;
    private final BitSet foundIndexes;

    public ColumnBtreeSampleCursor(ColumnBTree columnBTree, ColumnBTreeReadLeafFrame leafFrame,
            BTreeOpContext opContext, IColumnReadContext context, int componentSampleCardinality, long sampleSeed,
            int index, ILSMIndexBatchPointCursor searchCursor) {
        this.bTree = columnBTree;
        this.opCtx = opContext;
        this.leafFrame = leafFrame;
        this.context = context;
        this.componentSampleCardinality = componentSampleCardinality;
        this.sampleSeed = sampleSeed;
        this.batchPredicate = new BatchPredicateWithKeys();
        this.searchCursor = searchCursor;
        this.frameTuple = leafFrame.createTupleReference(index, this);
        this.searchKeys = new ArrayList<>();
        this.foundIndexes = new BitSet();
        this.seenTupleIndexes = new LongOpenHashSet();
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // Opening the cursor, and pinning the first random leaf page
        // initially might not be required to pin the segment pages and the column pages,
        // But anyway we need to pin at a later point when hasNext() is called
        if (page0 != null) {
            releasePages();
        }

        page0 = initialState.getPage();
        page0Id = ((BTreeCursorInitialState) initialState).getPageId();

        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();

        leafFrame.setPage(page0);
        if (leafFrame.getTupleCount() > 0) {
            context.preparePageZeroSegments(leafFrame, bufferCache, fileId);
            frameTuple.newPage();
            context.prepareColumns(leafFrame, bufferCache, fileId);
            // setting cursor position to the first tuple
            frameTuple.reset(0, leafFrame.getTupleCount() - 1);
            continueCurrentLeaf = true;
        }
    }

    private void releasePages() throws HyracksDataException {
        //Unpin all column pages first
        context.release(bufferCache);
        frameTuple.unpinColumnsPages();
        if (page0 != null) {
            bufferCache.unpin(page0, context);
        }
    }

    Map<Integer, List<Integer>> p = new HashMap<>();

    // todo: make the version iterative, instead of recursive
    @Override
    protected boolean doHasNext() throws HyracksDataException {
        if (sampledCount >= componentSampleCardinality || hasNextAttemptCount >= MAX_LEAF_FINDING_ATTEMPTS) {
            return false; // No more samples to take from this component.
        }

        if (!continueCurrentLeaf) {
            // pin the next random leaf page
            p.computeIfAbsent(page0Id, k -> new ArrayList<>()).add(-1);
            ICachedPage nextLeaf = findNextRandomLeafPage();
            System.out.println(page0Id + " ===================================");
            //todo: reset the tupleIndex, this all should be gone, once I have a proper random tuple reader
            // which can jump to any tuple in the page or just a bitmap of non-antimatter tuples in the page.
            tupleIndex = 0;
            // In 0th version, every hasNext() call will pin a random leaf page.
        }

        continueCurrentLeaf = false;
        int tupleIndex;
        try {
            tupleIndex = findRandomTuple();
        } catch (Exception e) {
            System.out.println("Exception in finding random tuple: " + p);
            throw e;
        }
        // Need some liveness check here, let's do it one check at a time for now, but in practical can be done in a batch
        // Like pull out a batch of the random keys from the page and do the liveness check on them.
        long pageTupleKey = getPageTupleKey(page0Id, tupleIndex);
        if (tupleIndex == -1 || seenTupleIndexes.contains(pageTupleKey)) {
            hasNextAttemptCount++;
            return doHasNext(); // repeat the process to find a valid tuple
        }
        // todo: We have a valid tuple in frameTuple, now check if it's live using the searchCursor
        searchKeys.clear();
        foundIndexes.clear();
        searchKeys.add(frameTuple);
        // todo: find a bunch of search keys from the page, and do a batch search
        batchPredicate.reset(searchKeys);
        // maynot need to set the predicate every time
        searchCursor.setPredicate(batchPredicate);

        // get the index of the key found, and remove it from the search keys
        searchCursor.doHasNextWithPredicate(foundIndexes);
        //        while (searchCursor.doHasNextWithPredicate()) {
        //            int foundIndex = batchPredicate.getKeyIndex();
        //            foundIndexes.set(foundIndex);
        //        }
        // todo: remove the found keys from the search keys
        // for now, since there is just one key, we can just check the foundIndexes size
        if (foundIndexes.isEmpty()) {
            hasNextAttemptCount = 0;

            // If we have a next tuple, increment the sampled count.
            seenTupleIndexes.add(pageTupleKey);
            sampledCount++;
            return true;
        }

        // search again, didn't find any live tuple
        hasNextAttemptCount++;
        return doHasNext();
    }

    private long getPageTupleKey(int pageId, int tupleIndex) {
        return (((long) pageId) << 32) | (tupleIndex & 0xffffffffL);
    }

    private ICachedPage findNextRandomLeafPage() throws HyracksDataException {
        int numberOfAttempts = 0;
        //todo: smells like a lock leak here
        // In case the pin fails, the previously pinned pages are not released.
        // releasePages first and then try to pin a new random page.
        // may need to update a few functions
        // releasePages(); -> unPin(currentLeafPage); -> walkDown() -> randomPageAlreadyPinned
        // context.pinNext() should not be needed.
        while (numberOfAttempts < MAX_LEAF_FINDING_ATTEMPTS) {
            ICachedPage rootPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPageId), context);
            // Here randomLeafPage is already pinned, so unpin it and pin with the columnCloudContext.
            ICachedPage randomLeafPage = bTree.getRandomLeafPage(rootPage, opCtx, context);
            // todo: what the hell is these two lines? I feel it used to make sense at some point of time
            // but now it looks fishy.
            // Ah, now I get it, since pinNext do releasePages and other stuff
            long leafPageDiskPageId = randomLeafPage.getDiskPageId();
            bufferCache.unpin(randomLeafPage, context);

            // Pin with the cloud context
            context.pinNext(leafFrame, leafPageDiskPageId, bufferCache);
            page0 = leafFrame.getPage();
            page0Id = BufferedFileHandle.getPageId(leafPageDiskPageId);
            // Tuple count can be checked here, as the pageZero is already pinned.
            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                numberOfAttempts++;
                continue;
            }
            context.preparePageZeroSegments(leafFrame, bufferCache, fileId);
            frameTuple.newPage();
            context.prepareColumns(leafFrame, bufferCache, fileId);
            // todo: this may be a place for perf improvement?
            // idk, but a leaf switch causes reset, and the columns are set again
            // very expensive operation (don't know how to cost it) I believe.
            // maybe batching amortize it.
            frameTuple.reset(0, leafFrame.getTupleCount() - 1);
            return page0;
        }
        // Replace with the proper exception
        throw new HyracksDataException("Attempt cycle exhausted while trying to find a random leaf page with tuples.");
    }

    private int findRandomTuple() throws HyracksDataException {
        // todo:Ig we need a resettable tuple reader, as the reading is moving forward
        // but since the random tuple can be any tuple in the page, which maybe lowerIndex than the previous index.
        int numberOfTuples = leafFrame.getTupleCount();
        // Since the definition levels are packed with BitPackedRLEHybridEncoder, we cannot know the definition level
        // for a random tuple index. Hence, not able to say if a tuple is an anti-matter or not based

        // We can but a bitmap in the leaf, 10001... indicating the anti-matter tuples.
        // for now, just take a random index in increasing range.
        int bucketSize = 10;
        while (tupleIndex < numberOfTuples) {
            // Pick random offset from [0, jumpSize)
            int jump = ThreadLocalRandom.current().nextInt(bucketSize) + 1; // bucket step size
            //            int jump = 1; // debug
            tupleIndex += jump;
            if (tupleIndex >= numberOfTuples)
                break;

            System.out.println(Thread.currentThread().getName() + " tupleIndex: " + tupleIndex);
            p.computeIfAbsent(page0Id, k -> new ArrayList<>()).add(tupleIndex);
            frameTuple.setAt(tupleIndex);
            if (!frameTuple.isAntimatter()) {
                break;
            }
        }

        if (tupleIndex >= numberOfTuples) {
            // It's fine, we didn't find a valid tuple in the current page.
            // should try next page, but throwing exception for now.

            // return false here
            // throw new HyracksDataException("No valid tuple found in the current page after sampling.");
            return -1;
        }
        return tupleIndex;
    }

    public void printTHEMap() {
        System.out.println(p);
    }

    @Override
    protected void doNext() throws HyracksDataException {
        //NoOp
    }

    @Override
    protected void doDestroy() throws HyracksDataException {
        // No Op all resources are released in the close call
    }

    @Override
    protected void doClose() throws HyracksDataException {
        System.out.println(p);
        releasePages();
        frameTuple.close();
        context.close(bufferCache);
        seenTupleIndexes.clear();
        page0 = null;
    }

    @Override
    protected ITupleReference doGetTuple() {
        return frameTuple;
    }

    @Override
    public ICachedPage pin(int pageId) throws HyracksDataException {
        return bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        bufferCache.unpin(page);
    }

    @Override
    public int getPageSize() {
        return bufferCache.getPageSize();
    }
}
