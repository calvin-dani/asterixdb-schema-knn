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
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.exceptions.ErrorCode;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public class ColumnBtreeSampleCursor extends EnforcedIndexCursor implements ITreeIndexCursor, IColumnReadMultiPageOp {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ColumnBTree bTree;
    private final BTreeOpContext opCtx;
    private final ColumnBTreeReadLeafFrame leafFrame;
    private final IColumnReadContext context;
    private final IColumnTupleIterator frameTuple;

    // u64: (pageId << 32) | tupleIndex
    private final LongSet seenTupleIndexes;

    private final int maxLeafFindingAttempts;
    private final long componentSampleCardinality;
    private final Random randomNumGen;

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

    private int rootPageId;
    private ICachedPage page0 = null;
    private int page0Id = -1;

    private IBufferCache bufferCache;
    private int fileId = -1;

    public ColumnBtreeSampleCursor(ColumnBTree columnBTree, ColumnBTreeReadLeafFrame leafFrame,
            BTreeOpContext opContext, IColumnReadContext context, long componentSampleCardinality, long sampleSeed,
            int index, ILSMIndexBatchPointCursor searchCursor, int maxLeafFindingAttempts) {
        this.bTree = columnBTree;
        this.opCtx = opContext;
        this.leafFrame = leafFrame;
        this.context = context;
        this.componentSampleCardinality = componentSampleCardinality;
        this.randomNumGen = new Random(sampleSeed);
        this.batchPredicate = new BatchPredicateWithKeys();
        this.searchCursor = searchCursor;
        this.frameTuple = leafFrame.createTupleReference(index, this);
        this.searchKeys = new ArrayList<>();
        this.foundIndexes = new BitSet();
        this.seenTupleIndexes = new LongOpenHashSet();
        this.totalAccessCount = 0;
        this.maxLeafFindingAttempts = maxLeafFindingAttempts;
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

        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();
    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        while (sampledCount < componentSampleCardinality && hasNextAttemptCount < maxLeafFindingAttempts) {
            totalAccessCount++;

            // findNextRandomLeafPage now picks the random tuple index and resets directly to it
            int randomTupleIndex = findNextRandomLeafPage();

            // Check if the tuple at the random index is antimatter
            int tupleIndex = checkTupleAtIndex(randomTupleIndex);
            long pageTupleKey = getPageTupleKey(page0Id, tupleIndex);

            // Skip if no valid tuple found or already seen
            if (tupleIndex == -1 || seenTupleIndexes.contains(pageTupleKey)) {
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
                seenTupleIndexes.add(pageTupleKey);
                sampledCount++;
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
     *
     * @return the random tuple index within the leaf page, or -1 if the page is empty
     */
    private int findNextRandomLeafPage() throws HyracksDataException {
        long nanos = System.nanoTime();
        int numberOfAttempts = 0;
        while (numberOfAttempts < maxLeafFindingAttempts) {
            // Release previously pinned pages
            context.release(bufferCache);
            // Pass the random generator to ensure reproducible sampling
            ICachedPage randomLeafPage = bTree.getRandomLeafPage(rootPageId, opCtx, context, randomNumGen);
            // randomLeafPage is already pinned, so unpin it and re-pin with the column context
            // since pinNext does releasePages and other column-specific setup
            long leafPageDiskPageId = randomLeafPage.getDiskPageId();
            // Unpin the current leaf page
            if (leafFrame.getPage() != null) {
                // if null, then it was the first page of the cursor
                bufferCache.unpin(leafFrame.getPage(), context);
            }
            leafFrame.setPage(randomLeafPage);

            page0 = leafFrame.getPage();
            page0Id = BufferedFileHandle.getPageId(leafPageDiskPageId);

            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                numberOfAttempts++;
                continue;
            }

            // Pick random tuple index BEFORE reset, so we reset directly to it.
            // This avoids the off-by-one issue with setAt() after reset(0, ...).
            // Column storage streams forward-only, and reset() positions both PKs and columns
            // correctly at the specified startIndex.
            int randomTupleIndex = (int) (randomNumGen.nextDouble() * tupleCount);

            context.preparePageZeroSegments(leafFrame, bufferCache, fileId);
            frameTuple.newPage();
            context.prepareColumns(leafFrame, bufferCache, fileId);
            frameTuple.reset(randomTupleIndex, leafFrame.getTupleCount() - 1);
            totalTimeTakenToFindRandomLeaf += (System.nanoTime() - nanos);
            return randomTupleIndex;
        }
        throw HyracksDataException.create(ErrorCode.RANDOM_SAMPLE_LEAF_NOT_FOUND, maxLeafFindingAttempts);
    }

    /**
     * Checks if the current tuple (already positioned by findNextRandomLeafPage) is antimatter.
     * <p>
     * NOTE: Unlike DiskBTreeSampleCursor, we don't pick a new random index here.
     * The random index was already picked in findNextRandomLeafPage and the iterator
     * was reset directly to that position. Column storage uses forward-only streaming,
     * so we can't seek backward to try a different tuple index. If the picked tuple
     * is antimatter, we return -1 and let the caller try a different leaf page instead.
     *
     * @param tupleIndex the tuple index that was already positioned by findNextRandomLeafPage
     * @return the same tupleIndex if the tuple is not antimatter, -1 otherwise
     */
    private int checkTupleAtIndex(int tupleIndex) throws HyracksDataException {
        long nanos = System.nanoTime();
        if (frameTuple.isAntimatter()) {
            totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
            return -1;
        }
        totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
        return tupleIndex;
    }

    private void releasePages() throws HyracksDataException {
        //Unpin all column pages first
        context.release(bufferCache);
        frameTuple.unpinColumnsPages();
        if (page0 != null) {
            bufferCache.unpin(page0, context);
        }
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
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "{} stats - sampledCount: {}, totalAccessCount: {}, "
                            + "totalTimeTakenToFindRandomLeaf: {} ns, totalTimeTakenToFindRandomTuples: {} ns, "
                            + "endedPreemptively: {}",
                    this.getClass().getName(), sampledCount, totalAccessCount, totalTimeTakenToFindRandomLeaf,
                    totalTimeTakenToFindRandomTuples, endedPreemptively);
        }
        releasePages();
        frameTuple.close();
        context.close(bufferCache);
        seenTupleIndexes.clear();
        page0 = null;
        // Reset counters for cursor reuse
        sampledCount = 0;
        hasNextAttemptCount = 0;
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
