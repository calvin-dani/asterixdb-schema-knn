/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Comparator;
import java.util.List;
import java.util.Random;

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

/**
 * Disk B-tree sample cursor for row storage.
 * <p>
 * Performs single-phase random sampling: picks random leaf pages and tuples,
 * checks antimatter and newer-component existence, and immediately yields
 * accepted samples. For row storage with typical low sampling density
 * (scattered samples across many pages), this approach minimizes page pins.
 */
public final class DiskBTreeSampleCursor extends EnforcedIndexCursor implements ITreeIndexCursor {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int DEFAULT_LEAF_DRAW_BATCH_SIZE = 128;

    // Cached comparator to prevent lambda allocation during sort operations
    private static final Comparator<LeafDraw> LEAF_DRAW_COMPARATOR = Comparator.comparingInt(d -> d.pageId);

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
    // Pre-enumerated leaf page IDs for perfectly uniform random access.
    // Enumeration touches only interior pages (level >= 1), skipping all
    // leaf-level I/O.
    private int[] leafPageIds = null;
    // Batched random draws. Pre-allocated to avoid GC pressure.
    private final List<LeafDraw> pendingLeafDraws;
    private int pendingLeafDrawIndex;
    private final int leafDrawBatchSize;
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

    // Rejection sampling static upper bound (Olken & Rotem)
    // to correct for bias from partially-filled pages
    private final int leafTupleCapacity;

    // Debug and traceability
    private long totalTimeTakenToFindRandomLeaf = 0;
    private long totalTimeTakenToFindRandomTuples = 0;
    private long totalLeafDrawBatches = 0;
    private long totalLeafDraws = 0;
    private long totalLeafDrawUniquePages = 0;
    private long totalLeafPins = 0;
    private long totalReusedPinnedPageHits = 0;
    private boolean endedPreemptively = false;

    public DiskBTreeSampleCursor(DiskBTree diskBTree, IBTreeLeafFrame leafFrame, long componentSampleCardinality,
            long sampleSeed, BTreeOpContext ctx, IBufferCacheReadContext bufferCacheOpCtx,
            ILSMIndexBatchPointCursor searchCursor, int maxLeafFindingAttempts, int maxLeafTupleCount) {
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
        this.leafTupleCapacity = maxLeafTupleCount;
        this.leafDrawBatchSize = Math.min(DEFAULT_LEAF_DRAW_BATCH_SIZE, maxLeafFindingAttempts);

        // Pre-allocate LeafDraw objects to prevent churn during batch refills
        this.pendingLeafDraws = new ArrayList<>(this.leafDrawBatchSize);
        for (int i = 0; i < this.leafDrawBatchSize; i++) {
            this.pendingLeafDraws.add(new LeafDraw(-1, 0.0, 0));
        }

        this.pendingLeafDrawIndex = this.leafDrawBatchSize;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        if (page != null) {
            releasePage();
        }
        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();
        // Enumerate all leaf page IDs by traversing only interior pages.
        // The optimised DFS skips leaf-level I/O (level-1 nodes record their
        // children's page IDs directly), so the cost is proportional to the
        // number of interior pages (~0.4 % of total pages for typical fan-out).
        // The resulting array gives perfectly uniform random access to any leaf.
        leafPageIds = bTree.enumerateLeafPageIds(rootPageId, bTreeOpCtx, bufferCacheOpCtx);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("DiskBTreeSampleCursor: {} leaf pages enumerated, target {} samples", leafPageIds.length,
                    componentSampleCardinality);
        }
    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        while (sampledCount < componentSampleCardinality && hasNextAttemptCount < maxLeafFindingAttempts) {
            LeafDraw leafDraw = nextLeafDraw();
            if (leafDraw == null) {
                break;
            }
            if (!pinAndAcceptLeafPage(leafDraw)) {
                hasNextAttemptCount++;
                continue;
            }
            int foundTupleIndex = findRandomTuple(leafDraw.tupleStartSeed);
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
                // Keep page pinned and return immediately
                hasNextAttemptCount = 0;
                seenTupleIndexes.add(pageTupleKey);
                sampledCount++;
                return true;
            } else {
                // Tuple exists in newer component, try again
                hasNextAttemptCount++;
            }
        }
        endedPreemptively = (sampledCount < componentSampleCardinality);
        return false;
    }

    private LeafDraw nextLeafDraw() {
        if (pendingLeafDrawIndex >= leafDrawBatchSize) {
            refillLeafDrawBatch();
            if (leafPageIds == null || leafPageIds.length == 0) {
                return null;
            }
        }
        return pendingLeafDraws.get(pendingLeafDrawIndex++);
    }

    private void refillLeafDrawBatch() {
        pendingLeafDrawIndex = 0;
        if (leafPageIds == null || leafPageIds.length == 0 || leafDrawBatchSize <= 0) {
            return;
        }
        for (int i = 0; i < leafDrawBatchSize; i++) {
            int randomLeafIndex = randomNumGen.nextInt(leafPageIds.length);
            int targetPageId = leafPageIds[randomLeafIndex];
            double acceptanceSample = randomNumGen.nextDouble();
            int tupleStartSeed = randomNumGen.nextInt();

            pendingLeafDraws.get(i).update(targetPageId, acceptanceSample, tupleStartSeed);
        }

        pendingLeafDraws.sort(LEAF_DRAW_COMPARATOR);

        int uniquePages = 0;
        int prevPageId = Integer.MIN_VALUE;
        for (int i = 0; i < leafDrawBatchSize; i++) {
            int currentPageId = pendingLeafDraws.get(i).pageId;
            if (currentPageId != prevPageId) {
                uniquePages++;
                prevPageId = currentPageId;
            }
        }
        totalLeafDrawBatches++;
        totalLeafDraws += leafDrawBatchSize;
        totalLeafDrawUniquePages += uniquePages;
    }

    private long getPageTupleKey(int pageId, int tupleIndex) {
        return (((long) pageId) << 32) | (tupleIndex & 0xffffffffL);
    }

    /**
     * Selects a random leaf page from the pre-enumerated leaf page ID array.
     * <p>
     * Each leaf has exactly {@code 1 / N_leaves} probability of being chosen.
     * The enumeration cost is amortized: it touches only interior pages once
     * during {@code doOpen()}, then every sample here is a single direct pin.
     * <p>
     * <b>Page-fill correction (Olken &amp; Rotem, VLDB 1989):</b> Each selected leaf
     * is accepted with probability {@code tupleCount / leafTupleCapacity} to
     * correct for the bias where tuples on partially-filled pages have higher
     * per-tuple selection probability.
     */
    private boolean pinAndAcceptLeafPage(LeafDraw leafDraw) throws HyracksDataException {
        long nanos = System.nanoTime();
        try {
            totalAccessCount++;
            if (pageId != leafDraw.pageId || page == null) {
                releasePage();
                int targetPageId = leafDraw.pageId;
                ICachedPage randomLeafPage =
                        bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, targetPageId), bufferCacheOpCtx);
                page = randomLeafPage;
                leafFrame.setPage(page);
                pageId = targetPageId;
                totalLeafPins++;
            } else {
                totalReusedPinnedPageHits++;
            }
            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                return false;
            }
            // Rejection sampling: accept this page with probability proportional to
            // its tuple count against a static capacity bound.
            double acceptProb = (double) tupleCount / leafTupleCapacity;
            if (leafDraw.acceptanceSample >= acceptProb) {
                return false;
            }
            return true;
        } finally {
            // Ensures elapsed time is correctly tracked even if the page is rejected
            totalTimeTakenToFindRandomLeaf += (System.nanoTime() - nanos);
        }
    }

    /**
     * Picks a random tuple from the current leaf page.
     * <p>
     * <b>Antimatter Rejection:</b> If the selected tuple is antimatter, it immediately
     * returns -1 (rejecting the draw completely). This is O(1) and preserves the unbiased
     * uniform distribution of live tuples, accurately reflecting the physical density of the data.
     * </p>
     */
    private int findRandomTuple(int tupleStartSeed) {
        long nanos = System.nanoTime();
        int numberOfTuples = leafFrame.getTupleCount();

        // Strips sign bit for fast, uniform modulo arithmetic
        int targetTupleIndex = (tupleStartSeed & 0x7FFFFFFF) % numberOfTuples;

        frameTuple.resetByTupleIndex(leafFrame, targetTupleIndex);
        int result = frameTuple.isAntimatter() ? -1 : targetTupleIndex;

        totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
        return result;
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
            double avgBatchDraws = totalLeafDrawBatches == 0 ? 0.0 : (double) totalLeafDraws / totalLeafDrawBatches;
            double avgBatchUniquePages =
                    totalLeafDrawBatches == 0 ? 0.0 : (double) totalLeafDrawUniquePages / totalLeafDrawBatches;
            double uniquePerDraw = totalLeafDraws == 0 ? 0.0 : (double) totalLeafDrawUniquePages / totalLeafDraws;
            double repinAvoidRate = (totalLeafPins + totalReusedPinnedPageHits) == 0 ? 0.0
                    : (double) totalReusedPinnedPageHits / (totalLeafPins + totalReusedPinnedPageHits);
            LOGGER.debug(
                    "{} stats - sampledCount: {}, totalAccessCount: {}, "
                            + "totalTimeTakenToFindRandomLeaf: {} ns, totalTimeTakenToFindRandomTuples: {} ns, "
                            + "leafDrawBatches: {}, totalLeafDraws: {}, totalLeafDrawUniquePages: {}, "
                            + "avgBatchDraws: {}, avgBatchUniquePages: {}, uniquePerDraw: {}, totalLeafPins: {}, "
                            + "reusedPinnedPageHits: {}, repinAvoidRate: {}, endedPreemptively: {}",
                    this.getClass().getName(), sampledCount, totalAccessCount, totalTimeTakenToFindRandomLeaf,
                    totalTimeTakenToFindRandomTuples, totalLeafDrawBatches, totalLeafDraws, totalLeafDrawUniquePages,
                    avgBatchDraws, avgBatchUniquePages, uniquePerDraw, totalLeafPins, totalReusedPinnedPageHits,
                    repinAvoidRate, endedPreemptively);
        }
        if (page != null) {
            releasePage();
        }
        sampledCount = 0;
        page = null;
        seenTupleIndexes.clear();
        pageId = -1;
        leafPageIds = null;

        pendingLeafDrawIndex = leafDrawBatchSize;

        totalAccessCount = 0;
        totalTimeTakenToFindRandomLeaf = 0;
        totalTimeTakenToFindRandomTuples = 0;
        totalLeafDrawBatches = 0;
        totalLeafDraws = 0;
        totalLeafDrawUniquePages = 0;
        totalLeafPins = 0;
        totalReusedPinnedPageHits = 0;
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
        pageId = -1;
    }

    private static final class LeafDraw {
        private int pageId;
        private double acceptanceSample;
        private int tupleStartSeed;

        private LeafDraw(int pageId, double acceptanceSample, int tupleStartSeed) {
            this.pageId = pageId;
            this.acceptanceSample = acceptanceSample;
            this.tupleStartSeed = tupleStartSeed;
        }

        private void update(int pageId, double acceptanceSample, int tupleStartSeed) {
            this.pageId = pageId;
            this.acceptanceSample = acceptanceSample;
            this.tupleStartSeed = tupleStartSeed;
        }
    }
}