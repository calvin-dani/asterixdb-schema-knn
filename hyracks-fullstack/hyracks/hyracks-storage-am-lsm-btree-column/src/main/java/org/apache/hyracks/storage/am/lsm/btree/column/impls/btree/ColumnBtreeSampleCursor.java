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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

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
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.AbstractColumnTupleReference;
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

/**
 * Two-phase column sample cursor.
 * <p>
 * <b>Phase 1 — Selection (page0-only):</b> picks random leaf pages via batched
 * sorted draws to minimize random I/O, pins only page0, uses
 * {@link AbstractColumnTupleReference#resetPKOnly} to position PKs without
 * loading column mega-pages. Checks antimatter and newer-component existence
 * from PK data alone. Accepted tuples are recorded as packed
 * {@code (pageId << 32) | tupleIndex} longs.
 * <p>
 * <b>Phase 2 — Collection (sorted, full column load):</b> the collected
 * samples are sorted by pageId (then tupleIndex within a page). For each
 * distinct page we do one full column load via {@code reset()}, then
 * advance to additional tuples on the same page via the cheap forward-only
 * {@code setAt()}. This amortises mega-page I/O and gives cache-friendly
 * sequential access.
 */
public class ColumnBtreeSampleCursor extends EnforcedIndexCursor implements ITreeIndexCursor, IColumnReadMultiPageOp {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Comparator<LeafDraw> LEAF_DRAW_COMPARATOR = Comparator.comparingInt(d -> d.pageId);

    private final ColumnBTree bTree;
    private final BTreeOpContext opCtx;
    private final ColumnBTreeReadLeafFrame leafFrame;
    private final IColumnReadContext context;
    private final IColumnTupleIterator frameTuple;
    private final AbstractColumnTupleReference columnTupleRef;

    // u64: (pageId << 32) | tupleIndex
    private final LongSet seenTupleIndexes;

    private final int maxLeafFindingAttempts;
    private final long componentSampleCardinality;
    private final Random randomNumGen;

    // Phase 1 Batched I/O draws
    private final List<LeafDraw> pendingLeafDraws;
    private int pendingLeafDrawIndex;
    private final int leafDrawBatchSize;

    // search predicate
    private final ILSMIndexBatchPointCursor searchCursor;
    private final BatchPredicateWithKeys batchPredicate;
    private final List<ITupleReference> searchKeys;
    private final BitSet foundIndexes;

    private int sampledCount;
    private int hasNextAttemptCount = 0;
    private int totalAccessCount;

    // Static upper bound for rejection sampling
    private final int leafTupleCapacity;

    // Debug and traceability
    private long totalTimeTakenToFindRandomLeaf = 0;
    private long totalTimeTakenToFindRandomTuples = 0;
    private boolean endedPreemptively = false;

    private ICachedPage page0 = null;
    private int rootPageId;
    private int[] leafPageIds = null;

    // Phase 1 collection: packed (pageId << 32) | tupleIndex
    private long[] collectedSamples = null;
    private int collectedCount = 0;

    // Phase 2 yield state
    private boolean selectionDone = false;
    private int yieldPos = 0;
    private int prevYieldPageId = -1;

    private IBufferCache bufferCache;
    private int fileId = -1;

    public ColumnBtreeSampleCursor(ColumnBTree columnBTree, ColumnBTreeReadLeafFrame leafFrame,
            BTreeOpContext opContext, IColumnReadContext context, long componentSampleCardinality, long sampleSeed,
            int index, ILSMIndexBatchPointCursor searchCursor, int maxLeafFindingAttempts, int leafDrawBatchSize,
            int maxLeafTupleCount) {
        this.bTree = columnBTree;
        this.opCtx = opContext;
        this.leafFrame = leafFrame;
        this.context = context;
        this.componentSampleCardinality = componentSampleCardinality;
        this.randomNumGen = new Random(sampleSeed);
        this.batchPredicate = new BatchPredicateWithKeys();
        this.searchCursor = searchCursor;
        this.frameTuple = leafFrame.createTupleReference(index, this);
        this.columnTupleRef = (AbstractColumnTupleReference) frameTuple;
        this.searchKeys = new ArrayList<>();
        this.foundIndexes = new BitSet();
        this.seenTupleIndexes = new LongOpenHashSet();
        this.totalAccessCount = 0;
        this.maxLeafFindingAttempts = maxLeafFindingAttempts;
        this.leafTupleCapacity = maxLeafTupleCount;

        this.leafDrawBatchSize = (int) Math.max(leafDrawBatchSize, componentSampleCardinality);
        this.pendingLeafDraws = new ArrayList<>(this.leafDrawBatchSize);
        for (int i = 0; i < this.leafDrawBatchSize; i++) {
            this.pendingLeafDraws.add(new LeafDraw(-1, 0.0, 0));
        }

        // FIX: Force a batch refill on the very first nextLeafDraw() call
        this.pendingLeafDrawIndex = this.leafDrawBatchSize;
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
        if (page0 != null) {
            releasePages();
        }

        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();
        leafPageIds = bTree.enumerateLeafPageIds(rootPageId, opCtx, context);

        collectedSamples = new long[(int) componentSampleCardinality];
        collectedCount = 0;
        selectionDone = false;
        yieldPos = 0;
        prevYieldPageId = -1;
    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        if (!selectionDone) {
            runPhase1Selection();
            selectionDone = true;
        }
        return yieldNextFromPhase2();
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Phase 1: PK-only selection (Batched & Sorted for sequential I/O)
    // ──────────────────────────────────────────────────────────────────────

    private void runPhase1Selection() throws HyracksDataException {
        while (collectedCount < componentSampleCardinality && hasNextAttemptCount < maxLeafFindingAttempts) {
            LeafDraw leafDraw = nextLeafDraw();
            if (leafDraw == null) {
                break;
            }

            totalAccessCount++;

            int selectedPageId = pinAndAcceptLeafPage0(leafDraw);
            if (selectedPageId == -1) {
                hasNextAttemptCount++;
                continue;
            }

            int randomTupleIndex = findRandomTuplePK(leafDraw.tupleStartSeed);
            if (randomTupleIndex == -1) {
                hasNextAttemptCount++;
                continue;
            }

            long pageTupleKey = getPageTupleKey(selectedPageId, randomTupleIndex);
            if (seenTupleIndexes.contains(pageTupleKey)) {
                hasNextAttemptCount++;
                continue;
            }

            // Newer-component check (uses PK fields from page0)
            searchKeys.clear();
            foundIndexes.clear();
            searchKeys.add(frameTuple);
            batchPredicate.reset(searchKeys);
            searchCursor.setPredicate(batchPredicate);
            searchCursor.doHasNextWithPredicate(foundIndexes);

            if (!foundIndexes.isEmpty()) {
                hasNextAttemptCount++;
                continue;
            }

            // Valid sample recorded
            hasNextAttemptCount = 0;
            seenTupleIndexes.add(pageTupleKey);
            collectedSamples[collectedCount++] = pageTupleKey;
        }

        endedPreemptively = (collectedCount < componentSampleCardinality);
        unpinCurrentPage0();

        // Sort by pageId (high 32 bits) then tupleIndex (low 32 bits) for Phase 2
        Arrays.sort(collectedSamples, 0, collectedCount);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("ColumnBtreeSampleCursor Phase 1: collected {} samples from {} leaf pages", collectedCount,
                    leafPageIds.length);
        }
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
            pendingLeafDraws.get(i).update(targetPageId, randomNumGen.nextDouble(), randomNumGen.nextInt());
        }
        // Sorting Phase 1 draws dramatically reduces random disk seeks for page0
        pendingLeafDraws.sort(LEAF_DRAW_COMPARATOR);
    }

    private int pinAndAcceptLeafPage0(LeafDraw leafDraw) throws HyracksDataException {
        long nanos = System.nanoTime();
        try {
            // Unpin previous page0 if we are moving to a new page
            if (page0 != null && prevYieldPageId != leafDraw.pageId) {
                unpinCurrentPage0();
            }

            if (page0 == null) {
                ICachedPage randomLeafPage =
                        bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafDraw.pageId), context);
                leafFrame.setPage(randomLeafPage);
                page0 = leafFrame.getPage();
                prevYieldPageId = leafDraw.pageId;
            }

            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                unpinCurrentPage0();
                return -1;
            }

            // Rejection sampling against static capacity
            double acceptProb = (double) tupleCount / leafTupleCapacity;
            if (leafDraw.acceptanceSample >= acceptProb) {
                unpinCurrentPage0();
                return -1;
            }

            return leafDraw.pageId;
        } finally {
            totalTimeTakenToFindRandomLeaf += (System.nanoTime() - nanos);
        }
    }

    private int findRandomTuplePK(int tupleStartSeed) throws HyracksDataException {
        long nanos = System.nanoTime();
        try {
            int tupleCount = leafFrame.getTupleCount();
            int targetTupleIndex = (tupleStartSeed & 0x7FFFFFFF) % tupleCount;

            frameTuple.newPage();
            columnTupleRef.resetPKOnly(targetTupleIndex);

            return frameTuple.isAntimatter() ? -1 : targetTupleIndex;
        } finally {
            totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Phase 2: Sorted column collection — one mega-page load per page
    // ──────────────────────────────────────────────────────────────────────

    private boolean yieldNextFromPhase2() throws HyracksDataException {
        if (yieldPos >= collectedCount) {
            return false;
        }

        long nanos = System.nanoTime();
        try {
            long packed = collectedSamples[yieldPos];
            int pageId = (int) (packed >>> 32);
            int tupleIdx = (int) packed;

            if (pageId != prevYieldPageId) {
                // Release previous column pages & page0
                context.release(bufferCache);
                unpinCurrentPage0();

                // Pin the new page0
                ICachedPage newPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), context);
                leafFrame.setPage(newPage);
                page0 = leafFrame.getPage();

                // Full column load
                context.preparePageZeroSegments(leafFrame, bufferCache, fileId);
                frameTuple.newPage();
                context.prepareColumns(leafFrame, bufferCache, fileId);
                frameTuple.reset(tupleIdx, leafFrame.getTupleCount() - 1);
                prevYieldPageId = pageId;
            } else {
                frameTuple.setAt(tupleIdx);
            }

            sampledCount++;
            yieldPos++;
            return true;
        } finally {
            // Track Phase 2 materialization time here
            totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
        }
    }

    private long getPageTupleKey(int pageId, int tupleIndex) {
        return (((long) pageId) << 32) | (tupleIndex & 0xffffffffL);
    }

    private void unpinCurrentPage0() {
        if (page0 != null) {
            bufferCache.unpin(page0, context);
            page0 = null;
            prevYieldPageId = -1;
        }
    }

    private void releasePages() throws HyracksDataException {
        context.release(bufferCache);
        frameTuple.unpinColumnsPages();
        unpinCurrentPage0();
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
        leafPageIds = null;
        collectedSamples = null;
        collectedCount = 0;
        selectionDone = false;
        yieldPos = 0;
        prevYieldPageId = -1;

        // FIX: Force a batch refill on the next reuse
        pendingLeafDrawIndex = leafDrawBatchSize;

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
        return bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), context);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        bufferCache.unpin(page);
    }

    @Override
    public int getPageSize() {
        return bufferCache.getPageSize();
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