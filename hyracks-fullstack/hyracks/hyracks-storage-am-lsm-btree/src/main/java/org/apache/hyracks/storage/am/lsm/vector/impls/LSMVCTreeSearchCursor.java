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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.tuples.ReferenceFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree.VectorClusteringTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;
import org.apache.logging.log4j.Level;

/**
 * LSM search cursor for Vector Clustering Tree.
 *
 * This cursor coordinates searches across multiple LSM components (memory and disk)
 * by delegating to VectorClusteringSearchCursor for each component and merging results.
 *
 * Supports two modes:
 *
 * 1. Query Mode (fullScanMode=false):
 *    - Used for ANN (Approximate Nearest Neighbor) queries
 *    - Level-wise + DFS cluster selection (nprobe logic)
 *    - All components advance to the SAME cluster (synchronized exploration)
 *    - Early termination based on nprobe and K parameters
 *    - INCLUDE field filtering support
 *
 * 2. Full Scan Mode (fullScanMode=true):
 *    - Used for merge operations where we need ALL data
 *    - Sequential cluster iteration (0 → 1 → 2 → ...)
 *    - All components advance through clusters TOGETHER in lock-step
 *    - No early termination (must process all data)
 *    - Returns antimatter tuples for proper reconciliation
 *
 * Following the pattern of LSMBTreeRangeSearchCursor:
 * - Extends LSMIndexSearchCursor for priority queue and component switching infrastructure
 * - Creates VectorClusteringTreeAccessor for each component
 * - Handles component state changes (memory → disk transitions)
 * - Uses priority queue to merge results sorted by <distance, primary_key> (query) or <primary_key> (merge)
 * - Filters antimatter tuples and handles matter/antimatter cancellation
 */
public class LSMVCTreeSearchCursor extends LSMIndexSearchCursor {

    // Accessor array for each component's VCTree
    private VectorClusteringTreeAccessor[] vcTreeAccessors;

    // Track component types to detect memory → disk transitions
    protected boolean[] isMemoryComponent;

    // Store search predicate for component switching
    private ISearchPredicate searchPredicate;

    // Track K (target limit) for cluster advancement decisions
    private int K;

    // Track nprobe (minimum clusters to probe before checking K)
    private int nprobe;

    // Cluster selection strategy (encapsulates level-wise + DFS logic)
    private IClusterSelectionStrategy clusterStrategy;

    // Track cluster exhaustion state for synchronized advancement
    private int[] currentClusterIndex; // Which cluster each component is currently on
    private boolean[] clusterExhausted; // Whether each component exhausted its current cluster
    private boolean stopAdvancing; // Flag to stop advancing after K reached

    // Debug counters to track reconciliation
    private int totalTuplesPopped; // Total tuples popped from priority queue (including cancelled)
    private int antimatterTuplesDetected; // Antimatter tuples detected
    private int cancellationsMade; // Matter tuples cancelled by antimatter
    private int getTupleCallCount; // Track how many times doGetTuple() is called
    private int tuplesFilteredOut; // Tuples that failed INCLUDE field filter

    // Tuple filter for INCLUDE field predicates (e.g., year > 2000)
    // When set, only tuples passing this filter are returned and counted toward K
    private ITupleFilter tupleFilter;

    // Wrapper to convert ITupleReference to IFrameTupleReference for filter evaluation
    private ReferenceFrameTupleReference referenceFilterTuple;

    // Full-scan mode flag (for merge operations)
    // true = merge mode (sequential cluster iteration, no early termination)
    // false = query mode (level-wise + DFS, nprobe/K-based early termination)
    private final boolean fullScanMode;

    public LSMVCTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false, false, NoOpIndexCursorStats.INSTANCE);
    }

    public LSMVCTreeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples, boolean fullScanMode,
            IIndexCursorStats stats) {
        super(opCtx, returnDeletedTuples, stats);
        this.fullScanMode = fullScanMode;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // Get LSM-specific initial state
        LSMVCTreeCursorInitialState lsmInitialState = (LSMVCTreeCursorInitialState) initialState;

        // Save search predicate for component switching
        this.searchPredicate = searchPred;

        // Extract K from search predicate for cluster advancement decisions
        this.K = extractK(searchPred);

        // Extract nprobe and epsilon from search predicate
        this.nprobe = extractNprobe(searchPred);
        double epsilon = extractEpsilon(searchPred);

        // Create cluster selection strategy based on mode
        if (fullScanMode) {
            // Merge mode: sequential cluster iteration, no early termination
            this.clusterStrategy = new SequentialClusterSelectionStrategy();
        } else {
            // Query mode: level-wise + DFS, nprobe/K-based early termination
            this.clusterStrategy = new NprobeClusterSelectionStrategy(nprobe, epsilon);
        }

        // Extract tuple filter from search predicate for INCLUDE field predicates
        this.tupleFilter = extractTupleFilter(searchPred);
        if (this.tupleFilter != null) {
            this.referenceFilterTuple = new ReferenceFrameTupleReference();
            System.err.println("[LSMVCTreeSearchCursor] Tuple filter is SET - will filter INCLUDE field predicates");
        } else {
            System.err.println("[LSMVCTreeSearchCursor] Tuple filter is NULL - no INCLUDE field filtering");
        }

        // Initialize debug counters
        this.totalTuplesPopped = 0;
        this.antimatterTuplesDetected = 0;
        this.cancellationsMade = 0;
        this.getTupleCallCount = 0;
        this.tuplesFilteredOut = 0;

        // Set up comparator and operational components
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();

        // For vector index, we don't need mutable component special handling initially
        includeMutableComponent = false;

        int numVCTrees = operationalComponents.size();

        // Initialize cluster tracking arrays for synchronized advancement
        currentClusterIndex = new int[numVCTrees];
        Arrays.fill(currentClusterIndex, 0); // All start at cluster 0
        clusterExhausted = new boolean[numVCTrees];
        Arrays.fill(clusterExhausted, false);
        stopAdvancing = false;

        // Initialize or resize accessor/cursor arrays
        if (rangeCursors == null) {
            // First open: create arrays
            rangeCursors = new IIndexCursor[numVCTrees];
            vcTreeAccessors = new VectorClusteringTreeAccessor[numVCTrees];
            isMemoryComponent = new boolean[numVCTrees];
        } else if (rangeCursors.length != numVCTrees) {
            // Component count changed (due to flush/merge): destroy and recreate
            Throwable failure = CleanupUtils.destroy(null, vcTreeAccessors);
            vcTreeAccessors = null;
            failure = CleanupUtils.destroy(failure, rangeCursors);
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
            rangeCursors = new IIndexCursor[numVCTrees];
            vcTreeAccessors = new VectorClusteringTreeAccessor[numVCTrees];
            isMemoryComponent = new boolean[numVCTrees];
        }

        // Create accessors and cursors for each component
        for (int i = 0; i < numVCTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            LSMComponentType type = component.getType();

            // Track if this is a memory component
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
            }

            // Check if we need to destroy incompatible accessor/cursor
            if (vcTreeAccessors[i] == null || destroyIncompatible(component, i)) {
                vcTreeAccessors[i] = createAccessor(component, i);
                rangeCursors[i] = createCursor(type, vcTreeAccessors[i]);
            } else {
                // Re-use existing cursor
                rangeCursors[i].close();
            }

            isMemoryComponent[i] = type == LSMComponentType.MEMORY;
        }

        // Open all cursors with the search predicate
        IndexCursorUtils.open(vcTreeAccessors, rangeCursors, searchPred);

        // Initialize strategy and set up DFS fallback
        if (numVCTrees > 0) {
            VectorClusteringSearchCursor firstCursor = (VectorClusteringSearchCursor) rangeCursors[0];
            double[] queryVector = firstCursor.getQueryVector();

            // Initialize strategy with first component's tree
            ILSMComponent firstComponent = operationalComponents.get(0);
            VectorClusteringTree vcTree = (VectorClusteringTree) firstComponent.getIndex();
            clusterStrategy.initialize(vcTree, queryVector, firstCursor.getDistanceFunction(), K);

            // Set first cursor for DFS fallback
            clusterStrategy.setFirstCursorForDFS(firstCursor);

            // Pass shared visited set from strategy to all VectorClusteringSearchCursors
            Set<Integer> visitedSet = clusterStrategy.getVisitedCentroidIds();
            for (int i = 0; i < numVCTrees; i++) {
                if (rangeCursors[i] instanceof VectorClusteringSearchCursor) {
                    ((VectorClusteringSearchCursor) rangeCursors[i]).setSharedVisitedSet(visitedSet);
                }
            }

            // IMPORTANT: DFS in each cursor might find a DIFFERENT closest cluster than level-wise!
            // Level-wise does global sort across ALL centroids, while DFS does greedy traversal.
            // We must re-open ALL cursors to the first level-wise cluster to ensure consistency.
            ClusterSearchResult firstCluster = clusterStrategy.getFirstCluster();
            if (firstCluster != null) {
                // Check what cluster DFS actually found
                ClusterSearchResult dfsCluster = firstCursor.getCurrentClusterResult();
                if (dfsCluster != null && dfsCluster.centroidId != firstCluster.centroidId) {
                    // DFS found different cluster than level-wise! Re-open all cursors to level-wise[0]
                    System.err.printf(
                            "[LSMVCTreeSearchCursor] DFS found cid=%d but level-wise[0] is cid=%d - re-opening all cursors to level-wise[0]%n",
                            dfsCluster.centroidId, firstCluster.centroidId);

                    for (int i = 0; i < numVCTrees; i++) {
                        if (rangeCursors[i] instanceof VectorClusteringSearchCursor) {
                            VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) rangeCursors[i];
                            // Reset cluster counter since we're replacing the initial DFS cluster
                            // with level-wise[0], not advancing to a second cluster
                            vcCursor.resetClustersProbed();
                            vcCursor.openClusterByResult(firstCluster);
                        }
                    }
                }

                System.err.printf(
                        "[LSMVCTreeSearchCursor] Computed %d level-wise clusters, first cluster cid=%d marked visited%n",
                        clusterStrategy.getLevelWiseClusterCount(), firstCluster.centroidId);
            } else {
                // No level-wise clusters - mark first cluster from DFS as visited
                ClusterSearchResult dfsFallbackCluster = firstCursor.getCurrentClusterResult();
                if (dfsFallbackCluster != null) {
                    visitedSet.add(dfsFallbackCluster.centroidId);
                    System.err.printf(
                            "[LSMVCTreeSearchCursor] Level-wise disabled, marked first DFS cluster as visited: cid=%d%n",
                            dfsFallbackCluster.centroidId);
                }
            }
        }

        // Initialize priority queue for merging results from all components
        try {
            setPriorityQueueComparator();
            initPriorityQueue();
        } catch (Throwable th) { // NOSONAR Must catch all
            IndexCursorUtils.close(rangeCursors, th);
            throw HyracksDataException.create(th);
        }
    }

    @Override
    public void initPriorityQueue() throws HyracksDataException {
        // Initialize priority queue and populate with first element from each cursor
        int pqInitSize = (rangeCursors.length > 0) ? rangeCursors.length : 1;
        if (outputPriorityQueue == null) {
            outputPriorityQueue = new PriorityQueue<>(pqInitSize, pqCmp);
            pqes = new PriorityQueueElement[pqInitSize];
            for (int i = 0; i < pqInitSize; i++) {
                pqes[i] = new PriorityQueueElement(i);
            }
        } else {
            outputPriorityQueue.clear();
            if (pqInitSize != pqes.length) {
                pqes = new PriorityQueueElement[pqInitSize];
                for (int i = 0; i < pqInitSize; i++) {
                    pqes[i] = new PriorityQueueElement(i);
                }
            }
        }

        // Populate priority queue with first element from each cursor
        // If a cursor has no data (empty cluster), mark it as exhausted
        for (int i = 0; i < rangeCursors.length; i++) {
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                pqes[i].reset(rangeCursors[i].getTuple());
                outputPriorityQueue.offer(pqes[i]);
            } else {
                // Cursor has no data in initial cluster - mark as exhausted
                clusterExhausted[i] = true;
                System.err.println(String.format(
                        "[LSMVCTreeSearchCursor] Component %d has empty initial cluster (marked exhausted)", i));
            }
        }

        // Check if ALL components started with empty clusters
        // If so, advance all to next cluster
        boolean allInitiallyExhausted = true;
        for (int i = 0; i < clusterExhausted.length; i++) {
            if (!clusterExhausted[i]) {
                allInitiallyExhausted = false;
                break;
            }
        }

        if (allInitiallyExhausted) {
            System.err.println("[LSMVCTreeSearchCursor] ALL components have empty initial cluster, advancing to next");
            advanceAllComponentsToNextCluster();
        }
    }

    @Override
    protected void pushIntoQueueFromCursorAndReplaceThisElement(PriorityQueueElement e) throws HyracksDataException {
        // Get next tuple from this cursor and add to priority queue
        int cursorIndex = e.getCursorIndex();
        if (rangeCursors[cursorIndex].hasNext()) {
            rangeCursors[cursorIndex].next();
            e.reset(rangeCursors[cursorIndex].getTuple());
            outputPriorityQueue.offer(e);
        }
    }

    /**
     * Check if accessor/cursor needs to be destroyed due to component type change.
     * This happens when a memory component is replaced with a disk component.
     */
    private boolean destroyIncompatible(ILSMComponent component, int index) throws HyracksDataException {
        // XOR: if component type changed (memory → disk or disk → memory)
        if (component.getType() == LSMComponentType.MEMORY ^ isMemoryComponent[index]) {
            Throwable failure = CleanupUtils.destroy(null, vcTreeAccessors[index]);
            vcTreeAccessors[index] = null;
            failure = CleanupUtils.destroy(failure, rangeCursors[index]);
            rangeCursors[index] = null;
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
            return true;
        }
        return false;
    }

    /**
     * Create accessor for a VCTree component.
     */
    protected VectorClusteringTreeAccessor createAccessor(ILSMComponent component, int index)
            throws HyracksDataException {
        VectorClusteringTree vcTree = (VectorClusteringTree) component.getIndex();
        // Get iap from operation context instead of using cursor's default iap
        LSMVCTreeOpContext vcOpCtx = (LSMVCTreeOpContext) opCtx;
        return (VectorClusteringTreeAccessor) vcTree.createAccessor(vcOpCtx.getIndexAccessParameters());
    }

    /**
     * Create cursor for a VCTree component.
     * Passes fullScanMode to enable sequential cluster iteration for merge operations.
     */
    protected IIndexCursor createCursor(LSMComponentType type, VectorClusteringTreeAccessor accessor)
            throws HyracksDataException {
        return accessor.createSearchCursor(false, fullScanMode);
    }

    @Override
    public void doClose() throws HyracksDataException {
        // Print final reconciliation summary
        int minClustersProbed = getMinClustersProbed();
        System.err.println("\n========== LSM Vector Index Search Summary ==========");
        System.err
                .println(String.format("Mode:                       %s", fullScanMode ? "MERGE (full scan)" : "QUERY"));
        System.err.println(String.format("K=%d, nprobe=%d", K, nprobe));
        System.err.println(String.format("Level-wise clusters:        %d",
                clusterStrategy != null ? clusterStrategy.getLevelWiseClusterCount() : 0));
        System.err.println(String.format("Level-wise phase complete:  %s",
                clusterStrategy != null && clusterStrategy.isLevelWisePhaseComplete()));
        System.err.println(String.format("Min clusters probed:        %d", minClustersProbed));
        System.err.println(String.format("Total tuples processed:     %d", totalTuplesPopped));
        System.err.println(String.format("Antimatter tuples detected: %d", antimatterTuplesDetected));
        System.err.println(String.format("Cancellations made:         %d", cancellationsMade));
        System.err.println(String.format("Tuples filtered out:        %d", tuplesFilteredOut));
        System.err.println(String.format("doGetTuple() was called:    %d times", getTupleCallCount));
        System.err.println("=====================================================\n");

        super.doClose();
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        hasNextCallCount++;
        checkPriorityQueue();
        // Use priority queue - check if there's a valid element at the top
        return !outputPriorityQueue.isEmpty();
    }

    @Override
    public void doNext() throws HyracksDataException {
        // Pop element from priority queue and mark for replacement
        outputElement = outputPriorityQueue.poll();
        needPushElementIntoQueue = true;

        // Track total tuples popped (including those that may be cancelled)
        totalTuplesPopped++;
    }

    @Override
    protected void checkPriorityQueue() throws HyracksDataException {
        // Periodically check if memory components need to be swapped with disk components
        checkAndSwitchMemoryComponentsIfNeeded();

        // Process queue until we find a valid output tuple or queue is exhausted
        while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue) {
            if (outputPriorityQueue.isEmpty()) {
                // Queue empty but pending element exists - refill it
                refillQueueFromPendingElement();
                continue;
            }

            PriorityQueueElement checkElement = outputPriorityQueue.peek();

            if (outputElement == null) {
                // No pending element - process top of queue
                if (processTopElement(checkElement)) {
                    break; // Found valid output tuple
                }
            } else {
                // Have pending element - check for antimatter cancellation
                processWithPendingElement(checkElement);
            }
        }
    }

    /**
     * Check if memory components should be swapped with disk components.
     */
    private void checkAndSwitchMemoryComponentsIfNeeded() throws HyracksDataException {
        if (hasNextCallCount >= SWITCH_COMPONENT_CYCLE) {
            replaceMemoryComponentWithDiskComponentIfNeeded();
            hasNextCallCount = 0;
        }
    }

    /**
     * Process the top element of the priority queue when no pending element exists.
     *
     * @param checkElement the element at top of queue
     * @return true if this is a valid output tuple, false if processing should continue
     */
    private boolean processTopElement(PriorityQueueElement checkElement) throws HyracksDataException {
        // Check if top element is antimatter
        if (isDeleted(checkElement) && !returnDeletedTuples) {
            // Antimatter tuple - hold for cancellation check with next tuple
            outputElement = outputPriorityQueue.poll();
            needPushElementIntoQueue = true;
            antimatterTuplesDetected++;
            return false; // Continue processing
        }

        // Matter tuple - apply INCLUDE field filter if configured
        if (!passesTupleFilter(checkElement)) {
            // Tuple fails filter - skip without counting toward K
            PriorityQueueElement skippedElement = outputPriorityQueue.poll();
            pushIntoQueueAndAdvanceClusterIfNeeded(skippedElement);
            return false; // Continue processing
        }

        // Valid output tuple - passed antimatter reconciliation and filter
        return true;
    }

    /**
     * Check if tuple passes the INCLUDE field filter.
     * Filter is applied AFTER antimatter reconciliation because:
     * 1. Antimatter from newer components comes first in queue (lower cursorIndex)
     * 2. By the time we reach here, any matching antimatter has cancelled this tuple
     * 3. Tuples that fail filter should NOT be counted toward K
     *
     * @param element the element to check
     * @return true if tuple passes filter (or no filter configured), false otherwise
     */
    private boolean passesTupleFilter(PriorityQueueElement element) throws HyracksDataException {
        if (tupleFilter == null) {
            return true;
        }

        ITupleReference tuple = element.getTuple();
        referenceFilterTuple.reset(tuple);

        if (tupleFilter.accept(referenceFilterTuple)) {
            return true;
        }

        // Tuple fails filter - log for debugging
        tuplesFilteredOut++;
        try {
            long pkValue = LongPointable.getLong(tuple.getFieldData(2), tuple.getFieldStart(2) + 1);
            long yearValue = LongPointable.getLong(tuple.getFieldData(3), tuple.getFieldStart(3) + 1);
            System.err.println(
                    String.format("[LSMVCTreeSearchCursor] Tuple FILTERED OUT (total filtered: %d) | pk=%d, year=%d",
                            tuplesFilteredOut, pkValue, yearValue));
        } catch (Exception e) {
            // Ignore logging errors
        }
        return false;
    }

    /**
     * Process queue element when we have a pending element (potential antimatter cancellation).
     *
     * @param checkElement the element at top of queue
     */
    private void processWithPendingElement(PriorityQueueElement checkElement) throws HyracksDataException {
        if (compare(cmp, outputElement.getTuple(), checkElement.getTuple()) == 0) {
            // Same key - antimatter cancellation
            performAntimatterCancellation(checkElement);
        } else {
            // Different key - refill pending element's cursor
            refillPendingElementCursor();
        }
    }

    /**
     * Perform antimatter cancellation - both matter and antimatter tuples are discarded.
     *
     * @param checkElement the matching element from queue
     */
    private void performAntimatterCancellation(PriorityQueueElement checkElement) throws HyracksDataException {
        cancellationsMade++;

        // Advance both cursors (don't lose remaining tuples!)
        PriorityQueueElement checkElem = outputPriorityQueue.poll();
        pushIntoQueueAndAdvanceClusterIfNeeded(checkElem);
        pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);

        // Reset state - both tuples discarded
        needPushElementIntoQueue = false;
        outputElement = null;
    }

    /**
     * Refill the pending element's cursor and reset state.
     */
    private void refillPendingElementCursor() throws HyracksDataException {
        if (needPushElementIntoQueue) {
            pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
            needPushElementIntoQueue = false;
        }
        outputElement = null;
    }

    /**
     * Refill queue from pending element when queue is empty.
     */
    private void refillQueueFromPendingElement() throws HyracksDataException {
        pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
        needPushElementIntoQueue = false;
        outputElement = null;
    }

    /**
     * Replace memory components with disk components if they were flushed.
     * This is called periodically to handle concurrent flushes during search.
     */
    private void replaceMemoryComponentWithDiskComponentIfNeeded() throws HyracksDataException {
        int replaceFrom = findFirstComponentToReplace();
        if (replaceFrom < 0) {
            // No switch needed
            return;
        }

        // Ask LSM harness to replace memory components with their flushed disk versions
        opCtx.getIndex().getHarness().replaceMemoryComponentsWithDiskComponents(getOpCtx(), replaceFrom);

        // Redo searches on the new disk components
        for (int i = replaceFrom; i < switchRequest.length && i < operationalComponents.size(); i++) {
            if (switchRequest[i]) {
                ILSMComponent component = operationalComponents.get(i);
                VectorClusteringTree vcTree = (VectorClusteringTree) component.getIndex();

                // Check if first component is now disk (no more mutable component)
                if (i == 0 && component.getType() != LSMComponentType.MEMORY) {
                    includeMutableComponent = false;
                }

                // If we had an active element from this component, restart search from that point
                if (switchedElements[i] != null) {
                    // Close cursor and reset accessor to the new disk component
                    rangeCursors[i].close();
                    vcTreeAccessors[i].reset(vcTree, iap);
                    vcTreeAccessors[i].search(rangeCursors[i], searchPredicate);

                    // Try to position cursor at the same element
                    if (rangeCursors[i].hasNext()) {
                        rangeCursors[i].next();
                        switchedElements[i].reset(rangeCursors[i].getTuple());
                    }
                }
            }
            switchRequest[i] = false;
            switchedElements[i] = null;
            // Any failed switch makes further switches pointless
            switchPossible = switchPossible && operationalComponents.get(i).getType() == LSMComponentType.DISK;
        }
    }

    /**
     * Find the first component that needs to be replaced (has been flushed).
     * Returns the index of the first component to replace, or -1 if no replacement needed.
     */
    private int findFirstComponentToReplace() throws HyracksDataException {
        int replaceFrom = -1;

        if (!switchPossible) {
            return replaceFrom;
        }

        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);

            if (component.getType() == LSMComponentType.DISK) {
                if (i == 0) {
                    // First component is already disk, no more switching possible
                    switchPossible = false;
                }
                break;
            } else if (component.getState() == ILSMComponent.ComponentState.UNREADABLE_UNWRITABLE) {
                // Component was flushed while cursor is active - mark for replacement
                if (replaceFrom < 0) {
                    replaceFrom = i;
                }

                // Find the element from this cursor (if any)
                PriorityQueueElement element = findElementInQueue(i);

                // Mark this cursor for switching
                rangeCursors[i].close();
                switchRequest[i] = true;
                switchedElements[i] = element;
            }
        }

        return replaceFrom;
    }

    /**
     * Find an element in the priority queue or output element from a specific cursor.
     */
    private PriorityQueueElement findElementInQueue(int cursorIndex) {
        // Check if output element is from this cursor
        if (outputElement != null && outputElement.getCursorIndex() == cursorIndex) {
            return outputElement;
        }

        // Search in priority queue
        for (PriorityQueueElement element : outputPriorityQueue) {
            if (element.getCursorIndex() == cursorIndex) {
                return element;
            }
        }

        return null;
    }

    @Override
    protected void setPriorityQueueComparator() {
        // For vector index: sort by distance (field 0), then primary key (field 2)
        // Tuple format: <distance:field0, centroid_id:field1, primary_key:field2>
        // We skip centroid_id (field 1) in comparisons
        if (pqCmp == null || pqCmp.getMultiComparator() != cmp) {
            pqCmp = new VectorPriorityQueueComparator(cmp);
        }
    }

    /**
     * Custom priority queue comparator for vector index tuples.
     * Compares field 0 (distance) and field 2 (primary_key), skipping field 1 (centroid_id).
     * Must manually skip type tags since fields are type-tagged but comparators expect raw data.
     */
    private class VectorPriorityQueueComparator extends PriorityQueueComparator {

        public VectorPriorityQueueComparator(MultiComparator cmp) {
            super(cmp);
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            ITupleReference tupleA = elementA.getTuple();
            ITupleReference tupleB = elementB.getTuple();

            try {
                // Tuple format: [distance, centroidId, primary_keys..., include_fields...]
                // Compare order: distance first, then all remaining fields (PKs + includes)

                // We skip centroidId since tuples from different clusters can coexist

                // Compare field 0 (distance) using ADM-aware comparator 0
                int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                        tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0),
                        tupleB.getFieldLength(0));

                if (result != 0) {
                    return result;
                }

                // Compare remaining fields starting at field 2 (include fields + primary keys)
                // We compare all fields for total ordering in the priority queue
                int numRemainingFields = cmp.getComparators().length - 2; // All fields after distance and centroidId
                for (int i = 0; i < numRemainingFields; i++) {
                    int fieldIdx = 2 + i;
                    int cmpIdx = 2 + i;

                    // Check if field exists in tuple before comparing
                    if (fieldIdx >= tupleA.getFieldCount() || fieldIdx >= tupleB.getFieldCount()) {
                        break; // One tuple has fewer fields, skip remaining comparisons
                    }

                    result = cmp.getComparators()[cmpIdx].compare(tupleA.getFieldData(fieldIdx),
                            tupleA.getFieldStart(fieldIdx), tupleA.getFieldLength(fieldIdx),
                            tupleB.getFieldData(fieldIdx), tupleB.getFieldStart(fieldIdx),
                            tupleB.getFieldLength(fieldIdx));
                    if (result != 0) {
                        return result;
                    }
                }
            } catch (Throwable e) {
                throw new IllegalArgumentException(e);
            }

            // Tiebreaker: prefer tuples from earlier components
            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    @Override
    public ITupleReference doGetTuple() {
        // Track how many times this method is called
        getTupleCallCount++;

        // Return tuple from priority queue output element
        return outputElement != null ? outputElement.getTuple() : null;
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {

        // Tuple format: [distance, centroidId, primary_keys..., include_fields...]
        // Compare order: distance first, then primary keys (skip centroidId)
        // TODO: In the future, we may implement filtered search based on INCLUDE fields
        //       to support predicates like "WHERE movie_year > 2000" during vector search

        // Compare field 0 (distance) using ADM-aware comparator 0
        int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0), tupleB.getFieldLength(0));

        if (result != 0) {
            return result;
        }

        // Compare remaining fields starting at field 2 (include fields + primary keys)
        // Comparators array: [0=distance, 1=centroidId, 2+=include/PK fields]
        // We skip comparator 1 (centroidId) since tuples from different clusters can coexist
        // We compare all remaining fields for total ordering
        int numRemainingFields = cmp.getComparators().length - 2;
        for (int i = 0; i < numRemainingFields; i++) {
            int fieldIdx = 2 + i;
            int cmpIdx = 2 + i;

            // Check if field exists in tuple before comparing
            if (fieldIdx >= tupleA.getFieldCount() || fieldIdx >= tupleB.getFieldCount()) {
                break; // One tuple has fewer fields, skip remaining comparisons
            }

            result = cmp.getComparators()[cmpIdx].compare(tupleA.getFieldData(fieldIdx), tupleA.getFieldStart(fieldIdx),
                    tupleA.getFieldLength(fieldIdx), tupleB.getFieldData(fieldIdx), tupleB.getFieldStart(fieldIdx),
                    tupleB.getFieldLength(fieldIdx));
            if (result != 0) {
                return result;
            }
        }

        return 0;
    }

    @Override
    protected boolean isDeleted(PriorityQueueElement element) throws HyracksDataException {
        // Check if tuple has antimatter bit set (indicates deleted record)
        // During merge with full-scan mode, tuples may be rebuilt as ArrayTupleReference
        // which doesn't have antimatter bit - treat those as matter tuples
        ITupleReference tuple = element.getTuple();
        return ((ILSMTreeTupleReference) tuple).isAntimatter();
    }

    /**
     * Extract K value from search predicate for cluster advancement decisions.
     */
    private int extractK(ISearchPredicate searchPred) {
        return ((VectorPointPredicate) searchPred).getK();
    }

    /**
     * Extract nprobe value from search predicate (minimum clusters to probe before K-check).
     */
    private int extractNprobe(ISearchPredicate searchPred) {
        return ((VectorPointPredicate) searchPred).getNprobe();
    }

    /**
     * Extract epsilon value from search predicate (distance threshold for level-wise).
     */
    private double extractEpsilon(ISearchPredicate searchPred) {
        return ((VectorPointPredicate) searchPred).getEpsilon();
    }

    /**
     * Get the minimum number of clusters probed across all VectorClusteringSearchCursors.
     * This ensures synchronized nprobe checking across all LSM components.
     */
    private int getMinClustersProbed() {
        int minProbed = Integer.MAX_VALUE;

        for (int i = 0; i < rangeCursors.length; i++) {
            VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) rangeCursors[i];
            int probed = vcCursor.getClustersProbed();
            if (probed < minProbed) {
                minProbed = probed;
            }
        }

        // If no VectorClusteringSearchCursors found, return 0
        return minProbed == Integer.MAX_VALUE ? 0 : minProbed;
    }

    /**
     * Extract tuple filter from search predicate for INCLUDE field predicates.
     * When set, only tuples passing this filter are returned and counted toward K.
     */
    private ITupleFilter extractTupleFilter(ISearchPredicate searchPred) {
        if (searchPred instanceof VectorPointPredicate) {
            return ((VectorPointPredicate) searchPred).getTupleFilter();
        }
        return null;
    }

    /**
     * Push next element from component cursor into queue.
     * If cursor's current cluster is exhausted, mark it as exhausted.
     * When ALL components' clusters are exhausted, decide whether to advance ALL to next cluster.
     *
     * @param e the priority queue element to refill
     * @throws HyracksDataException if an error occurs
     */
    private void pushIntoQueueAndAdvanceClusterIfNeeded(PriorityQueueElement e) throws HyracksDataException {
        int cursorIndex = e.getCursorIndex();
        IIndexCursor cursor = rangeCursors[cursorIndex];

        if (cursor.hasNext()) {
            // Current cluster/page has more data
            cursor.next();
            e.reset(cursor.getTuple());
            outputPriorityQueue.offer(e);
            return;
        }

        // Current cluster exhausted for THIS component
        clusterExhausted[cursorIndex] = true;

        System.err.println(String.format("[LSMVCTreeSearchCursor] Component %d cluster exhausted (cluster_index=%d)",
                cursorIndex, currentClusterIndex[cursorIndex]));

        // Check if ALL components have exhausted their current cluster
        boolean allExhausted = true;
        for (int i = 0; i < clusterExhausted.length; i++) {
            if (!clusterExhausted[i]) {
                allExhausted = false;
                break;
            }
        }

        if (!allExhausted) {
            // Some components still have data in their current cluster
            // Don't advance yet - wait for all to exhaust
            System.err.println(String.format(
                    "[LSMVCTreeSearchCursor] Component %d exhausted, but waiting for other components", cursorIndex));
            return;
        }

        // ALL components exhausted their current cluster
        System.err.println(String.format("[LSMVCTreeSearchCursor] ALL components exhausted cluster %d, K=%d",
                currentClusterIndex[0], K));

        // Decision: Should we advance ALL components to next cluster?
        if (stopAdvancing) {
            System.err.println("[LSMVCTreeSearchCursor] Already decided to stop advancing");
            return;
        }

        // Calculate minimum clusters explored across all components
        int minClustersExplored = getMinClustersProbed();
        int resultsCollected = getTupleCallCount;

        // Use strategy to decide if we should stop advancing
        if (clusterStrategy.shouldStopAdvancing(minClustersExplored, resultsCollected)) {
            // We have enough results AND probed enough clusters - set flag to stop advancing
            stopAdvancing = true;
            System.err.println(
                    String.format("[LSMVCTreeSearchCursor] Early termination: minClustersExplored=%d, returned=%d",
                            minClustersExplored, resultsCollected));
            return;
        }

        // Not enough results or haven't probed enough clusters yet - advance ALL components
        System.err.println(String.format(
                "[LSMVCTreeSearchCursor] Need more: minClusters=%d (nprobe=%d), returned=%d (K=%d), advancing to next cluster",
                minClustersExplored, nprobe, resultsCollected, K));
        advanceAllComponentsToNextCluster();
    }

    /**
     * Advance ALL component cursors to next cluster.
     *
     * All components advance to the SAME cluster determined by level-wise selection or DFS.
     * This ensures synchronized exploration for nprobe logic.
     *
     * Uses iterative loop instead of recursion to avoid StackOverflowError
     * when many consecutive clusters are empty.
     */
    private void advanceAllComponentsToNextCluster() throws HyracksDataException {
        advanceAllComponentsToSameCluster();
    }

    /**
     * All components advance to the SAME cluster determined by level-wise selection or DFS.
     * This ensures synchronized exploration for nprobe logic.
     */
    private void advanceAllComponentsToSameCluster() throws HyracksDataException {
        // Loop to handle consecutive empty clusters without recursion
        // This avoids StackOverflowError with pathological data distributions
        while (true) {
            // Reset exhaustion flags for new cluster
            Arrays.fill(clusterExhausted, false);

            // Determine which cluster ALL components should advance to (via strategy)
            ClusterSearchResult nextCluster = clusterStrategy.getNextCluster();

            if (nextCluster == null) {
                // No more clusters available
                System.err.println("[LSMVCTreeSearchCursor] No more clusters available globally");
                for (int i = 0; i < clusterExhausted.length; i++) {
                    clusterExhausted[i] = true;
                }
                return;
            }

            System.err.println(
                    String.format("[LSMVCTreeSearchCursor] Global cluster selected: cid=%d, distance=%.4f, dirPage=%d",
                            nextCluster.centroidId, nextCluster.distance, nextCluster.directoryPageId));

            // Tell ALL components to open this SAME cluster (using O(1) directoryPageId access)
            for (int i = 0; i < rangeCursors.length; i++) {
                advanceComponentToCluster(i, nextCluster);
            }

            // Check if all components found empty cluster
            // If so, continue loop to try next cluster (instead of recursion)
            if (!shouldSkipToNextCluster()) {
                return; // At least one component has data, or we should stop advancing
            }
            // All components empty and should continue - loop to next cluster
        }
    }

    /**
     * Advance a single component to a specific cluster using ClusterSearchResult.
     * Uses O(1) directoryPageId access when available.
     */
    private void advanceComponentToCluster(int componentIndex, ClusterSearchResult cluster)
            throws HyracksDataException {
        IIndexCursor cursor = rangeCursors[componentIndex];

        if (!(cursor instanceof VectorClusteringSearchCursor)) {
            clusterExhausted[componentIndex] = true;
            System.err.println(
                    String.format("[LSMVCTreeSearchCursor] Component %d is not VectorClusteringSearchCursor, skipping",
                            componentIndex));
            return;
        }

        VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) cursor;

        // Open specific cluster using ClusterSearchResult (O(1) access via directoryPageId)
        boolean hasData = vcCursor.openClusterByResult(cluster);

        // Increment cluster index
        currentClusterIndex[componentIndex]++;

        // Check if cluster has data
        if (hasData && vcCursor.hasNext()) {
            vcCursor.next();
            PriorityQueueElement pqe = pqes[componentIndex];
            pqe.reset(vcCursor.getTuple());
            outputPriorityQueue.offer(pqe);
            System.err.println(
                    String.format("[LSMVCTreeSearchCursor] Component %d opened cluster cid=%d (has data, O(1) access)",
                            componentIndex, cluster.centroidId));
        } else {
            clusterExhausted[componentIndex] = true;
            System.err.println(String.format("[LSMVCTreeSearchCursor] Component %d cluster cid=%d is empty",
                    componentIndex, cluster.centroidId));
        }
    }

    /**
     * Check if we should skip to next cluster (all components empty) or stop.
     * Returns true if we should continue to next cluster, false otherwise.
     */
    private boolean shouldSkipToNextCluster() {
        // Check if ALL components found the cluster empty
        boolean allExhausted = true;
        for (int i = 0; i < clusterExhausted.length; i++) {
            if (!clusterExhausted[i]) {
                allExhausted = false;
                break;
            }
        }

        if (!allExhausted) {
            // At least one component has data - don't skip
            return false;
        }

        // All components exhausted this cluster - check if we should continue
        if (stopAdvancing) {
            System.err.println("[LSMVCTreeSearchCursor] Reached stop flag, halting advancement");
            return false;
        }

        // Check if any component has more clusters available (via strategy)
        boolean hasMoreClusters = clusterStrategy.hasMoreClusters();
        if (!hasMoreClusters) {
            System.err.println("[LSMVCTreeSearchCursor] All components exhausted, no more clusters to scan");
            return false;
        }

        System.err.println(
                String.format("[LSMVCTreeSearchCursor] ALL components found cluster %d empty, skipping to next cluster",
                        currentClusterIndex[0]));
        return true; // Continue to next cluster
    }
}
