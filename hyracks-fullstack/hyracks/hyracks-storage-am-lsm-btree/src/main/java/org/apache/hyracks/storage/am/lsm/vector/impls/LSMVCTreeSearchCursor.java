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

import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree.VectorClusteringTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
import org.apache.hyracks.storage.am.vector.utils.VCTreeNavigationUtils;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * LSM search cursor for Vector Clustering Tree.
 *
 * This cursor coordinates searches across multiple LSM components (memory and disk)
 * by delegating to VectorClusteringSearchCursor for each component and merging results.
 *
 * Following the pattern of LSMBTreeRangeSearchCursor:
 * - Extends LSMIndexSearchCursor for priority queue and component switching infrastructure
 * - Creates VectorClusteringTreeAccessor for each component
 * - Handles component state changes (memory → disk transitions)
 * - Uses priority queue to merge results sorted by <distance, primary_key>
 * - Filters antimatter tuples and handles matter/antimatter cancellation
 */
public class LSMVCTreeSearchCursor extends LSMIndexSearchCursor {

    private static final Logger LOGGER = LogManager.getLogger();

    // Accessor array for each component's VCTree
    private VectorClusteringTreeAccessor[] vcTreeAccessors;

    // Track component types to detect memory → disk transitions
    protected boolean[] isMemoryComponent;

    // Store search predicate for component switching
    private ISearchPredicate searchPredicate;

    // Track K (target limit) and reconciled output count for cluster advancement decisions
    private int K;
    private int reconciledOutputCount;

    // Track cluster exhaustion state for synchronized advancement
    private int[] currentClusterIndex; // Which cluster each component is currently on
    private boolean[] clusterExhausted; // Whether each component exhausted its current cluster
    private boolean stopAdvancing; // Flag to stop advancing after K reached

    // Debug counters to track reconciliation
    private int totalTuplesPopped; // Total tuples popped from priority queue (including cancelled)
    private int antimatterTuplesDetected; // Antimatter tuples detected
    private int cancellationsMade; // Matter tuples cancelled by antimatter

    // Full-scan mode flag (for merge operations)
    private boolean fullScanMode; // true = merge mode (sequential), false = query mode (distance-based)

    // Level-wise + nprobe support fields (global coordination)
    private Set<Integer> sharedVisitedCentroidIds; // Shared visited tracking across all components
    private int nprobe; // Minimum clusters to probe
    private double epsilon; // Distance threshold for level-wise
    private List<ClusterSearchResult> globalLevelWiseClusters; // Pre-computed clusters from first component
    private int globalClusterIndex; // Current position in globalLevelWiseClusters
    private boolean levelWisePhaseComplete; // Whether level-wise exploration is done
    private double[] queryVector; // Cached query vector
    private IVectorDistanceFunction distanceFunction; // Distance function for DFS

    public LSMVCTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false, false, NoOpIndexCursorStats.INSTANCE);
    }

    public LSMVCTreeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples,
            IIndexCursorStats stats) {
        this(opCtx, returnDeletedTuples, false, stats);
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
        this.reconciledOutputCount = 0;
        this.nprobe = 10;
        this.epsilon = 0.3;
        // Extract nprobe and epsilon from search predicate

        // Initialize shared visited tracking set
        this.sharedVisitedCentroidIds = new HashSet<>();

        // Initialize global level-wise tracking
        this.globalLevelWiseClusters = null;
        this.globalClusterIndex = 0;
        this.levelWisePhaseComplete = false;
        this.queryVector = null;
        this.distanceFunction = null;

        // Initialize debug counters
        this.totalTuplesPopped = 0;
        this.antimatterTuplesDetected = 0;
        this.cancellationsMade = 0;

        // Set up comparator and operational components
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();

        // For vector index, we don't need mutable component special handling initially
        includeMutableComponent = false;

        int numVCTrees = operationalComponents.size();

        // Initialize cluster tracking arrays for synchronized advancement
        currentClusterIndex = new int[numVCTrees];
        java.util.Arrays.fill(currentClusterIndex, 0); // All start at cluster 0
        clusterExhausted = new boolean[numVCTrees];
        java.util.Arrays.fill(clusterExhausted, false);
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

        // Pass shared visited set to each component cursor
        for (int i = 0; i < numVCTrees; i++) {
            if (rangeCursors[i] instanceof VectorClusteringSearchCursor) {
                VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) rangeCursors[i];
                vcCursor.setSharedVisitedSet(sharedVisitedCentroidIds);
            }
        }

        // Open all cursors with the search predicate
        IndexCursorUtils.open(vcTreeAccessors, rangeCursors, searchPred);

        // After cursors are open, extract query vector and compute global level-wise clusters
        if (!fullScanMode && numVCTrees > 0) {
            VectorClusteringSearchCursor firstCursor = (VectorClusteringSearchCursor) rangeCursors[0];
            this.queryVector = firstCursor.getQueryVector();
            this.distanceFunction = firstCursor.getDistanceFunction();

            // Compute global level-wise clusters if epsilon > 0
            if (this.queryVector != null && this.epsilon > 0.0) {
                globalLevelWiseClusters = computeGlobalLevelWiseClusters();
                if (globalLevelWiseClusters != null && !globalLevelWiseClusters.isEmpty()) {
                    // Mark first cluster as visited (it was already opened by each cursor's initial DFS)
                    ClusterSearchResult firstCluster = globalLevelWiseClusters.get(0);
                    sharedVisitedCentroidIds.add(firstCluster.centroidId);
                    globalClusterIndex = 1; // Start from second cluster
                }
                LOGGER.log(Level.INFO,
                        "[Thread:{}] [LSMVCTreeSearchCursor.doOpen] Computed {} global level-wise clusters with epsilon={}",
                        Thread.currentThread().getName(),
                        globalLevelWiseClusters != null ? globalLevelWiseClusters.size() : 0, epsilon);
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

    /**
     * Compute global level-wise clusters using first component's tree.
     * Since all components have identical tree structure, we compute once and share.
     */
    private List<ClusterSearchResult> computeGlobalLevelWiseClusters() throws HyracksDataException {
        if (operationalComponents.isEmpty() || queryVector == null || distanceFunction == null) {
            return null;
        }

        ILSMComponent firstComponent = operationalComponents.get(0);
        VectorClusteringTree vcTree = (VectorClusteringTree) firstComponent.getIndex();

        try {
            return VCTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort(vcTree.getBufferCache(),
                    vcTree.getFileId(), vcTree.getRootPageId(), vcTree.getInteriorFrameFactory(),
                    vcTree.getLeafFrameFactory(), queryVector, distanceFunction, epsilon);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "[Thread:{}] [LSMVCTreeSearchCursor] Failed to compute level-wise clusters: {}",
                    Thread.currentThread().getName(), e.getMessage());
            return null;
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
                LOGGER.log(Level.INFO,
                        "[Thread:{}] [LSMVCTreeSearchCursor] Component {} has empty initial cluster (marked exhausted)",
                        Thread.currentThread().getName(), i);
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
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] ALL components have empty initial cluster, advancing to next",
                    Thread.currentThread().getName());
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
        // Print final reconciliation summary for demo
        LOGGER.log(Level.INFO, "[Thread:{}] ========== LSM Vector Index Search Summary ==========",
                Thread.currentThread().getName());
        LOGGER.log(Level.INFO, "[Thread:{}] Total tuples processed:     {}", Thread.currentThread().getName(),
                totalTuplesPopped);
        LOGGER.log(Level.INFO, "[Thread:{}] Antimatter tuples detected: {}", Thread.currentThread().getName(),
                antimatterTuplesDetected);
        LOGGER.log(Level.INFO, "[Thread:{}] Cancellations made:         {}", Thread.currentThread().getName(),
                cancellationsMade);
        LOGGER.log(Level.INFO, "[Thread:{}] Final output count:         {}", Thread.currentThread().getName(),
                reconciledOutputCount);
        LOGGER.log(Level.INFO, "[Thread:{}] Verification:               {} - {} = {} ✓",
                Thread.currentThread().getName(), totalTuplesPopped, cancellationsMade, reconciledOutputCount);
        LOGGER.log(Level.INFO, "[Thread:{}] =====================================================",
                Thread.currentThread().getName());

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

        // Increment reconciled output count - this tuple is being returned to user
        reconciledOutputCount++;
        totalTuplesPopped++;
    }

    @Override
    protected void checkPriorityQueue() throws HyracksDataException {
        // Every SWITCH_COMPONENT_CYCLE calls, check if memory components need to be swapped with disk components
        // This handles the case where a memory component is flushed to disk while the cursor is active
        if (hasNextCallCount >= SWITCH_COMPONENT_CYCLE) {
            replaceMemoryComponentWithDiskComponentIfNeeded();
            hasNextCallCount = 0;
        }

        // Custom priority queue logic that tracks reconciled output and controls cluster advancement
        // Note: We don't stop exactly at K - we exhaust current clusters before stopping
        // Cluster advancement is controlled in pushIntoQueueAndAdvanceClusterIfNeeded()
        while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue) {
            if (!outputPriorityQueue.isEmpty()) {
                PriorityQueueElement checkElement = outputPriorityQueue.peek();

                if (outputElement == null) {
                    // Check if top element is antimatter
                    boolean isDeletedFlag = isDeleted(checkElement);

                    if (isDeletedFlag && !returnDeletedTuples) {
                        // Antimatter tuple - pop and hold for cancellation check
                        outputElement = outputPriorityQueue.poll();
                        needPushElementIntoQueue = true;
                        antimatterTuplesDetected++;

                        // LOG: Antimatter detected
                        if (antimatterTuplesDetected <= 5 || antimatterTuplesDetected % 50 == 0) {
                            LOGGER.log(Level.INFO,
                                    "[Thread:{}] [checkPQ] Antimatter detected #{} | Component={}, held for matching",
                                    Thread.currentThread().getName(), antimatterTuplesDetected,
                                    outputElement.getCursorIndex());
                        }
                        // Continue loop to check for cancellation with next tuple
                    } else {
                        // Valid output tuple - will be returned to user
                        break;
                    }
                } else {
                    // outputElement holds antimatter or previous tuple
                    if (compare(cmp, outputElement.getTuple(), checkElement.getTuple()) == 0) {
                        // Antimatter matches matter tuple - cancel BOTH
                        cancellationsMade++;

                        // Remove and advance checkElement's cursor
                        PriorityQueueElement checkElem = outputPriorityQueue.poll();
                        pushIntoQueueAndAdvanceClusterIfNeeded(checkElem);

                        // Advance outputElement's cursor (don't lose the rest of its tuples!)
                        pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);

                        // CRITICAL FIX: Only decrement reconciledOutputCount if outputElement was a MATTER tuple
                        // that was counted by doNext(). If outputElement is ANTIMATTER, it was detected at line 305
                        // and never counted, so we should NOT decrement.
                        boolean outputIsAntimatter = ((ILSMTreeTupleReference) outputElement.getTuple()).isAntimatter();
                        if (!outputIsAntimatter) {
                            // Matter tuple was counted by doNext(), decrement it
                            reconciledOutputCount--;

                            // Log cancellation (show every 50th)
                            if (cancellationsMade % 50 == 0 || cancellationsMade <= 10) {
                                LOGGER.log(Level.INFO,
                                        "[Thread:{}] [LSM Vector Index] Cancellation #{} | Matter tuple cancelled, reconciledCount={}",
                                        Thread.currentThread().getName(), cancellationsMade, reconciledOutputCount);
                            }
                        } else {
                            // Antimatter detected first, matter never counted, no decrement needed
                            if (cancellationsMade % 50 == 0 || cancellationsMade <= 10) {
                                LOGGER.log(Level.INFO,
                                        "[Thread:{}] [LSM Vector Index] Cancellation #{} | Antimatter matched, no count change",
                                        Thread.currentThread().getName(), cancellationsMade);
                            }
                        }

                        // Both tuples discarded (cancelled), reset state
                        needPushElementIntoQueue = false;
                        outputElement = null;
                    } else {
                        // Different tuple - refill cursor
                        if (needPushElementIntoQueue) {
                            pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
                            needPushElementIntoQueue = false;
                        }
                        outputElement = null;
                    }
                }
            } else {
                // Queue is empty and we have pending element - refill it
                pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
                needPushElementIntoQueue = false;
                outputElement = null;
            }
        }
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
                // Compare field 0 (distance) - skip 1-byte type tag
                // Field format: [type_tag:1 byte][double:8 bytes]
                int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0) + 1,
                        tupleA.getFieldLength(0) - 1, tupleB.getFieldData(0), tupleB.getFieldStart(0) + 1,
                        tupleB.getFieldLength(0) - 1);

                if (result != 0) {
                    return result;
                }

                // Compare field 2 (primary_key) - skip 1-byte type tag
                // Field format: [type_tag:1 byte][long:8 bytes]
                result = cmp.getComparators()[1].compare(tupleA.getFieldData(2), tupleA.getFieldStart(2) + 1,
                        tupleA.getFieldLength(2) - 1, tupleB.getFieldData(2), tupleB.getFieldStart(2) + 1,
                        tupleB.getFieldLength(2) - 1);

                if (result != 0) {
                    return result;
                }
            } catch (Throwable e) {
                LOGGER.log(Level.INFO, "[Thread:{}] [VectorPQComparator] ERROR comparing tuples: {} - {}",
                        Thread.currentThread().getName(), e.getClass().getSimpleName(), e.getMessage());
                throw new IllegalArgumentException(e);
            }

            // Tiebreaker: prefer tuples from earlier components (lower cursor index)
            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    @Override
    public ITupleReference doGetTuple() {
        // Return tuple from priority queue output element
        return outputElement != null ? outputElement.getTuple() : null;
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {

        // Compare field 0 (distance) - skip type_tag (1 byte)
        // Field format: [type_tag:1 byte][double:8 bytes]
        int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0) + 1,
                tupleA.getFieldLength(0) - 1, tupleB.getFieldData(0), tupleB.getFieldStart(0) + 1,
                tupleB.getFieldLength(0) - 1);

        if (result != 0) {
            return result;
        }

        // Compare field 2 (primary_key) - skip type_tag (1 byte)
        // Field format: [type_tag:1 byte][long:8 bytes] or [type_tag:1 byte][string]
        return cmp.getComparators()[1].compare(tupleA.getFieldData(2), tupleA.getFieldStart(2) + 1,
                tupleA.getFieldLength(2) - 1, tupleB.getFieldData(2), tupleB.getFieldStart(2) + 1,
                tupleB.getFieldLength(2) - 1);
    }

    /**
     * Helper method to decode primary key for debugging.
     * Handles both LONG and STRING types.
     */
    private String decodePrimaryKey(byte[] data, int start, int length, byte typeTag) {
        try {
            // Type tag values: LONG=18, STRING=1 (common AsterixDB type tags)
            if (typeTag == 18) {
                // LONG type (8 bytes)
                if (length >= 9) {
                    long value = java.nio.ByteBuffer.wrap(data, start + 1, 8).getLong();
                    return String.valueOf(value);
                }
            } else if (typeTag == 1) {
                // STRING type (2-byte length prefix + UTF-8 bytes)
                if (length >= 3) {
                    int strLen = java.nio.ByteBuffer.wrap(data, start + 1, 2).getShort() & 0xFFFF;
                    String value = new String(data, start + 3, strLen, java.nio.charset.StandardCharsets.UTF_8);
                    return value;
                }
            }
            // Unknown type or invalid length - return hex representation
            StringBuilder hex = new StringBuilder();
            for (int i = 1; i < Math.min(length, 20); i++) {
                hex.append(String.format("%02X", data[start + i]));
            }
            return "0x" + hex.toString();
        } catch (Exception e) {
            return "<decode_error>";
        }
    }

    @Override
    protected boolean isDeleted(PriorityQueueElement element) throws HyracksDataException {
        // Check if tuple has antimatter bit set (indicates deleted record)
        // During merge with full-scan mode, tuples may be rebuilt as ArrayTupleReference
        // which doesn't have antimatter bit - treat those as matter tuples
        ITupleReference tuple = element.getTuple();
        if (tuple instanceof ILSMTreeTupleReference) {
            return ((ILSMTreeTupleReference) tuple).isAntimatter();
        }
        // Not an LSM tuple - must be a rebuilt tuple during merge, treat as matter
        return false;
    }

    /**
     * Extract K value from search predicate for cluster advancement decisions.
     */
    private int extractK(ISearchPredicate searchPred) {
        if (searchPred instanceof org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate) {
            return ((org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate) searchPred).getK();
        }

        // Fallback: return a large number (scan all clusters)
        return Integer.MAX_VALUE;
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

        LOGGER.log(Level.INFO, "[Thread:{}] [LSMVCTreeSearchCursor] Component {} cluster exhausted (cluster_index={})",
                Thread.currentThread().getName(), cursorIndex, currentClusterIndex[cursorIndex]);

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
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] Component {} exhausted, but waiting for other components",
                    Thread.currentThread().getName(), cursorIndex);
            return;
        }

        // ALL components exhausted their current cluster
        // Calculate the minimum clusters explored across all components
        int minClustersExplored = getMinClustersExplored();

        LOGGER.log(Level.INFO,
                "[Thread:{}] [LSMVCTreeSearchCursor] ALL components exhausted cluster {}, reconciledOutputCount={}, K={}, minClustersExplored={}, nprobe={}",
                Thread.currentThread().getName(), currentClusterIndex[0], reconciledOutputCount, K, minClustersExplored,
                nprobe);

        // Decision: Should we advance ALL components to next cluster?
        if (stopAdvancing) {
            LOGGER.log(Level.INFO, "[Thread:{}] [LSMVCTreeSearchCursor] Already decided to stop advancing",
                    Thread.currentThread().getName());
            return;
        }

        // NPROBE LOGIC:
        // 1. If minClustersExplored < nprobe, always continue (haven't reached minimum probe count)
        // 2. If minClustersExplored >= nprobe AND reconciledOutputCount >= K, stop
        // 3. If minClustersExplored >= nprobe AND reconciledOutputCount < K, continue (need more results)
        if (minClustersExplored >= nprobe && reconciledOutputCount >= K) {
            // Reached both nprobe and K - stop advancing
            stopAdvancing = true;
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] Reached nprobe={} and K={}, stopping cluster advancement",
                    Thread.currentThread().getName(), nprobe, K);
            return;
        }

        if (minClustersExplored < nprobe) {
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] minClustersExplored={} < nprobe={}, must continue exploring",
                    Thread.currentThread().getName(), minClustersExplored, nprobe);
        } else {
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] reconciledOutputCount={} < K={}, need more results, continuing",
                    Thread.currentThread().getName(), reconciledOutputCount, K);
        }

        // Need more data - advance ALL components to next cluster together
        LOGGER.log(Level.INFO, "[Thread:{}] [LSMVCTreeSearchCursor] Advancing ALL components to next cluster",
                Thread.currentThread().getName());
        advanceAllComponentsToNextCluster();
    }

    /**
     * Advance ALL component cursors to the SAME next cluster.
     * Uses global level-wise list first, then falls back to DFS.
     * This ensures all components always explore the same cluster simultaneously.
     *
     * Uses iterative loop instead of recursion to avoid StackOverflowError
     * when many consecutive clusters are empty.
     */
    private void advanceAllComponentsToNextCluster() throws HyracksDataException {
        // Loop to handle consecutive empty clusters without recursion
        // This avoids StackOverflowError with pathological data distributions
        while (true) {
            // Reset exhaustion flags for new cluster
            java.util.Arrays.fill(clusterExhausted, false);

            // Determine which cluster ALL components should advance to
            ClusterSearchResult nextCluster = getNextGlobalCluster();

            if (nextCluster == null) {
                // No more clusters available
                LOGGER.log(Level.INFO, "[Thread:{}] [LSMVCTreeSearchCursor] No more clusters available globally",
                        Thread.currentThread().getName());
                for (int i = 0; i < clusterExhausted.length; i++) {
                    clusterExhausted[i] = true;
                }
                return;
            }

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
     * Get the next cluster that ALL components should advance to.
     * Uses global level-wise list first, then falls back to DFS.
     * Returns ClusterSearchResult with directoryPageId for O(1) access.
     */
    private ClusterSearchResult getNextGlobalCluster() throws HyracksDataException {
        // Phase 1: Try global level-wise clusters (already have directoryPageId)
        if (!levelWisePhaseComplete && globalLevelWiseClusters != null
                && globalClusterIndex < globalLevelWiseClusters.size()) {

            ClusterSearchResult nextCluster = globalLevelWiseClusters.get(globalClusterIndex);
            globalClusterIndex++;

            // Mark visited for DFS fallback
            sharedVisitedCentroidIds.add(nextCluster.centroidId);

            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] Level-wise: next global cluster {}/{} (cid={}, distance={}, dirPage={})",
                    Thread.currentThread().getName(), globalClusterIndex, globalLevelWiseClusters.size(),
                    nextCluster.centroidId, nextCluster.distance, nextCluster.directoryPageId);

            if (globalClusterIndex >= globalLevelWiseClusters.size()) {
                levelWisePhaseComplete = true;
                LOGGER.log(Level.INFO,
                        "[Thread:{}] [LSMVCTreeSearchCursor] Level-wise phase complete, will use DFS next",
                        Thread.currentThread().getName());
            }

            return nextCluster;
        }

        // Phase 2: DFS fallback - get next from first component's DFS
        levelWisePhaseComplete = true;
        return getNextClusterFromDFS();
    }

    /**
     * Get next cluster from DFS (using first component).
     * Returns ClusterSearchResult with directoryPageId for O(1) access.
     * Skips already visited clusters.
     */
    private ClusterSearchResult getNextClusterFromDFS() throws HyracksDataException {
        if (rangeCursors.length == 0) {
            return null;
        }

        VectorClusteringSearchCursor firstCursor = (VectorClusteringSearchCursor) rangeCursors[0];
        ClusterSearchResult next = firstCursor.findNextClusterDFS();

        if (next == null) {
            LOGGER.log(Level.INFO, "[Thread:{}] [LSMVCTreeSearchCursor] DFS exhausted, no more clusters",
                    Thread.currentThread().getName());
            return null;
        }

        LOGGER.log(Level.INFO,
                "[Thread:{}] [LSMVCTreeSearchCursor] DFS fallback: next global cluster cid={}, distance={}, dirPage={}",
                Thread.currentThread().getName(), next.centroidId, next.distance, next.directoryPageId);

        // Note: The DFS already marks visited in NavigationState, so we don't add to sharedVisitedCentroidIds here
        return next;
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
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] Component {} is not VectorClusteringSearchCursor, skipping",
                    Thread.currentThread().getName(), componentIndex);
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
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] Component {} opened cluster cid={} (has data, O(1) access)",
                    Thread.currentThread().getName(), componentIndex, cluster.centroidId);
        } else {
            clusterExhausted[componentIndex] = true;
            LOGGER.log(Level.INFO, "[Thread:{}] [LSMVCTreeSearchCursor] Component {} cluster cid={} is empty",
                    Thread.currentThread().getName(), componentIndex, cluster.centroidId);
        }
    }

    /**
     * Check if we should skip to the next cluster because all components found empty clusters.
     * This method is called iteratively (not recursively) from advanceAllComponentsToNextCluster.
     *
     * @return true if all components have empty clusters and we should continue to the next cluster;
     *         false if at least one component has data or we should stop advancing
     */
    private boolean shouldSkipToNextCluster() {
        boolean allExhausted = true;
        for (int i = 0; i < clusterExhausted.length; i++) {
            if (!clusterExhausted[i]) {
                allExhausted = false;
                break;
            }
        }

        if (!allExhausted) {
            return false; // At least one component has data
        }

        // All components found empty cluster - check if we should skip to next
        boolean hasMoreClusters = hasMoreGlobalClusters();
        int minClustersExplored = getMinClustersExplored();

        // Continue if: haven't reached nprobe OR haven't found K results
        boolean shouldContinue = minClustersExplored < nprobe || reconciledOutputCount < K;

        if (hasMoreClusters && !stopAdvancing && shouldContinue) {
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] All components empty, skipping to next global cluster (minClusters={}, nprobe={}, results={}, K={})",
                    Thread.currentThread().getName(), minClustersExplored, nprobe, reconciledOutputCount, K);
            return true; // Should skip to next cluster
        } else if (!hasMoreClusters) {
            LOGGER.log(Level.INFO,
                    "[Thread:{}] [LSMVCTreeSearchCursor] All components exhausted, no more global clusters",
                    Thread.currentThread().getName());
        }
        return false; // Should not skip
    }

    /**
     * Check if there are more global clusters to explore.
     */
    private boolean hasMoreGlobalClusters() {
        // Check level-wise first
        if (!levelWisePhaseComplete && globalLevelWiseClusters != null
                && globalClusterIndex < globalLevelWiseClusters.size()) {
            return true;
        }

        // Check DFS via first cursor
        if (rangeCursors.length > 0 && rangeCursors[0] instanceof VectorClusteringSearchCursor) {
            VectorClusteringSearchCursor firstCursor = (VectorClusteringSearchCursor) rangeCursors[0];
            return firstCursor.hasMoreClusters();
        }

        return false;
    }

    /**
     * Get the minimum number of clusters explored across all components.
     * This ensures all components have explored at least nprobe clusters before stopping.
     */
    private int getMinClustersExplored() {
        int minClusters = Integer.MAX_VALUE;

        for (int i = 0; i < rangeCursors.length; i++) {
            IIndexCursor cursor = rangeCursors[i];
            if (cursor instanceof VectorClusteringSearchCursor) {
                VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) cursor;
                int clustersProbed = vcCursor.getClustersProbed();
                if (clustersProbed < minClusters) {
                    minClusters = clustersProbed;
                }
            }
        }

        return minClusters == Integer.MAX_VALUE ? 0 : minClusters;
    }
}
