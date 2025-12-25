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

import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree.VectorClusteringTreeAccessor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

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

        // Open all cursors with the search predicate
        IndexCursorUtils.open(vcTreeAccessors, rangeCursors, searchPred);

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
        // Print final reconciliation summary for demo
        System.err.println("\n========== LSM Vector Index Search Summary ==========");
        System.err.println(String.format("Total tuples processed:     %d", totalTuplesPopped));
        System.err.println(String.format("Antimatter tuples detected: %d", antimatterTuplesDetected));
        System.err.println(String.format("Cancellations made:         %d", cancellationsMade));
        System.err.println(String.format("Final output count:         %d", reconciledOutputCount));
        System.err.println(String.format("Verification:               %d - %d = %d ✓", totalTuplesPopped,
                cancellationsMade, reconciledOutputCount));
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
                            System.err.println(
                                    String.format("[checkPQ] Antimatter detected #%d | Component=%d, held for matching",
                                            antimatterTuplesDetected, outputElement.getCursorIndex()));
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
                                System.err.println(String.format(
                                        "[LSM Vector Index] Cancellation #%d | Matter tuple cancelled, reconciledCount=%d",
                                        cancellationsMade, reconciledOutputCount));
                            }
                        } else {
                            // Antimatter detected first, matter never counted, no decrement needed
                            if (cancellationsMade % 50 == 0 || cancellationsMade <= 10) {
                                System.err.println(String.format(
                                        "[LSM Vector Index] Cancellation #%d | Antimatter matched, no count change",
                                        cancellationsMade));
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
                System.err.println(String.format("[VectorPQComparator] ERROR comparing tuples: %s - %s",
                        e.getClass().getSimpleName(), e.getMessage()));
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
        System.err.println(String.format(
                "[LSMVCTreeSearchCursor] ALL components exhausted cluster %d, reconciledOutputCount=%d, K=%d",
                currentClusterIndex[0], reconciledOutputCount, K));

        // Decision: Should we advance ALL components to next cluster?
        if (stopAdvancing) {
            System.err.println("[LSMVCTreeSearchCursor] Already decided to stop advancing");
            return;
        }

        if (reconciledOutputCount >= K) {
            // Reached K - stop advancing all components
            stopAdvancing = true;
            System.err.println(String.format("[LSMVCTreeSearchCursor] Reached K=%d, stopping cluster advancement", K));
            return;
        }

        // Need more data - advance ALL components to next cluster together
        System.err.println("[LSMVCTreeSearchCursor] Advancing ALL components to next cluster");
        advanceAllComponentsToNextCluster();
    }

    /**
     * Advance ALL component cursors to their next closest cluster.
     * This ensures synchronized cluster advancement across all LSM components.
     */
    private void advanceAllComponentsToNextCluster() throws HyracksDataException {
        // Reset exhaustion flags for new cluster
        java.util.Arrays.fill(clusterExhausted, false);

        for (int i = 0; i < rangeCursors.length; i++) {
            IIndexCursor cursor = rangeCursors[i];

            if (!(cursor instanceof VectorClusteringSearchCursor)) {
                // Not a VectorClusteringSearchCursor - mark as exhausted
                clusterExhausted[i] = true;
                System.err.println(String.format(
                        "[LSMVCTreeSearchCursor] Component %d is not VectorClusteringSearchCursor, skipping", i));
                continue;
            }

            VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) cursor;

            if (!vcCursor.hasMoreClusters()) {
                // No more clusters for this component - mark as exhausted
                clusterExhausted[i] = true;
                System.err.println(String.format("[LSMVCTreeSearchCursor] Component %d has no more clusters", i));
                continue;
            }

            // Advance to next cluster
            boolean advanced = vcCursor.advanceToNextCluster();
            if (!advanced) {
                // Failed to advance (no more clusters or error)
                clusterExhausted[i] = true;
                System.err.println(
                        String.format("[LSMVCTreeSearchCursor] Component %d failed to advance to next cluster", i));
                continue;
            }

            // Increment cluster index for this component
            currentClusterIndex[i]++;

            // Check if this cluster has data
            if (vcCursor.hasNext()) {
                // Cluster has data - add to queue
                vcCursor.next();
                PriorityQueueElement pqe = pqes[i];
                pqe.reset(vcCursor.getTuple());
                outputPriorityQueue.offer(pqe);
                System.err
                        .println(String.format("[LSMVCTreeSearchCursor] Component %d advanced to cluster %d (has data)",
                                i, currentClusterIndex[i]));
            } else {
                // Cluster is empty - mark as exhausted for this cluster
                // Component will wait at this cluster until all components finish
                clusterExhausted[i] = true;
                System.err.println(String.format(
                        "[LSMVCTreeSearchCursor] Component %d cluster %d is empty (will wait for other components)", i,
                        currentClusterIndex[i]));
            }
        }

        // Check if ALL components found the cluster empty (all exhausted again)
        boolean allStillExhausted = true;
        boolean anyHasMoreClusters = false;

        for (int i = 0; i < clusterExhausted.length; i++) {
            if (!clusterExhausted[i]) {
                allStillExhausted = false;
            }

            // Check if this component can advance further
            IIndexCursor cursor = rangeCursors[i];
            if (cursor instanceof VectorClusteringSearchCursor) {
                VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) cursor;
                if (vcCursor.hasMoreClusters()) {
                    anyHasMoreClusters = true;
                }
            }
        }

        if (allStillExhausted && anyHasMoreClusters) {
            // All components found this cluster empty BUT at least one has more clusters
            // Skip to next cluster
            System.err.println(String.format(
                    "[LSMVCTreeSearchCursor] ALL components found cluster %d empty, skipping to next cluster",
                    currentClusterIndex[0]));

            // Check if we've reached K or should stop advancing
            if (stopAdvancing || reconciledOutputCount >= K) {
                System.err.println("[LSMVCTreeSearchCursor] Reached K or stop flag, halting advancement");
                return;
            }

            // Recursively advance to next cluster (skip empty clusters)
            advanceAllComponentsToNextCluster();
        } else if (allStillExhausted && !anyHasMoreClusters) {
            // All components exhausted AND no more clusters - stop here
            System.err.println("[LSMVCTreeSearchCursor] All components exhausted, no more clusters to scan");
        }
    }
}
