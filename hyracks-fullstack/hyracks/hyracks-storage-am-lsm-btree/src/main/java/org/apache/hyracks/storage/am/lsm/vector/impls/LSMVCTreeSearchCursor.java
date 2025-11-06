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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
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
 * - For vector index: simplified sequential iteration (no priority queue merging needed for now)
 */
public class LSMVCTreeSearchCursor extends LSMIndexSearchCursor {

    // Accessor array for each component's VCTree
    private VectorClusteringTreeAccessor[] vcTreeAccessors;

    // Track component types to detect memory → disk transitions
    protected boolean[] isMemoryComponent;

    // Store search predicate for component switching
    private ISearchPredicate searchPredicate;

    // Current component being iterated
    private int currentComponentIndex;
    private IIndexCursor currentComponentCursor;

    public LSMVCTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false, NoOpIndexCursorStats.INSTANCE);
    }

    public LSMVCTreeSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples,
            IIndexCursorStats stats) {
        super(opCtx, returnDeletedTuples, stats);
        this.currentComponentIndex = 0;
        this.currentComponentCursor = null;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // Get LSM-specific initial state
        LSMVCTreeCursorInitialState lsmInitialState = (LSMVCTreeCursorInitialState) initialState;

        // Save search predicate for component switching
        this.searchPredicate = searchPred;

        // Set up comparator and operational components
        cmp = lsmInitialState.getOriginalKeyComparator();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();

        // For vector index, we don't need mutable component special handling initially
        includeMutableComponent = false;

        int numVCTrees = operationalComponents.size();

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

        // Initialize sequential iteration
        currentComponentIndex = 0;
        currentComponentCursor = (rangeCursors.length > 0) ? rangeCursors[0] : null;

        // Note: Priority queue setup is kept for compatibility with base class,
        // but not used in simplified sequential iteration
        try {
            setPriorityQueueComparator();
            initPriorityQueue();
        } catch (Throwable th) { // NOSONAR Must catch all
            IndexCursorUtils.close(rangeCursors, th);
            throw HyracksDataException.create(th);
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
     */
    protected IIndexCursor createCursor(LSMComponentType type, VectorClusteringTreeAccessor accessor)
            throws HyracksDataException {
        return accessor.createSearchCursor(false);
    }

    @Override
    public void doClose() throws HyracksDataException {
        super.doClose();
        // Additional cleanup specific to vector index if needed
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        hasNextCallCount++;
        checkPriorityQueue();
        // Check sequential iteration state instead of priority queue
        return currentComponentCursor != null && currentComponentCursor.hasNext();
    }

    @Override
    public void doNext() throws HyracksDataException {
        // Simplified version: just advance current component cursor
        // No priority queue sorting needed - return all tuples from all components
        if (currentComponentCursor != null && currentComponentCursor.hasNext()) {
            currentComponentCursor.next();
        }
    }

    @Override
    protected void checkPriorityQueue() throws HyracksDataException {
        // Every SWITCH_COMPONENT_CYCLE calls, check if memory components need to be swapped with disk components
        // This handles the case where a memory component is flushed to disk while the cursor is active
        if (hasNextCallCount >= SWITCH_COMPONENT_CYCLE) {
            replaceMemoryComponentWithDiskComponentIfNeeded();
            hasNextCallCount = 0;
        }

        // Simplified version: sequential iteration through components
        // No priority queue sorting - just return all tuples from all components in order
        // TODO: Future optimization - use priority queue to merge results sorted by distance
        //       for more efficient top-k selection at higher layers

        // Find next component with results
        while (currentComponentIndex < rangeCursors.length) {
            if (currentComponentCursor != null && currentComponentCursor.hasNext()) {
                // Current component still has results
                return;
            }

            // Current component exhausted, move to next
            currentComponentIndex++;
            if (currentComponentIndex < rangeCursors.length) {
                currentComponentCursor = rangeCursors[currentComponentIndex];
            } else {
                currentComponentCursor = null;
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
        // For vector index: sort by distance (field 0 in tuple)
        // Tuple format: <distance, cosine, embedding, pk>
        if (pqCmp == null || pqCmp.getMultiComparator() != cmp) {
            pqCmp = new PriorityQueueComparator(cmp);
        }
    }

    @Override
    public ITupleReference doGetTuple() {
        // Return tuple from current component cursor
        if (currentComponentCursor != null) {
            return currentComponentCursor.getTuple();
        }
        return null;
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB) throws HyracksDataException {
        // For vector index: compare by distance (first field)
        // This ensures results are sorted by distance (nearest first)
        return cmp.compare(tupleA, tupleB);
    }

    @Override
    protected boolean isDeleted(PriorityQueueElement element) {
        // Vector index doesn't have delete markers in tuples
        // (deletions handled at primary index level)
        return false;
    }
}