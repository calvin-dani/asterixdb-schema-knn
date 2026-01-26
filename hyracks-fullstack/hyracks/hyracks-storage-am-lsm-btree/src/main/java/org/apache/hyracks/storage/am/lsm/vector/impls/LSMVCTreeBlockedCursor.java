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

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringBidirectionCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree.VectorClusteringTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;

/**
 * LSM blocked cursor for optimized vector search using triangle inequality.
 *
 * This cursor performs bidirectional search from the pivot point (where D(x,C) ≈ D(q,C))
 * and uses triangle inequality for early termination:
 * - Right terminates when: D(x',C) > max{D(q,x)} + D(q,C)
 * - Left terminates when: D(x',C) < D(q,C) - max{D(q,x)}
 *
 * "Blocked" means all search work is done in open(), and results are stored in topKWindow.
 * Calls to hasNext()/next()/getTuple() simply drain the window.
 *
 * Uses three priority queues:
 * 1. rightQueue: <D(x,C) ASC, pk, component_id ASC> - for right direction + antimatter reconciliation
 * 2. leftQueue: <D(x,C) DESC, pk, component_id ASC> - for left direction + antimatter reconciliation
 * 3. topKWindow: max-heap by D(q,x) - stores top-K results, peek() provides termination threshold
 */
public class LSMVCTreeBlockedCursor implements IIndexCursor {

    // Operation context
    private ILSMIndexOperationContext opCtx;
    private List<ILSMComponent> operationalComponents;

    // Bidirectional cursors - one per LSM component
    private VectorClusteringBidirectionCursor[] vcbCursors;
    private VectorClusteringTreeAccessor[] vcTreeAccessors;
    private int numComponents;

    // Right direction priority queue: <D(x,C) ASC, pk, component_id ASC>
    private PriorityQueue<PriorityQueueElement> rightQueue;
    private PriorityQueueElement[] rightPqes;
    private PriorityQueueElement rightOutputElement;
    private boolean rightNeedPush;
    private boolean rightTerminated;

    // Left direction priority queue: <D(x,C) DESC, pk, component_id ASC>
    private PriorityQueue<PriorityQueueElement> leftQueue;
    private PriorityQueueElement[] leftPqes;
    private PriorityQueueElement leftOutputElement;
    private boolean leftNeedPush;
    private boolean leftTerminated;

    // Top-K window: max-heap by D(q,x) - peek() gives the termination threshold
    private PriorityQueue<ResultEntry> topKWindow;

    // Search parameters
    private double dqc; // D(q, C) - distance from query to centroid
    private int K;
    private double[] queryVector;
    private MultiComparator cmp;

    // Vector accessor for extracting vectors from tuples
    private IVectorBinaryAccessor vectorAccessor;

    // Cursor state
    private boolean isOpen;
    private ResultEntry currentResult;

    // Statistics
    private int totalTuplesProcessed;
    private int antimatterCancellations;

    public LSMVCTreeBlockedCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
        this.isOpen = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.isOpen = true;
        this.totalTuplesProcessed = 0;
        this.antimatterCancellations = 0;

        // Get initial state
        LSMVCTreeCursorInitialState lsmInitialState = (LSMVCTreeCursorInitialState) initialState;
        this.cmp = lsmInitialState.getOriginalKeyComparator();
        this.operationalComponents = lsmInitialState.getOperationalComponents();
        this.numComponents = operationalComponents.size();

        // Extract K from predicate
        VectorPointPredicate vectorPred = (VectorPointPredicate) searchPred;
        this.K = vectorPred.getK();

        // Initialize vector accessor from factory in parameters
        IVectorBinaryAccessorFactory vectorAccessorFactory = (IVectorBinaryAccessorFactory) ((LSMVCTreeOpContext) opCtx)
                .getIndexAccessParameters().getParameters().get(HyracksConstants.VECTOR_QUERY);
        if (vectorAccessorFactory != null) {
            this.vectorAccessor = vectorAccessorFactory.createAccessor();
        }

        // Initialize priority queues
        initializePriorityQueues();

        // Create accessors and cursors for each component
        vcbCursors = new VectorClusteringBidirectionCursor[numComponents];
        vcTreeAccessors = new VectorClusteringTreeAccessor[numComponents];

        for (int i = 0; i < numComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            VectorClusteringTree vcTree = (VectorClusteringTree) component.getIndex();
            vcTreeAccessors[i] = (VectorClusteringTreeAccessor) vcTree.createAccessor(
                    ((LSMVCTreeOpContext) opCtx).getIndexAccessParameters());
            vcbCursors[i] = (VectorClusteringBidirectionCursor) vcTreeAccessors[i].createBidirectionCursor();
        }

        // Find closest cluster using first component's tree
        VectorClusteringTree firstTree = (VectorClusteringTree) operationalComponents.get(0).getIndex();
        // TODO: Get query vector from predicate/initial state
        // For now, we assume the cluster finding is done externally and passed via ClusterSearchResult

        // The actual search is performed in openCluster() which should be called after this
    }

    /**
     * Open all cursors for a specific cluster and perform the bidirectional search.
     * This is the main entry point for optimized search.
     *
     * @param cluster the cluster to search (contains centroid and directoryPageId)
     * @param queryVector the query vector
     * @param dqc D(q, C) - distance from query to centroid
     */
    public void openClusterAndSearch(ClusterSearchResult cluster, double[] queryVector, double dqc)
            throws HyracksDataException {
        this.queryVector = queryVector;
        this.dqc = dqc;

        // Reset state
        rightQueue.clear();
        leftQueue.clear();
        topKWindow.clear();
        rightTerminated = false;
        leftTerminated = false;
        rightOutputElement = null;
        leftOutputElement = null;
        rightNeedPush = false;
        leftNeedPush = false;

        // Open all VCB cursors for this cluster
        for (int i = 0; i < numComponents; i++) {
            vcbCursors[i].openCluster(cluster.directoryPageId, dqc);
        }

        // Seed the priority queues
        seedRightQueue();
        seedLeftQueue();

        // Perform bidirectional search
        performBidirectionalSearch();
    }

    /**
     * Initialize the three priority queues.
     */
    private void initializePriorityQueues() {
        // Right queue: D(x,C) ASC, pk ASC, component_id ASC
        rightQueue = new PriorityQueue<>(Math.max(numComponents, 1), new RightQueueComparator());
        rightPqes = new PriorityQueueElement[numComponents];
        for (int i = 0; i < numComponents; i++) {
            rightPqes[i] = new PriorityQueueElement(i);
        }

        // Left queue: D(x,C) DESC, pk ASC, component_id ASC
        leftQueue = new PriorityQueue<>(Math.max(numComponents, 1), new LeftQueueComparator());
        leftPqes = new PriorityQueueElement[numComponents];
        for (int i = 0; i < numComponents; i++) {
            leftPqes[i] = new PriorityQueueElement(i);
        }

        // Top-K window: max-heap by D(q,x)
        topKWindow = new PriorityQueue<>(Math.max(K, 1), (a, b) -> Double.compare(b.dqx, a.dqx));
    }

    /**
     * Seed the right priority queue with first right tuple from each component.
     */
    private void seedRightQueue() throws HyracksDataException {
        for (int i = 0; i < numComponents; i++) {
            if (vcbCursors[i].hasNextRight()) {
                vcbCursors[i].nextRight();
                ITupleReference tuple = vcbCursors[i].getTupleRight();
                double dxc = extractDistanceToCentroid(tuple);
                rightPqes[i].reset(tuple, dxc, isAntimatter(tuple));
                rightQueue.offer(rightPqes[i]);
            }
        }
    }

    /**
     * Seed the left priority queue with first left tuple from each component.
     */
    private void seedLeftQueue() throws HyracksDataException {
        for (int i = 0; i < numComponents; i++) {
            if (vcbCursors[i].hasNextLeft()) {
                vcbCursors[i].nextLeft();
                ITupleReference tuple = vcbCursors[i].getTupleLeft();
                double dxc = extractDistanceToCentroid(tuple);
                leftPqes[i].reset(tuple, dxc, isAntimatter(tuple));
                leftQueue.offer(leftPqes[i]);
            }
        }
    }

    /**
     * Perform bidirectional search with triangle inequality termination.
     */
    private void performBidirectionalSearch() throws HyracksDataException {
        while (!rightTerminated || !leftTerminated) {
            // Process right direction
            if (!rightTerminated) {
                ITupleReference rightTuple = getNextValidTupleFromRight();
                if (rightTuple != null) {
                    double dqx = computeApproximateDistance(rightTuple);
                    addToTopKWindow(rightTuple, dqx);

                    // Check right termination
                    if (topKWindow.size() >= K && !rightQueue.isEmpty()) {
                        double nextDxc = rightQueue.peek().dxc;
                        double threshold = topKWindow.peek().dqx + dqc;
                        if (nextDxc > threshold) {
                            rightTerminated = true;
                            System.err.println(String.format(
                                    "[LSMVCTreeBlockedCursor] Right terminated: nextDxc=%.4f > threshold=%.4f",
                                    nextDxc, threshold));
                        }
                    }
                } else {
                    rightTerminated = true;
                }
            }

            // Process left direction
            if (!leftTerminated) {
                ITupleReference leftTuple = getNextValidTupleFromLeft();
                if (leftTuple != null) {
                    double dqx = computeApproximateDistance(leftTuple);
                    addToTopKWindow(leftTuple, dqx);

                    // Check left termination
                    if (topKWindow.size() >= K && !leftQueue.isEmpty()) {
                        double nextDxc = leftQueue.peek().dxc;
                        double threshold = dqc - topKWindow.peek().dqx;
                        if (nextDxc < threshold) {
                            leftTerminated = true;
                            System.err.println(String.format(
                                    "[LSMVCTreeBlockedCursor] Left terminated: nextDxc=%.4f < threshold=%.4f",
                                    nextDxc, threshold));
                        }
                    }
                } else {
                    leftTerminated = true;
                }
            }
        }

        System.err.println(String.format(
                "[LSMVCTreeBlockedCursor] Search complete: topK=%d, processed=%d, cancellations=%d",
                topKWindow.size(), totalTuplesProcessed, antimatterCancellations));
    }

    /**
     * Get next valid tuple from right queue with antimatter reconciliation.
     * Uses the hold-and-check pattern from LSMVCTreeSearchCursor.checkPriorityQueue().
     */
    private ITupleReference getNextValidTupleFromRight() throws HyracksDataException {
        while (!rightQueue.isEmpty() || rightNeedPush) {
            if (rightQueue.isEmpty()) {
                refillRightFromPending();
                continue;
            }

            PriorityQueueElement checkElement = rightQueue.peek();

            if (rightOutputElement == null) {
                if (checkElement.isAntimatter) {
                    // Hold antimatter for cancellation check
                    rightOutputElement = rightQueue.poll();
                    rightNeedPush = true;
                    advanceRightCursor(rightOutputElement.componentId);
                } else {
                    // Valid matter tuple
                    PriorityQueueElement elem = rightQueue.poll();
                    advanceRightCursor(elem.componentId);
                    totalTuplesProcessed++;
                    return elem.tuple;
                }
            } else {
                // Have pending antimatter - check for cancellation
                if (samePrimaryKey(rightOutputElement.tuple, checkElement.tuple)) {
                    // Same PK - cancel both
                    PriorityQueueElement matchElem = rightQueue.poll();
                    advanceRightCursor(matchElem.componentId);
                    advanceRightCursor(rightOutputElement.componentId);
                    rightNeedPush = false;
                    rightOutputElement = null;
                    antimatterCancellations++;
                } else {
                    // Different PK - clear pending
                    rightNeedPush = false;
                    rightOutputElement = null;
                }
            }
        }
        return null; // Queue exhausted
    }

    /**
     * Get next valid tuple from left queue with antimatter reconciliation.
     */
    private ITupleReference getNextValidTupleFromLeft() throws HyracksDataException {
        while (!leftQueue.isEmpty() || leftNeedPush) {
            if (leftQueue.isEmpty()) {
                refillLeftFromPending();
                continue;
            }

            PriorityQueueElement checkElement = leftQueue.peek();

            if (leftOutputElement == null) {
                if (checkElement.isAntimatter) {
                    // Hold antimatter for cancellation check
                    leftOutputElement = leftQueue.poll();
                    leftNeedPush = true;
                    advanceLeftCursor(leftOutputElement.componentId);
                } else {
                    // Valid matter tuple
                    PriorityQueueElement elem = leftQueue.poll();
                    advanceLeftCursor(elem.componentId);
                    totalTuplesProcessed++;
                    return elem.tuple;
                }
            } else {
                // Have pending antimatter - check for cancellation
                if (samePrimaryKey(leftOutputElement.tuple, checkElement.tuple)) {
                    // Same PK - cancel both
                    PriorityQueueElement matchElem = leftQueue.poll();
                    advanceLeftCursor(matchElem.componentId);
                    advanceLeftCursor(leftOutputElement.componentId);
                    leftNeedPush = false;
                    leftOutputElement = null;
                    antimatterCancellations++;
                } else {
                    // Different PK - clear pending
                    leftNeedPush = false;
                    leftOutputElement = null;
                }
            }
        }
        return null; // Queue exhausted
    }

    private void refillRightFromPending() throws HyracksDataException {
        advanceRightCursor(rightOutputElement.componentId);
        rightNeedPush = false;
        rightOutputElement = null;
    }

    private void refillLeftFromPending() throws HyracksDataException {
        advanceLeftCursor(leftOutputElement.componentId);
        leftNeedPush = false;
        leftOutputElement = null;
    }

    private void advanceRightCursor(int componentId) throws HyracksDataException {
        if (vcbCursors[componentId].hasNextRight()) {
            vcbCursors[componentId].nextRight();
            ITupleReference tuple = vcbCursors[componentId].getTupleRight();
            double dxc = extractDistanceToCentroid(tuple);
            rightPqes[componentId].reset(tuple, dxc, isAntimatter(tuple));
            rightQueue.offer(rightPqes[componentId]);
        }
    }

    private void advanceLeftCursor(int componentId) throws HyracksDataException {
        if (vcbCursors[componentId].hasNextLeft()) {
            vcbCursors[componentId].nextLeft();
            ITupleReference tuple = vcbCursors[componentId].getTupleLeft();
            double dxc = extractDistanceToCentroid(tuple);
            leftPqes[componentId].reset(tuple, dxc, isAntimatter(tuple));
            leftQueue.offer(leftPqes[componentId]);
        }
    }

    /**
     * Add tuple to top-K window if it improves the results.
     */
    private void addToTopKWindow(ITupleReference tuple, double dqx) {
        if (topKWindow.size() < K) {
            topKWindow.offer(new ResultEntry(tuple, dqx));
        } else if (dqx < topKWindow.peek().dqx) {
            topKWindow.poll(); // Remove worst
            topKWindow.offer(new ResultEntry(tuple, dqx));
        }
        // else: tuple is worse than all in window, reject
    }

    /**
     * Extract D(x, C) from tuple. First field is distance_to_centroid.
     */
    private double extractDistanceToCentroid(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(0);
        int offset = tuple.getFieldStart(0);
        return DoublePointable.getDouble(data, offset);
    }

    /**
     * Compute distance D(q, x) between query vector and tuple's vector.
     * Uses IVectorBinaryAccessor to extract the vector from the tuple.
     *
     * Tuple format:
     * - Field 0: distance_to_centroid (double)
     * - Field 1: vector (stored in format handled by IVectorBinaryAccessor)
     * - Field 2+: include_fields, primary_key
     */
    private double computeApproximateDistance(ITupleReference tuple) throws HyracksDataException {
        // Extract vector from field 1 using the vector accessor
        int vectorFieldIndex = 1;
        byte[] data = tuple.getFieldData(vectorFieldIndex);
        int offset = tuple.getFieldStart(vectorFieldIndex);
        int length = tuple.getFieldLength(vectorFieldIndex);

        vectorAccessor.reset(data, offset, length);
        double[] tupleVector = vectorAccessor.getVector();

        // Compute actual Euclidean distance
        return computeEuclideanDistance(queryVector, tupleVector);
    }

    /**
     * Compute Euclidean distance between two vectors.
     */
    private double computeEuclideanDistance(double[] v1, double[] v2) {
        if (v1 == null || v2 == null || v1.length != v2.length) {
            throw new IllegalArgumentException("Vectors must have same dimension");
        }

        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Check if tuple is antimatter.
     */
    private boolean isAntimatter(ITupleReference tuple) {
        if (tuple instanceof ILSMTreeTupleReference) {
            return ((ILSMTreeTupleReference) tuple).isAntimatter();
        }
        return false;
    }

    /**
     * Check if two tuples have the same primary key.
     * Primary key is at field index 2 (after distance and centroidId).
     */
    private boolean samePrimaryKey(ITupleReference tuple1, ITupleReference tuple2) throws HyracksDataException {
        int pkFieldIndex = 2;
        return cmp.getComparators()[pkFieldIndex].compare(
                tuple1.getFieldData(pkFieldIndex), tuple1.getFieldStart(pkFieldIndex), tuple1.getFieldLength(pkFieldIndex),
                tuple2.getFieldData(pkFieldIndex), tuple2.getFieldStart(pkFieldIndex), tuple2.getFieldLength(pkFieldIndex)
        ) == 0;
    }

    // ==================== IIndexCursor Interface ====================

    @Override
    public boolean hasNext() throws HyracksDataException {
        return !topKWindow.isEmpty();
    }

    @Override
    public void next() throws HyracksDataException {
        if (!hasNext()) {
            throw HyracksDataException.create(new IllegalStateException("No more tuples"));
        }
        currentResult = topKWindow.poll();
    }

    @Override
    public ITupleReference getTuple() {
        return currentResult != null ? currentResult.tuple : null;
    }

    @Override
    public void close() throws HyracksDataException {
        if (isOpen) {
            for (int i = 0; i < numComponents; i++) {
                if (vcbCursors[i] != null) {
                    vcbCursors[i].close();
                }
            }
        }
        isOpen = false;
    }

    @Override
    public void destroy() throws HyracksDataException {
        close();
    }

    // ==================== Inner Classes ====================

    /**
     * Priority queue element holding tuple, distance, and component info.
     */
    private static class PriorityQueueElement {
        int componentId;
        ITupleReference tuple;
        double dxc; // D(x, C)
        boolean isAntimatter;

        PriorityQueueElement(int componentId) {
            this.componentId = componentId;
        }

        void reset(ITupleReference tuple, double dxc, boolean isAntimatter) {
            this.tuple = tuple;
            this.dxc = dxc;
            this.isAntimatter = isAntimatter;
        }
    }

    /**
     * Result entry for top-K window.
     */
    private static class ResultEntry {
        ITupleReference tuple;
        double dqx; // D(q, x) approximate

        ResultEntry(ITupleReference tuple, double dqx) {
            this.tuple = tuple;
            this.dqx = dqx;
        }
    }

    /**
     * Comparator for right queue: D(x,C) ASC, pk, component_id ASC
     */
    private class RightQueueComparator implements Comparator<PriorityQueueElement> {
        @Override
        public int compare(PriorityQueueElement a, PriorityQueueElement b) {
            // D(x,C) ascending
            int result = Double.compare(a.dxc, b.dxc);
            if (result != 0) return result;

            // Primary key comparison (field 2)
            try {
                result = cmp.getComparators()[2].compare(
                        a.tuple.getFieldData(2), a.tuple.getFieldStart(2), a.tuple.getFieldLength(2),
                        b.tuple.getFieldData(2), b.tuple.getFieldStart(2), b.tuple.getFieldLength(2));
                if (result != 0) return result;
            } catch (HyracksDataException e) {
                throw new RuntimeException(e);
            }

            // Component ID ascending (newer component first)
            return Integer.compare(a.componentId, b.componentId);
        }
    }

    /**
     * Comparator for left queue: D(x,C) DESC, pk, component_id ASC
     */
    private class LeftQueueComparator implements Comparator<PriorityQueueElement> {
        @Override
        public int compare(PriorityQueueElement a, PriorityQueueElement b) {
            // D(x,C) descending
            int result = Double.compare(b.dxc, a.dxc);
            if (result != 0) return result;

            // Primary key comparison (field 2)
            try {
                result = cmp.getComparators()[2].compare(
                        a.tuple.getFieldData(2), a.tuple.getFieldStart(2), a.tuple.getFieldLength(2),
                        b.tuple.getFieldData(2), b.tuple.getFieldStart(2), b.tuple.getFieldLength(2));
                if (result != 0) return result;
            } catch (HyracksDataException e) {
                throw new RuntimeException(e);
            }

            // Component ID ascending (newer component first)
            return Integer.compare(a.componentId, b.componentId);
        }
    }
}
