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
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringBidirectionCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree.VectorClusteringTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

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
 *
 * Antimatter reconciliation follows LSMVCTreeSearchCursor pattern:
 * - Priority queue ordering ensures tuples with same key are adjacent (by D(x,C), pk, componentId)
 * - Lower componentId (newer component) comes first, so antimatter appears before matter
 * - Hold-and-check pattern: hold antimatter, check next tuple for same key, cancel if match
 */
public class LSMVCTreeBlockedCursor implements IIndexCursor {

    // Operation context
    private ILSMIndexOperationContext opCtx;
    private List<ILSMComponent> operationalComponents;

    // Bidirectional cursors - one per LSM component
    private VectorClusteringBidirectionCursor[] vcbCursors;
    private VectorClusteringTreeAccessor[] vcTreeAccessors;
    private int numComponents;

    // Direction contexts for unified antimatter reconciliation
    private DirectionContext rightCtx;
    private DirectionContext leftCtx;

    // Top-K window: max-heap by D(q,x) - peek() gives the termination threshold
    private PriorityQueue<ResultEntry> topKWindow;

    // Search parameters
    private double dqc; // D(q, C) - distance from query to centroid
    private int K;
    private int nprobe;
    private double epsilon;
    private double[] queryVector;
    private MultiComparator cmp;

    // Vector accessor for extracting vectors from tuples
    private IVectorBinaryAccessor vectorAccessor;

    // Distance function (from first cursor, not hardcoded)
    private IVectorDistanceFunction distanceFunction;

    // Cluster selection strategy (nprobe + DFS fallback)
    private IClusterSelectionStrategy clusterStrategy;

    // First component's search cursor (for query vector/distance function extraction and DFS fallback)
    private VectorClusteringSearchCursor firstSearchCursor;

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

        // Extract search parameters from predicate
        VectorPointPredicate vectorPred = (VectorPointPredicate) searchPred;
        this.K = vectorPred.getK();
        this.nprobe = vectorPred.getNprobe();
        this.epsilon = vectorPred.getEpsilon();

        // Get index access parameters
        IIndexAccessParameters iap = ((LSMVCTreeOpContext) opCtx).getIndexAccessParameters();

        // Initialize vector accessor from factory in parameters
        IVectorBinaryAccessorFactory vectorAccessorFactory =
                (IVectorBinaryAccessorFactory) iap.getParameters().get(HyracksConstants.VECTOR_QUERY);
        if (vectorAccessorFactory != null) {
            this.vectorAccessor = vectorAccessorFactory.createAccessor();
        }

        // Create cluster selection strategy (nprobe + DFS fallback)
        this.clusterStrategy = new NprobeClusterSelectionStrategy(nprobe, epsilon);

        // Initialize priority queues
        initializePriorityQueues();

        // Create accessors and bidirectional cursors for each component
        vcbCursors = new VectorClusteringBidirectionCursor[numComponents];
        vcTreeAccessors = new VectorClusteringTreeAccessor[numComponents];

        for (int i = 0; i < numComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            VectorClusteringTree vcTree = (VectorClusteringTree) component.getIndex();
            vcTreeAccessors[i] = (VectorClusteringTreeAccessor) vcTree.createAccessor(iap);
            vcbCursors[i] = (VectorClusteringBidirectionCursor) vcTreeAccessors[i].createBidirectionCursor();
        }

        // Following LSMVCTreeSearchCursor pattern:
        // Create a VectorClusteringSearchCursor for first component to extract query vector and distance function
        if (numComponents > 0) {
            ILSMComponent firstComponent = operationalComponents.get(0);
            VectorClusteringTree vcTree = (VectorClusteringTree) firstComponent.getIndex();

            // Create and open first search cursor (this triggers query vector and distance function extraction)
            this.firstSearchCursor = (VectorClusteringSearchCursor) vcTreeAccessors[0].createSearchCursor(false);
            vcTreeAccessors[0].search(firstSearchCursor, searchPred);

            // Get query vector and distance function from first cursor (like LSMVCTreeSearchCursor does)
            this.queryVector = firstSearchCursor.getQueryVector();
            this.distanceFunction = firstSearchCursor.getDistanceFunction();

            if (this.queryVector == null) {
                throw HyracksDataException
                        .create(new IllegalArgumentException("Query vector must be provided for optimized search"));
            }

            // Initialize cluster selection strategy with first component's tree
            clusterStrategy.initialize(vcTree, queryVector, distanceFunction, K);

            // Set first cursor for DFS fallback (like LSMVCTreeSearchCursor does)
            clusterStrategy.setFirstCursorForDFS(firstSearchCursor);

            System.err.println(String.format(
                    "[LSMVCTreeBlockedCursor] Initialized with queryVector dim=%d, K=%d, nprobe=%d, epsilon=%.4f",
                    queryVector.length, K, nprobe, epsilon));
        }

        // Get first cluster from strategy (level-wise selection)
        ClusterSearchResult firstCluster = clusterStrategy.getFirstCluster();

        if (firstCluster == null) {
            // No clusters available - empty tree
            System.err.println("[LSMVCTreeBlockedCursor] No clusters available (empty tree)");
            return;
        }

        // D(q, C) is the distance from query to closest centroid
        this.dqc = firstCluster.distance;

        System.err.println(String.format(
                "[LSMVCTreeBlockedCursor] First cluster from strategy: centroidId=%d, D(q,C)=%.4f, directoryPageId=%d, level-wise count=%d",
                firstCluster.centroidId, dqc, firstCluster.directoryPageId, clusterStrategy.getLevelWiseClusterCount()));

        // Perform the bidirectional search on first cluster
        openClusterAndSearch(firstCluster, queryVector, dqc);
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
        rightCtx.reset();
        leftCtx.reset();
        topKWindow.clear();

        // Open all VCB cursors for this cluster
        for (int i = 0; i < numComponents; i++) {
            vcbCursors[i].openCluster(cluster.directoryPageId, dqc);
        }

        // Seed the priority queues
        seedQueue(rightCtx);
        seedQueue(leftCtx);

        // Perform bidirectional search
        performBidirectionalSearch();
    }

    /**
     * Initialize the priority queues and direction contexts.
     */
    private void initializePriorityQueues() {
        int queueSize = Math.max(numComponents, 1);

        // Right context: D(x,C) ASC for increasing distance traversal
        PriorityQueueElement[] rightPqes = new PriorityQueueElement[numComponents];
        for (int i = 0; i < numComponents; i++) {
            rightPqes[i] = new PriorityQueueElement(i);
        }
        rightCtx = new DirectionContext(Direction.RIGHT,
                new PriorityQueue<>(queueSize, new DirectionalQueueComparator(true)), rightPqes);

        // Left context: D(x,C) DESC for decreasing distance traversal
        PriorityQueueElement[] leftPqes = new PriorityQueueElement[numComponents];
        for (int i = 0; i < numComponents; i++) {
            leftPqes[i] = new PriorityQueueElement(i);
        }
        leftCtx = new DirectionContext(Direction.LEFT,
                new PriorityQueue<>(queueSize, new DirectionalQueueComparator(false)), leftPqes);

        // Top-K window: max-heap by D(q,x)
        topKWindow = new PriorityQueue<>(Math.max(K, 1), (a, b) -> Double.compare(b.dqx, a.dqx));
    }

    /**
     * Seed a priority queue with the first tuple from each component.
     * IMPORTANT: We must copy the tuple because the cursor's internal buffer is reused.
     */
    private void seedQueue(DirectionContext ctx) throws HyracksDataException {
        for (int i = 0; i < numComponents; i++) {
            boolean hasNext = ctx.direction == Direction.RIGHT ? vcbCursors[i].hasNextRight()
                    : vcbCursors[i].hasNextLeft();

            if (hasNext) {
                if (ctx.direction == Direction.RIGHT) {
                    vcbCursors[i].nextRight();
                } else {
                    vcbCursors[i].nextLeft();
                }

                ITupleReference tuple = ctx.direction == Direction.RIGHT ? vcbCursors[i].getTupleRight()
                        : vcbCursors[i].getTupleLeft();

                // Copy tuple because cursor's internal buffer is reused on next()
                ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
                double dxc = extractDistanceToCentroid(tupleCopy);
                ctx.pqes[i].reset(tupleCopy, dxc, isAntimatter(tuple));
                ctx.queue.offer(ctx.pqes[i]);
            }
        }
    }

    /**
     * Perform bidirectional search with triangle inequality termination.
     */
    private void performBidirectionalSearch() throws HyracksDataException {
        while (!rightCtx.terminated || !leftCtx.terminated) {
            // Process right direction
            if (!rightCtx.terminated) {
                ITupleReference rightTuple = getNextValidTuple(rightCtx);
                if (rightTuple != null) {
                    double dqx = computeApproximateDistance(rightTuple);
                    addToTopKWindow(rightTuple, dqx);

                    // Check right termination: D(x',C) > max{D(q,x)} + D(q,C)
                    if (topKWindow.size() >= K && !rightCtx.queue.isEmpty()) {
                        double nextDxc = rightCtx.queue.peek().dxc;
                        double threshold = topKWindow.peek().dqx + dqc;
                        if (nextDxc > threshold) {
                            rightCtx.terminated = true;
                            System.err.println(String.format(
                                    "[LSMVCTreeBlockedCursor] Right terminated: nextDxc=%.4f > threshold=%.4f",
                                    nextDxc, threshold));
                        }
                    }
                } else {
                    rightCtx.terminated = true;
                }
            }

            // Process left direction
            if (!leftCtx.terminated) {
                ITupleReference leftTuple = getNextValidTuple(leftCtx);
                if (leftTuple != null) {
                    double dqx = computeApproximateDistance(leftTuple);
                    addToTopKWindow(leftTuple, dqx);

                    // Check left termination: D(x',C) < D(q,C) - max{D(q,x)}
                    if (topKWindow.size() >= K && !leftCtx.queue.isEmpty()) {
                        double nextDxc = leftCtx.queue.peek().dxc;
                        double threshold = dqc - topKWindow.peek().dqx;
                        if (nextDxc < threshold) {
                            leftCtx.terminated = true;
                            System.err.println(String.format(
                                    "[LSMVCTreeBlockedCursor] Left terminated: nextDxc=%.4f < threshold=%.4f",
                                    nextDxc, threshold));
                        }
                    }
                } else {
                    leftCtx.terminated = true;
                }
            }
        }

        System.err.println(String.format(
                "[LSMVCTreeBlockedCursor] Search complete: topK=%d, processed=%d, cancellations=%d",
                topKWindow.size(), totalTuplesProcessed, antimatterCancellations));
    }

    // ==================== Antimatter Reconciliation ====================

    /**
     * Get next valid tuple with antimatter reconciliation.
     * Follows the checkPriorityQueue() pattern from LSMVCTreeSearchCursor.
     *
     * The hold-and-check pattern:
     * 1. If top element is antimatter, hold it in outputElement and advance cursor
     * 2. Check next element - if same key, cancel both (antimatter reconciliation)
     * 3. If different key, discard the held antimatter and continue
     *
     * @param ctx the direction context (right or left)
     * @return next valid matter tuple, or null if queue exhausted
     */
    private ITupleReference getNextValidTuple(DirectionContext ctx) throws HyracksDataException {
        while (!ctx.queue.isEmpty() || ctx.needPush) {
            if (ctx.queue.isEmpty()) {
                // Queue empty but pending element exists - refill from cursor
                refillFromPending(ctx);
                continue;
            }

            PriorityQueueElement checkElement = ctx.queue.peek();

            if (ctx.outputElement == null) {
                // No pending element - process top of queue
                if (processTopElement(ctx, checkElement)) {
                    // Found valid matter tuple - return savedTuple (saved before advanceCursor)
                    ITupleReference result = ctx.savedTuple;
                    ctx.outputElement = null; // Clear to avoid false antimatter check on next call
                    ctx.savedTuple = null;
                    return result;
                }
            } else {
                // Have pending antimatter - check for cancellation
                processWithPendingElement(ctx, checkElement);
            }
        }
        return null; // Queue exhausted
    }

    /**
     * Process top element when no pending element exists.
     * Returns true if this is a valid matter tuple that should be returned.
     *
     * IMPORTANT: We must save the tuple BEFORE calling advanceCursor() because
     * advanceCursor() reuses the same PriorityQueueElement object (ctx.pqes[componentId]).
     * After advanceCursor(), the polled element's tuple field points to the NEXT tuple.
     */
    private boolean processTopElement(DirectionContext ctx, PriorityQueueElement checkElement)
            throws HyracksDataException {
        if (checkElement.isAntimatter) {
            // Antimatter - hold for cancellation check with next tuple
            ctx.outputElement = ctx.queue.poll();
            ctx.savedTuple = ctx.outputElement.tuple; // Save BEFORE advanceCursor modifies it
            ctx.needPush = true;
            advanceCursor(ctx, ctx.outputElement.componentId);
            return false; // Continue processing
        }

        // Valid matter tuple - save tuple BEFORE advanceCursor modifies the PQE
        ctx.outputElement = ctx.queue.poll();
        ctx.savedTuple = ctx.outputElement.tuple; // Save BEFORE advanceCursor modifies it
        int componentId = ctx.outputElement.componentId;
        advanceCursor(ctx, componentId);
        totalTuplesProcessed++;
        return true;
    }

    /**
     * Process queue element when we have a pending antimatter element.
     * Performs antimatter cancellation if keys match.
     * Uses ctx.savedTuple for comparison (saved before advanceCursor modified the PQE).
     */
    private void processWithPendingElement(DirectionContext ctx, PriorityQueueElement checkElement)
            throws HyracksDataException {
        if (compare(ctx.savedTuple, checkElement.tuple) == 0) {
            // Same key - antimatter cancellation
            performAntimatterCancellation(ctx, checkElement);
        } else {
            // Different key - refill pending element's cursor
            refillPendingElementCursor(ctx);
        }
    }

    /**
     * Perform antimatter cancellation - both matter and antimatter tuples are discarded.
     */
    private void performAntimatterCancellation(DirectionContext ctx, PriorityQueueElement checkElement)
            throws HyracksDataException {
        antimatterCancellations++;

        // Save componentId before any modifications (componentId is immutable but being safe)
        int pendingComponentId = ctx.outputElement.componentId;

        // Advance both cursors (don't lose remaining tuples!)
        PriorityQueueElement matchElem = ctx.queue.poll();
        advanceCursor(ctx, matchElem.componentId);
        advanceCursor(ctx, pendingComponentId);

        // Reset state - both tuples discarded
        ctx.needPush = false;
        ctx.outputElement = null;
        ctx.savedTuple = null;
    }

    /**
     * Refill the pending element's cursor and reset state.
     */
    private void refillPendingElementCursor(DirectionContext ctx) throws HyracksDataException {
        if (ctx.needPush) {
            advanceCursor(ctx, ctx.outputElement.componentId);
            ctx.needPush = false;
        }
        ctx.outputElement = null;
        ctx.savedTuple = null;
    }

    /**
     * Refill queue from pending element when queue is empty.
     */
    private void refillFromPending(DirectionContext ctx) throws HyracksDataException {
        advanceCursor(ctx, ctx.outputElement.componentId);
        ctx.needPush = false;
        ctx.outputElement = null;
        ctx.savedTuple = null;
    }

    /**
     * Advance cursor for a component and add next tuple to queue.
     * IMPORTANT: We must copy the tuple because the cursor's internal buffer is reused.
     */
    private void advanceCursor(DirectionContext ctx, int componentId) throws HyracksDataException {
        boolean hasNext = ctx.direction == Direction.RIGHT ? vcbCursors[componentId].hasNextRight()
                : vcbCursors[componentId].hasNextLeft();

        if (hasNext) {
            if (ctx.direction == Direction.RIGHT) {
                vcbCursors[componentId].nextRight();
            } else {
                vcbCursors[componentId].nextLeft();
            }

            ITupleReference tuple = ctx.direction == Direction.RIGHT ? vcbCursors[componentId].getTupleRight()
                    : vcbCursors[componentId].getTupleLeft();

            // Copy tuple because cursor's internal buffer is reused on next()
            ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
            double dxc = extractDistanceToCentroid(tupleCopy);
            ctx.pqes[componentId].reset(tupleCopy, dxc, isAntimatter(tuple));
            ctx.queue.offer(ctx.pqes[componentId]);
        }
    }

    /**
     * Compare two tuples for antimatter reconciliation.
     * Follows LSMVCTreeSearchCursor.compare() pattern:
     * - Compare distance (field 0), then remaining fields starting at field 2
     * - Skip centroid_id (field 1) since tuples from different clusters can coexist
     *
     * @return 0 if tuples have the same key (should cancel), non-zero otherwise
     */
    private int compare(ITupleReference tupleA, ITupleReference tupleB) throws HyracksDataException {
        // Compare field 0 (distance)
        int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0), tupleB.getFieldLength(0));
        if (result != 0) {
            return result;
        }

        // Compare remaining fields starting at field 2 (skip centroid_id at field 1)
        int numRemainingFields = cmp.getComparators().length - 2;
        for (int i = 0; i < numRemainingFields; i++) {
            int fieldIdx = 2 + i;
            int cmpIdx = 2 + i;

            // Check if field exists in tuple before comparing
            if (fieldIdx >= tupleA.getFieldCount() || fieldIdx >= tupleB.getFieldCount()) {
                break;
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

    /**
     * Add tuple to top-K window if it improves the results.
     * IMPORTANT: We must copy the tuple because the cursor's tuple buffer is reused.
     */
    private void addToTopKWindow(ITupleReference tuple, double dqx) throws HyracksDataException {
        if (topKWindow.size() < K) {
            // Copy tuple before storing - the original buffer will be reused
            ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
            topKWindow.offer(new ResultEntry(tupleCopy, dqx));
        } else if (dqx < topKWindow.peek().dqx) {
            topKWindow.poll(); // Remove worst
            // Copy tuple before storing - the original buffer will be reused
            ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
            topKWindow.offer(new ResultEntry(tupleCopy, dqx));
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
     * - Field 1: centroid_id (int)
     * - Field 2: vector (stored in format handled by IVectorBinaryAccessor)
     * - Field 3: primary_key
     */
    private double computeApproximateDistance(ITupleReference tuple) throws HyracksDataException {
        // Extract vector from field 2 using the vector accessor
        int vectorFieldIndex = 2;
        byte[] data = tuple.getFieldData(vectorFieldIndex);
        int offset = tuple.getFieldStart(vectorFieldIndex);
        int length = tuple.getFieldLength(vectorFieldIndex);

        vectorAccessor.reset(data, offset, length);
        double[] tupleVector = vectorAccessor.getVector();

        // Compute distance using the configured distance function (from first cursor)
        return distanceFunction.apply(queryVector, tupleVector);
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
            // Close bidirectional cursors
            for (int i = 0; i < numComponents; i++) {
                if (vcbCursors[i] != null) {
                    vcbCursors[i].close();
                }
            }
            // Close first search cursor (used for query vector/distance function extraction and DFS fallback)
            if (firstSearchCursor != null) {
                firstSearchCursor.close();
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
     * Direction enum for bidirectional search.
     */
    private enum Direction {
        RIGHT, // Increasing D(x,C) - tuples farther from centroid
        LEFT // Decreasing D(x,C) - tuples closer to centroid
    }

    /**
     * Context for a search direction (right or left).
     * Encapsulates state needed for antimatter reconciliation.
     */
    private static class DirectionContext {
        final Direction direction;
        final PriorityQueue<PriorityQueueElement> queue;
        final PriorityQueueElement[] pqes;
        PriorityQueueElement outputElement;
        ITupleReference savedTuple; // Saved before advanceCursor() modifies the PQE
        boolean needPush;
        boolean terminated;

        DirectionContext(Direction direction, PriorityQueue<PriorityQueueElement> queue, PriorityQueueElement[] pqes) {
            this.direction = direction;
            this.queue = queue;
            this.pqes = pqes;
        }

        void reset() {
            queue.clear();
            outputElement = null;
            savedTuple = null;
            needPush = false;
            terminated = false;
        }
    }

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
     * Unified comparator for both right and left queues.
     * Follows LSMVCTreeSearchCursor.VectorPriorityQueueComparator pattern.
     *
     * Comparison order:
     * 1. D(x,C) - ascending for right queue, descending for left queue
     * 2. Remaining fields starting at field 2 (skip centroid_id at field 1)
     * 3. Component ID ascending (newer component first for antimatter reconciliation)
     */
    private class DirectionalQueueComparator implements Comparator<PriorityQueueElement> {
        private final boolean ascending; // true for right queue (D(x,C) ASC), false for left (DESC)

        DirectionalQueueComparator(boolean ascending) {
            this.ascending = ascending;
        }

        @Override
        public int compare(PriorityQueueElement a, PriorityQueueElement b) {
            // D(x,C) comparison - direction-specific
            int result = ascending ? Double.compare(a.dxc, b.dxc) : Double.compare(b.dxc, a.dxc);
            if (result != 0) {
                return result;
            }

            // Compare remaining fields starting at field 2 (skip centroid_id)
            // Following LSMVCTreeSearchCursor.VectorPriorityQueueComparator pattern
            try {
                ITupleReference tupleA = a.tuple;
                ITupleReference tupleB = b.tuple;

                int numRemainingFields = cmp.getComparators().length - 2;
                for (int i = 0; i < numRemainingFields; i++) {
                    int fieldIdx = 2 + i;
                    int cmpIdx = 2 + i;

                    // Check if field exists in tuple before comparing
                    if (fieldIdx >= tupleA.getFieldCount() || fieldIdx >= tupleB.getFieldCount()) {
                        break;
                    }

                    result = cmp.getComparators()[cmpIdx].compare(tupleA.getFieldData(fieldIdx),
                            tupleA.getFieldStart(fieldIdx), tupleA.getFieldLength(fieldIdx),
                            tupleB.getFieldData(fieldIdx), tupleB.getFieldStart(fieldIdx),
                            tupleB.getFieldLength(fieldIdx));
                    if (result != 0) {
                        return result;
                    }
                }
            } catch (HyracksDataException e) {
                throw new RuntimeException(e);
            }

            // Component ID ascending (newer component first for antimatter reconciliation)
            return Integer.compare(a.componentId, b.componentId);
        }
    }
}
