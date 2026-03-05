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
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.tuples.ReferenceFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVectorQuantizer;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringBidirectionCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree.VectorClusteringTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VectorSearchPredicate;
import org.apache.hyracks.storage.am.vector.utils.VCTreeDataTupleConstants;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final Logger LOGGER = LogManager.getLogger();

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
    /** Number of candidates to send to PK for reranking (hardcoded 2*K for now). */
    private int candidateLimit;
    private int nprobe;
    private double epsilon;
    private double[] queryVector;
    private MultiComparator cmp;

    // Vector accessor for extracting vectors from tuples
    private IVectorBinaryAccessor vectorAccessor;

    // Distance function (from first cursor, not hardcoded)
    private IVectorDistanceFunction distanceFunction;

    // Quantization state (propagated from first search cursor)
    private double[] quantizedQueryVector;
    private IVectorQuantizer quantizer;

    // Cluster selection strategy (nprobe + DFS fallback)
    private IClusterSelectionStrategy clusterStrategy;

    // First component's search cursor (for query vector/distance function extraction and DFS fallback)
    private VectorClusteringSearchCursor firstSearchCursor;

    // Per-component resolver cursors for directory page resolution.
    // Each LSM component may have different directory page IDs for the same cluster
    // (memory components use centroidDirPageMap, disk components have their own leaf page layout).
    // The cluster strategy navigates only component 0's tree, so its directoryPageId may be
    // invalid for other components. These cursors resolve the correct local directoryPageId.
    private VectorClusteringSearchCursor[] resolverCursors;

    // Cursor state
    private boolean isOpen;
    private ResultEntry currentResult;

    // Multi-cluster probing state (following LSMVCTreeSearchCursor pattern)
    private int clustersExplored;
    private boolean stopAdvancing;

    // Field index where primary keys start in the data tuple
    // Non-quantized format: 2 (distance, centroidId, PK...)
    // Quantized format: 4 (distance, centroidId, quantized_distance, quantized_embedding, PK...)
    private int pkStartField;

    // Tuple filter for INCLUDE field predicates (e.g., year > 2000)
    // When set, only tuples passing this filter are added to topKWindow and counted toward K
    private ITupleFilter tupleFilter;

    // Wrapper to convert ITupleReference to IFrameTupleReference for filter evaluation
    private ReferenceFrameTupleReference referenceFilterTuple;

    // Statistics
    private int totalTuplesProcessed;
    private int antimatterCancellations;
    private int tuplesFilteredOut;
    private int validTuplesFromCurrentCluster; // Valid tuples from current cluster (for empty-cluster nprobe)

    public LSMVCTreeBlockedCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
        this.isOpen = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.isOpen = true;
        this.totalTuplesProcessed = 0;
        this.antimatterCancellations = 0;
        this.tuplesFilteredOut = 0;
        this.validTuplesFromCurrentCluster = 0;
        this.addToTopKWindowCallCount = 0;

        // Get initial state
        LSMVCTreeCursorInitialState lsmInitialState = (LSMVCTreeCursorInitialState) initialState;
        this.cmp = lsmInitialState.getOriginalKeyComparator();
        this.operationalComponents = lsmInitialState.getOperationalComponents();
        this.numComponents = operationalComponents.size();

        // Extract search parameters from predicate
        VectorSearchPredicate vectorPred = (VectorSearchPredicate) searchPred;
        this.K = vectorPred.getK();
        int mult = vectorPred.getKMultiplier();
        this.candidateLimit = K * Math.max(1, mult); // Send K*kMultiplier to PK for reranking
        this.nprobe = vectorPred.getNprobe();
        this.epsilon = vectorPred.getEpsilon();
        this.pkStartField = vectorPred.getPkStartField();

        // Extract tuple filter from search predicate for INCLUDE field predicates
        this.tupleFilter = vectorPred.getTupleFilter();
        if (this.tupleFilter != null) {
            this.referenceFilterTuple = new ReferenceFrameTupleReference();
            LOGGER.log(Level.TRACE,
                    "[LSMVCTreeBlockedCursor] Tuple filter is SET - will filter INCLUDE field predicates");
        } else {
            LOGGER.log(Level.TRACE, "[LSMVCTreeBlockedCursor] Tuple filter is NULL - no INCLUDE field filtering");
        }

        // Get index access parameters
        IIndexAccessParameters iap = ((LSMVCTreeOpContext) opCtx).getIndexAccessParameters();

        // Initialize vector accessor from factory in parameters
        IVectorBinaryAccessorFactory vectorAccessorFactory =
                (IVectorBinaryAccessorFactory) iap.getParameters().get(HyracksConstants.VECTOR_QUERY);
        if (vectorAccessorFactory != null) {
            this.vectorAccessor = vectorAccessorFactory.createAccessor();
        }

        // Create cluster selection strategy
        Boolean useSequentialScan = (Boolean) iap.getParameters().get(HyracksConstants.USE_SEQUENTIAL_SCAN);
        if (Boolean.TRUE.equals(useSequentialScan)) {
            this.clusterStrategy = new SequentialClusterSelectionStrategy();
        } else {
            this.clusterStrategy = new NprobeClusterSelectionStrategy(nprobe, epsilon);
        }

        // Initialize priority queues
        initializePriorityQueues();

        // Create accessors, bidirectional cursors, and resolver cursors for each component
        vcbCursors = new VectorClusteringBidirectionCursor[numComponents];
        vcTreeAccessors = new VectorClusteringTreeAccessor[numComponents];
        resolverCursors = new VectorClusteringSearchCursor[numComponents];

        for (int i = 0; i < numComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            VectorClusteringTree vcTree = (VectorClusteringTree) component.getIndex();
            vcTreeAccessors[i] = (VectorClusteringTreeAccessor) vcTree.createAccessor(iap);
            vcbCursors[i] = (VectorClusteringBidirectionCursor) vcTreeAccessors[i].createBidirectionCursor();
            // Create resolver cursor for per-component directory page resolution.
            // NOT opened for search — only used for resolveDirectoryPageId().
            resolverCursors[i] = (VectorClusteringSearchCursor) vcTreeAccessors[i].createSearchCursor(false);
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

            // Extract quantized state from first cursor (null = non-quantized path)
            this.quantizedQueryVector = firstSearchCursor.getQuantizedQueryVector();
            this.quantizer = firstSearchCursor.getQuantizer();

            if (this.queryVector == null) {
                throw HyracksDataException
                        .create(new IllegalArgumentException("Query vector must be provided for optimized search"));
            }

            // Set quantizer on strategy before initialize() so level-wise navigation
            // can compute quantized D(q,C) for each leaf centroid in ClusterSearchResult
            if (quantizedQueryVector != null && quantizer != null) {
                clusterStrategy.setQuantizer(quantizedQueryVector, quantizer);
            }

            // Initialize cluster selection strategy with first component's tree (candidateLimit so we collect 2*K for reranking)
            clusterStrategy.initialize(vcTree, queryVector, distanceFunction, candidateLimit);

            // Set first cursor for DFS fallback (like LSMVCTreeSearchCursor does)
            clusterStrategy.setFirstCursorForDFS(firstSearchCursor);

            // Sync visited set so DFS fallback skips clusters already returned by level-wise
            firstSearchCursor.setSharedVisitedSet(clusterStrategy.getVisitedCentroidIds());

            LOGGER.log(Level.TRACE,
                    "[LSMVCTreeBlockedCursor] Initialized with queryVector dim={}, K={}, nprobe={}, epsilon={}",
                    queryVector.length, K, nprobe, epsilon);
        }

        // Get first cluster from strategy (level-wise selection)
        ClusterSearchResult firstCluster = clusterStrategy.getFirstCluster();

        if (firstCluster == null) {
            // No clusters available - empty tree
            LOGGER.log(Level.TRACE, "[LSMVCTreeBlockedCursor] No clusters available (empty tree)");
            return;
        }

        // D(q, C) — use quantized distance when available to keep all three
        // triangle inequality distances in the same (quantized) metric space
        if (quantizer != null && firstCluster.hasQuantizedDistance()) {
            this.dqc = firstCluster.quantizedDistance;
        } else {
            this.dqc = firstCluster.distance;
        }

        LOGGER.log(Level.TRACE,
                "[LSMVCTreeBlockedCursor] First cluster: cid={}, D(q,C)_full={}, D(q,C)_quant={}, dqc_used={}, dirPage={}, levelWise={}",
                firstCluster.centroidId, firstCluster.distance,
                firstCluster.hasQuantizedDistance() ? firstCluster.quantizedDistance : Double.NaN, dqc,
                firstCluster.directoryPageId, clusterStrategy.getLevelWiseClusterCount());

        // Perform the bidirectional search on first cluster
        openClusterAndSearch(firstCluster, queryVector, dqc);
        clustersExplored = 1;
        stopAdvancing = false;

        // Multi-cluster probing loop (following LSMVCTreeSearchCursor pattern)
        // After each cluster's bidirectional search terminates, check strategy for more clusters
        while (!stopAdvancing) {
            if (clusterStrategy.shouldStopAdvancing(clustersExplored, topKWindow.size())) {
                stopAdvancing = true;
                break;
            }

            if (!clusterStrategy.hasMoreClusters()) {
                break;
            }

            ClusterSearchResult nextCluster = clusterStrategy.getNextCluster();
            if (nextCluster == null) {
                break;
            }

            LOGGER.log(Level.TRACE,
                    "[LSMVCTreeBlockedCursor] Advancing to cluster: cid={}, D(q,C)_full={}, D(q,C)_quant={}, dirPage={}",
                    nextCluster.centroidId, nextCluster.distance,
                    nextCluster.hasQuantizedDistance() ? nextCluster.quantizedDistance : Double.NaN,
                    nextCluster.directoryPageId);

            // Exclude empty clusters from nprobe
            if (validTuplesFromCurrentCluster == 0) {
                clustersExplored = Math.max(0, clustersExplored - 1);
            }
            validTuplesFromCurrentCluster = 0;
            advanceAllComponentsToNextCluster(nextCluster);
            clustersExplored++;
        }

        LOGGER.log(Level.TRACE,
                "[LSMVCTreeBlockedCursor] open() COMPLETE: {} clusters probed, topKWindow.size()={} (BEFORE consumption)",
                clustersExplored, topKWindow.size());
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

        // TODO: Quantize the query vector per-cluster here. The centroid is available from cluster.centroid,
        // which can be used to compute the residual (queryVector - centroid) for product quantization.

        // Reset state
        rightCtx.reset();
        leftCtx.reset();
        topKWindow.clear();

        // Open all VCB cursors for this cluster.
        // Use full-precision distance for pivot positioning (data pages sorted by full-precision D(x,C)).
        // IMPORTANT: Resolve directoryPageId per-component because each LSM component may have
        // different directory page IDs (memory components use centroidDirPageMap, disk components
        // have their own leaf page layout). The cluster's directoryPageId comes from the strategy
        // which navigated only component 0's tree.
        for (int i = 0; i < numComponents; i++) {
            long resolvedDirPageId = resolverCursors[i].resolveDirectoryPageId(cluster);
            vcbCursors[i].openCluster(resolvedDirPageId, cluster.distance);
        }

        // Seed the priority queues
        seedQueue(rightCtx);
        seedQueue(leftCtx);

        // Perform bidirectional search
        performBidirectionalSearch();
    }

    /**
     * Advance all component cursors to the next cluster and perform bidirectional search.
     * Follows LSMVCTreeSearchCursor.advanceAllComponentsToSameCluster() pattern:
     * all components open the SAME cluster in lock-step.
     *
     * Unlike openClusterAndSearch(), this method preserves the topKWindow so that
     * results from previous clusters contribute to the termination threshold.
     */
    private void advanceAllComponentsToNextCluster(ClusterSearchResult cluster) throws HyracksDataException {
        // Update D(q, C) for the new cluster's centroid — use quantized distance when available
        if (quantizer != null && cluster.hasQuantizedDistance()) {
            this.dqc = cluster.quantizedDistance;
        } else {
            this.dqc = cluster.distance;
        }

        // TODO: Quantize the query vector per-cluster here. The centroid is available from cluster.centroid,
        // which can be used to compute the residual (queryVector - centroid) for product quantization.

        // Reset direction contexts (NOT topKWindow - keep results from previous clusters)
        rightCtx.reset();
        leftCtx.reset();

        // Open all VCB cursors for this cluster (same cluster for all components).
        // Use full-precision distance for pivot positioning (data pages sorted by full-precision D(x,C)).
        // Resolve directoryPageId per-component (same reason as openClusterAndSearch).
        for (int i = 0; i < numComponents; i++) {
            long resolvedDirPageId = resolverCursors[i].resolveDirectoryPageId(cluster);
            vcbCursors[i].openCluster(resolvedDirPageId, cluster.distance);
        }

        // Seed the priority queues
        seedQueue(rightCtx);
        seedQueue(leftCtx);

        // Perform bidirectional search (uses existing topKWindow for threshold)
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

        // Top-K window: max-heap by D(q,x); capacity = candidateLimit (2*K) for reranking
        topKWindow = new PriorityQueue<>(Math.max(candidateLimit, 1), (a, b) -> Double.compare(b.dqx, a.dqx));
    }

    /**
     * Seed a priority queue with the first tuple from each component.
     * IMPORTANT: We must copy the tuple because the cursor's internal buffer is reused.
     */
    private void seedQueue(DirectionContext ctx) throws HyracksDataException {
        for (int i = 0; i < numComponents; i++) {
            boolean hasNext =
                    ctx.direction == Direction.RIGHT ? vcbCursors[i].hasNextRight() : vcbCursors[i].hasNextLeft();

            if (hasNext) {
                if (ctx.direction == Direction.RIGHT) {
                    vcbCursors[i].nextRight();
                } else {
                    vcbCursors[i].nextLeft();
                }

                ITupleReference tuple =
                        ctx.direction == Direction.RIGHT ? vcbCursors[i].getTupleRight() : vcbCursors[i].getTupleLeft();

                // Copy tuple because cursor's internal buffer is reused on next()
                ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
                double dxc = extractDistanceToCentroid(tupleCopy);
                boolean antimatter = isAntimatter(tuple);
                ctx.pqes[i].reset(tupleCopy, dxc, antimatter);
                ctx.queue.offer(ctx.pqes[i]);

                // Per-tuple logging removed to avoid stderr blocking with large clusters
            }
        }
    }

    /**
     * Perform bidirectional search with triangle inequality termination.
     */
    private void performBidirectionalSearch() throws HyracksDataException {
        int iterCount = 0;
        while (!rightCtx.terminated || !leftCtx.terminated) {
            // Process right direction
            if (!rightCtx.terminated) {
                ITupleReference rightTuple = getNextValidTuple(rightCtx);
                // Debug: log first few tuples to verify getNextValidTuple is working
                if (iterCount < 3) {
                    LOGGER.log(Level.TRACE, "[bidir] RIGHT iter={}, tupleReturned={}, fields={}", iterCount,
                            rightTuple != null, rightTuple != null ? rightTuple.getFieldCount() : -1);
                }
                if (rightTuple != null) {
                    double dxcFull = extractFullPrecisionDxc(rightTuple);
                    double dxcQuant = (quantizer != null) ? extractDistanceToCentroid(rightTuple) : Double.NaN;
                    double dqx = Double.NaN;

                    // Apply INCLUDE field filter before adding to top-K window
                    if (passesTupleFilter(rightTuple)) {
                        dqx = computeApproximateDistance(rightTuple);
                        addToTopKWindow(rightTuple, dqx);
                    }

                    // Per-tuple logging removed to avoid stderr blocking with large clusters

                    // Check right termination: D(x',C) > max{D(q,x)} + D(q,C)
                    if (topKWindow.size() >= candidateLimit && !rightCtx.queue.isEmpty()) {
                        double nextDxc = rightCtx.queue.peek().dxc;
                        double threshold = topKWindow.peek().dqx + dqc;
                        if (nextDxc > threshold) {
                            rightCtx.terminated = true;
                        }
                    }
                } else {
                    rightCtx.terminated = true;
                }
            }

            // Process left direction
            if (!leftCtx.terminated) {
                ITupleReference leftTuple = getNextValidTuple(leftCtx);
                // Debug: log first few tuples to verify getNextValidTuple is working
                if (iterCount < 3) {
                    LOGGER.log(Level.TRACE, "[bidir] LEFT iter={}, tupleReturned={}, fields={}", iterCount,
                            leftTuple != null, leftTuple != null ? leftTuple.getFieldCount() : -1);
                }
                if (leftTuple != null) {
                    double dxcFull = extractFullPrecisionDxc(leftTuple);
                    double dxcQuant = (quantizer != null) ? extractDistanceToCentroid(leftTuple) : Double.NaN;
                    double dqx = Double.NaN;

                    // Apply INCLUDE field filter before adding to top-K window
                    if (passesTupleFilter(leftTuple)) {
                        dqx = computeApproximateDistance(leftTuple);
                        addToTopKWindow(leftTuple, dqx);
                    }

                    // Per-tuple logging removed to avoid stderr blocking with large clusters

                    // Check left termination: D(x',C) < D(q,C) - max{D(q,x)}
                    // Note: When threshold is negative (kth_dqx > dqc), left cannot terminate early
                    // and must scan all tuples. This is expected when query is far from centroid.
                    if (topKWindow.size() >= candidateLimit && !leftCtx.queue.isEmpty()) {
                        double nextDxc = leftCtx.queue.peek().dxc;
                        double threshold = dqc - topKWindow.peek().dqx;
                        if (nextDxc < threshold) {
                            leftCtx.terminated = true;
                        }
                    }
                } else {
                    leftCtx.terminated = true;
                }
            }
            iterCount++;
        }

        // Summary log kept for diagnostics
        LOGGER.log(Level.TRACE, "[LSMVCTreeBlockedCursor] Search complete: topK={}, processed={}, cancellations={}",
                topKWindow.size(), totalTuplesProcessed, antimatterCancellations);
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
        // Per-tuple logging removed to avoid stderr blocking with large clusters

        if (checkElement.isAntimatter) {
            // Antimatter - hold for cancellation check with next tuple
            ctx.outputElement = ctx.queue.poll();
            ctx.savedTuple = ctx.outputElement.tuple; // Save BEFORE advanceCursor modifies it
            ctx.needPush = false; // Fixed: advanceCursor called here, so don't call again in refillPendingElementCursor
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
        int cmpResult = compare(ctx.savedTuple, checkElement.tuple);

        if (cmpResult == 0) {
            // Same key - antimatter cancellation
            performAntimatterCancellation(ctx, checkElement);
        } else {
            // Different key - discard antimatter, refill pending element's cursor
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
            boolean antimatter = isAntimatter(tuple);
            ctx.pqes[componentId].reset(tupleCopy, dxc, antimatter);
            ctx.queue.offer(ctx.pqes[componentId]);
            // Per-tuple logging removed to avoid stderr blocking with large clusters
        }
        // else: component exhausted, no more tuples
    }

    /**
     * Compare two tuples for antimatter reconciliation.
     * Follows LSMVCTreeSearchCursor.compare() pattern:
     * - Compare distance (field 0), then PK fields starting at pkStartField
     * - Skip secondary fields (centroidId, and optionally quantized_distance/quantized_embedding)
     *
     * @return 0 if tuples have the same key (should cancel), non-zero otherwise
     */
    private int compare(ITupleReference tupleA, ITupleReference tupleB) throws HyracksDataException {
        // Compare field 0 (distance)
        int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0), tupleB.getFieldLength(0));

        // Per-tuple logging removed to avoid stderr blocking with large clusters

        if (result != 0) {
            return result;
        }

        // Compare PK fields starting at pkStartField (skip secondary fields)
        int numPKFields = cmp.getComparators().length - pkStartField;
        for (int i = 0; i < numPKFields; i++) {
            int fieldIdx = pkStartField + i;
            int cmpIdx = pkStartField + i;

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
    // Debug counter for addToTopKWindow
    private int addToTopKWindowCallCount = 0;

    private void addToTopKWindow(ITupleReference tuple, double dqx) throws HyracksDataException {
        addToTopKWindowCallCount++;
        // Log first few calls to see if we're even getting here
        if (addToTopKWindowCallCount <= 3) {
            LOGGER.log(Level.TRACE, "[addToTopKWindow] call #{}, dqx={}, topKSize={}, K={}, tupleFields={}",
                    addToTopKWindowCallCount, dqx, topKWindow.size(), K, tuple.getFieldCount());
        }
        if (topKWindow.size() < candidateLimit) {
            validTuplesFromCurrentCluster++;
            // Copy tuple before storing - the original buffer will be reused
            ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
            topKWindow.offer(new ResultEntry(tupleCopy, dqx));
        } else if (dqx < topKWindow.peek().dqx) {
            validTuplesFromCurrentCluster++;
            topKWindow.poll(); // Remove worst
            // Copy tuple before storing - the original buffer will be reused
            ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
            topKWindow.offer(new ResultEntry(tupleCopy, dqx));
        }
        // else: tuple is worse than all in window, reject
    }

    /**
     * Check if tuple passes the INCLUDE field filter.
     * Filter is applied AFTER antimatter reconciliation because:
     * 1. Antimatter from newer components comes first in queue (lower componentId)
     * 2. By the time we reach here, any matching antimatter has cancelled this tuple
     * 3. Tuples that fail filter should NOT be added to topKWindow or counted toward K
     *
     * @param tuple the tuple to check
     * @return true if tuple passes filter (or no filter configured), false otherwise
     */
    private boolean passesTupleFilter(ITupleReference tuple) throws HyracksDataException {
        if (tupleFilter == null) {
            return true;
        }

        referenceFilterTuple.reset(tuple);

        if (tupleFilter.accept(referenceFilterTuple)) {
            return true;
        }

        // Tuple fails filter
        tuplesFilteredOut++;
        return false;
    }

    /**
     * Extract D(x, C) from tuple.
     * When quantizer is available, reads field 2 (Q_QUANTIZED_DISTANCE_FIELD = quantized D(x,C))
     * to keep all three triangle inequality distances in the same (quantized) metric space.
     * Otherwise reads field 0 (full-precision D(x,C)).
     */
    private double extractDistanceToCentroid(ITupleReference tuple) {
        int fieldIndex = (quantizer != null) ? 2 : 0; // field 2 = D(x,C)_quant, field 0 = D(x,C)_full
        byte[] data = tuple.getFieldData(fieldIndex);
        int offset = tuple.getFieldStart(fieldIndex);
        return DoublePointable.getDouble(data, offset);
    }

    /**
     * Always extract field 0 (full-precision D(x,C)) for diagnostic logging.
     */
    private double extractFullPrecisionDxc(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(0);
        int offset = tuple.getFieldStart(0);
        return DoublePointable.getDouble(data, offset);
    }

    /**
     * Compute distance D(q, x) between query vector and tuple's vector.
     * Uses IVectorBinaryAccessor to extract the vector from the tuple.
     *
     * Non-quantized format: | distance(0) | centroidId(1) | PK...(2+) |
     * Quantized format:     | distance(0) | centroidId(1) | quantized_distance(2) | quantized_embedding(3) | PK...(4+) |
     */
    private double computeApproximateDistance(ITupleReference tuple) throws HyracksDataException {
        // When quantized, compute the approximate distance from the quantized representations
        if (quantizedQueryVector != null && quantizer != null) {
            // Read quantized bytes from field 3 (Q_QUANTIZED_EMBEDDING_FIELD)
            // Field is serialized by ByteArraySerializerDeserializer with a VarLen length prefix
            int vectorFieldIndex = VCTreeDataTupleConstants.Q_QUANTIZED_EMBEDDING_FIELD;
            byte[] data = tuple.getFieldData(vectorFieldIndex);
            int offset = tuple.getFieldStart(vectorFieldIndex);
            int contentLength = ByteArrayPointable.getContentLength(data, offset);
            int metaLength = ByteArrayPointable.getNumberBytesToStoreMeta(contentLength);
            byte[] qBytes = new byte[contentLength];
            System.arraycopy(data, offset + metaLength, qBytes, 0, contentLength);
            double[] dequantized = quantizer.dequantize(qBytes);
            return distanceFunction.apply(quantizedQueryVector, dequantized);
        }

        // Fallback: quantizer is null but we have quantized data format — this is a misconfiguration.
        // The caller must set VECTOR_QUANTIZER in index access parameters.
        throw HyracksDataException.create(new IllegalStateException(
                "computeApproximateDistance: quantizer is null but quantized data format detected. "
                        + "Ensure VECTOR_QUANTIZER is set in index access parameters."));
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
        boolean result = !topKWindow.isEmpty();
        LOGGER.log(Level.TRACE, "[hasNext] called, topKWindow.size()={}, returning={}", topKWindow.size(), result);
        return result;
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
            // Summary logging via LOGGER (not stderr to avoid blocking)
            LOGGER.log(Level.TRACE,
                    "[LSMVCTreeBlockedCursor] Summary: K={}, nprobe={}, clusters={}, processed={}, addToTopKCalls={}, cancellations={}, filtered={}, topK={}",
                    K, nprobe, clustersExplored, totalTuplesProcessed, addToTopKWindowCallCount,
                    antimatterCancellations, tuplesFilteredOut, topKWindow.size());

            // Close bidirectional cursors
            for (int i = 0; i < numComponents; i++) {
                if (vcbCursors[i] != null) {
                    vcbCursors[i].close();
                }
            }
            // Close resolver cursors (not opened for search, but may hold lazy-built local maps)
            if (resolverCursors != null) {
                for (int i = 0; i < numComponents; i++) {
                    if (resolverCursors[i] != null) {
                        resolverCursors[i].close();
                    }
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
     * 2. PK fields starting at pkStartField (skip secondary fields)
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

            // Compare PK fields starting at pkStartField (skip secondary fields)
            try {
                ITupleReference tupleA = a.tuple;
                ITupleReference tupleB = b.tuple;

                int numPKFields = cmp.getComparators().length - pkStartField;
                for (int i = 0; i < numPKFields; i++) {
                    int fieldIdx = pkStartField + i;
                    int cmpIdx = pkStartField + i;

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
