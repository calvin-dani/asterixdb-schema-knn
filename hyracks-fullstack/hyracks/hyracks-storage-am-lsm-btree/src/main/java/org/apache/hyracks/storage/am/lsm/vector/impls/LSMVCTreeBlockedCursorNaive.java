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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
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
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree.VectorClusteringTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

/**
 * Naive blocked cursor for vector ANN search.
 *
 * This cursor combines the multi-component priority queue merging of LSMVCTreeSearchCursor
 * with the blocked execution model of LSMVCTreeBlockedCursor. All search work is done in
 * open(), results are collected into a top-K window, and hasNext()/next()/getTuple() simply
 * drain the window.
 *
 * Key differences from LSMVCTreeBlockedCursor:
 * - Uses VectorClusteringSearchCursor (NOT bidirectional cursor)
 * - No triangle inequality pruning
 * - Scans clusters sequentially (closest first) via the cluster selection strategy
 *
 * Key differences from LSMVCTreeSearchCursor:
 * - All work done in open() (blocked execution)
 * - Maintains top-K window with approximate distance computed from quantized embedding
 * - Supports ITupleFilter inline filtering (filtered tuples don't enter top-K window)
 *
 * "Blocked" means all search work is done in open(), and results are stored in topKWindow.
 * Calls to hasNext()/next()/getTuple() simply drain the window.
 */
public class LSMVCTreeBlockedCursorNaive implements IIndexCursor {

    // Operation context
    private ILSMIndexOperationContext opCtx;
    private List<ILSMComponent> operationalComponents;

    // Per-component accessors and cursors (same as LSMVCTreeSearchCursor)
    private VectorClusteringTreeAccessor[] vcTreeAccessors;
    private IIndexCursor[] rangeCursors;
    private int numComponents;

    // Priority queue for merging results from multiple components
    private PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    private PriorityQueueElement[] pqes;
    private MultiComparator cmp;

    // Antimatter reconciliation state (following LSMVCTreeSearchCursor pattern)
    private PriorityQueueElement outputElement;
    private boolean needPushElementIntoQueue;

    // Top-K window: max-heap by D(q,x) - peek() gives the worst result
    private PriorityQueue<ResultEntry> topKWindow;

    // Search parameters
    private int K;
    private int nprobe;
    private double epsilon;
    private double[] queryVector;
    private IVectorDistanceFunction distanceFunction;

    // Vector accessor for extracting vectors from tuples
    private IVectorBinaryAccessor vectorAccessor;

    // Quantization state (propagated from first search cursor)
    private double[] quantizedQueryVector;
    private IVectorQuantizer quantizer;

    // Cluster selection strategy (nprobe + DFS fallback)
    private IClusterSelectionStrategy clusterStrategy;

    // First component's search cursor (for query vector/distance function extraction and DFS)
    private VectorClusteringSearchCursor firstSearchCursor;

    // Cluster tracking (synchronized advancement like LSMVCTreeSearchCursor)
    private int[] currentClusterIndex;
    private boolean[] clusterExhausted;
    private boolean stopAdvancing;
    private int clustersExplored;

    // Tuple filter for INCLUDE field predicates (e.g., year > 2000)
    private ITupleFilter tupleFilter;
    private ReferenceFrameTupleReference referenceFilterTuple;

    // Field index where primary keys start in the data tuple
    private int pkStartField;

    // Cursor state
    private boolean isOpen;
    private ResultEntry currentResult;

    // Statistics
    private int totalTuplesProcessed;
    private int nextCallCount;
    private int antimatterCancellations;
    private int tuplesFilteredOut;

    public LSMVCTreeBlockedCursorNaive(ILSMIndexOperationContext opCtx) {
        this.opCtx = opCtx;
        this.isOpen = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.isOpen = true;
        this.totalTuplesProcessed = 0;
        this.nextCallCount = 0;
        this.antimatterCancellations = 0;
        this.tuplesFilteredOut = 0;

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
        this.pkStartField = vectorPred.getPkStartField();

        // Extract tuple filter from search predicate for INCLUDE field predicates
        this.tupleFilter = vectorPred.getTupleFilter();
        if (this.tupleFilter != null) {
            this.referenceFilterTuple = new ReferenceFrameTupleReference();
            System.err.println(
                    "[LSMVCTreeBlockedCursorNaive] Tuple filter is SET - will filter INCLUDE field predicates");
        } else {
            System.err.println("[LSMVCTreeBlockedCursorNaive] Tuple filter is NULL - no INCLUDE field filtering");
        }

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

        // Initialize top-K window: max-heap by D(q,x)
        topKWindow = new PriorityQueue<>(Math.max(K, 1), (a, b) -> Double.compare(b.dqx, a.dqx));

        // Initialize cluster tracking arrays
        currentClusterIndex = new int[numComponents];
        Arrays.fill(currentClusterIndex, 0);
        clusterExhausted = new boolean[numComponents];
        Arrays.fill(clusterExhausted, false);
        stopAdvancing = false;
        clustersExplored = 0;

        // Create accessors and cursors for each component (same as LSMVCTreeSearchCursor)
        vcTreeAccessors = new VectorClusteringTreeAccessor[numComponents];
        rangeCursors = new IIndexCursor[numComponents];

        for (int i = 0; i < numComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            VectorClusteringTree vcTree = (VectorClusteringTree) component.getIndex();
            vcTreeAccessors[i] = (VectorClusteringTreeAccessor) vcTree.createAccessor(iap);
            rangeCursors[i] = vcTreeAccessors[i].createSearchCursor(false);
        }

        // Open all cursors with the search predicate
        IndexCursorUtils.open(vcTreeAccessors, rangeCursors, searchPred);

        // Initialize strategy and set up DFS fallback (same as LSMVCTreeSearchCursor)
        if (numComponents > 0) {
            this.firstSearchCursor = (VectorClusteringSearchCursor) rangeCursors[0];
            this.queryVector = firstSearchCursor.getQueryVector();
            this.distanceFunction = firstSearchCursor.getDistanceFunction();

            // Extract quantized state from first cursor (null = non-quantized path)
            this.quantizedQueryVector = firstSearchCursor.getQuantizedQueryVector();
            this.quantizer = firstSearchCursor.getQuantizer();

            if (this.queryVector == null) {
                throw HyracksDataException
                        .create(new IllegalArgumentException("Query vector must be provided for naive blocked search"));
            }

            // Initialize strategy with first component's tree
            ILSMComponent firstComponent = operationalComponents.get(0);
            VectorClusteringTree vcTree = (VectorClusteringTree) firstComponent.getIndex();
            clusterStrategy.initialize(vcTree, queryVector, distanceFunction, K);

            // Set first cursor for DFS fallback
            clusterStrategy.setFirstCursorForDFS(firstSearchCursor);

            // Pass shared visited set from strategy to all cursors
            Set<Integer> visitedSet = clusterStrategy.getVisitedCentroidIds();
            for (int i = 0; i < numComponents; i++) {
                if (rangeCursors[i] instanceof VectorClusteringSearchCursor) {
                    ((VectorClusteringSearchCursor) rangeCursors[i]).setSharedVisitedSet(visitedSet);
                }
            }

            // Re-open all cursors to the first level-wise cluster for consistency
            ClusterSearchResult firstCluster = clusterStrategy.getFirstCluster();
            if (firstCluster != null) {
                ClusterSearchResult dfsCluster = firstSearchCursor.getCurrentClusterResult();
                if (dfsCluster != null && dfsCluster.centroidId != firstCluster.centroidId) {
                    System.err.printf(
                            "[LSMVCTreeBlockedCursorNaive] DFS found cid=%d but level-wise[0] is cid=%d - re-opening%n",
                            dfsCluster.centroidId, firstCluster.centroidId);
                    for (int i = 0; i < numComponents; i++) {
                        if (rangeCursors[i] instanceof VectorClusteringSearchCursor) {
                            VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) rangeCursors[i];
                            vcCursor.resetClustersProbed();
                            vcCursor.openClusterByResult(firstCluster);
                        }
                    }
                }
                System.err.printf(
                        "[LSMVCTreeBlockedCursorNaive] Initialized with K=%d, nprobe=%d, epsilon=%.4f, level-wise clusters=%d%n",
                        K, nprobe, epsilon, clusterStrategy.getLevelWiseClusterCount());
            }
        }

        // Initialize priority queue for merging results from all components
        initPriorityQueue();

        // Perform the blocked search: drain all clusters and collect results
        performBlockedSearch();

        System.err.println(String.format(
                "[LSMVCTreeBlockedCursorNaive] Search complete: topK=%d, processed=%d, filtered=%d, cancellations=%d, clusters=%d",
                topKWindow.size(), totalTuplesProcessed, tuplesFilteredOut, antimatterCancellations, clustersExplored));
    }

    /**
     * Initialize priority queue and populate with first element from each cursor.
     */
    private void initPriorityQueue() throws HyracksDataException {
        int pqInitSize = Math.max(numComponents, 1);
        outputPriorityQueue = new PriorityQueue<>(pqInitSize, new NaivePriorityQueueComparator());
        pqes = new PriorityQueueElement[pqInitSize];
        for (int i = 0; i < pqInitSize; i++) {
            pqes[i] = new PriorityQueueElement(i);
        }

        // Populate priority queue with first element from each cursor
        for (int i = 0; i < numComponents; i++) {
            if (rangeCursors[i].hasNext()) {
                rangeCursors[i].next();
                pqes[i].reset(rangeCursors[i].getTuple());
                outputPriorityQueue.offer(pqes[i]);
            } else {
                clusterExhausted[i] = true;
            }
        }

        clustersExplored = 1; // First cluster opened

        // If all components started empty, advance to next cluster
        if (allComponentsExhausted()) {
            advanceAllComponentsToNextCluster();
        }
    }

    /**
     * Perform the blocked search: drain the priority queue, apply antimatter reconciliation
     * and filtering, compute distances, and collect results into topKWindow.
     *
     * This continues until we've probed enough clusters and have enough results,
     * or all clusters are exhausted.
     */
    private void performBlockedSearch() throws HyracksDataException {
        while (true) {
            // Process current cluster's data via priority queue
            while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue) {
                ITupleReference validTuple = getNextValidTuple();
                if (validTuple != null) {
                    // Apply INCLUDE field filter
                    if (passesTupleFilter(validTuple)) {
                        // Compute approximate distance using quantized embedding
                        double dqx = computeApproximateDistance(validTuple);
                        addToTopKWindow(validTuple, dqx);
                    }
                    totalTuplesProcessed++;
                }
            }

            // Current cluster(s) exhausted - check if we should advance
            if (stopAdvancing) {
                break;
            }

            // Check strategy for stop condition
            int minClustersExplored = getMinClustersProbed();
            if (clusterStrategy.shouldStopAdvancing(minClustersExplored, topKWindow.size())) {
                stopAdvancing = true;
                System.err
                        .println(String.format("[LSMVCTreeBlockedCursorNaive] Early termination: clusters=%d, topK=%d",
                                minClustersExplored, topKWindow.size()));
                break;
            }

            // Try to advance to next cluster
            if (!clusterStrategy.hasMoreClusters()) {
                System.err.println("[LSMVCTreeBlockedCursorNaive] No more clusters available");
                break;
            }

            advanceAllComponentsToNextCluster();
        }
    }

    /**
     * Get next valid tuple with antimatter reconciliation.
     * Follows the checkPriorityQueue() pattern from LSMVCTreeSearchCursor.
     *
     * @return next valid matter tuple, or null if queue exhausted
     */
    private ITupleReference getNextValidTuple() throws HyracksDataException {
        while (!outputPriorityQueue.isEmpty() || needPushElementIntoQueue) {
            if (outputPriorityQueue.isEmpty()) {
                // Queue empty but pending element exists - refill
                pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
                needPushElementIntoQueue = false;
                outputElement = null;
                continue;
            }

            PriorityQueueElement checkElement = outputPriorityQueue.peek();

            if (outputElement == null) {
                // No pending element - check if top is antimatter
                if (isAntimatter(checkElement.tuple)) {
                    // Hold antimatter for cancellation check
                    outputElement = outputPriorityQueue.poll();
                    needPushElementIntoQueue = true;
                    continue;
                }
                // Valid matter tuple
                PriorityQueueElement validElem = outputPriorityQueue.poll();
                ITupleReference result = TupleUtils.copyTuple(validElem.tuple);
                pushIntoQueueAndAdvanceClusterIfNeeded(validElem);
                return result;
            } else {
                // Have pending antimatter - check for cancellation
                int cmpResult = compare(outputElement.tuple, checkElement.tuple);
                if (cmpResult == 0) {
                    // Same key - antimatter cancellation
                    antimatterCancellations++;
                    PriorityQueueElement matchElem = outputPriorityQueue.poll();
                    pushIntoQueueAndAdvanceClusterIfNeeded(matchElem);
                    pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
                    needPushElementIntoQueue = false;
                    outputElement = null;
                } else {
                    // Different key - discard antimatter
                    if (needPushElementIntoQueue) {
                        pushIntoQueueAndAdvanceClusterIfNeeded(outputElement);
                        needPushElementIntoQueue = false;
                    }
                    outputElement = null;
                }
            }
        }
        return null; // Queue exhausted
    }

    /**
     * Push next element from component cursor into queue.
     * If cursor's current cluster is exhausted, mark it as exhausted.
     * When ALL components' clusters are exhausted, the loop in performBlockedSearch handles advancement.
     */
    private void pushIntoQueueAndAdvanceClusterIfNeeded(PriorityQueueElement e) throws HyracksDataException {
        int cursorIndex = e.componentId;
        IIndexCursor cursor = rangeCursors[cursorIndex];

        if (cursor.hasNext()) {
            cursor.next();
            e.reset(cursor.getTuple());
            outputPriorityQueue.offer(e);
            return;
        }

        // Current cluster exhausted for this component
        clusterExhausted[cursorIndex] = true;
    }

    /**
     * Advance ALL component cursors to the next cluster.
     * Uses iterative loop to handle consecutive empty clusters.
     */
    private void advanceAllComponentsToNextCluster() throws HyracksDataException {
        while (true) {
            Arrays.fill(clusterExhausted, false);

            ClusterSearchResult nextCluster = clusterStrategy.getNextCluster();
            if (nextCluster == null) {
                System.err.println("[LSMVCTreeBlockedCursorNaive] No more clusters available globally");
                Arrays.fill(clusterExhausted, true);
                stopAdvancing = true;
                return;
            }

            System.err.println(String.format(
                    "[LSMVCTreeBlockedCursorNaive] Advancing to cluster cid=%d, distance=%.4f, dirPage=%d",
                    nextCluster.centroidId, nextCluster.distance, nextCluster.directoryPageId));

            // Open all components to this cluster
            for (int i = 0; i < numComponents; i++) {
                advanceComponentToCluster(i, nextCluster);
            }
            clustersExplored++;

            // Check if all components found empty cluster - try next
            if (!allComponentsExhausted()) {
                return; // At least one component has data
            }

            // All empty - check if should continue
            if (!clusterStrategy.hasMoreClusters()) {
                stopAdvancing = true;
                return;
            }
            // Loop to try next cluster
        }
    }

    /**
     * Advance a single component to a specific cluster.
     */
    private void advanceComponentToCluster(int componentIndex, ClusterSearchResult cluster)
            throws HyracksDataException {
        IIndexCursor cursor = rangeCursors[componentIndex];

        if (!(cursor instanceof VectorClusteringSearchCursor)) {
            clusterExhausted[componentIndex] = true;
            return;
        }

        VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) cursor;
        boolean hasData = vcCursor.openClusterByResult(cluster);
        currentClusterIndex[componentIndex]++;

        if (hasData && vcCursor.hasNext()) {
            vcCursor.next();
            pqes[componentIndex].reset(vcCursor.getTuple());
            outputPriorityQueue.offer(pqes[componentIndex]);
        } else {
            clusterExhausted[componentIndex] = true;
        }
    }

    /**
     * Check if all components have exhausted their current cluster.
     */
    private boolean allComponentsExhausted() {
        for (boolean exhausted : clusterExhausted) {
            if (!exhausted) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the minimum number of clusters probed across all VectorClusteringSearchCursors.
     */
    private int getMinClustersProbed() {
        int minProbed = Integer.MAX_VALUE;
        for (int i = 0; i < rangeCursors.length; i++) {
            if (rangeCursors[i] instanceof VectorClusteringSearchCursor) {
                int probed = ((VectorClusteringSearchCursor) rangeCursors[i]).getClustersProbed();
                if (probed < minProbed) {
                    minProbed = probed;
                }
            }
        }
        return minProbed == Integer.MAX_VALUE ? 0 : minProbed;
    }

    /**
     * Compare two tuples for antimatter reconciliation.
     * Compares distance (field 0) then PK fields starting at pkStartField.
     */
    private int compare(ITupleReference tupleA, ITupleReference tupleB) throws HyracksDataException {
        // Compare field 0 (distance)
        int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0), tupleB.getFieldLength(0));
        if (result != 0) {
            return result;
        }

        // Compare PK fields starting at pkStartField
        int numPKFields = cmp.getComparators().length - pkStartField;
        for (int i = 0; i < numPKFields; i++) {
            int fieldIdx = pkStartField + i;
            if (fieldIdx >= tupleA.getFieldCount() || fieldIdx >= tupleB.getFieldCount()) {
                break;
            }
            result = cmp.getComparators()[pkStartField + i].compare(tupleA.getFieldData(fieldIdx),
                    tupleA.getFieldStart(fieldIdx), tupleA.getFieldLength(fieldIdx), tupleB.getFieldData(fieldIdx),
                    tupleB.getFieldStart(fieldIdx), tupleB.getFieldLength(fieldIdx));
            if (result != 0) {
                return result;
            }
        }
        return 0;
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
     * Check if tuple passes the INCLUDE field filter.
     * Applied AFTER antimatter reconciliation. Tuples that fail filter don't enter topKWindow.
     */
    private boolean passesTupleFilter(ITupleReference tuple) throws HyracksDataException {
        if (tupleFilter == null) {
            return true;
        }
        referenceFilterTuple.reset(tuple);
        if (tupleFilter.accept(referenceFilterTuple)) {
            return true;
        }
        tuplesFilteredOut++;
        try {
            long pkValue =
                    LongPointable.getLong(tuple.getFieldData(pkStartField), tuple.getFieldStart(pkStartField) + 1);
            System.err.println(String.format("[LSMVCTreeBlockedCursorNaive] Tuple FILTERED OUT (total: %d) | pk=%d",
                    tuplesFilteredOut, pkValue));
        } catch (Exception e) {
            // Ignore logging errors
        }
        return false;
    }

    /**
     * Compute approximate distance D(q, x) using quantized embedding.
     *
     * This cursor is dedicated for quantized vector indexes.
     * Quantized data tuple format (pkStartField=4):
     *   Field 0: distance_to_centroid, Field 1: centroidId,
     *   Field 2: quantized_distance, Field 3: quantized_embedding, Field 4+: PKs
     *
     * Dequantizes the stored embedding bytes (field 3) and computes distance
     * against the quantized query vector.
     */
    private double computeApproximateDistance(ITupleReference tuple) throws HyracksDataException {
        // Read quantized bytes from field 3 (quantized_embedding)
        // Field is serialized by ByteArraySerializerDeserializer with a VarLen length prefix
        int vectorFieldIndex = 3;
        byte[] data = tuple.getFieldData(vectorFieldIndex);
        int offset = tuple.getFieldStart(vectorFieldIndex);
        int contentLength = ByteArrayPointable.getContentLength(data, offset);
        int metaLength = ByteArrayPointable.getNumberBytesToStoreMeta(contentLength);
        byte[] qBytes = new byte[contentLength];
        System.arraycopy(data, offset + metaLength, qBytes, 0, contentLength);
        double[] dequantized = quantizer.dequantize(qBytes);
        return distanceFunction.apply(quantizedQueryVector, dequantized);
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
     * Add tuple to top-K window if it improves the results.
     */
    private void addToTopKWindow(ITupleReference tuple, double dqx) throws HyracksDataException {
        if (topKWindow.size() < K) {
            ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
            topKWindow.offer(new ResultEntry(tupleCopy, dqx));
        } else if (dqx < topKWindow.peek().dqx) {
            topKWindow.poll();
            ITupleReference tupleCopy = TupleUtils.copyTuple(tuple);
            topKWindow.offer(new ResultEntry(tupleCopy, dqx));
        }
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
        nextCallCount++;
    }

    @Override
    public ITupleReference getTuple() {
        return currentResult != null ? currentResult.tuple : null;
    }

    @Override
    public void close() throws HyracksDataException {
        if (isOpen) {
            // Print final summary
            System.err.println("\n========== LSMVCTreeBlockedCursorNaive Search Summary ==========");
            System.err.println(String.format("K=%d, nprobe=%d, epsilon=%.4f", K, nprobe, epsilon));
            System.err.println(String.format("Clusters explored:          %d", clustersExplored));
            System.err.println(String.format("Total tuples processed:     %d", totalTuplesProcessed));
            System.err.println(String.format("Antimatter cancellations:   %d", antimatterCancellations));
            System.err.println(String.format("Tuples filtered out:        %d", tuplesFilteredOut));
            System.err.println(String.format("Results returned (next()):   %d", nextCallCount));
            System.err.println("================================================================\n");

            // Close all component cursors
            for (int i = 0; i < numComponents; i++) {
                if (rangeCursors[i] != null) {
                    rangeCursors[i].close();
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
     * Priority queue element holding tuple and component info.
     */
    private static class PriorityQueueElement {
        int componentId;
        ITupleReference tuple;

        PriorityQueueElement(int componentId) {
            this.componentId = componentId;
        }

        void reset(ITupleReference tuple) {
            this.tuple = tuple;
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
     * Priority queue comparator for merging results from multiple components.
     * Compares by distance (field 0), then PK fields, then component ID.
     * Follows LSMVCTreeSearchCursor.VectorPriorityQueueComparator pattern.
     */
    private class NaivePriorityQueueComparator implements Comparator<PriorityQueueElement> {
        @Override
        public int compare(PriorityQueueElement a, PriorityQueueElement b) {
            ITupleReference tupleA = a.tuple;
            ITupleReference tupleB = b.tuple;

            try {
                // Compare field 0 (distance to centroid)
                int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                        tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0),
                        tupleB.getFieldLength(0));
                if (result != 0) {
                    return result;
                }

                // Compare PK fields starting at pkStartField
                int numRemainingFields = cmp.getComparators().length - pkStartField;
                for (int i = 0; i < numRemainingFields; i++) {
                    int fieldIdx = pkStartField + i;
                    if (fieldIdx >= tupleA.getFieldCount() || fieldIdx >= tupleB.getFieldCount()) {
                        break;
                    }
                    result = cmp.getComparators()[pkStartField + i].compare(tupleA.getFieldData(fieldIdx),
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

            // Tiebreaker: prefer tuples from earlier components (for antimatter reconciliation)
            return Integer.compare(a.componentId, b.componentId);
        }
    }
}
