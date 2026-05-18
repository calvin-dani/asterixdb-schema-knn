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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.tuples.ReferenceFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTree.VTreeAccessor;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleConstants;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Naive blocked cursor for vector ANN search.
 *
 * This cursor combines the multi-component priority queue merging of LSMVTreeSearchCursor
 * with the blocked execution model of LSMVTreePrunedTopKSearchCursor. All search work is done in
 * open(), results are collected into a top-K window, and hasNext()/next()/getTuple() simply
 * drain the window.
 *
 * Key differences from LSMVTreePrunedTopKSearchCursor:
 * - Uses VTreeSearchCursor (NOT bidirectional cursor)
 * - No triangle inequality pruning
 * - Scans clusters sequentially (closest first) via the cluster selection strategy
 *
 * Key differences from LSMVTreeSearchCursor:
 * - All work done in open() (blocked execution)
 * - Maintains top-K window with approximate distance computed from quantized embedding
 * - Supports ITupleFilter inline filtering (filtered tuples don't enter top-K window)
 *
 * "Blocked" means all search work is done in open(), and results are stored in topKWindow.
 * Calls to hasNext()/next()/getTuple() simply drain the window.
 */
public class LSMVTreeTopKSearchCursor implements IIndexCursor {

    private static final Logger LOGGER = LogManager.getLogger();

    // Operation context
    private ILSMIndexOperationContext opCtx;
    private List<ILSMComponent> operationalComponents;

    // Per-component accessors and cursors (same as LSMVTreeSearchCursor)
    private VTreeAccessor[] vTreeAccessors;
    private IIndexCursor[] rangeCursors;
    private int numComponents;

    // Priority queue for merging results from multiple components
    private PriorityQueue<PriorityQueueElement> outputPriorityQueue;
    private PriorityQueueElement[] pqes;
    private MultiComparator cmp;

    // Antimatter reconciliation state (following LSMVTreeSearchCursor pattern)
    private PriorityQueueElement outputElement;
    private boolean needPushElementIntoQueue;

    // Spillable top-K buffer: frame-backed in-memory heap with disk spill on budget exceeded.
    // Encapsulates MaxHeap + VariableDeletableTupleMemoryManager + RunFileWriter spill.
    private SpillableTopKBuffer topKBuffer;
    private SpillableTopKDrainIterator drainIterator;

    // Search parameters
    private int K;
    /** Number of candidates to send to PK for reranking (hardcoded 2*K for now). */
    private int candidateLimit;
    private int nprobe;
    private double epsilon;
    private double[] queryVector;
    private IVTreeDistanceFunction distanceFunction;

    // Vector accessor for extracting vectors from tuples
    private IVTreeBinaryAccessor vectorAccessor;

    // Quantization state (propagated from first search cursor)
    private double[] quantizedQueryVector;
    private IVTreeQuantizer quantizer;

    // Cluster selection strategy (nprobe + DFS fallback)
    private IClusterSelectionStrategy clusterStrategy;

    // First component's search cursor (for query vector/distance function extraction and DFS)
    private VTreeSearchCursor firstSearchCursor;

    // Cluster tracking (synchronized advancement like LSMVTreeSearchCursor)
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

    // Statistics
    private int totalTuplesProcessed;
    private int nextCallCount;
    private int antimatterCancellations;
    private int tuplesFilteredOut;
    private int validTuplesFromCurrentCluster; // Valid tuples from current cluster (for empty-cluster nprobe)

    // Per-cluster contribution tracking
    private ClusterSearchResult currentClusterForTracking;
    private int currentClusterTuplesScanned;
    private int currentClusterTuplesInserted;
    private Map<Integer, int[]> clusterContributions; // cid -> [scanned, insertedToTopK]
    private Map<Integer, Integer> finalTopKClusters; // cid -> count of tuples in final results

    public LSMVTreeTopKSearchCursor(ILSMIndexOperationContext opCtx) {
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
        this.validTuplesFromCurrentCluster = 0;
        this.clusterContributions = new LinkedHashMap<>();
        this.finalTopKClusters = new LinkedHashMap<>();
        this.currentClusterForTracking = null;
        this.currentClusterTuplesScanned = 0;
        this.currentClusterTuplesInserted = 0;

        // Get initial state
        LSMVTreeCursorInitialState lsmInitialState = (LSMVTreeCursorInitialState) initialState;
        this.cmp = lsmInitialState.getOriginalKeyComparator();
        this.operationalComponents = lsmInitialState.getOperationalComponents();
        this.numComponents = operationalComponents.size();

        // Extract search parameters from predicate
        VTreeSearchPredicate vectorPred = (VTreeSearchPredicate) searchPred;
        this.K = vectorPred.getK();
        int mult = vectorPred.getKMultiplier();
        this.candidateLimit = K * Math.max(1, mult); // Send K*kMultiplier to PK for reranking
        this.nprobe = 1; // Will be computed by NprobeClusterSelectionStrategy from minProbeFraction
        this.epsilon = vectorPred.getEpsilon();
        this.pkStartField = vectorPred.getPkStartField();

        // Extract tuple filter from search predicate for INCLUDE field predicates
        this.tupleFilter = vectorPred.getTupleFilter();
        if (this.tupleFilter != null) {
            this.referenceFilterTuple = new ReferenceFrameTupleReference();
            LOGGER.trace("Tuple filter is SET - will filter INCLUDE field predicates");
        } else {
            LOGGER.trace("Tuple filter is NULL - no INCLUDE field filtering");
        }

        // Get index access parameters
        IIndexAccessParameters iap = ((LSMVTreeOpContext) opCtx).getIndexAccessParameters();

        // Initialize vector accessor from factory in parameters
        IVTreeBinaryAccessorFactory vectorAccessorFactory =
                (IVTreeBinaryAccessorFactory) iap.getParameters().get(HyracksConstants.VECTOR_QUERY);
        if (vectorAccessorFactory != null) {
            this.vectorAccessor = vectorAccessorFactory.createAccessor();
        }

        // Create cluster selection strategy (minProbeFraction → nprobe + DFS fallback)
        this.clusterStrategy = new NprobeClusterSelectionStrategy(vectorPred.getMinProbeFraction(), epsilon);

        // Create spillable top-K buffer (follows inverted index pattern: pass ctx via IAP)
        IHyracksTaskContext ctx = (IHyracksTaskContext) iap.getParameters().get(HyracksConstants.HYRACKS_TASK_CONTEXT);
        this.topKBuffer = new SpillableTopKBuffer(candidateLimit, ctx);

        // Initialize cluster tracking arrays
        currentClusterIndex = new int[numComponents];
        Arrays.fill(currentClusterIndex, 0);
        clusterExhausted = new boolean[numComponents];
        Arrays.fill(clusterExhausted, false);
        stopAdvancing = false;
        clustersExplored = 0;

        // Create accessors and cursors for each component (same as LSMVTreeSearchCursor)
        vTreeAccessors = new VTreeAccessor[numComponents];
        rangeCursors = new IIndexCursor[numComponents];

        for (int i = 0; i < numComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            VTree vTree = (VTree) component.getIndex();
            vTreeAccessors[i] = (VTreeAccessor) vTree.createAccessor(iap);
            rangeCursors[i] = vTreeAccessors[i].createSearchCursor(false);
        }

        // Open all cursors with the search predicate
        IndexCursorUtils.open(vTreeAccessors, rangeCursors, searchPred);

        long tNavStart = System.nanoTime();

        // Initialize strategy and set up DFS fallback (same as LSMVTreeSearchCursor)
        if (numComponents > 0) {
            this.firstSearchCursor = (VTreeSearchCursor) rangeCursors[0];
            this.queryVector = firstSearchCursor.getQueryVector();
            this.distanceFunction = firstSearchCursor.getDistanceFunction();

            // Extract quantized state from first cursor (null = non-quantized path)
            this.quantizedQueryVector = firstSearchCursor.getQuantizedQueryVector();
            this.quantizer = firstSearchCursor.getQuantizer();

            if (this.queryVector == null) {
                throw HyracksDataException
                        .create(new IllegalArgumentException("Query vector must be provided for naive blocked search"));
            }

            // Initialize strategy with first component's tree (candidateLimit so we collect 2*K for reranking)
            ILSMComponent firstComponent = operationalComponents.get(0);
            VTree vTree = (VTree) firstComponent.getIndex();
            clusterStrategy.initialize(vTree, queryVector, distanceFunction, candidateLimit);

            // Set first cursor for DFS fallback
            clusterStrategy.setFirstCursorForDFS(firstSearchCursor);

            // Pass shared visited set from strategy to all cursors
            Set<Integer> visitedSet = clusterStrategy.getVisitedCentroidIds();
            for (int i = 0; i < numComponents; i++) {
                if (rangeCursors[i] instanceof VTreeSearchCursor) {
                    ((VTreeSearchCursor) rangeCursors[i]).setSharedVisitedSet(visitedSet);
                }
            }

            // Re-open all cursors to the first level-wise cluster for consistency
            ClusterSearchResult firstCluster = clusterStrategy.getFirstCluster();
            if (firstCluster != null) {
                ClusterSearchResult dfsCluster = firstSearchCursor.getCurrentClusterResult();
                if (dfsCluster != null && dfsCluster.centroidId != firstCluster.centroidId) {
                    LOGGER.trace("DFS found cid={} but level-wise[0] is cid={} - re-opening", dfsCluster.centroidId,
                            firstCluster.centroidId);
                    for (int i = 0; i < numComponents; i++) {
                        if (rangeCursors[i] instanceof VTreeSearchCursor) {
                            VTreeSearchCursor vcCursor = (VTreeSearchCursor) rangeCursors[i];
                            vcCursor.resetClustersProbed();
                            vcCursor.openClusterByResult(firstCluster);
                        }
                    }
                }
                LOGGER.trace("Initialized with K={}, nprobe={}, epsilon={}, level-wise clusters={}", K, nprobe, epsilon,
                        clusterStrategy.getLevelWiseClusterCount());
            }
        }

        long tNavEnd = System.nanoTime();

        // Initialize priority queue for merging results from all components
        initPriorityQueue();

        // Perform the blocked search: drain all clusters and collect results
        performBlockedSearch();

        long tScanEnd = System.nanoTime();

        // Prepare drain: sort entries by dqx ascending for output (with merge if spilled)
        this.drainIterator = topKBuffer.drain();

        long tDrainEnd = System.nanoTime();

        // Record the last cluster
        finishCurrentClusterTracking();

        LOGGER.warn(String.format("[Timing] navigation=%.2fms, scan=%.2fms, drain=%.2fms, total=%.2fms",
                (tNavEnd - tNavStart) / 1e6, (tScanEnd - tNavEnd) / 1e6, (tDrainEnd - tScanEnd) / 1e6,
                (tDrainEnd - tNavStart) / 1e6));

        // Log per-cluster contribution summary (insertions during search)
        logClusterContributions();
    }

    private void finishCurrentClusterTracking() {
        if (currentClusterForTracking != null && currentClusterTuplesScanned > 0) {
            clusterContributions.put(currentClusterForTracking.centroidId,
                    new int[] { currentClusterTuplesScanned, currentClusterTuplesInserted });
        }
    }

    private void logClusterContributions() {
        if (clusterContributions.isEmpty()) {
            return;
        }
        int contributing = 0;
        int totalScanned = 0;
        int totalInserted = 0;
        StringBuilder sb = new StringBuilder();
        sb.append("[ClusterContributions] K=").append(K).append(", clustersProbed=").append(clusterContributions.size())
                .append("\n");
        for (Map.Entry<Integer, int[]> entry : clusterContributions.entrySet()) {
            int cid = entry.getKey();
            int scanned = entry.getValue()[0];
            int inserted = entry.getValue()[1];
            totalScanned += scanned;
            totalInserted += inserted;
            if (inserted > 0) {
                contributing++;
            }
            sb.append(String.format("  cid=%3d: scanned=%4d, insertedToTopK=%3d%s\n", cid, scanned, inserted,
                    inserted > 0 ? " *" : ""));
        }
        sb.append(String.format("  TOTAL: %d clusters probed, %d contributing, %d scanned, %d inserted",
                clusterContributions.size(), contributing, totalScanned, totalInserted));
        LOGGER.warn(sb.toString());
    }

    private void logFinalTopKComposition() {
        if (finalTopKClusters.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[FinalTopK] K=").append(K).append(", returned=").append(nextCallCount).append(", from ")
                .append(finalTopKClusters.size()).append(" clusters:\n");
        // Sort by count descending
        finalTopKClusters.entrySet().stream().sorted(Map.Entry.<Integer, Integer> comparingByValue().reversed())
                .forEach(e -> sb
                        .append(String.format("  cid=%3d: %d tuples in final top-K\n", e.getKey(), e.getValue())));
        LOGGER.warn(sb.toString());
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

        // Track first cluster
        if (firstSearchCursor != null) {
            currentClusterForTracking = firstSearchCursor.getCurrentClusterResult();
            currentClusterTuplesScanned = 0;
            currentClusterTuplesInserted = 0;
        }

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
                    currentClusterTuplesScanned++;
                    // Apply INCLUDE field filter
                    if (passesTupleFilter(validTuple)) {
                        validTuplesFromCurrentCluster++;
                        // Compute approximate distance using quantized embedding
                        double dqx = computeApproximateDistance(validTuple);
                        boolean accepted = topKBuffer.wouldAccept(dqx);
                        addToTopKWindow(validTuple, dqx);
                        if (accepted) {
                            currentClusterTuplesInserted++;
                        }
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
            if (clusterStrategy.shouldStopAdvancing(minClustersExplored, topKBuffer.getNumEntries())) {
                stopAdvancing = true;
                LOGGER.trace("Early termination: clusters={}, topK={}", minClustersExplored,
                        topKBuffer.getNumEntries());
                break;
            }

            // Try to advance to next cluster
            if (!clusterStrategy.hasMoreClusters()) {
                LOGGER.trace("No more clusters available");
                break;
            }

            // Exclude empty clusters from nprobe
            if (validTuplesFromCurrentCluster == 0) {
                for (int i = 0; i < rangeCursors.length; i++) {
                    if (rangeCursors[i] instanceof VTreeSearchCursor) {
                        ((VTreeSearchCursor) rangeCursors[i]).decrementClustersProbed();
                    }
                }
            }
            validTuplesFromCurrentCluster = 0;
            advanceAllComponentsToNextCluster();
        }
    }

    /**
     * Get next valid tuple with antimatter reconciliation.
     * Follows the checkPriorityQueue() pattern from LSMVTreeSearchCursor.
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

            // Record stats for the cluster we just finished
            finishCurrentClusterTracking();

            ClusterSearchResult nextCluster = clusterStrategy.getNextCluster();
            if (nextCluster == null) {
                LOGGER.trace("No more clusters available globally");
                Arrays.fill(clusterExhausted, true);
                stopAdvancing = true;
                return;
            }

            // Start tracking the new cluster
            currentClusterForTracking = nextCluster;
            currentClusterTuplesScanned = 0;
            currentClusterTuplesInserted = 0;

            LOGGER.trace("Advancing to cluster cid={}, distance={}, dirPage={}", nextCluster.centroidId,
                    nextCluster.distance, nextCluster.directoryPageId);

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

        if (!(cursor instanceof VTreeSearchCursor)) {
            clusterExhausted[componentIndex] = true;
            return;
        }

        VTreeSearchCursor vcCursor = (VTreeSearchCursor) cursor;
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
     * Get the minimum number of clusters probed across all VTreeSearchCursors.
     */
    private int getMinClustersProbed() {
        int minProbed = Integer.MAX_VALUE;
        for (int i = 0; i < rangeCursors.length; i++) {
            if (rangeCursors[i] instanceof VTreeSearchCursor) {
                int probed = ((VTreeSearchCursor) rangeCursors[i]).getClustersProbed();
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
            LOGGER.trace("Tuple filtered out (total: {}) | pk={}", tuplesFilteredOut, pkValue);
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
        // Read quantized embedding bytes from field 3 (Q_QUANTIZED_EMBEDDING_FIELD).
        // Field 3 uses ByteArrayPointable serialization: VarLen length prefix + content bytes.
        // Strip the prefix and pass raw content bytes to the quantizer.
        int vectorFieldIndex = VTreeDataTupleConstants.Q_QUANTIZED_EMBEDDING_FIELD;
        byte[] data = tuple.getFieldData(vectorFieldIndex);
        int offset = tuple.getFieldStart(vectorFieldIndex);

        int contentLength = ByteArrayPointable.getContentLength(data, offset);
        int prefixSize = ByteArrayPointable.getNumberBytesToStoreMeta(contentLength);
        int contentOffset = offset + prefixSize;

        byte[] qBytes = new byte[contentLength];
        System.arraycopy(data, contentOffset, qBytes, 0, contentLength);
        double[] dequantized = quantizer.dequantize(qBytes);
        return distanceFunction.apply(quantizedQueryVector, dequantized);
    }

    /**
     * Add tuple to top-K window if it improves the results.
     * Delegates to SpillableTopKBuffer which handles frame storage, eviction, and disk spill.
     */
    private void addToTopKWindow(ITupleReference tuple, double dqx) throws HyracksDataException {
        topKBuffer.insert(tuple, dqx);
    }

    // ==================== IIndexCursor Interface ====================

    @Override
    public boolean hasNext() throws HyracksDataException {
        return drainIterator != null && drainIterator.hasNext();
    }

    @Override
    public void next() throws HyracksDataException {
        if (!hasNext()) {
            throw HyracksDataException.create(new IllegalStateException("No more tuples"));
        }
        drainIterator.next();
        nextCallCount++;
        // Track which cluster this final result came from (centroidId is field 1)
        try {
            ITupleReference t = drainIterator.getTuple();
            if (t != null && t.getFieldCount() > 1) {
                int cid = org.apache.hyracks.data.std.primitive.IntegerPointable.getInteger(t.getFieldData(1),
                        t.getFieldStart(1));
                finalTopKClusters.merge(cid, 1, Integer::sum);
            }
        } catch (Exception e) {
            // ignore tracking errors
        }
    }

    @Override
    public ITupleReference getTuple() {
        if (drainIterator == null) {
            return null;
        }
        return drainIterator.getTuple();
    }

    @Override
    public void close() throws HyracksDataException {
        if (isOpen) {
            logFinalTopKComposition();

            LOGGER.trace("Search Summary: K={}, nprobe={}, epsilon={}", K, nprobe, epsilon);
            LOGGER.trace("Clusters explored:          {}", clustersExplored);
            LOGGER.trace("Total tuples processed:     {}", totalTuplesProcessed);
            LOGGER.trace("Antimatter cancellations:   {}", antimatterCancellations);
            LOGGER.trace("Tuples filtered out:        {}", tuplesFilteredOut);
            LOGGER.trace("Results returned (next()):   {}", nextCallCount);

            for (int i = 0; i < numComponents; i++) {
                if (rangeCursors[i] != null) {
                    rangeCursors[i].close();
                }
            }
            if (drainIterator != null) {
                drainIterator.close();
                drainIterator = null;
            }
            if (topKBuffer != null) {
                topKBuffer.close();
                topKBuffer = null;
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
     * Priority queue element holding tuple and component info (for multi-component merging).
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
     * Priority queue comparator for merging results from multiple components.
     * Compares by distance (field 0), then PK fields, then component ID.
     */
    private class NaivePriorityQueueComparator implements Comparator<PriorityQueueElement> {
        @Override
        public int compare(PriorityQueueElement a, PriorityQueueElement b) {
            ITupleReference tupleA = a.tuple;
            ITupleReference tupleB = b.tuple;

            try {
                int result = cmp.getComparators()[0].compare(tupleA.getFieldData(0), tupleA.getFieldStart(0),
                        tupleA.getFieldLength(0), tupleB.getFieldData(0), tupleB.getFieldStart(0),
                        tupleB.getFieldLength(0));
                if (result != 0) {
                    return result;
                }

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

            return Integer.compare(a.componentId, b.componentId);
        }
    }
}
