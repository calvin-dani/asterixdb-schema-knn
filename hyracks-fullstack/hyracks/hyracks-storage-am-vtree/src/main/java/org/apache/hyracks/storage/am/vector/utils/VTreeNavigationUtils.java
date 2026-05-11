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
package org.apache.hyracks.storage.am.vector.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class for VTree navigation operations.
 * Contains common logic for finding closest centroids in tree structures.
 */
public class VTreeNavigationUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final ThreadLocal<Long> distanceComputations = ThreadLocal.withInitial(() -> 0L);
    private static final ThreadLocal<Long> pagePins = ThreadLocal.withInitial(() -> 0L);
    private static final ThreadLocal<String> levelBreakdown = ThreadLocal.withInitial(() -> "");

    public static void resetCounters() {
        distanceComputations.set(0L);
        pagePins.set(0L);
        levelBreakdown.set("");
    }

    public static long getAndResetDistanceComputations() {
        long val = distanceComputations.get();
        distanceComputations.set(0L);
        return val;
    }

    public static long getAndResetPagePins() {
        long val = pagePins.get();
        pagePins.set(0L);
        return val;
    }

    public static String getAndResetLevelBreakdown() {
        String val = levelBreakdown.get();
        levelBreakdown.set("");
        return val;
    }

    /**
     * Find the closest centroid by traversing the tree from root to leaf,
     * optionally computing a quantized distance for the best result.
     *
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param rootPageId Root page ID to start traversal
     * @param interiorFrameFactory Factory for creating interior frames
     * @param leafFrameFactory Factory for creating leaf frames
     * @param queryVector Query vector to find closest centroid for
     * @param distanceFunction Distance function to use for centroid finding
     * @param quantizedQueryVector Quantized form of queryVector (nullable — pass null to skip quantized distance)
     * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable — pass null to skip)
     * @return ClusterSearchResult containing closest centroid information (with quantizedDistance if quantizer provided)
     * @throws HyracksDataException if any error occurs during traversal
     */
    public static ClusterSearchResult findClosestCentroid(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory, double[] queryVector,
            IVTreeDistanceFunction distanceFunction, double[] quantizedQueryVector, IVTreeQuantizer quantizer)
            throws HyracksDataException {

        // Start from root page
        int currentPageId = rootPageId;
        ClusterSearchResult bestResult = null;
        int loopCounter = 0; // Safety check to prevent infinite loops

        // Traverse from root to leaf
        while (true) {
            loopCounter++;
            if (loopCounter > 10) { // Safety check to prevent infinite loops
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Infinite loop detected in tree traversal");
            }

            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));

            try {
                page.acquireReadLatch();

                // Check if this is a leaf page
                IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                if (isLeaf) {
                    // Leaf level - collect all centroids and pick the closest
                    List<VTreeLeafCentroid> sortedCentroids =
                            collectAllLeafCentroids(bufferCache, fileId, queryVector, currentPageId, leafFrame,
                                    leafFrameFactory, distanceFunction, quantizedQueryVector, quantizer);
                    if (!sortedCentroids.isEmpty()) {
                        VTreeLeafCentroid best = sortedCentroids.get(0);
                        bestResult = ClusterSearchResult.create(best.pageId, best.tupleIndex, best.centroid,
                                best.distance, best.centroidId, best.directoryPageId, best.quantizedDistance);
                    }
                    break; // Found leaf level result

                } else {
                    // Interior level - collect all children and descend to closest
                    IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);
                    List<VTreeChildCentroid> sortedChildren = collectAllChildCentroids(bufferCache, fileId, queryVector,
                            currentPageId, interiorFrame, interiorFrameFactory, distanceFunction);
                    if (sortedChildren.isEmpty()) {
                        throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                                "No valid centroid found in interior cluster");
                    }

                    currentPageId = sortedChildren.get(0).childPageId;
                }

            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        if (bestResult == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No closest cluster found");
        }

        return bestResult;
    }

    /**
     * Extract centroid from an interior frame tuple (format: <cid, centroid, child_ptr>).
     * Uses TupleUtils.deserializeTuple() which correctly handles TypeAwareTupleWriter's
     * VarLengthInt field size encoding.
     */
    private static double[] extractCentroidFromInteriorTuple(ITreeIndexTupleReference tuple)
            throws HyracksDataException {
        // Centroid is always field 1. Tuple format varies:
        // Interior/non-quantized leaf: <cid:int, centroid:double[], ptr:int> (3 fields)
        // Quantized leaf: <cid:int, centroid:double[], quantizedBytes:byte[], ptr:int> (4 fields)
        // We only need fields 0 and 1, so deserialize just those.
        ISerializerDeserializer<?>[] fieldSerdes = new ISerializerDeserializer<?>[2];
        fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE;
        fieldSerdes[1] = DoubleArraySerializerDeserializer.INSTANCE;

        Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (double[]) fieldValues[1];
    }

    /**
     * Collect all child centroids from an interior page and its overflow chain, sorted by distance.
     * Unified method that replaces findClosestInInteriorPage, collectAndSortChildren,
     * collectChildrenFromOverflow, and collectChildrenForLevelWise.
     *
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param queryVector Query vector to compute distances against
     * @param startPageId Starting page ID of the interior page
     * @param initialFrame Interior frame already set to the initial page (already pinned by caller)
     * @param interiorFrameFactory Factory for creating interior frames for overflow pages
     * @param distanceFunction Distance function to use
     * @return List of child centroids sorted by distance (closest first)
     * @throws HyracksDataException if any error occurs
     */
    private static List<VTreeChildCentroid> collectAllChildCentroids(IBufferCache bufferCache, int fileId,
            double[] queryVector, int startPageId, IVTreeInteriorFrame initialFrame,
            ITreeIndexFrameFactory interiorFrameFactory, IVTreeDistanceFunction distanceFunction)
            throws HyracksDataException {

        List<VTreeChildCentroid> children = new ArrayList<>();
        int currentPageId = startPageId;
        IVTreeInteriorFrame currentFrame = initialFrame;
        boolean isFirstPage = true;
        ICachedPage currentPage = null;

        while (currentPageId != -1) {
            try {
                if (!isFirstPage) {
                    currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    pagePins.set(pagePins.get() + 1);
                    currentPage.acquireReadLatch();
                    currentFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
                    currentFrame.setPage(currentPage);
                }

                int tupleCount = currentFrame.getTupleCount();
                boolean hasOverflow = currentFrame.getOverflowFlagBit();
                int nextPageId = hasOverflow ? currentFrame.getNextPage() : -1;

                for (int i = 0; i < tupleCount; i++) {
                    try {
                        ITreeIndexTupleReference tuple = currentFrame.createTupleReference();
                        tuple.resetByTupleIndex(currentFrame, i);
                        double[] centroid = extractCentroidFromInteriorTuple(tuple);

                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = distanceFunction.apply(queryVector, centroid);
                        distanceComputations.set(distanceComputations.get() + 1);
                        int childPageId = currentFrame.getChildPageId(i);
                        children.add(new VTreeChildCentroid(childPageId, distance, i));
                    } catch (Exception e) {
                        continue;
                    }
                }

                currentPageId = nextPageId;
                isFirstPage = false;

            } finally {
                if (!isFirstPage && currentPage != null) {
                    currentPage.releaseReadLatch();
                    bufferCache.unpin(currentPage);
                    currentPage = null;
                }
            }
        }

        children.sort(Comparator.comparingDouble(c -> c.distance));
        return children;
    }

    /**
     * Collect all leaf centroids from a leaf page and its overflow chain, sorted by distance.
    * Optionally computes quantized D(q,C) for each centroid when quantizer is provided.
     * Unified method that replaces findClosestInLeafPage, collectAndSortLeafCentroids,
     * collectCentroidsFromOverflow, and collectLeafCentroidsForLevelWise.
     *
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param queryVector Query vector to compute distances against
     * @param startPageId Starting page ID of the leaf page
     * @param initialFrame Leaf frame already set to the initial page (already pinned by caller)
     * @param leafFrameFactory Factory for creating leaf frames for overflow pages
     * @param distanceFunction Distance function to use
     * @param quantizedQueryVector Quantized form of queryVector (nullable — pass null to skip)
     * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable — pass null to skip)
     * @return List of leaf centroids sorted by distance (closest first)
     * @throws HyracksDataException if any error occurs
     */
    private static List<VTreeLeafCentroid> collectAllLeafCentroids(IBufferCache bufferCache, int fileId,
            double[] queryVector, int startPageId, IVTreeLeafFrame initialFrame,
            ITreeIndexFrameFactory leafFrameFactory, IVTreeDistanceFunction distanceFunction,
            double[] quantizedQueryVector, IVTreeQuantizer quantizer) throws HyracksDataException {

        List<VTreeLeafCentroid> centroids = new ArrayList<>();
        int currentPageId = startPageId;
        IVTreeLeafFrame currentFrame = initialFrame;
        boolean isFirstPage = true;
        ICachedPage currentPage = null;

        while (currentPageId != -1) {
            try {
                if (!isFirstPage) {
                    currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    pagePins.set(pagePins.get() + 1);
                    currentPage.acquireReadLatch();
                    currentFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                    currentFrame.setPage(currentPage);
                }

                int tupleCount = currentFrame.getTupleCount();
                boolean hasOverflow = currentFrame.getOverflowFlagBit();
                int nextPageId = hasOverflow ? currentFrame.getNextLeaf() : -1;

                for (int i = 0; i < tupleCount; i++) {
                    try {
                        ITreeIndexTupleReference frameTuple = currentFrame.createTupleReference();
                        frameTuple.resetByTupleIndex(currentFrame, i);
                        double[] centroid = extractCentroidFromInteriorTuple(frameTuple);
                        int centroidId = currentFrame.getCentroidId(i);
                        long directoryPageId = currentFrame.getMetadataPagePointer(i);

                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = distanceFunction.apply(queryVector, centroid);
                        distanceComputations.set(distanceComputations.get() + 1);

                        double quantizedDistance = Double.NaN;
                        if (quantizer != null && quantizedQueryVector != null) {
                            byte[] quantizedCentroidBytes = currentFrame.getQuantizedCentroidBytes(i);
                            if (quantizedCentroidBytes != null) {
                                double[] dequantizedCentroid = quantizer.dequantize(quantizedCentroidBytes);
                                quantizedDistance = distanceFunction.apply(quantizedQueryVector, dequantizedCentroid);
                                distanceComputations.set(distanceComputations.get() + 1);
                            }
                        }

                        centroids.add(new VTreeLeafCentroid(centroidId, distance, i, currentPageId, centroid.clone(),
                                directoryPageId, quantizedDistance));
                    } catch (Exception e) {
                        continue;
                    }
                }

                currentPageId = nextPageId;
                isFirstPage = false;

            } finally {
                if (!isFirstPage && currentPage != null) {
                    currentPage.releaseReadLatch();
                    bufferCache.unpin(currentPage);
                    currentPage = null;
                }
            }
        }

        centroids.sort(Comparator.comparingDouble(c -> c.distance));
        return centroids;
    }

    // ==================== Multi-Cluster Iterative DFS Support ====================

    /**
     * Initialize the cluster iterator by building navigation stack from root to first leaf.
     * This performs DFS to find the closest cluster and sets up the stack for backtracking.
     *
     * @param state Navigation state to initialize
     * @return The first (closest) cluster, or null if tree is empty
     * @throws HyracksDataException if any error occurs
     */
    public static ClusterSearchResult initializeClusterIterator(VTreeNavigationState state,
            IVTreeDistanceFunction distanceFunction) throws HyracksDataException {
        if (state.initialized) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Iterator already initialized");
        }

        state.stack.clear();
        state.initialized = true;

        // Start DFS from root
        int currentPageId = state.rootPageId;

        while (true) {
            ICachedPage page = state.bufferCache.pin(BufferedFileHandle.getDiskPageId(state.fileId, currentPageId));
            try {
                page.acquireReadLatch();

                // Check if leaf
                IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) state.leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                if (isLeaf) {
                    // At leaf level: collect and sort all centroids in this page (including overflow)
                    List<VTreeLeafCentroid> sortedCentroids =
                            collectAllLeafCentroids(state.bufferCache, state.fileId, state.queryVector, currentPageId,
                                    leafFrame, state.leafFrameFactory, distanceFunction, null, null);

                    if (sortedCentroids.isEmpty()) {
                        return null; // Empty tree
                    }

                    // Push leaf frame onto stack
                    VTreeNavigationFrame leafFrame_nav = new VTreeNavigationFrame(currentPageId, sortedCentroids, true);
                    state.stack.push(leafFrame_nav);

                    // Return first centroid as closest cluster, marking it visited
                    VTreeLeafCentroid first = leafFrame_nav.nextCentroid();
                    state.markVisited(first.centroidId);
                    return ClusterSearchResult.create(first.pageId, first.tupleIndex, first.centroid, first.distance,
                            first.centroidId, first.directoryPageId);

                } else {
                    // Interior level: collect and sort children
                    IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) state.interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);

                    List<VTreeChildCentroid> sortedChildren =
                            collectAllChildCentroids(state.bufferCache, state.fileId, state.queryVector, currentPageId,
                                    interiorFrame, state.interiorFrameFactory, distanceFunction);

                    if (sortedChildren.isEmpty()) {
                        throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                                "Interior node has no valid children");
                    }

                    // Push interior frame onto stack
                    VTreeNavigationFrame interiorFrame_nav = new VTreeNavigationFrame(currentPageId, sortedChildren);
                    state.stack.push(interiorFrame_nav);

                    // Descend to closest child
                    VTreeChildCentroid closest = interiorFrame_nav.nextChild();
                    currentPageId = closest.childPageId;
                }

            } finally {
                page.releaseReadLatch();
                state.bufferCache.unpin(page);
            }
        }
    }

    /**
     * Find the next closest cluster using DFS with backtracking.
     *
     * Algorithm:
     * 1. Try next centroid on current leaf page
     * 2. If leaf exhausted, pop stack (backtrack to parent)
     * 3. Try next child from parent
     * 4. Descend to new leaf
     * 5. Return next centroid
     *
     * @param state Navigation state with stack
     * @return Next closest cluster, or null if all clusters exhausted
     * @throws HyracksDataException if any error occurs
     */
    public static ClusterSearchResult findNextClosestCluster(VTreeNavigationState state,
            IVTreeDistanceFunction distanceFunction) throws HyracksDataException {
        if (!state.initialized) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "Iterator not initialized. Call initializeClusterIterator() first");
        }

        while (!state.stack.isEmpty()) {
            VTreeNavigationFrame topFrame = state.stack.peek();

            if (topFrame.isLeaf) {
                // At leaf level: try next centroid in current page, skipping visited ones
                while (topFrame.hasNext()) {
                    VTreeLeafCentroid next = topFrame.nextCentroid();

                    // Skip if already visited (e.g., by level-wise exploration)
                    if (state.isVisited(next.centroidId)) {
                        LOGGER.log(Level.TRACE, String.format("[DFS] Skipping visited centroid: cid=%d, distance=%.4f",
                                next.centroidId, next.distance));
                        continue;
                    }

                    // Mark as visited and return
                    state.markVisited(next.centroidId);
                    LOGGER.log(Level.TRACE,
                            String.format("[DFS] Returning centroid: cid=%d, distance=%.4f, pageId=%d, nextIndex=%d/%d",
                                    next.centroidId, next.distance, topFrame.pageId, topFrame.nextIndex,
                                    topFrame.sortedCentroids.size()));
                    return ClusterSearchResult.create(next.pageId, next.tupleIndex, next.centroid, next.distance,
                            next.centroidId, next.directoryPageId);
                }

                // All centroids in this leaf exhausted or visited, backtrack
                state.stack.pop();
                continue;

            } else {
                // At interior level: try next child
                if (topFrame.hasNext()) {
                    VTreeChildCentroid nextChild = topFrame.nextChild();

                    // Descend to this child and navigate to leaf
                    ClusterSearchResult result = descendToLeaf(state, nextChild.childPageId, distanceFunction);
                    if (result != null) {
                        return result;
                    }
                    // If descend failed, continue with next child
                    continue;

                } else {
                    // All children explored, backtrack
                    state.stack.pop();
                    continue;
                }
            }
        }

        // Stack exhausted, no more clusters
        LOGGER.log(Level.TRACE, "[DFS] Stack exhausted, no more clusters available");
        return null;
    }

    /**
     * Descend from given page to leaf level, building stack along the way.
     * Always picks closest child at each interior level.
     *
     * @param state Navigation state
     * @param startPageId Page to start descent from
     * @return First centroid at leaf level, or null if no valid path
     * @throws HyracksDataException if any error occurs
     */
    private static ClusterSearchResult descendToLeaf(VTreeNavigationState state, int startPageId,
            IVTreeDistanceFunction distanceFunction) throws HyracksDataException {

        int currentPageId = startPageId;

        while (true) {
            ICachedPage page = state.bufferCache.pin(BufferedFileHandle.getDiskPageId(state.fileId, currentPageId));
            try {
                page.acquireReadLatch();

                // Check if leaf
                IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) state.leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                if (isLeaf) {
                    // Reached leaf: collect and sort centroids
                    List<VTreeLeafCentroid> sortedCentroids =
                            collectAllLeafCentroids(state.bufferCache, state.fileId, state.queryVector, currentPageId,
                                    leafFrame, state.leafFrameFactory, distanceFunction, null, null);

                    if (sortedCentroids.isEmpty()) {
                        return null; // Empty leaf
                    }

                    // Push leaf frame onto stack
                    VTreeNavigationFrame leafFrame_nav = new VTreeNavigationFrame(currentPageId, sortedCentroids, true);
                    state.stack.push(leafFrame_nav);

                    // Find first unvisited centroid
                    while (leafFrame_nav.hasNext()) {
                        VTreeLeafCentroid first = leafFrame_nav.nextCentroid();
                        if (!state.isVisited(first.centroidId)) {
                            state.markVisited(first.centroidId);
                            return ClusterSearchResult.create(first.pageId, first.tupleIndex, first.centroid,
                                    first.distance, first.centroidId, first.directoryPageId);
                        }
                        // Skip visited centroid
                        LOGGER.log(Level.TRACE, String.format("[DFS descendToLeaf] Skipping visited centroid: cid=%d",
                                first.centroidId));
                    }
                    // All centroids in this leaf are visited
                    return null;

                } else {
                    // Interior: collect and sort children
                    IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) state.interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);

                    List<VTreeChildCentroid> sortedChildren =
                            collectAllChildCentroids(state.bufferCache, state.fileId, state.queryVector, currentPageId,
                                    interiorFrame, state.interiorFrameFactory, distanceFunction);

                    if (sortedChildren.isEmpty()) {
                        return null; // No valid children
                    }

                    // Push interior frame
                    VTreeNavigationFrame interiorFrame_nav = new VTreeNavigationFrame(currentPageId, sortedChildren);
                    state.stack.push(interiorFrame_nav);

                    // Descend to closest child
                    VTreeChildCentroid closest = interiorFrame_nav.nextChild();
                    currentPageId = closest.childPageId;
                }

            } finally {
                page.releaseReadLatch();
                state.bufferCache.unpin(page);
            }
        }
    }

    // ==================== Level-Wise Cluster Selection Support ====================

    /**
     * Find close centroids using level-by-level cross-pollination with global sorting.
     * At each interior node, explores all children within closestDistance + epsilon.
     * At leaf level, collects ALL centroids, then sorts globally and filters by epsilon.
     *
     * This is the FAISS/SPANN-style approach:
     * 1. Traverse tree using epsilon threshold at interior levels
     * 2. Collect ALL reachable leaf centroids
     * 3. Sort globally by distance to query
     * 4. Filter by global closest distance + epsilon
     *
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param rootPageId Root page ID to start traversal
     * @param interiorFrameFactory Factory for creating interior frames
     * @param leafFrameFactory Factory for creating leaf frames
     * @param queryVector Query vector to find closest centroids for
     * @param distanceFunction Distance function to use
     * @param epsilon Absolute distance threshold added to closest distance
     * @return List of ClusterSearchResult containing all qualifying centroids, sorted by distance
     * @throws HyracksDataException if any error occurs during traversal
     */
    public static List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(IBufferCache bufferCache, int fileId,
            int rootPageId, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector, IVTreeDistanceFunction distanceFunction, double epsilon) throws HyracksDataException {
        return findCloseCentroidsLevelWiseGlobalSort(bufferCache, fileId, rootPageId, interiorFrameFactory,
                leafFrameFactory, queryVector, distanceFunction, epsilon, null, null);
    }

    /**
     * Overload that accepts quantizer parameters for computing quantized D(q,C).
     * Navigation still uses full-precision distances; quantizedDistance is extra metadata
     * populated in each ClusterSearchResult for triangle inequality pruning at the cursor level.
     *
     * @param quantizedQueryVector Dequantized form of query vector (nullable)
     * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable)
     */
    public static List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(IBufferCache bufferCache, int fileId,
            int rootPageId, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector, IVTreeDistanceFunction distanceFunction, double epsilon,
            double[] quantizedQueryVector, IVTreeQuantizer quantizer) throws HyracksDataException {

        List<ClusterSearchResult> allCentroids = new ArrayList<>();
        Set<Integer> visitedLeafPages = new HashSet<>();
        Queue<VTreeLevelNode> queue = new ArrayDeque<>();
        queue.add(new VTreeLevelNode(rootPageId, 0));
        StringBuilder breakdown = new StringBuilder();

        // Phase 1: Collect all centroids from all reachable leaf pages
        while (!queue.isEmpty()) {
            int currentLevel = queue.peek().level;
            List<VTreeLevelNode> currentLevelNodes = new ArrayList<>();

            // Collect all nodes at current level
            while (!queue.isEmpty() && queue.peek().level == currentLevel) {
                currentLevelNodes.add(queue.poll());
            }

            long levelDistComps = 0;
            long levelPagePins = 0;
            int levelCentroidsFound = 0;
            int levelChildrenExplored = 0;
            boolean isLeafLevel = false;

            // Process all nodes at current level
            for (VTreeLevelNode node : currentLevelNodes) {
                long preDistComps = distanceComputations.get();
                long prePagePins = pagePins.get();

                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, node.pageId));
                pagePins.set(pagePins.get() + 1);
                try {
                    page.acquireReadLatch();

                    IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                    leafFrame.setPage(page);
                    boolean isLeaf = leafFrame.isLeaf();

                    if (isLeaf) {
                        isLeafLevel = true;
                        // Leaf node processing - collect ALL centroids (no threshold filtering yet)
                        if (!visitedLeafPages.add(node.pageId)) {
                            continue; // Already visited
                        }

                        List<VTreeLeafCentroid> leafCentroids =
                                collectAllLeafCentroids(bufferCache, fileId, queryVector, node.pageId, leafFrame,
                                        leafFrameFactory, distanceFunction, quantizedQueryVector, quantizer);

                        if (leafCentroids.isEmpty()) {
                            continue;
                        }

                        levelCentroidsFound += leafCentroids.size();

                        // Add ALL centroids from this leaf page to global collection
                        for (VTreeLeafCentroid centroid : leafCentroids) {
                            allCentroids.add(ClusterSearchResult.create(centroid.pageId, centroid.tupleIndex,
                                    centroid.centroid, centroid.distance, centroid.centroidId, centroid.directoryPageId,
                                    centroid.quantizedDistance));
                        }

                    } else {
                        // Interior node processing - explore children within epsilon
                        IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
                        interiorFrame.setPage(page);

                        List<VTreeChildCentroid> sortedChildren = collectAllChildCentroids(bufferCache, fileId,
                                queryVector, node.pageId, interiorFrame, interiorFrameFactory, distanceFunction);

                        if (sortedChildren.isEmpty()) {
                            continue;
                        }

                        double closestDistance = sortedChildren.get(0).distance;
                        double localThreshold =
                                closestDistance < 0 ? closestDistance * (1.0 - epsilon) : closestDistance + epsilon;

                        for (VTreeChildCentroid child : sortedChildren) {
                            if (child.distance <= localThreshold) {
                                queue.add(new VTreeLevelNode(child.childPageId, currentLevel + 1));
                                levelChildrenExplored++;
                            } else {
                                break; // Children are sorted, no more qualify
                            }
                        }
                    }

                } finally {
                    page.releaseReadLatch();
                    bufferCache.unpin(page);
                }
                levelDistComps += distanceComputations.get() - preDistComps;
                levelPagePins += pagePins.get() - prePagePins;
            }

            String type = isLeafLevel ? "LEAF" : "INTERIOR";
            if (isLeafLevel) {
                breakdown.append(String.format("L%d(%s): pages=%d, centroids=%d, distComps=%d, pagePins=%d; ",
                        currentLevel, type, currentLevelNodes.size(), levelCentroidsFound, levelDistComps,
                        levelPagePins));
            } else {
                breakdown.append(String.format(
                        "L%d(%s): pages=%d, centroidsEvaluated=%d, childrenPassedEpsilon=%d, distComps=%d, pagePins=%d; ",
                        currentLevel, type, currentLevelNodes.size(), levelDistComps, levelChildrenExplored,
                        levelDistComps, levelPagePins));
            }
        }
        levelBreakdown.set(breakdown.toString());

        if (allCentroids.isEmpty()) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No closest clusters found");
        }

        // Phase 2: Sort ALL centroids globally by distance to query vector
        allCentroids.sort(Comparator.comparingDouble(r -> r.distance));

        // Phase 3: Apply epsilon threshold based on globally closest centroid
        if (epsilon > 0.0) {
            double globalClosestDistance = allCentroids.get(0).distance;
            double globalThreshold = globalClosestDistance < 0 ? globalClosestDistance * (1.0 - epsilon)
                    : globalClosestDistance + epsilon;

            // Filter centroids that exceed the global threshold
            List<ClusterSearchResult> filteredCentroids = new ArrayList<>();
            for (ClusterSearchResult result : allCentroids) {
                if (result.distance <= globalThreshold) {
                    filteredCentroids.add(result);
                } else {
                    // Centroids are sorted, so we can break early
                    break;
                }
            }

            return filteredCentroids;
        }

        return allCentroids;
    }

}
