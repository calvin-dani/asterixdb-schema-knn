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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
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
 * Static helpers for navigating a VTree. Provides:
 * <ul>
 *   <li>{@link #findClosestCentroid} - root-to-leaf descent picking the single closest centroid.</li>
 *   <li>{@link #initializeClusterIterator} / {@link #findNextClosestCluster} - iterative DFS
 *       over leaf centroids in non-decreasing distance order with visited-id dedup.</li>
 *   <li>{@link #findCloseCentroidsLevelWiseGlobalSort} - level-wise probing with an epsilon
 *       window followed by a global sort and threshold filter at the leaf layer.</li>
 * </ul>
 * All methods pin/unpin buffer-cache pages internally; callers must not hold latches on the
 * traversed pages. Stateless and thread-safe.
 */
public class VTreeNavigationUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    /** Upper bound on routing embedding dimension when deserializing index tuples (guards corrupt pointers). */
    private static final int MAX_ROUTING_EMBEDDING_DIMENSION = 32768;

    /**
     * Maximum number of root-to-leaf hops in {@link #findClosestCentroid}; serves as a safety net
     * against malformed trees / cyclic child pointers. The real VTree is shallow (~3-5 levels).
     */
    private static final int MAX_TREE_DEPTH = 10;

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

        int currentPageId = rootPageId;
        ClusterSearchResult bestResult = null;
        int hops = 0;

        while (true) {
            if (++hops > MAX_TREE_DEPTH) {
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
     * Extract the routing embedding from field 1 of an index tuple.
     * Interior: {@code <cid, embedding, child_ptr>}; leaf (quantized): {@code <cid, embedding, quantizedBytes, metadataPtr>}.
     * Only field 1 is deserialized (never field 2+), with a bounded array length check.
     */
    private static double[] extractCentroidEmbedding(ITreeIndexTupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() < 2) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "Routing tuple has fewer than 2 fields (fieldCount=" + tuple.getFieldCount() + ")");
        }
        return deserializeBoundedDoubleArray(tuple.getFieldData(1), tuple.getFieldStart(1), tuple.getFieldLength(1),
                MAX_ROUTING_EMBEDDING_DIMENSION);
    }

    private static double[] deserializeBoundedDoubleArray(byte[] data, int start, int length, int maxDim)
            throws HyracksDataException {
        if (length < 4) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Embedding field too short: " + length);
        }
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data, start, length));
            int len = dis.readInt();
            if (len <= 0 || len > maxDim) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "Invalid routing embedding length: " + len + " (max " + maxDim + ")");
            }
            long requiredBytes = 4L + (long) len * 8;
            if (length < requiredBytes) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "Embedding field length " + length + " < declared " + requiredBytes + " bytes");
            }
            double[] array = new double[len];
            for (int i = 0; i < len; i++) {
                array[i] = dis.readDouble();
            }
            return array;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Extract centroid from an interior frame tuple (format: {@code <cid, centroid, child_ptr>}).
     */
    private static double[] extractCentroidFromInteriorTuple(ITreeIndexTupleReference tuple)
            throws HyracksDataException {
        return extractCentroidEmbedding(tuple);
    }

    /**
     * Collects every child of an interior page (including its overflow chain) and returns them
     * sorted by distance to {@code queryVector}, closest first. {@code initialFrame} must already
     * be set to {@code startPageId} and that page must be pinned/latched by the caller; overflow
     * pages are pinned and released internally. Malformed tuples are skipped.
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
                        int childPageId = currentFrame.getChildPageId(i);
                        children.add(new VTreeChildCentroid(childPageId, distance, i));
                    } catch (Exception e) {
                        // Skip malformed interior tuples; do not abort the whole page traversal.
                        LOGGER.log(Level.TRACE, "Skipping interior tuple {} on page {}: {}", i, currentPageId,
                                e.getMessage());
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
     * Collects every centroid of a leaf page (including its overflow chain) and returns them
     * sorted by distance to {@code queryVector}, closest first. {@code initialFrame} must already
     * be set to {@code startPageId} and that page must be pinned/latched by the caller; overflow
     * pages are pinned and released internally.
     * <p>
     * When both {@code quantizer} and {@code quantizedQueryVector} are non-null, the quantized
     * distance D(q̃, C̃) is computed for each centroid and placed in the resulting
     * {@link VTreeLeafCentroid#quantizedDistance}; otherwise that field is {@code NaN}.
     * Malformed tuples are skipped.
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
                        double[] centroid = extractCentroidEmbedding(frameTuple);
                        int centroidId = currentFrame.getCentroidId(i);
                        long directoryPageId = currentFrame.getMetadataPagePointer(i);

                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = distanceFunction.apply(queryVector, centroid);

                        double quantizedDistance = Double.NaN;
                        if (quantizer != null && quantizedQueryVector != null) {
                            byte[] quantizedCentroidBytes = currentFrame.getQuantizedCentroidBytes(i);
                            if (quantizedCentroidBytes != null) {
                                double[] dequantizedCentroid = quantizer.dequantize(quantizedCentroidBytes);
                                quantizedDistance = distanceFunction.apply(quantizedQueryVector, dequantizedCentroid);
                            }
                        }

                        centroids.add(new VTreeLeafCentroid(centroidId, distance, i, currentPageId, centroid.clone(),
                                directoryPageId, quantizedDistance));
                    } catch (Exception e) {
                        // Skip malformed leaf tuples; do not abort the whole page traversal.
                        LOGGER.log(Level.TRACE, "Skipping leaf tuple {} on page {}: {}", i, currentPageId,
                                e.getMessage());
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

                    if (state.isVisited(next.centroidId)) {
                        LOGGER.log(Level.TRACE, "[DFS] skipping visited cid={} d={}", next.centroidId, next.distance);
                        continue;
                    }

                    state.markVisited(next.centroidId);
                    LOGGER.log(Level.TRACE, "[DFS] return cid={} d={} pageId={} idx={}/{}", next.centroidId,
                            next.distance, topFrame.pageId, topFrame.nextIndex, topFrame.sortedCentroids.size());
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

        LOGGER.log(Level.TRACE, "[DFS] stack exhausted");
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

                    // Find the first unvisited centroid.
                    while (leafFrame_nav.hasNext()) {
                        VTreeLeafCentroid first = leafFrame_nav.nextCentroid();
                        if (!state.isVisited(first.centroidId)) {
                            state.markVisited(first.centroidId);
                            return ClusterSearchResult.create(first.pageId, first.tupleIndex, first.centroid,
                                    first.distance, first.centroidId, first.directoryPageId);
                        }
                        LOGGER.log(Level.TRACE, "[DFS descendToLeaf] skipping visited cid={}", first.centroidId);
                    }
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
     * At each interior node, explores all children within closestDistance * (1 + epsilon)
     * (for positive distances) / closestDistance * (1 - epsilon) (for negative distances
     * such as negated dot product).
     * At leaf level, collects ALL centroids, then sorts globally and filters by epsilon.
     * <p>
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
     * @param epsilon Relative distance threshold (multiplicative). Threshold for level/global
     *                pruning is computed as {@code closestDistance + |closestDistance| * epsilon},
     *                i.e. (1+epsilon)*d for positive d and (1-epsilon)*d for negative d.
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

        // Phase 1: Collect all centroids from all reachable leaf pages
        while (!queue.isEmpty()) {
            int currentLevel = queue.peek().level;
            List<VTreeLevelNode> currentLevelNodes = new ArrayList<>();

            // Collect all nodes at current level
            while (!queue.isEmpty() && queue.peek().level == currentLevel) {
                currentLevelNodes.add(queue.poll());
            }

            // Process all nodes at current level
            for (VTreeLevelNode node : currentLevelNodes) {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, node.pageId));
                try {
                    page.acquireReadLatch();

                    IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                    leafFrame.setPage(page);
                    boolean isLeaf = leafFrame.isLeaf();

                    if (isLeaf) {
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
                        // Multiplicative epsilon (relative to |minDistance|). Equivalent to (1+epsilon)*minDistance
                        // for positive distances and (1-epsilon)*minDistance for negative ones (dot product is
                        // negated -> smaller=better). The previous additive form (d+epsilon) on the positive side
                        // is essentially a no-op for euclidean_squared / high-dim L2 where distances are
                        // O(10)-O(10^3), so the search collapsed to ~1 cluster regardless of nprobe/epsilon.
                        double localThreshold = closestDistance + Math.abs(closestDistance) * epsilon;

                        for (VTreeChildCentroid child : sortedChildren) {
                            if (child.distance <= localThreshold) {
                                queue.add(new VTreeLevelNode(child.childPageId, currentLevel + 1));
                            } else {
                                break; // Children are sorted, no more qualify
                            }
                        }
                    }

                } finally {
                    page.releaseReadLatch();
                    bufferCache.unpin(page);
                }
            }
        }

        if (allCentroids.isEmpty()) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No closest clusters found");
        }

        // Phase 2: Sort ALL centroids globally by distance to query vector
        allCentroids.sort(Comparator.comparingDouble(r -> r.distance));

        // Phase 3: Apply epsilon threshold based on globally closest centroid
        if (epsilon > 0.0) {
            double globalClosestDistance = allCentroids.get(0).distance;
            // Multiplicative epsilon (relative to |minDistance|). See note on the local threshold above:
            // (1+epsilon)*minDistance for positive distances, (1-epsilon)*minDistance for negative ones.
            double globalThreshold = globalClosestDistance + Math.abs(globalClosestDistance) * epsilon;

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
