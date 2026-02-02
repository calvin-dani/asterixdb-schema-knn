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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility class for VCTree navigation operations.
 * Contains common logic for finding closest centroids in tree structures.
 */
public class VCTreeNavigationUtils {

    /**
     * Find the closest centroid by traversing the tree from root to leaf.
     * 
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param rootPageId Root page ID to start traversal
     * @param interiorFrameFactory Factory for creating interior frames
     * @param leafFrameFactory Factory for creating leaf frames
     * @param queryVector Query vector to find closest centroid for
     * @param distanceFunction Distance function to use for centroid finding
     * @return ClusterSearchResult containing closest centroid information
     * @throws HyracksDataException if any error occurs during traversal
     */

    private static final Logger LOGGER = LogManager.getLogger();
    public static ClusterSearchResult findClosestCentroid(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory, double[] queryVector,
            IVectorDistanceFunction distanceFunction) throws HyracksDataException {

        //        Map<String, Object> startFields = new HashMap<>();
        //        startFields.put("treeFileId", fileId);
        //        startFields.put("rootPageId", rootPageId);
        //        startFields.put("vectorDim", queryVector.length);
        //        startFields.put("queryVector", queryVector);
        //        logTraversalEvent("traversal_start", startFields);

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
                IVectorClusteringLeafFrame leafFrame = (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                Map<String, Object> pageVisitFields = new HashMap<>();
                //                pageVisitFields.put("pageId", currentPageId);
                //                pageVisitFields.put("isLeaf", isLeaf);
                //                pageVisitFields.put("loopIteration", loopCounter);
                //                logTraversalEvent("page_visit", pageVisitFields);

                if (isLeaf) {
                    // Leaf level - find closest centroid
                    //                    Map<String, Object> leafEnterFields = new HashMap<>();
                    //                    leafEnterFields.put("pageId", currentPageId);
                    //                    leafEnterFields.put("fileId", fileId);
                    //                    logTraversalEvent("leaf_page_enter", leafEnterFields);

                    bestResult = findClosestInLeafPage(bufferCache, fileId, queryVector, currentPageId, leafFrame,
                            leafFrameFactory, distanceFunction);
                    break; // Found leaf level result

                } else {
                    // Interior level - find closest centroid and descend
                    //                    Map<String, Object> interiorEnterFields = new HashMap<>();
                    //                    interiorEnterFields.put("pageId", currentPageId);
                    //                    interiorEnterFields.put("fileId", fileId);
                    //                    logTraversalEvent("interior_page_enter", interiorEnterFields);

                    IVectorClusteringInteriorFrame interiorFrame =
                            (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);
                    int nextPageId = findClosestInInteriorPage(bufferCache, fileId, queryVector, currentPageId,
                            interiorFrame, interiorFrameFactory, distanceFunction);
                    if (nextPageId == -1) {
                        throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                                "No valid centroid found in interior cluster");
                    }

                    //                    Map<String, Object> interiorDescendFields = new HashMap<>();
                    //                    interiorDescendFields.put("pageId", currentPageId);
                    //                    interiorDescendFields.put("selectedChildPageId", nextPageId);
                    //                    interiorDescendFields.put("fileId", fileId);
                    //                    logTraversalEvent("interior_descend", interiorDescendFields);

                    currentPageId = nextPageId;
                }

            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        if (bestResult == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No closest cluster found");
        }

        //        Map<String, Object> finishFields = new HashMap<>();
        //        finishFields.put("leafPageId", bestResult.leafPageId);
        //        finishFields.put("centroidId", bestResult.centroidId);
        //        finishFields.put("bestDistance", bestResult.distance);
        //        finishFields.put("vectorDim", queryVector.length);
        //        finishFields.put("queryVector", queryVector);
        //        logTraversalEvent("traversal_finish", finishFields);

        return bestResult;
    }

    /**
     * Extract centroid from an interior frame tuple (format: <cid, centroid, child_ptr>).
     */
    private static double[] extractCentroidFromInteriorTuple(ITreeIndexTupleReference tuple) {
        // Centroid is the second field in interior frame tuples
        try {
            // Create field serializers array - specify only the centroid field we need
            ISerializerDeserializer<?>[] fieldSerdes = new ISerializerDeserializer<?>[3];
            fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE; // Field 0: cid
            fieldSerdes[1] = DoubleArraySerializerDeserializer.INSTANCE; // Field 1: centroid
            fieldSerdes[2] = IntegerSerializerDeserializer.INSTANCE; // Field 2: metadata_pointer

            // Deserialize the tuple using the proper TupleUtils method
            Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);

            // Extract the centroid from the deserialized fields
            double[] doubleCentroid = (double[]) fieldValues[1];

            return doubleCentroid;

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to extract centroid from interior tuple using TupleUtils.deserializeTuple()", e);
        }
    }

    /**
     * Format a double array as a JSON array string.
     * 
     * @param vector Double array to format
     * @return JSON array string like "[1.23,4.56,7.89]"
     */
    private static String formatVectorAsJsonArray(double[] vector) {
        if (vector == null) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(vector[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Log a traversal event as a single-line JSON object to System.err.
     * 
     * @param eventType Event type identifier
     * @param fields Map of field names to values
     */
    private static void logTraversalEvent(String eventType, Map<String, Object> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"event\":\"");
        sb.append(eventType);
        sb.append("\"");

        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            sb.append(",\"");
            sb.append(entry.getKey());
            sb.append("\":");

            Object value = entry.getValue();
            if (value instanceof String) {
                sb.append("\"");
                sb.append(value);
                sb.append("\"");
            } else if (value instanceof Boolean) {
                sb.append(value);
            } else if (value instanceof double[]) {
                sb.append(formatVectorAsJsonArray((double[]) value));
            } else {
                sb.append(value);
            }
        }

        sb.append("}");
        System.err.println(sb.toString());
    }

    /**
     * Find the closest centroid in a leaf cluster, handling overflow pages.
     * Unified loop iterates through all pages in the overflow chain (e.g., p10 -> p20 -> p21).
     *
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param queryVector Query vector to find closest centroid for
     * @param startPageId Starting page ID of the leaf cluster
     * @param initialLeafFrame Leaf frame already set to the initial page (already pinned by caller)
     * @param leafFrameFactory Factory for creating leaf frames for overflow pages
     * @return ClusterSearchResult containing closest centroid information (pageId, tupleIndex, centroid, distance, centroidId)
     * @param distanceFunction Distance function to use for distance calculation
     * @return ClusterSearchResult containing closest centroid information
     * @throws HyracksDataException if any error occurs during search
     */
    public static List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(IBufferCache bufferCache, int fileId,
            int rootPageId, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector, IVectorDistanceFunction distanceFunction, double epsilon)
            throws HyracksDataException {

        List<ClusterSearchResult> allCentroids = new ArrayList<>();
        Set<Integer> visitedLeafPages = new HashSet<>();
        Queue<LevelNode> queue = new ArrayDeque<>();
        queue.add(new LevelNode(rootPageId, 0));

        int levelsProcessed = 0;

        // Phase 1: Collect all centroids from all leaf pages
        while (!queue.isEmpty()) {
            int currentLevel = queue.peek().level;
            List<LevelNode> currentLevelNodes = new ArrayList<>();

            // Collect all nodes at current level
            while (!queue.isEmpty() && queue.peek().level == currentLevel) {
                currentLevelNodes.add(queue.poll());
            }

            levelsProcessed = currentLevel;

            // Process all nodes at current level
            for (LevelNode node : currentLevelNodes) {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, node.pageId));
                try {
                    page.acquireReadLatch();

                    IVectorClusteringLeafFrame leafFrame = (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
                    leafFrame.setPage(page);
                    boolean isLeaf = leafFrame.isLeaf();

                    if (isLeaf) {
                        // Leaf node processing - collect ALL centroids (no threshold filtering yet)
                        if (!visitedLeafPages.add(node.pageId)) {
                            continue; // Already visited
                        }

                        LeafCollectionStats stats = collectLeafCentroids(bufferCache, fileId, queryVector, node.pageId,
                                leafFrame, leafFrameFactory, distanceFunction);

                        if (stats.centroids.isEmpty()) {
                            continue;
                        }

                        // Add ALL centroids from this leaf page to global collection
                        // (distance is already computed and stored in LeafCentroid)
                        for (LeafCentroid centroid : stats.centroids) {
                            allCentroids.add(
                                    ClusterSearchResult.create(centroid.pageId, centroid.tupleIndex, centroid.centroid,
                                            centroid.distance, centroid.centroidId, centroid.directoryPageId));
                        }

                    } else {
                        // Interior node processing - same as original level-wise
                        IVectorClusteringInteriorFrame interiorFrame =
                                (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                        interiorFrame.setPage(page);

                        List<ChildCentroid> sortedChildren = collectChildrenForFrontier(bufferCache, fileId,
                                queryVector, node.pageId, interiorFrame, interiorFrameFactory, distanceFunction);

                        if (sortedChildren.isEmpty()) {
                            continue;
                        }

                        double closestDistance = sortedChildren.get(0).distance;
                        double localThreshold = closestDistance + epsilon;

                        for (ChildCentroid child : sortedChildren) {
                            if (child.distance <= localThreshold) {
                                queue.add(new LevelNode(child.childPageId, currentLevel + 1));
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
        // This ensures nprobe selects the truly closest clusters (FAISS/SPANN approach)
        allCentroids.sort(Comparator.comparingDouble(r -> r.distance));

        // Phase 3: Apply epsilon threshold based on globally closest centroid
        // This is more accurate than per-page local thresholds
        if (epsilon > 0.0) {
            double globalClosestDistance = allCentroids.get(0).distance;
            double globalThreshold = globalClosestDistance + epsilon;

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

    /**
     * Find close centroids using a hybrid approach: level-wise search with DFS fallback.
     * 
     * This method first uses level-wise global sort to find clusters within epsilon threshold.
     * If the number of clusters found is less than minClusters, it falls back to DFS traversal
     * to explore siblings and cousins (nodes that were rejected by the epsilon threshold).
     * 
     * The DFS fallback uses the NavigationState infrastructure which maintains a stack of the
     * traversal path, enabling backtracking to explore unexplored siblings at each level.
     * 
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param rootPageId Root page ID to start traversal
     * @param interiorFrameFactory Factory for creating interior frames
     * @param leafFrameFactory Factory for creating leaf frames
     * @param queryVector Query vector to find closest centroids for
     * @param distanceFunction Distance function to use for centroid finding
     * @param epsilon Absolute distance threshold added to closest sibling/centroid at each level
     * @param minClusters Minimum number of clusters to return (will use DFS fallback if level-wise finds fewer)
     * @return List of ClusterSearchResult containing at least minClusters (or all available), globally sorted by distance
     * @throws HyracksDataException if any error occurs during traversal
     */
    public static List<ClusterSearchResult> findCloseCentroidsHybrid(IBufferCache bufferCache, int fileId,
            int rootPageId, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector, IVectorDistanceFunction distanceFunction, double epsilon, int minClusters)
            throws HyracksDataException {

        // Phase 1: Use level-wise global sort to find clusters within epsilon threshold
        List<ClusterSearchResult> results = findCloseCentroidsLevelWiseGlobalSort(bufferCache, fileId, rootPageId,
                interiorFrameFactory, leafFrameFactory, queryVector, distanceFunction, epsilon);

        // If we have enough clusters, return them
        if (results.size() >= minClusters) {
            return results;
        }

        // Phase 2: DFS fallback to explore siblings and cousins
        // Create a visited set with all centroid IDs already found
        Set<Integer> visitedCentroidIds = new HashSet<>();
        for (ClusterSearchResult result : results) {
            visitedCentroidIds.add(result.centroidId);
        }

        // Initialize DFS navigation state with shared visited set
        NavigationState dfsState = new NavigationState(bufferCache, fileId, rootPageId, interiorFrameFactory,
                leafFrameFactory, queryVector, visitedCentroidIds);

        // Initialize the DFS iterator (this will skip already-visited centroids)
        ClusterSearchResult firstFromDfs = initializeClusterIterator(dfsState, distanceFunction);

        // The first result from DFS might be one we already have (since it starts from root)
        // But since we passed the visitedCentroidIds, it should skip those
        if (firstFromDfs != null && !visitedCentroidIds.contains(firstFromDfs.centroidId)) {
            results.add(firstFromDfs);
            visitedCentroidIds.add(firstFromDfs.centroidId);
        }

        // Keep finding next closest clusters until we have enough or exhaust the tree
        while (results.size() < minClusters) {
            ClusterSearchResult nextCluster = findNextClosestCluster(dfsState, distanceFunction);
            if (nextCluster == null) {
                // No more clusters available in the tree
                break;
            }
            // The DFS already marks visited, but double-check to be safe
            if (!visitedCentroidIds.contains(nextCluster.centroidId)) {
                results.add(nextCluster);
                visitedCentroidIds.add(nextCluster.centroidId);
            }
        }

        // Re-sort results by distance since DFS may have found clusters out of order
        results.sort(Comparator.comparingDouble(r -> r.distance));

        return results;
    }

    /**
     * Find close centroids using level-by-level cross-pollination.
     * At each interior node, finds closest sibling and explores all siblings within closestDistance + epsilon.
     * At each leaf node, finds closest centroid and collects all centroids within closestDistance + epsilon.
     * 
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param rootPageId Root page ID to start traversal
     * @param interiorFrameFactory Factory for creating interior frames
     * @param leafFrameFactory Factory for creating leaf frames
     * @param queryVector Query vector to find closest centroids for
     * @param distanceFunction Distance function to use for centroid finding
     * @param epsilon Absolute distance threshold added to closest sibling/centroid at each level
     * @return List of ClusterSearchResult containing all qualifying centroids
     * @throws HyracksDataException if any error occurs during traversal
     */
    public static List<ClusterSearchResult> findCloseCentroidsLevelWise(IBufferCache bufferCache, int fileId,
            int rootPageId, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector, IVectorDistanceFunction distanceFunction, double epsilon)
            throws HyracksDataException {

        //        Map<String, Object> startFields = new HashMap<>();
        //        startFields.put("treeFileId", fileId);
        //        startFields.put("rootPageId", rootPageId);
        //        startFields.put("vectorDim", queryVector.length);
        //        startFields.put("queryVector", queryVector);
        //        startFields.put("epsilon", epsilon);
        //        startFields.put("strategy", "levelwise");
        //        logTraversalEvent("traversal_start", startFields);

        List<ClusterSearchResult> results = new ArrayList<>();
        Set<Integer> visitedLeafPages = new HashSet<>();
        Queue<LevelNode> queue = new ArrayDeque<>();
        queue.add(new LevelNode(rootPageId, 0));

        int levelsProcessed = 0;

        while (!queue.isEmpty()) {
            int currentLevel = queue.peek().level;
            List<LevelNode> currentLevelNodes = new ArrayList<>();

            // Collect all nodes at current level
            while (!queue.isEmpty() && queue.peek().level == currentLevel) {
                currentLevelNodes.add(queue.poll());
            }

            levelsProcessed = currentLevel;

            // Process all nodes at current level
            for (LevelNode node : currentLevelNodes) {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, node.pageId));
                try {
                    page.acquireReadLatch();

                    IVectorClusteringLeafFrame leafFrame = (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
                    leafFrame.setPage(page);
                    boolean isLeaf = leafFrame.isLeaf();

                    //                    Map<String, Object> pageVisitFields = new HashMap<>();
                    //                    pageVisitFields.put("pageId", node.pageId);
                    //                    pageVisitFields.put("isLeaf", isLeaf);
                    //                    pageVisitFields.put("level", currentLevel);
                    //                    logTraversalEvent("page_visit", pageVisitFields);

                    if (isLeaf) {
                        // Leaf node processing
                        if (!visitedLeafPages.add(node.pageId)) {
                            continue; // Already visited
                        }

                        //                        Map<String, Object> leafEnterFields = new HashMap<>();
                        //                        leafEnterFields.put("pageId", node.pageId);
                        //                        leafEnterFields.put("fileId", fileId);
                        //                        leafEnterFields.put("level", currentLevel);
                        //                        logTraversalEvent("leaf_page_enter", leafEnterFields);

                        //                        Map<String, Object> searchStartFields = new HashMap<>();
                        //                        searchStartFields.put("startPageId", node.pageId);
                        //                        searchStartFields.put("vectorDim", queryVector.length);
                        //                        searchStartFields.put("queryVector", queryVector);
                        //                        searchStartFields.put("level", currentLevel);
                        //                        logTraversalEvent("leaf_search_start", searchStartFields);

                        LeafCollectionStats stats = collectLeafCentroids(bufferCache, fileId, queryVector, node.pageId,
                                leafFrame, leafFrameFactory, distanceFunction);

                        if (stats.centroids.isEmpty()) {
                            continue;
                        }

                        double closestDistance = stats.centroids.get(0).distance;
                        double localThreshold = closestDistance + epsilon;
                        int resultsAdded = 0;

                        for (LeafCentroid centroid : stats.centroids) {
                            if (centroid.distance <= localThreshold) {
                                results.add(ClusterSearchResult.create(centroid.pageId, centroid.tupleIndex,
                                        centroid.centroid, centroid.distance, centroid.centroidId,
                                        centroid.directoryPageId));
                                resultsAdded++;
                            } else {
                                break; // Centroids are sorted, no more qualify
                            }
                        }

                        //                        Map<String, Object> searchSelectFields = new HashMap<>();
                        //                        LeafCentroid bestLeaf = stats.centroids.get(0);
                        //                        searchSelectFields.put("bestPageId", bestLeaf.pageId);
                        //                        searchSelectFields.put("selectedTupleIndex", bestLeaf.tupleIndex);
                        //                        searchSelectFields.put("centroidId", bestLeaf.centroidId);
                        //                        searchSelectFields.put("bestDistance", bestLeaf.distance);
                        //                        searchSelectFields.put("candidatesProcessed", stats.candidatesProcessed);
                        //                        searchSelectFields.put("pagesProcessed", stats.pagesProcessed);
                        //                        searchSelectFields.put("resultsAdded", resultsAdded);
                        //                        searchSelectFields.put("epsilon", epsilon);
                        //                        searchSelectFields.put("localThreshold", localThreshold);
                        //                        searchSelectFields.put("level", currentLevel);
                        //                        logTraversalEvent("leaf_search_select", searchSelectFields);

                    } else {
                        // Interior node processing
                        //                        Map<String, Object> interiorEnterFields = new HashMap<>();
                        //                        interiorEnterFields.put("pageId", node.pageId);
                        //                        interiorEnterFields.put("fileId", fileId);
                        //                        interiorEnterFields.put("level", currentLevel);
                        //                        logTraversalEvent("interior_page_enter", interiorEnterFields);

                        //                        Map<String, Object> searchStartFields = new HashMap<>();
                        //                        searchStartFields.put("startPageId", node.pageId);
                        //                        searchStartFields.put("vectorDim", queryVector.length);
                        //                        searchStartFields.put("queryVector", queryVector);
                        //                        searchStartFields.put("level", currentLevel);
                        //                        logTraversalEvent("interior_search_start", searchStartFields);

                        IVectorClusteringInteriorFrame interiorFrame =
                                (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                        interiorFrame.setPage(page);

                        List<ChildCentroid> sortedChildren = collectChildrenForFrontier(bufferCache, fileId,
                                queryVector, node.pageId, interiorFrame, interiorFrameFactory, distanceFunction);

                        if (sortedChildren.isEmpty()) {
                            continue;
                        }

                        double closestDistance = sortedChildren.get(0).distance;
                        double localThreshold = closestDistance + epsilon;
                        int childrenEnqueued = 0;

                        //                        Map<String, Object> searchSelectFields = new HashMap<>();
                        //                        searchSelectFields.put("pageId", node.pageId);
                        //                        searchSelectFields.put("closestDistance", closestDistance);
                        //                        searchSelectFields.put("localThreshold", localThreshold);
                        //                        searchSelectFields.put("candidatesProcessed", sortedChildren.size());
                        //                        searchSelectFields.put("epsilon", epsilon);
                        //                        searchSelectFields.put("level", currentLevel);
                        //                        logTraversalEvent("interior_search_select", searchSelectFields);

                        for (ChildCentroid child : sortedChildren) {
                            if (child.distance <= localThreshold) {
                                queue.add(new LevelNode(child.childPageId, currentLevel + 1));
                                childrenEnqueued++;

                                //                                Map<String, Object> candidateFields = new HashMap<>();
                                //                                candidateFields.put("pageId", node.pageId);
                                //                                candidateFields.put("tupleIndex", child.tupleIndex);
                                //                                candidateFields.put("centroidDim", queryVector.length);
                                //                                candidateFields.put("distance", child.distance);
                                //                                candidateFields.put("childPageId", child.childPageId);
                                //                                candidateFields.put("enqueued", true);
                                //                                candidateFields.put("level", currentLevel);
                                //                                logTraversalEvent("interior_candidate", candidateFields);
                            } else {
                                break; // Children are sorted, no more qualify
                            }
                        }

                        //                        Map<String, Object> descendFields = new HashMap<>();
                        //                        descendFields.put("pageId", node.pageId);
                        //                        descendFields.put("childrenEnqueued", childrenEnqueued);
                        //                        descendFields.put("level", currentLevel);
                        //                        logTraversalEvent("interior_descend", descendFields);
                    }

                } finally {
                    page.releaseReadLatch();
                    bufferCache.unpin(page);
                }
            }
        }

        if (results.isEmpty()) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No closest clusters found");
        }

        //        Map<String, Object> finishFields = new HashMap<>();
        //        ClusterSearchResult bestResult = results.get(0);
        //        finishFields.put("leafPageId", bestResult.leafPageId);
        //        finishFields.put("centroidId", bestResult.centroidId);
        //        finishFields.put("bestDistance", bestResult.distance);
        //        finishFields.put("vectorDim", queryVector.length);
        //        finishFields.put("queryVector", queryVector);
        //        finishFields.put("epsilon", epsilon);
        //        finishFields.put("resultsCount", results.size());
        //        finishFields.put("levelsProcessed", levelsProcessed);
        //        finishFields.put("strategy", "levelwise");
        //        logTraversalEvent("traversal_finish", finishFields);
    private static ClusterSearchResult findClosestInLeafPage(IBufferCache bufferCache, int fileId, double[] queryVector,
            int startPageId, IVectorClusteringLeafFrame initialLeafFrame, ITreeIndexFrameFactory leafFrameFactory,
            IVectorDistanceFunction distanceFunction) throws HyracksDataException {

        double bestDistance = Double.MAX_VALUE;
        int bestTupleIndex = -1;
        int bestPageId = -1;
        double[] bestCentroid = null;
        int bestCentroidId = -1;
        long bestDirectoryPageId = -1;
        int candidatesProcessed = 0;
        int pagesProcessed = 0;

        //        Map<String, Object> searchStartFields = new HashMap<>();
        //        searchStartFields.put("startPageId", startPageId);
        //        searchStartFields.put("vectorDim", queryVector.length);
        //        searchStartFields.put("queryVector", queryVector);
        //        logTraversalEvent("leaf_search_start", searchStartFields);

        // Unified loop: iterate through all pages in the overflow chain (p10 -> p20 -> p21)
        int currentPageId = startPageId;
        IVectorClusteringLeafFrame currentFrame = initialLeafFrame;
        boolean isFirstPage = true;
        ICachedPage currentPage = null;

        while (currentPageId != -1) {
            try {
                // For the first page, use the frame passed by caller (already pinned/latched)
                // For overflow pages, pin and latch them ourselves
                if (!isFirstPage) {
                    currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    currentPage.acquireReadLatch();
                    currentFrame = (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
                    currentFrame.setPage(currentPage);
                }

                int tupleCount = currentFrame.getTupleCount();
                boolean hasOverflow = currentFrame.getOverflowFlagBit();
                int nextPageId = hasOverflow ? currentFrame.getNextLeaf() : -1;
                pagesProcessed++;

                //                Map<String, Object> pageFields = new HashMap<>();
                //                pageFields.put("pageId", currentPageId);
                //                pageFields.put("tupleCount", tupleCount);
                //                pageFields.put("hasOverflow", hasOverflow);
                //                pageFields.put("nextPageId", nextPageId);
                //                pageFields.put("isFirstPage", isFirstPage);
                //                logTraversalEvent("leaf_page_search", pageFields);

                // Search all centroids in this page
                for (int i = 0; i < tupleCount; i++) {
                    try {
                        ITreeIndexTupleReference frameTuple = currentFrame.createTupleReference();
                        frameTuple.resetByTupleIndex(currentFrame, i);
                        double[] centroid = extractCentroidFromInteriorTuple(frameTuple);
                        int centroidID = currentFrame.getCentroidId(i);

                        // Check vector dimensionality before distance calculation
                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = distanceFunction.apply(queryVector, centroid);
                        candidatesProcessed++;

                        //                        Map<String, Object> candidateFields = new HashMap<>();
                        //                        candidateFields.put("pageId", currentPageId);
                        //                        candidateFields.put("tupleIndex", i);
                        //                        candidateFields.put("centroidId", centroidID);
                        //                        candidateFields.put("centroidDim", centroid.length);
                        //                        candidateFields.put("distance", distance);
                        //                        logTraversalEvent("leaf_candidate", candidateFields);

                        if (distance < bestDistance) {
                            bestDistance = distance;
                            bestTupleIndex = i;
                            bestPageId = currentPageId;
                            bestCentroid = centroid.clone();
                            bestCentroidId = centroidID;
                            bestDirectoryPageId = currentFrame.getMetadataPagePointer(i);
                        }
                    } catch (Exception e) {
                        System.err.println(
                                "ERROR processing tuple " + i + " on page " + currentPageId + ": " + e.getMessage());
                        continue;
                    }
                }

                // Move to next page in chain
                currentPageId = nextPageId;
                isFirstPage = false;

            } finally {
                // Only unpin/unlatch overflow pages, not the first page (caller handles that)
                if (!isFirstPage && currentPage != null) {
                    currentPage.releaseReadLatch();
                    bufferCache.unpin(currentPage);
                    currentPage = null;
                }
            }
        }

        if (bestTupleIndex >= 0) {
            //            Map<String, Object> searchSelectFields = new HashMap<>();
            //            searchSelectFields.put("bestPageId", bestPageId);
            //            searchSelectFields.put("selectedTupleIndex", bestTupleIndex);
            //            searchSelectFields.put("centroidId", bestCentroidId);
            //            searchSelectFields.put("bestDistance", bestDistance);
            //            searchSelectFields.put("candidatesProcessed", candidatesProcessed);
            //            searchSelectFields.put("pagesProcessed", pagesProcessed);
            //            logTraversalEvent("leaf_search_select", searchSelectFields);

            return ClusterSearchResult.create(bestPageId, bestTupleIndex, bestCentroid, bestDistance, bestCentroidId,
                    bestDirectoryPageId);
        }
        // TODO : SOME RETURN EMPTY
        return null;
    }

    /**
     * Find the closest centroid in an interior cluster and return child page ID, handling overflow pages.
     * Unified loop iterates through all pages in the overflow chain (e.g., p10 -> p20 -> p21).
     *
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param queryVector Query vector to find closest centroid for
     * @param startPageId Starting page ID of the interior cluster
     * @param initialInteriorFrame Interior frame already set to the initial page (already pinned by caller)
     * @param interiorFrameFactory Factory for creating interior frames for overflow pages
     * @param distanceFunction Distance function to use for distance calculation
     * @return Child page ID to descend to, or -1 if no valid child found
     * @throws HyracksDataException if any error occurs during search
     */
    private static int findClosestInInteriorPage(IBufferCache bufferCache, int fileId, double[] queryVector,
            int startPageId, IVectorClusteringInteriorFrame initialInteriorFrame,
            ITreeIndexFrameFactory interiorFrameFactory, IVectorDistanceFunction distanceFunction)
            throws HyracksDataException {

        double bestDistance = Double.MAX_VALUE;
        int bestChildPageId = -1;
        int candidatesProcessed = 0;
        int pagesProcessed = 0;

        //        Map<String, Object> searchStartFields = new HashMap<>();
        //        searchStartFields.put("startPageId", startPageId);
        //        searchStartFields.put("vectorDim", queryVector.length);
        //        searchStartFields.put("queryVector", queryVector);
        //        logTraversalEvent("interior_search_start", searchStartFields);

        // Unified loop: iterate through all pages in the overflow chain (p10 -> p20 -> p21)
        int currentPageId = startPageId;
        IVectorClusteringInteriorFrame currentFrame = initialInteriorFrame;
        boolean isFirstPage = true;
        ICachedPage currentPage = null;

        while (currentPageId != -1) {
            try {
                // For the first page, use the frame passed by caller (already pinned/latched)
                // For overflow pages, pin and latch them ourselves
                if (!isFirstPage) {
                    currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    currentPage.acquireReadLatch();
                    currentFrame = (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                    currentFrame.setPage(currentPage);
                }

                int tupleCount = currentFrame.getTupleCount();
                boolean hasOverflow = currentFrame.getOverflowFlagBit();
                int nextPageId = hasOverflow ? currentFrame.getNextPage() : -1;
                pagesProcessed++;

                //                Map<String, Object> pageFields = new HashMap<>();
                //                pageFields.put("pageId", currentPageId);
                //                pageFields.put("tupleCount", tupleCount);
                //                pageFields.put("hasOverflow", hasOverflow);
                //                pageFields.put("nextPageId", nextPageId);
                //                pageFields.put("isFirstPage", isFirstPage);
                //                logTraversalEvent("interior_page_search", pageFields);

                // Search all centroids in this page
                for (int i = 0; i < tupleCount; i++) {
                    try {
                        ITreeIndexTupleReference frameTuple = currentFrame.createTupleReference();
                        frameTuple.resetByTupleIndex(currentFrame, i);
                        double[] centroid = extractCentroidFromInteriorTuple(frameTuple);

                        // Check vector dimensionality before distance calculation
                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = distanceFunction.apply(queryVector, centroid);
                        int childPageId = currentFrame.getChildPageId(i);
                        candidatesProcessed++;

                        //                        Map<String, Object> candidateFields = new HashMap<>();
                        //                        candidateFields.put("pageId", currentPageId);
                        //                        candidateFields.put("tupleIndex", i);
                        //                        candidateFields.put("centroidDim", centroid.length);
                        //                        candidateFields.put("distance", distance);
                        //                        candidateFields.put("childPageId", childPageId);
                        //                        logTraversalEvent("interior_candidate", candidateFields);

                        if (distance < bestDistance) {
                            bestDistance = distance;
                            bestChildPageId = childPageId;
                        }
                    } catch (Exception e) {
                        System.err.println(
                                "ERROR processing tuple " + i + " on page " + currentPageId + ": " + e.getMessage());
                        continue;
                    }
                }

                // Move to next page in chain
                currentPageId = nextPageId;
                isFirstPage = false;

            } finally {
                // Only unpin/unlatch overflow pages, not the first page (caller handles that)
                if (!isFirstPage && currentPage != null) {
                    currentPage.releaseReadLatch();
                    bufferCache.unpin(currentPage);
                    currentPage = null;
                }
            }
        }

        //        Map<String, Object> searchSelectFields = new HashMap<>();
        //        searchSelectFields.put("selectedChildPageId", bestChildPageId);
        //        searchSelectFields.put("bestDistance", bestDistance);
        //        searchSelectFields.put("candidatesProcessed", candidatesProcessed);
        //        searchSelectFields.put("pagesProcessed", pagesProcessed);
        //        logTraversalEvent("interior_search_select", searchSelectFields);

        return bestChildPageId;
    }

    /**
     * Extract centroid from a leaf frame tuple (format: <cid, centroid, metadata_ptr>).
     * Uses direct byte parsing to match the tuple writer's getCentroid() method.
     *
     * @param tuple Leaf frame tuple
     * @return Centroid vector
     */

    /**
     * Perform a breadth-first traversal of the static structure starting at root and print
     * a human-readable dump of all interior/leaf pages and their tuples. Distances are computed
     * w.r.t the provided query vector when dimensionality matches; otherwise marked as NA.
     */
    public static void bfsPrintStaticStructure(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory, double[] queryVector,
            int embeddingPrintLimit) throws HyracksDataException {

        if (bufferCache == null || interiorFrameFactory == null || leafFrameFactory == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Required components are not initialized");
        }

        final int printLimit = embeddingPrintLimit > 0 ? embeddingPrintLimit : 8;

        Queue<int[]> queue = new ArrayDeque<>();
        Set<Integer> visited = new HashSet<>();
        queue.add(new int[] { rootPageId, 0 });
        visited.add(rootPageId);

        int visitedPages = 0;
        long processedTuples = 0L;

        while (!queue.isEmpty()) {
            int[] entry = queue.poll();
            int currentPageId = entry[0];
            int level = entry[1];

            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
            try {
                page.acquireReadLatch();

                IVectorClusteringLeafFrame leafFrame = (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                if (isLeaf) {
                    LOGGER.info("=== LEVEL " + level + " | PAGE " + currentPageId + " | TYPE: LEAF ===");
                    int tupleCount = leafFrame.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        try {
                            ITreeIndexTupleReference frameTuple = leafFrame.createTupleReference();
                            frameTuple.resetByTupleIndex(leafFrame, i);

                            ISerializerDeserializer<?>[] serdes = new ISerializerDeserializer<?>[3];
                            serdes[0] = IntegerSerializerDeserializer.INSTANCE;
                            serdes[1] = DoubleArraySerializerDeserializer.INSTANCE;
                            serdes[2] = IntegerSerializerDeserializer.INSTANCE;
                            Object[] fields = TupleUtils.deserializeTuple(frameTuple, serdes);

                            int cid = (Integer) fields[0];
                            double[] centroid = (double[]) fields[1];
                            int metadataPtr = (Integer) fields[2];
                            int centroidId = leafFrame.getCentroidId(i);

                            String centroidStr = formatCentroid(centroid, printLimit);
                            String distStr = computeDistanceString(queryVector, centroid);

                            LOGGER.info(
                                    "tuple=" + i + " | cid=" + cid + " | centroidId=" + centroidId + " | centroid="
                                            + centroidStr + " | dist=" + distStr + " | metadata=" + metadataPtr);
                            processedTuples++;
                        } catch (Exception e) {
                            System.err.println("ERROR processing leaf tuple " + i + " on page " + currentPageId + ": "
                                    + e.getMessage());
                        }
                    }

                    int nextLeaf = leafFrame.getNextLeaf();
                    if (nextLeaf != -1 && visited.add(nextLeaf)) {
                        queue.add(new int[] { nextLeaf, level });
                    }

                } else {
                    IVectorClusteringInteriorFrame interiorFrame =
                            (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);
                    LOGGER.info("=== LEVEL " + level + " | PAGE " + currentPageId + " | TYPE: INTERIOR ===");
                    int tupleCount = interiorFrame.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        try {
                            ITreeIndexTupleReference frameTuple = interiorFrame.createTupleReference();
                            frameTuple.resetByTupleIndex(interiorFrame, i);

                            ISerializerDeserializer<?>[] serdes = new ISerializerDeserializer<?>[3];
                            serdes[0] = IntegerSerializerDeserializer.INSTANCE;
                            serdes[1] = DoubleArraySerializerDeserializer.INSTANCE;
                            serdes[2] = IntegerSerializerDeserializer.INSTANCE;
                            Object[] fields = TupleUtils.deserializeTuple(frameTuple, serdes);

                            int cid = (Integer) fields[0];
                            double[] centroid = (double[]) fields[1];
                            int childPageId = interiorFrame.getChildPageId(i);

                            String centroidStr = formatCentroid(centroid, printLimit);
                            String distStr = computeDistanceString(queryVector, centroid);

                            LOGGER.info("tuple=" + i + " | cid=" + cid + " | centroid=" + centroidStr
                                    + " | dist=" + distStr + " | child=" + childPageId);
                            processedTuples++;

                            if (childPageId != -1 && visited.add(childPageId)) {
                                queue.add(new int[] { childPageId, level + 1 });
                            }
                        } catch (Exception e) {
                            System.err.println("ERROR processing interior tuple " + i + " on page " + currentPageId
                                    + ": " + e.getMessage());
                        }
                    }

                    int nextPage = interiorFrame.getNextPage();
                    if (nextPage != 0 && visited.add(nextPage)) {
                        queue.add(new int[] { nextPage, level });
                    }
                }

                visitedPages++;
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        System.err.println("=== BFS PRINT COMPLETE | pages=" + visitedPages + " | tuples=" + processedTuples + " ===");
    }

    private static String computeDistanceString(double[] queryVector, double[] centroid) {
        if (queryVector == null || centroid == null) {
            return "NA";
        }
        if (centroid.length != queryVector.length) {
            return "NA (dim mismatch)";
        }
        double d = VectorUtils.calculateEuclideanDistance(queryVector, centroid);
        return String.format("%.4f", d);
    }

    private static String formatCentroid(double[] centroid, int limit) {
        if (centroid == null) {
            return "null";
        }
        int n = centroid.length;
        int toPrint = Math.min(limit, n);
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < toPrint; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(String.format("%.4f", centroid[i]));
        }
        sb.append(']');
        if (n > toPrint) {
            sb.append(" (+").append(n - toPrint).append(" more)");
        }
        return sb.toString();
    }

    // ==================== Multi-Cluster Iterative DFS Support ====================

    /**
     * Represents a child centroid with its distance to query vector.
     * Used for sorting children by distance at interior nodes.
     */
    public static class ChildCentroid {
        public final int childPageId;
        public final double distance;
        public final int tupleIndex; // Index in parent page

        public ChildCentroid(int childPageId, double distance, int tupleIndex) {
            this.childPageId = childPageId;
            this.distance = distance;
            this.tupleIndex = tupleIndex;
        }
    }

    /**
     * Represents a leaf centroid with its metadata.
     * Used for iterating through centroids in a leaf page.
     */
    public static class LeafCentroid {
        public final int centroidId;
        public final double distance;
        public final int tupleIndex;
        public final int pageId;
        public final double[] centroid;
        public final long directoryPageId; // Direct pointer to cluster's directory page

        public LeafCentroid(int centroidId, double distance, int tupleIndex, int pageId, double[] centroid,
                long directoryPageId) {
            this.centroidId = centroidId;
            this.distance = distance;
            this.tupleIndex = tupleIndex;
            this.pageId = pageId;
            this.centroid = centroid;
            this.directoryPageId = directoryPageId;
        }

        // Legacy constructor for backwards compatibility
        public LeafCentroid(int centroidId, double distance, int tupleIndex, int pageId, double[] centroid) {
            this(centroidId, distance, tupleIndex, pageId, centroid, -1);
        }
    }

    /**
     * Stack frame for DFS navigation through the tree.
     * Each frame represents a node (interior or leaf level) in the traversal path.
     */
    public static class NavigationFrame {
        public final int pageId;
        public final boolean isLeaf;
        public final List<ChildCentroid> sortedChildren; // For interior nodes
        public final List<LeafCentroid> sortedCentroids; // For leaf nodes
        public int nextIndex; // Next child/centroid to explore

        // Constructor for interior frame
        public NavigationFrame(int pageId, List<ChildCentroid> sortedChildren) {
            this.pageId = pageId;
            this.isLeaf = false;
            this.sortedChildren = sortedChildren;
            this.sortedCentroids = null;
            this.nextIndex = 0;
        }

        // Constructor for leaf frame
        public NavigationFrame(int pageId, List<LeafCentroid> sortedCentroids, boolean isLeaf) {
            this.pageId = pageId;
            this.isLeaf = isLeaf;
            this.sortedChildren = null;
            this.sortedCentroids = sortedCentroids;
            this.nextIndex = 0;
        }

        public boolean hasNext() {
            if (isLeaf) {
                return sortedCentroids != null && nextIndex < sortedCentroids.size();
            } else {
                return sortedChildren != null && nextIndex < sortedChildren.size();
            }
        }

        public ChildCentroid nextChild() {
            if (!isLeaf && hasNext()) {
                return sortedChildren.get(nextIndex++);
            }
            return null;
        }

        public LeafCentroid nextCentroid() {
            if (isLeaf && hasNext()) {
                return sortedCentroids.get(nextIndex++);
            }
            return null;
        }
    }

    /**
     * State for iterative DFS navigation through the tree.
     * Maintains the navigation stack for finding clusters in distance order.
     * Also tracks visited centroids to avoid duplicates when using level-wise + DFS fallback.
     */
    public static class NavigationState {
        public final IBufferCache bufferCache;
        public final int fileId;
        public final int rootPageId;
        public final ITreeIndexFrameFactory interiorFrameFactory;
        public final ITreeIndexFrameFactory leafFrameFactory;
        public final double[] queryVector;
        public final Deque<NavigationFrame> stack;
        public boolean initialized;

        // Visited centroid tracking (can be shared across LSM components)
        private Set<Integer> visitedCentroidIds;

        public NavigationState(IBufferCache bufferCache, int fileId, int rootPageId,
                ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
                double[] queryVector) {
            this(bufferCache, fileId, rootPageId, interiorFrameFactory, leafFrameFactory, queryVector, new HashSet<>());
        }

        /**
         * Constructor with shared visited set.
         * Use this when sharing visited tracking across multiple LSM components.
         */
        public NavigationState(IBufferCache bufferCache, int fileId, int rootPageId,
                ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
                double[] queryVector, Set<Integer> sharedVisitedSet) {
            this.bufferCache = bufferCache;
            this.fileId = fileId;
            this.rootPageId = rootPageId;
            this.interiorFrameFactory = interiorFrameFactory;
            this.leafFrameFactory = leafFrameFactory;
            this.queryVector = queryVector;
            this.stack = new ArrayDeque<>();
            this.initialized = false;
            this.visitedCentroidIds = sharedVisitedSet != null ? sharedVisitedSet : new HashSet<>();
        }

        /**
         * Check if a centroid has already been visited.
         */
        public boolean isVisited(int centroidId) {
            return visitedCentroidIds.contains(centroidId);
        }

        /**
         * Mark a centroid as visited.
         */
        public void markVisited(int centroidId) {
            visitedCentroidIds.add(centroidId);
        }

        /**
         * Set the visited set (for sharing with other components).
         */
        public void setVisitedSet(Set<Integer> visitedSet) {
            this.visitedCentroidIds = visitedSet;
        }

        /**
         * Get the visited set.
         */
        public Set<Integer> getVisitedSet() {
            return visitedCentroidIds;
        }

        /**
         * Get the count of visited centroids.
         */
        public int getVisitedCount() {
            return visitedCentroidIds.size();
        }
    }

    /**
     * Initialize the cluster iterator by building navigation stack from root to first leaf.
     * This performs DFS to find the closest cluster and sets up the stack for backtracking.
     *
     * @param state Navigation state to initialize
     * @return The first (closest) cluster, or null if tree is empty
     * @throws HyracksDataException if any error occurs
     */
    public static ClusterSearchResult initializeClusterIterator(NavigationState state,
            IVectorDistanceFunction distanceFunction) throws HyracksDataException {
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
                IVectorClusteringLeafFrame leafFrame =
                        (IVectorClusteringLeafFrame) state.leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                if (isLeaf) {
                    // At leaf level: collect and sort all centroids in this page (including overflow)
                    List<LeafCentroid> sortedCentroids =
                            collectAndSortLeafCentroids(state, currentPageId, leafFrame, distanceFunction);

                    if (sortedCentroids.isEmpty()) {
                        return null; // Empty tree
                    }

                    // Push leaf frame onto stack
                    NavigationFrame leafFrame_nav = new NavigationFrame(currentPageId, sortedCentroids, true);
                    state.stack.push(leafFrame_nav);

                    // Return first centroid as closest cluster
                    LeafCentroid first = leafFrame_nav.nextCentroid();
                    return ClusterSearchResult.create(first.pageId, first.tupleIndex, first.centroid, first.distance,
                            first.centroidId, first.directoryPageId);

                } else {
                    // Interior level: collect and sort children
                    IVectorClusteringInteriorFrame interiorFrame =
                            (IVectorClusteringInteriorFrame) state.interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);

                    List<ChildCentroid> sortedChildren =
                            collectAndSortChildren(state, currentPageId, interiorFrame, distanceFunction);

                    if (sortedChildren.isEmpty()) {
                        throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                                "Interior node has no valid children");
                    }

                    // Push interior frame onto stack
                    NavigationFrame interiorFrame_nav = new NavigationFrame(currentPageId, sortedChildren);
                    state.stack.push(interiorFrame_nav);

                    // Descend to closest child
                    ChildCentroid closest = interiorFrame_nav.nextChild();
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
    public static ClusterSearchResult findNextClosestCluster(NavigationState state,
            IVectorDistanceFunction distanceFunction) throws HyracksDataException {
        if (!state.initialized) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "Iterator not initialized. Call initializeClusterIterator() first");
        }

        while (!state.stack.isEmpty()) {
            NavigationFrame topFrame = state.stack.peek();

            if (topFrame.isLeaf) {
                // At leaf level: try next centroid in current page, skipping visited ones
                while (topFrame.hasNext()) {
                    LeafCentroid next = topFrame.nextCentroid();

                    // Skip if already visited (e.g., by level-wise exploration)
                    if (state.isVisited(next.centroidId)) {
                        System.err.println(String.format("[DFS] Skipping visited centroid: cid=%d, distance=%.4f",
                                next.centroidId, next.distance));
                        continue;
                    }

                    // Mark as visited and return
                    state.markVisited(next.centroidId);
                    System.err.println(String.format(
                            "[DFS] Leaf frame pageId=%d returning centroid: cid=%d, distance=%.4f, nextIndex=%d/%d",
                            topFrame.pageId, next.centroidId, next.distance, topFrame.nextIndex,
                            topFrame.sortedCentroids.size()));
                    return ClusterSearchResult.create(next.pageId, next.tupleIndex, next.centroid, next.distance,
                            next.centroidId, next.directoryPageId);
                }

                // All centroids in this leaf exhausted or visited, backtrack
                {
                    // Current leaf exhausted, backtrack
                    System.err.println(String.format(
                            "[DFS] Leaf frame pageId=%d exhausted (nextIndex=%d, size=%d), popping stack (depth=%d)",
                            topFrame.pageId, topFrame.nextIndex, topFrame.sortedCentroids.size(), state.stack.size()));
                    state.stack.pop();
                    continue;
                }

            } else {
                // At interior level: try next child
                if (topFrame.hasNext()) {
                    ChildCentroid nextChild = topFrame.nextChild();
                    System.err.println(String.format(
                            "[DFS] Interior frame pageId=%d exploring child: childPageId=%d, distance=%.4f, nextIndex=%d/%d",
                            topFrame.pageId, nextChild.childPageId, nextChild.distance, topFrame.nextIndex,
                            topFrame.sortedChildren.size()));

                    // Descend to this child and navigate to leaf
                    ClusterSearchResult result = descendToLeaf(state, nextChild.childPageId, distanceFunction);
                    if (result != null) {
                        return result;
                    }
                    // If descend failed, continue with next child
                    System.err.println(
                            String.format("[DFS] descendToLeaf returned null for childPageId=%d, trying next child",
                                    nextChild.childPageId));
                    continue;

                } else {
                    // All children explored, backtrack
                    System.err.println(String.format(
                            "[DFS] Interior frame pageId=%d exhausted (nextIndex=%d, size=%d), popping stack (depth=%d)",
                            topFrame.pageId, topFrame.nextIndex, topFrame.sortedChildren.size(), state.stack.size()));
                    state.stack.pop();
                    continue;
                }
            }
        }

        // Stack exhausted, no more clusters
        System.err.println("[DFS] Stack exhausted, no more clusters available");
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
    private static ClusterSearchResult descendToLeaf(NavigationState state, int startPageId,
            IVectorDistanceFunction distanceFunction) throws HyracksDataException {

        int currentPageId = startPageId;

        while (true) {
            ICachedPage page = state.bufferCache.pin(BufferedFileHandle.getDiskPageId(state.fileId, currentPageId));
            try {
                page.acquireReadLatch();

                // Check if leaf
                IVectorClusteringLeafFrame leafFrame =
                        (IVectorClusteringLeafFrame) state.leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                if (isLeaf) {
                    // Reached leaf: collect and sort centroids
                    List<LeafCentroid> sortedCentroids =
                            collectAndSortLeafCentroids(state, currentPageId, leafFrame, distanceFunction);

                    if (sortedCentroids.isEmpty()) {
                        return null; // Empty leaf
                    }

                    // Push leaf frame onto stack
                    NavigationFrame leafFrame_nav = new NavigationFrame(currentPageId, sortedCentroids, true);
                    state.stack.push(leafFrame_nav);

                    // Find first unvisited centroid
                    while (leafFrame_nav.hasNext()) {
                        LeafCentroid first = leafFrame_nav.nextCentroid();
                        if (!state.isVisited(first.centroidId)) {
                            state.markVisited(first.centroidId);
                            return ClusterSearchResult.create(first.pageId, first.tupleIndex, first.centroid,
                                    first.distance, first.centroidId, first.directoryPageId);
                        }
                        // Skip visited centroid
                        System.err.println(String.format("[DFS descendToLeaf] Skipping visited centroid: cid=%d",
                                first.centroidId));
                    }
                    // All centroids in this leaf are visited
                    return null;

                } else {
                    // Interior: collect and sort children
                    IVectorClusteringInteriorFrame interiorFrame =
                            (IVectorClusteringInteriorFrame) state.interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);

                    List<ChildCentroid> sortedChildren =
                            collectAndSortChildren(state, currentPageId, interiorFrame, distanceFunction);

                    if (sortedChildren.isEmpty()) {
                        return null; // No valid children
                    }

                    // Push interior frame
                    NavigationFrame interiorFrame_nav = new NavigationFrame(currentPageId, sortedChildren);
                    state.stack.push(interiorFrame_nav);

                    // Descend to closest child
                    ChildCentroid closest = interiorFrame_nav.nextChild();
                    currentPageId = closest.childPageId;
                }

            } finally {
                page.releaseReadLatch();
                state.bufferCache.unpin(page);
            }
        }
    }

    /**
     * Collect and sort all children in an interior page by distance to query.
     * Handles overflow pages if present.
     *
     * @param state Navigation state
     * @param startPageId Starting page ID
     * @param initialFrame Initial interior frame (already set to first page)
     * @return List of children sorted by distance (closest first)
     * @throws HyracksDataException if any error occurs
     */
    private static List<ChildCentroid> collectAndSortChildren(NavigationState state, int startPageId,
            IVectorClusteringInteriorFrame initialFrame, IVectorDistanceFunction distanceFunction)
            throws HyracksDataException {

        List<ChildCentroid> children = new ArrayList<>();

        // Collect from first page (already have frame)
        for (int i = 0; i < initialFrame.getTupleCount(); i++) {
            try {
                ITreeIndexTupleReference tuple = initialFrame.createTupleReference();
                tuple.resetByTupleIndex(initialFrame, i);
                double[] centroid = extractCentroidFromInteriorTuple(tuple);

                if (centroid.length == state.queryVector.length) {
                    double distance = distanceFunction.apply(state.queryVector, centroid);
                    int childPageId = initialFrame.getChildPageId(i);
                    children.add(new ChildCentroid(childPageId, distance, i));
                }
            } catch (Exception e) {
                // Skip malformed tuples
            }
        }

        // Handle overflow pages
        boolean hasOverflow = initialFrame.getOverflowFlagBit();
        if (hasOverflow) {
            int nextPageId = initialFrame.getNextPage();
            collectChildrenFromOverflow(state, nextPageId, children, distanceFunction);
        }

        // Sort by distance (ascending)
        children.sort(Comparator.comparingDouble(c -> c.distance));
        return children;
    }

    /**
     * Collect children from overflow pages.
     */
    private static void collectChildrenFromOverflow(NavigationState state, int pageId, List<ChildCentroid> children,
            IVectorDistanceFunction distanceFunction) throws HyracksDataException {

        int currentPageId = pageId;
        while (currentPageId != -1) {
            ICachedPage page = state.bufferCache.pin(BufferedFileHandle.getDiskPageId(state.fileId, currentPageId));
            try {
                page.acquireReadLatch();
                IVectorClusteringInteriorFrame frame =
                        (IVectorClusteringInteriorFrame) state.interiorFrameFactory.createFrame();
                frame.setPage(page);

                for (int i = 0; i < frame.getTupleCount(); i++) {
                    try {
                        ITreeIndexTupleReference tuple = frame.createTupleReference();
                        tuple.resetByTupleIndex(frame, i);
                        double[] centroid = extractCentroidFromInteriorTuple(tuple);

                        if (centroid.length == state.queryVector.length) {
                            double distance = distanceFunction.apply(state.queryVector, centroid);
                            int childPageId = frame.getChildPageId(i);
                            children.add(new ChildCentroid(childPageId, distance, i));
                        }
                    } catch (Exception e) {
                        // Skip malformed tuples
                    }
                }

                boolean hasOverflow = frame.getOverflowFlagBit();
                currentPageId = hasOverflow ? frame.getNextPage() : -1;

            } finally {
                page.releaseReadLatch();
                state.bufferCache.unpin(page);
            }
        }
    }

    /**
     * Collect and sort all centroids in a leaf page by distance to query.
     * Handles overflow pages if present.
     *
     * @param state Navigation state
     * @param startPageId Starting page ID
     * @param initialFrame Initial leaf frame (already set to first page)
     * @return List of centroids sorted by distance (closest first)
     * @throws HyracksDataException if any error occurs
     */
    private static List<LeafCentroid> collectAndSortLeafCentroids(NavigationState state, int startPageId,
            IVectorClusteringLeafFrame initialFrame, IVectorDistanceFunction distanceFunction)
            throws HyracksDataException {

        List<LeafCentroid> centroids = new ArrayList<>();

        // Collect from first page
        for (int i = 0; i < initialFrame.getTupleCount(); i++) {
            try {
                ITreeIndexTupleReference tuple = initialFrame.createTupleReference();
                tuple.resetByTupleIndex(initialFrame, i);
                double[] centroid = extractCentroidFromInteriorTuple(tuple);

                if (centroid.length == state.queryVector.length) {
                    double distance = distanceFunction.apply(state.queryVector, centroid);
                    int centroidId = initialFrame.getCentroidId(i);
                    long directoryPageId = initialFrame.getMetadataPagePointer(i);
                    centroids.add(new LeafCentroid(centroidId, distance, i, startPageId, centroid, directoryPageId));
                }
            } catch (Exception e) {
                // Skip malformed tuples
            }
        }

        // Handle overflow pages
        boolean hasOverflow = initialFrame.getOverflowFlagBit();
        if (hasOverflow) {
            int nextPageId = initialFrame.getNextLeaf();
            collectCentroidsFromOverflow(state, nextPageId, centroids, distanceFunction);
        }

        // Sort by distance (ascending)
        centroids.sort(Comparator.comparingDouble(c -> c.distance));
        return centroids;
    }

    /**
     * Collect centroids from overflow pages.
     */
    private static void collectCentroidsFromOverflow(NavigationState state, int pageId, List<LeafCentroid> centroids,
            IVectorDistanceFunction distanceFunction) throws HyracksDataException {

        int currentPageId = pageId;
        while (currentPageId != -1) {
            ICachedPage page = state.bufferCache.pin(BufferedFileHandle.getDiskPageId(state.fileId, currentPageId));
            try {
                page.acquireReadLatch();
                IVectorClusteringLeafFrame frame = (IVectorClusteringLeafFrame) state.leafFrameFactory.createFrame();
                frame.setPage(page);

                for (int i = 0; i < frame.getTupleCount(); i++) {
                    try {
                        ITreeIndexTupleReference tuple = frame.createTupleReference();
                        tuple.resetByTupleIndex(frame, i);
                        double[] centroid = extractCentroidFromInteriorTuple(tuple);

                        if (centroid.length == state.queryVector.length) {
                            double distance = distanceFunction.apply(state.queryVector, centroid);
                            int centroidId = frame.getCentroidId(i);
                            long directoryPageId = frame.getMetadataPagePointer(i);
                            centroids.add(new LeafCentroid(centroidId, distance, i, currentPageId, centroid,
                                    directoryPageId));
                        }
                    } catch (Exception e) {
                        // Skip malformed tuples
                    }
                }

                boolean hasOverflow = frame.getOverflowFlagBit();
                currentPageId = hasOverflow ? frame.getNextLeaf() : -1;

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
            double[] queryVector, IVectorDistanceFunction distanceFunction, double epsilon)
            throws HyracksDataException {

        List<ClusterSearchResult> allCentroids = new ArrayList<>();
        Set<Integer> visitedLeafPages = new HashSet<>();
        Queue<LevelNode> queue = new ArrayDeque<>();
        queue.add(new LevelNode(rootPageId, 0));

        // Phase 1: Collect all centroids from all reachable leaf pages
        while (!queue.isEmpty()) {
            int currentLevel = queue.peek().level;
            List<LevelNode> currentLevelNodes = new ArrayList<>();

            // Collect all nodes at current level
            while (!queue.isEmpty() && queue.peek().level == currentLevel) {
                currentLevelNodes.add(queue.poll());
            }

            // Process all nodes at current level
            for (LevelNode node : currentLevelNodes) {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, node.pageId));
                try {
                    page.acquireReadLatch();

                    IVectorClusteringLeafFrame leafFrame = (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
                    leafFrame.setPage(page);
                    boolean isLeaf = leafFrame.isLeaf();

                    if (isLeaf) {
                        // Leaf node processing - collect ALL centroids (no threshold filtering yet)
                        if (!visitedLeafPages.add(node.pageId)) {
                            continue; // Already visited
                        }

                        LeafCollectionStats stats = collectLeafCentroidsForLevelWise(bufferCache, fileId, queryVector,
                                node.pageId, leafFrame, leafFrameFactory, distanceFunction);

                        if (stats.centroids.isEmpty()) {
                            continue;
                        }

                        // Add ALL centroids from this leaf page to global collection
                        for (LeafCentroid centroid : stats.centroids) {
                            allCentroids.add(
                                    ClusterSearchResult.create(centroid.pageId, centroid.tupleIndex, centroid.centroid,
                                            centroid.distance, centroid.centroidId, centroid.directoryPageId));
                        }

                    } else {
                        // Interior node processing - explore children within epsilon
                        IVectorClusteringInteriorFrame interiorFrame =
                                (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                        interiorFrame.setPage(page);

                        List<ChildCentroid> sortedChildren = collectChildrenForLevelWise(bufferCache, fileId,
                                queryVector, node.pageId, interiorFrame, interiorFrameFactory, distanceFunction);

                        if (sortedChildren.isEmpty()) {
                            continue;
                        }

                        double closestDistance = sortedChildren.get(0).distance;
                        double localThreshold = closestDistance + epsilon;

                        for (ChildCentroid child : sortedChildren) {
                            if (child.distance <= localThreshold) {
                                queue.add(new LevelNode(child.childPageId, currentLevel + 1));
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
            double globalThreshold = globalClosestDistance + epsilon;

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

    /**
     * Collect all centroids from a leaf page and its overflow chain for level-wise traversal.
     */
    private static LeafCollectionStats collectLeafCentroidsForLevelWise(IBufferCache bufferCache, int fileId,
            double[] queryVector, int startPageId, IVectorClusteringLeafFrame initialLeafFrame,
            ITreeIndexFrameFactory leafFrameFactory, IVectorDistanceFunction distanceFunction)
            throws HyracksDataException {

        List<LeafCentroid> centroids = new ArrayList<>();
        int currentPageId = startPageId;
        IVectorClusteringLeafFrame currentFrame = initialLeafFrame;
        boolean isFirstPage = true;
        ICachedPage currentPage = null;
        int candidatesProcessed = 0;
        int pagesProcessed = 0;

        while (currentPageId != -1) {
            try {
                if (!isFirstPage) {
                    currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    currentPage.acquireReadLatch();
                    currentFrame = (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
                    currentFrame.setPage(currentPage);
                }

                int tupleCount = currentFrame.getTupleCount();
                boolean hasOverflow = currentFrame.getOverflowFlagBit();
                int nextPageId = hasOverflow ? currentFrame.getNextLeaf() : -1;
                pagesProcessed++;

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
                        candidatesProcessed++;

                        centroids.add(new LeafCentroid(centroidId, distance, i, currentPageId, centroid.clone(),
                                directoryPageId));
                    } catch (Exception e) {
                        // Skip malformed tuples
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
        return new LeafCollectionStats(centroids, candidatesProcessed, pagesProcessed);
    }

    /**
     * Collect all children from an interior page and its overflow chain for level-wise traversal.
     */
    private static List<ChildCentroid> collectChildrenForLevelWise(IBufferCache bufferCache, int fileId,
            double[] queryVector, int startPageId, IVectorClusteringInteriorFrame initialFrame,
            ITreeIndexFrameFactory interiorFrameFactory, IVectorDistanceFunction distanceFunction)
            throws HyracksDataException {

        List<ChildCentroid> children = new ArrayList<>();
        int currentPageId = startPageId;
        IVectorClusteringInteriorFrame currentFrame = initialFrame;
        boolean isFirstPage = true;
        ICachedPage currentPage = null;

        while (currentPageId != -1) {
            try {
                if (!isFirstPage) {
                    currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    currentPage.acquireReadLatch();
                    currentFrame = (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                    currentFrame.setPage(currentPage);
                }

                int tupleCount = currentFrame.getTupleCount();
                boolean hasOverflow = currentFrame.getOverflowFlagBit();
                int nextPageId = hasOverflow ? currentFrame.getNextPage() : -1;

                for (int i = 0; i < tupleCount; i++) {
                    try {
                        ITreeIndexTupleReference frameTuple = currentFrame.createTupleReference();
                        frameTuple.resetByTupleIndex(currentFrame, i);
                        double[] centroid = extractCentroidFromInteriorTuple(frameTuple);

                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = distanceFunction.apply(queryVector, centroid);
                        int childPageId = currentFrame.getChildPageId(i);

                        children.add(new ChildCentroid(childPageId, distance, i));
                    } catch (Exception e) {
                        // Skip malformed tuples
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

        // Sort children by distance
        children.sort(Comparator.comparingDouble(c -> c.distance));
        return children;
    }

    /**
     * Statistics from collecting leaf centroids.
     */
    private static class LeafCollectionStats {
        public final List<LeafCentroid> centroids;
        public final int candidatesProcessed;
        public final int pagesProcessed;

        private LeafCollectionStats(List<LeafCentroid> centroids, int candidatesProcessed, int pagesProcessed) {
            this.centroids = centroids;
            this.candidatesProcessed = candidatesProcessed;
            this.pagesProcessed = pagesProcessed;
        }
    }

    /**
     * Node in the level-wise traversal queue.
     */
    private static class LevelNode {
        public final int pageId;
        public final int level;

        private LevelNode(int pageId, int level) {
            this.pageId = pageId;
            this.level = level;
        }
    }
}
