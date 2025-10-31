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
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

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
     * @return ClusterSearchResult containing closest centroid information
     * @throws HyracksDataException if any error occurs during traversal
     */
    public static ClusterSearchResult findClosestCentroid(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory, double[] queryVector)
            throws HyracksDataException {

        Map<String, Object> startFields = new HashMap<>();
        startFields.put("treeFileId", fileId);
        startFields.put("rootPageId", rootPageId);
        startFields.put("vectorDim", queryVector.length);
        startFields.put("queryVector", queryVector);
        logTraversalEvent("traversal_start", startFields);

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
                pageVisitFields.put("pageId", currentPageId);
                pageVisitFields.put("isLeaf", isLeaf);
                pageVisitFields.put("loopIteration", loopCounter);
                logTraversalEvent("page_visit", pageVisitFields);

                if (isLeaf) {
                    // Leaf level - find closest centroid
                    Map<String, Object> leafEnterFields = new HashMap<>();
                    leafEnterFields.put("pageId", currentPageId);
                    leafEnterFields.put("fileId", fileId);
                    logTraversalEvent("leaf_page_enter", leafEnterFields);

                    bestResult = findClosestInLeafPage(queryVector, currentPageId, leafFrame);
                    break; // Found leaf level result

                } else {
                    // Interior level - find closest centroid and descend
                    Map<String, Object> interiorEnterFields = new HashMap<>();
                    interiorEnterFields.put("pageId", currentPageId);
                    interiorEnterFields.put("fileId", fileId);
                    logTraversalEvent("interior_page_enter", interiorEnterFields);

                    IVectorClusteringInteriorFrame interiorFrame =
                            (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);
                    int nextPageId = findClosestInInteriorPage(queryVector, currentPageId, interiorFrame);
                    if (nextPageId == -1) {
                        throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                                "No valid centroid found in interior cluster");
                    }

                    Map<String, Object> interiorDescendFields = new HashMap<>();
                    interiorDescendFields.put("pageId", currentPageId);
                    interiorDescendFields.put("selectedChildPageId", nextPageId);
                    interiorDescendFields.put("fileId", fileId);
                    logTraversalEvent("interior_descend", interiorDescendFields);

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

        Map<String, Object> finishFields = new HashMap<>();
        finishFields.put("leafPageId", bestResult.leafPageId);
        finishFields.put("centroidId", bestResult.centroidId);
        finishFields.put("bestDistance", bestResult.distance);
        finishFields.put("vectorDim", queryVector.length);
        finishFields.put("queryVector", queryVector);
        logTraversalEvent("traversal_finish", finishFields);

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
     * Find the closest centroid in a leaf page.
     * 
     * @param queryVector Query vector to find closest centroid for
     * @param pageId Page ID of the leaf page
     * @param leafFrame Leaf frame for accessing page data
     * @return ClusterSearchResult containing closest centroid information
     * @throws HyracksDataException if any error occurs during search
     */
    private static ClusterSearchResult findClosestInLeafPage(double[] queryVector, int pageId,
            IVectorClusteringLeafFrame leafFrame) throws HyracksDataException {

        int tupleCount = leafFrame.getTupleCount();

        double bestDistance = Double.MAX_VALUE;
        int bestClusterIndex = -1;
        double[] bestCentroid = null;
        int bestCentroidId = -1;
        int candidatesProcessed = 0;

        Map<String, Object> searchStartFields = new HashMap<>();
        searchStartFields.put("pageId", pageId);
        searchStartFields.put("tupleCount", tupleCount);
        searchStartFields.put("vectorDim", queryVector.length);
        searchStartFields.put("queryVector", queryVector);
        logTraversalEvent("leaf_search_start", searchStartFields);

        // Search all centroids in this page
        for (int i = 0; i < tupleCount; i++) {
            try {
                ITreeIndexTupleReference frameTuple = leafFrame.createTupleReference();
                frameTuple.resetByTupleIndex(leafFrame, i);
                double[] centroid = extractCentroidFromInteriorTuple(frameTuple);
                int centroidID = leafFrame.getCentroidId(i);

                // Check vector dimensionality before distance calculation
                if (centroid.length != queryVector.length) {
                    continue;
                }

                double distance = VectorUtils.calculateEuclideanDistance(queryVector, centroid);
                candidatesProcessed++;

                Map<String, Object> candidateFields = new HashMap<>();
                candidateFields.put("pageId", pageId);
                candidateFields.put("tupleIndex", i);
                candidateFields.put("centroidId", centroidID);
                candidateFields.put("centroidDim", centroid.length);
                candidateFields.put("distance", distance);
                logTraversalEvent("leaf_candidate", candidateFields);

                if (distance < bestDistance) {
                    bestDistance = distance;
                    bestClusterIndex = i;
                    bestCentroid = centroid.clone();
                    bestCentroidId = centroidID;
                }
            } catch (Exception e) {
                System.err.println("ERROR processing tuple " + i + ": " + e.getMessage());
                continue;
            }
        }

        if (bestClusterIndex >= 0) {
            Map<String, Object> searchSelectFields = new HashMap<>();
            searchSelectFields.put("pageId", pageId);
            searchSelectFields.put("selectedTupleIndex", bestClusterIndex);
            searchSelectFields.put("centroidId", bestCentroidId);
            searchSelectFields.put("bestDistance", bestDistance);
            searchSelectFields.put("candidatesProcessed", candidatesProcessed);
            logTraversalEvent("leaf_search_select", searchSelectFields);

            return ClusterSearchResult.create(pageId, bestClusterIndex, bestCentroid, bestDistance, bestCentroidId);
        }
        // TODO : SOME RETURN EMPTY
        return null;
    }

    /**
     * Find the closest centroid in an interior page and return child page ID.
     * 
     * @param queryVector Query vector to find closest centroid for
     * @param pageId Page ID of the interior page
     * @param interiorFrame Interior frame for accessing page data
     * @return Child page ID to descend to, or -1 if no valid child found
     * @throws HyracksDataException if any error occurs during search
     */
    private static int findClosestInInteriorPage(double[] queryVector, int pageId,
            IVectorClusteringInteriorFrame interiorFrame) throws HyracksDataException {

        int tupleCount = interiorFrame.getTupleCount();
        double bestDistance = Double.MAX_VALUE;
        int bestChildPageId = -1;
        int candidatesProcessed = 0;

        Map<String, Object> searchStartFields = new HashMap<>();
        searchStartFields.put("pageId", pageId);
        searchStartFields.put("tupleCount", tupleCount);
        searchStartFields.put("vectorDim", queryVector.length);
        searchStartFields.put("queryVector", queryVector);
        logTraversalEvent("interior_search_start", searchStartFields);

        // Search all centroids in this page
        for (int i = 0; i < tupleCount; i++) {
            ITreeIndexTupleReference frameTuple = interiorFrame.createTupleReference();
            frameTuple.resetByTupleIndex(interiorFrame, i);
            double[] centroid = extractCentroidFromInteriorTuple(frameTuple);

            // Check vector dimensionality before distance calculation
            if (centroid.length != queryVector.length) {
                continue;
            }

            double distance = VectorUtils.calculateEuclideanDistance(queryVector, centroid);
            int childPageId = interiorFrame.getChildPageId(i);
            candidatesProcessed++;

            Map<String, Object> candidateFields = new HashMap<>();
            candidateFields.put("pageId", pageId);
            candidateFields.put("tupleIndex", i);
            candidateFields.put("centroidDim", centroid.length);
            candidateFields.put("distance", distance);
            candidateFields.put("childPageId", childPageId);
            logTraversalEvent("interior_candidate", candidateFields);

            //                    VectorDistanceArrCalculation.euclidean_squared(centroid, queryVector);

            if (distance < bestDistance) {
                bestDistance = distance;
                bestChildPageId = childPageId;
            }
        }

        Map<String, Object> searchSelectFields = new HashMap<>();
        searchSelectFields.put("pageId", pageId);
        searchSelectFields.put("selectedChildPageId", bestChildPageId);
        searchSelectFields.put("bestDistance", bestDistance);
        searchSelectFields.put("candidatesProcessed", candidatesProcessed);
        logTraversalEvent("interior_search_select", searchSelectFields);

        return bestChildPageId;
    }

    private static int findClosestInInteriorPage(double[] queryVector, IVectorClusteringInteriorFrame interiorFrame)
            throws HyracksDataException {

        int tupleCount = interiorFrame.getTupleCount();
        double bestDistance = Double.MAX_VALUE;
        int bestChildPageId = -1;

        // Search all centroids in this page
        for (int i = 0; i < tupleCount; i++) {
            ITreeIndexTupleReference frameTuple = interiorFrame.createTupleReference();
            frameTuple.resetByTupleIndex(interiorFrame, i);
            double[] centroid = extractCentroidFromInteriorTuple(frameTuple);

            // Check vector dimensionality before distance calculation
            if (centroid.length != queryVector.length) {
                continue;
            }

            double distance = VectorUtils.calculateEuclideanDistance(queryVector, centroid);

            //                    VectorDistanceArrCalculation.euclidean_squared(centroid, queryVector);

            if (distance < bestDistance) {
                bestDistance = distance;
                bestChildPageId = interiorFrame.getChildPageId(i);
            }
        }

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
                    System.err.println("=== LEVEL " + level + " | PAGE " + currentPageId + " | TYPE: LEAF ===");
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

                            System.err.println(
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
                    System.err.println("=== LEVEL " + level + " | PAGE " + currentPageId + " | TYPE: INTERIOR ===");
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

                            System.err.println("tuple=" + i + " | cid=" + cid + " | centroid=" + centroidStr
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
}
