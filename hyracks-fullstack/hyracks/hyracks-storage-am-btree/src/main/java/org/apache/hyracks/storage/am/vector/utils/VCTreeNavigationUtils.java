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

                System.err
                        .println("Page " + currentPageId + " - isLeaf: " + isLeaf + ", level: " + leafFrame.getLevel());

                if (isLeaf) {
                    // Leaf level - find closest centroid
                    bestResult = findClosestInLeafPage(queryVector, currentPageId, leafFrame);
                    break; // Found leaf level result

                } else {
                    // Interior level - find closest centroid and descend
                    IVectorClusteringInteriorFrame interiorFrame =
                            (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);
                    int nextPageId = findClosestInInteriorPage(queryVector, currentPageId, interiorFrame);
                    if (nextPageId == -1) {
                        throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                                "No valid centroid found in interior cluster");
                    }
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

        return bestResult;
    }

    /**
     * Extract centroid from an interior frame tuple (format: <cid, centroid, child_ptr>).
     */
    private static double[] extractCentroidFromInteriorTuple(ITreeIndexTupleReference tuple) {
        // Centroid is the second field in interior frame tuples
        try {
            // Create field serializers array - specify only the centroid field we need
            ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[3];
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
        System.err.println("Leaf page " + pageId + " has " + tupleCount + " tuples");

        double bestDistance = Double.MAX_VALUE;
        int bestClusterIndex = -1;
        double[] bestCentroid = null;
        int bestCentroidId = -1;

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
            return ClusterSearchResult.create(pageId, bestClusterIndex, bestCentroid, bestDistance, bestCentroidId);
        }

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

}
