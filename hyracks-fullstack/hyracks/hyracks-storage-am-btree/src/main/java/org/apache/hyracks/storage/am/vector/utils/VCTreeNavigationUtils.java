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

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

import static org.apache.hyracks.storage.am.vector.tuples.VectorClusteringTupleUtils.extractVectorFromTuple;

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
                
                System.err.println("Page " + currentPageId + " - isLeaf: " + isLeaf + ", level: " + leafFrame.getLevel());

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
                double[] centroid = extractCentroidFromLeafTuple(frameTuple);
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
            double[] centroid =  extractVectorFromTuple(frameTuple);
//                    extractCentroidFromInteriorTuple(frameTuple);

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
    private static double[] extractCentroidFromLeafTuple(ITreeIndexTupleReference tuple) {
        try {
            int fieldCount = tuple.getFieldCount();
            
            // Try to find the centroid field - it should be the largest field
            int centroidFieldIndex = -1;
            int maxFieldLength = 0;
            for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
                int fieldLength = tuple.getFieldLength(fieldIndex);
                if (fieldLength > maxFieldLength) {
                    maxFieldLength = fieldLength;
                    centroidFieldIndex = fieldIndex;
                }
            }
            
            // Use the largest field as the centroid field
            byte[] data = tuple.getFieldData(centroidFieldIndex);
            int offset = tuple.getFieldStart(centroidFieldIndex);
            int length = tuple.getFieldLength(centroidFieldIndex);

            // Try both float and double interpretations
            int floatDimensions = length / 4; // 4 bytes per float
            int doubleDimensions = length / 8; // 8 bytes per double
            
            // Check if the length makes sense for either interpretation
            if (floatDimensions > 0 && floatDimensions <= 10000) { // Reasonable range for vector dimensions
                return extractCentroidAsFloatArray(data, offset, length, floatDimensions);
            } else if (doubleDimensions > 0 && doubleDimensions <= 10000) {
                return extractCentroidAsDoubleArray(data, offset, length, doubleDimensions);
            } else {
                throw new RuntimeException("Invalid centroid dimensions: float=" + floatDimensions + 
                                         ", double=" + doubleDimensions + ", length=" + length);
            }

        } catch (Exception e) {
            System.err.println("ERROR extracting centroid: " + e.getMessage());
            throw new RuntimeException("Failed to extract centroid from leaf tuple using direct byte parsing", e);
        }
    }
    
    private static double[] extractCentroidAsFloatArray(byte[] data, int offset, int length, int numDimensions) {
        double[] centroid = new double[numDimensions];

        for (int i = 0; i < numDimensions; i++) {
            int floatOffset = offset + (i * 4);
            if (floatOffset + 3 >= data.length) {
                throw new RuntimeException("Insufficient data for float at index " + i + 
                                         ", floatOffset=" + floatOffset + ", data.length=" + data.length);
            }
            
            int bits = (data[floatOffset] << 24) | ((data[floatOffset + 1] & 0xFF) << 16)
                    | ((data[floatOffset + 2] & 0xFF) << 8) | (data[floatOffset + 3] & 0xFF);
            float floatValue = Float.intBitsToFloat(bits);
            centroid[i] = (double) floatValue; // Convert float to double
        }

        return centroid;
    }
    
    private static double[] extractCentroidAsDoubleArray(byte[] data, int offset, int length, int numDimensions) {
        double[] centroid = new double[numDimensions];

        for (int i = 0; i < numDimensions; i++) {
            int doubleOffset = offset + (i * 8);
            if (doubleOffset + 7 >= data.length) {
                throw new RuntimeException("Insufficient data for double at index " + i + 
                                         ", doubleOffset=" + doubleOffset + ", data.length=" + data.length);
            }
            
            long bits = ((long) data[doubleOffset] << 56) | (((long) data[doubleOffset + 1] & 0xFF) << 48)
                    | (((long) data[doubleOffset + 2] & 0xFF) << 40) | (((long) data[doubleOffset + 3] & 0xFF) << 32)
                    | (((long) data[doubleOffset + 4] & 0xFF) << 24) | (((long) data[doubleOffset + 5] & 0xFF) << 16)
                    | (((long) data[doubleOffset + 6] & 0xFF) << 8) | ((long) data[doubleOffset + 7] & 0xFF);
            centroid[i] = Double.longBitsToDouble(bits);
        }

        return centroid;
    }

    /**
     * Extract centroid from an interior frame tuple (format: <cid, centroid, child_ptr>).
     * Uses direct byte parsing to match the tuple writer's getCentroid() method.
     * 
     * @param tuple Interior frame tuple
     * @return Centroid vector
     */
    private static double[] extractCentroidFromInteriorTuple(ITreeIndexTupleReference tuple) {
        try {
            // Use the same approach as VectorClusteringInteriorTupleWriter.getCentroid()
            byte[] data = tuple.getFieldData(1); // CENTROID_FIELD = 1
            int offset = tuple.getFieldStart(1);
            int length = tuple.getFieldLength(1);

            // Assuming centroid is stored as array of doubles
            int numDimensions = length / 8; // 8 bytes per double
            double[] centroid = new double[numDimensions];

            for (int i = 0; i < numDimensions; i++) {
                int doubleOffset = offset + (i * 8);
                long bits = ((long) data[doubleOffset] << 56) | (((long) data[doubleOffset + 1] & 0xFF) << 48)
                        | (((long) data[doubleOffset + 2] & 0xFF) << 40) | (((long) data[doubleOffset + 3] & 0xFF) << 32)
                        | (((long) data[doubleOffset + 4] & 0xFF) << 24) | (((long) data[doubleOffset + 5] & 0xFF) << 16)
                        | (((long) data[doubleOffset + 6] & 0xFF) << 8) | ((long) data[doubleOffset + 7] & 0xFF);
                centroid[i] = Double.longBitsToDouble(bits);
            }

            return centroid;

        } catch (Exception e) {
            throw new RuntimeException("Failed to extract centroid from interior tuple using direct byte parsing", e);
        }
    }
}
