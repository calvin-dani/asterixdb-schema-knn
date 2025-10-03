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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.am.vector.predicates.VectorPointPredicate;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Search cursor for vector clustering tree operations.
 * Performs centroid finding via tree traversal and then iterates through data pages of the selected cluster.
 */
public class VectorClusteringSearchCursor implements IIndexCursor {

    // Tree navigation fields
    private IBufferCache bufferCache;
    private int fileId;
    private int rootPageId;
    private ITreeIndexFrameFactory interiorFrameFactory;
    private ITreeIndexFrameFactory leafFrameFactory;
    private ITreeIndexFrameFactory metadataFrameFactory;
    private ITreeIndexFrameFactory dataFrameFactory;

    // Cursor state fields
    private long targetMetadataPageId;
    private long currentDataPageId;
    private float[] queryVector;
    private boolean isOpen;
    private ITupleReference currentTuple;
    private ICachedPage currentPage;
    private IVectorClusteringDataFrame dataFrame;
    private ITreeIndexTupleReference frameTuple;
    private int currentTupleIndex;
    private int tupleCount;

    public VectorClusteringSearchCursor() {
        this.isOpen = false;
        this.currentTupleIndex = 0;
        this.tupleCount = 0;
        this.currentDataPageId = -1;
        this.targetMetadataPageId = -1;
    }

    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public void setRootPageId(int rootPageId) {
        this.rootPageId = rootPageId;
    }

    public void setTargetMetadataPageId(long targetMetadataPageId) {
        this.targetMetadataPageId = targetMetadataPageId;
    }

    public void setFrameFactories(ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            ITreeIndexFrameFactory metadataFrameFactory, ITreeIndexFrameFactory dataFrameFactory) {
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
    }

    public void setQueryVector(float[] queryVector) {
        this.queryVector = queryVector;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.isOpen = true;

        // Extract query vector from search predicate if available
        if (searchPred instanceof VectorPointPredicate) {
            VectorPointPredicate vectorPred = (VectorPointPredicate) searchPred;
            this.queryVector = vectorPred.getQueryVector();
        }

        // If initial state is provided, use its information
        if (initialState instanceof VectorCursorInitialState) {
            VectorCursorInitialState vectorState = (VectorCursorInitialState) initialState;
            if (vectorState.getQueryVector() != null) {
                this.queryVector = vectorState.getQueryVector();
            }
            // If targetMetadataPageId is already set in the state, use it directly
            if (vectorState.getMetadataPageId() != -1) {
                this.targetMetadataPageId = vectorState.getMetadataPageId();
            }
            // Use rootPageId from initial state if provided
            if (vectorState.getRootPageId() != 0) {
                this.rootPageId = vectorState.getRootPageId();
            }
        }

        // If targetMetadataPageId is not set, find the closest cluster first
        if (this.targetMetadataPageId == -1) {
            if (this.queryVector == null) {
                throw HyracksDataException
                        .create(new IllegalArgumentException("Query vector must be provided for centroid finding"));
            }

            // Find closest cluster via tree traversal
            VectorClusteringTree.ClusterSearchResult clusterResult = findClosestClusterFromRoot(this.queryVector);
            this.targetMetadataPageId = getMetadataPageIdFromCluster(clusterResult);
        }

        // Start from the first data page of the target cluster
        this.currentDataPageId = getFirstDataPageFromMetadata();

        if (this.currentDataPageId != -1) {
            openDataPage(this.currentDataPageId);
        } else {
            // No data pages in this cluster
            this.tupleCount = 0;
        }
        this.currentTupleIndex = 0;
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (!isOpen) {
            return false;
        }

        // Check if there are more tuples in current data page
        if (currentTupleIndex < tupleCount) {
            return true;
        }

        // Current page exhausted, try to move to next data page
        return moveToNextDataPage();
    }

    @Override
    public void next() throws HyracksDataException {
        if (!isOpen) {
            throw HyracksDataException.create(new IllegalStateException("Cursor is not open"));
        }
        if (!hasNext()) {
            throw HyracksDataException.create(new IllegalStateException("No more tuples"));
        }

        // Position on next tuple using frameTuple
        if (this.dataFrame != null && this.frameTuple != null) {
            this.frameTuple.resetByTupleIndex(this.dataFrame, currentTupleIndex);
            this.currentTuple = this.frameTuple;
        }
        currentTupleIndex++;
    }

    @Override
    public ITupleReference getTuple() {
        return currentTuple;
    }

    /**
     * Get the query vector used for this search.
     */
    public float[] getQueryVector() {
        return queryVector;
    }

    /**
     * Get the first data page ID from the target metadata page.
     */
    private long getFirstDataPageFromMetadata() throws HyracksDataException {
        if (targetMetadataPageId == -1) {
            return -1;
        }

        ICachedPage metadataPage =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) targetMetadataPageId));
        try {
            metadataPage.acquireReadLatch();
            IVectorClusteringMetadataFrame metadataFrame = createMetadataFrame();
            metadataFrame.setPage(metadataPage);

            int tupleCount = metadataFrame.getTupleCount();
            if (tupleCount > 0) {
                // Get the first data page from the metadata page
                ITreeIndexTupleReference tuple = metadataFrame.createTupleReference();
                tuple.resetByTupleIndex(metadataFrame, 0);
                return extractDataPageIdFromMetadataTuple(tuple);
            }
            return -1;
        } finally {
            metadataPage.releaseReadLatch();
            bufferCache.unpin(metadataPage);
        }
    }

    /**
     * Open a specific data page and initialize the cursor on it.
     */
    private void openDataPage(long dataPageId) throws HyracksDataException {
        // Close current page if open
        closeCurrentPage();

        // Pin and acquire the new data page
        this.currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) dataPageId));
        this.currentPage.acquireReadLatch();

        // Initialize data frame
        this.dataFrame = createDataFrame();
        this.dataFrame.setPage(currentPage);
        this.frameTuple = this.dataFrame.createTupleReference();
        this.tupleCount = this.dataFrame.getTupleCount();
        this.currentTupleIndex = 0;
    }

    /**
     * Move to the next data page using the linked list structure.
     * This is more efficient than going back to the metadata page each time.
     * @return true if successfully moved to next page, false if no more pages
     */
    private boolean moveToNextDataPage() throws HyracksDataException {
        if (dataFrame == null) {
            return false;
        }

        // Get the next page ID from the current data frame's linked list pointer
        int nextDataPageId = dataFrame.getNextPage();
        if (nextDataPageId == -1) {
            return false; // No more data pages in the linked list
        }

        // Move to the next data page
        this.currentDataPageId = nextDataPageId;
        openDataPage(nextDataPageId);
        return this.tupleCount > 0; // Return true if new page has tuples
    }

    /**
     * Extract data page ID from a metadata tuple.
     */
    private long extractDataPageIdFromMetadataTuple(ITreeIndexTupleReference tuple) {
        // Metadata tuple format: <max_distance, data_page_id>
        // Data page ID is in the second field

        // Metadata tuple format: <max_distance, data_page_id>
        // Data page ID is in the second field (field index 1)
        byte[] data = tuple.getFieldData(1); // Returns entire buffer
        int offset = tuple.getFieldStart(1); // Offset to field 1 within buffer

        // FIXED: Use IntegerPointable to extract 4-byte integer (not 8-byte long)
        return IntegerPointable.getInteger(data, offset);
    }

    /**
     * Close current data page if open.
     */
    private void closeCurrentPage() throws HyracksDataException {
        if (currentPage != null) {
            currentPage.releaseReadLatch();
            bufferCache.unpin(currentPage);
            currentPage = null;
        }
    }

    /**
     * Create a metadata frame instance using the frame factory.
     */
    private IVectorClusteringMetadataFrame createMetadataFrame() {
        if (metadataFrameFactory == null) {
            throw new IllegalStateException("Metadata frame factory not set");
        }
        return (IVectorClusteringMetadataFrame) metadataFrameFactory.createFrame();
    }

    /**
     * Create a data frame instance using the frame factory.
     */
    private IVectorClusteringDataFrame createDataFrame() {
        if (dataFrameFactory == null) {
            throw new IllegalStateException("Data frame factory not set");
        }
        return (IVectorClusteringDataFrame) dataFrameFactory.createFrame();
    }

    /**
     * Find the closest cluster by traversing the tree from root to leaf.
     * This integrates the centroid finding logic into the cursor.
     */
    private VectorClusteringTree.ClusterSearchResult findClosestClusterFromRoot(float[] queryVector)
            throws HyracksDataException {
        // Start from root page
        int currentPageId = rootPageId;
        double bestDistance = Double.MAX_VALUE;
        VectorClusteringTree.ClusterSearchResult bestResult = null;
        int loopCounter = 0;

        // Traverse from root to leaf
        while (true) {
            loopCounter++;
            if (loopCounter > 10) { // Safety check to prevent infinite loops
                throw HyracksDataException
                        .create(new IllegalStateException("Infinite loop detected in tree traversal"));
            }

            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));

            try {
                page.acquireReadLatch();

                // Check if this is a leaf page
                IVectorClusteringLeafFrame leafFrame = createLeafFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                if (isLeaf) {
                    // Leaf level - find closest centroid
                    int tupleCount = leafFrame.getTupleCount();
                    int bestClusterIndex = -1;
                    double leafBestDistance = Double.MAX_VALUE;

                    for (int i = 0; i < tupleCount; i++) {
                        // Get centroid from leaf frame tuple
                        ITreeIndexTupleReference frameTuple = leafFrame.createTupleReference();
                        frameTuple.resetByTupleIndex(leafFrame, i);
                        double[] centroid = extractCentroidFromLeafTuple(frameTuple);

                        // Check vector dimensionality
                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = VectorUtils.calculateEuclideanDistance(queryVector, centroid);

                        if (distance < leafBestDistance) {
                            leafBestDistance = distance;
                            bestClusterIndex = i;
                        }
                    }

                    if (bestClusterIndex >= 0) {
                        ITreeIndexTupleReference frameTuple = leafFrame.createTupleReference();
                        frameTuple.resetByTupleIndex(leafFrame, bestClusterIndex);
                        double[] bestCentroid = extractCentroidFromLeafTuple(frameTuple);
                        bestResult = new VectorClusteringTree.ClusterSearchResult(currentPageId, bestClusterIndex,
                                bestCentroid, leafBestDistance);
                    }
                    break; // Found leaf level result

                } else {
                    // Interior level - find closest centroid and descend
                    IVectorClusteringInteriorFrame interiorFrame = createInteriorFrame();
                    interiorFrame.setPage(page);
                    int tupleCount = interiorFrame.getTupleCount();
                    int bestChildIndex = -1;
                    bestDistance = Double.MAX_VALUE;

                    for (int i = 0; i < tupleCount; i++) {
                        // Get centroid from interior frame tuple
                        ITreeIndexTupleReference frameTuple = interiorFrame.createTupleReference();
                        frameTuple.resetByTupleIndex(interiorFrame, i);
                        double[] centroid = extractCentroidFromInteriorTuple(frameTuple);

                        // Check vector dimensionality
                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = VectorUtils.calculateEuclideanDistance(queryVector, centroid);

                        if (distance < bestDistance) {
                            bestDistance = distance;
                            bestChildIndex = i;
                        }
                    }

                    if (bestChildIndex >= 0) {
                        // Get child page ID for best centroid
                        int nextPageId = interiorFrame.getChildPageId(bestChildIndex);
                        currentPageId = nextPageId;
                    } else {
                        throw HyracksDataException
                                .create(new IllegalStateException("No valid centroid found in interior page"));
                    }
                }

            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        if (bestResult == null) {
            throw HyracksDataException.create(new IllegalStateException("No closest cluster found"));
        }

        return bestResult;
    }

    /**
     * Get metadata page ID from cluster search result.
     */
    private long getMetadataPageIdFromCluster(VectorClusteringTree.ClusterSearchResult clusterResult)
            throws HyracksDataException {
        ICachedPage leafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, clusterResult.leafPageId));
        try {
            leafPage.acquireReadLatch();
            IVectorClusteringLeafFrame leafFrame = createLeafFrame();
            leafFrame.setPage(leafPage);
            return leafFrame.getMetadataPagePointer(clusterResult.clusterIndex);
        } finally {
            leafPage.releaseReadLatch();
            bufferCache.unpin(leafPage);
        }
    }

    /**
     * Create frame instances.
     */
    private IVectorClusteringInteriorFrame createInteriorFrame() {
        if (interiorFrameFactory == null) {
            throw new IllegalStateException("Interior frame factory not set");
        }
        return (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
    }

    private IVectorClusteringLeafFrame createLeafFrame() {
        if (leafFrameFactory == null) {
            throw new IllegalStateException("Leaf frame factory not set");
        }
        return (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
    }

    /**
     * Extract centroid from a leaf frame tuple (format: <cid, centroid, metadata_ptr>).
     */
    private double[] extractCentroidFromLeafTuple(ITreeIndexTupleReference tuple) {
        // Centroid is the second field in leaf frame tuples
        try {
            // Create field serializers array - specify only the centroid field we need
            ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[3];
            fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE; // Field 0: cid
            fieldSerdes[1] = FloatArraySerializerDeserializer.INSTANCE; // Field 1: centroid
            fieldSerdes[2] = IntegerSerializerDeserializer.INSTANCE; // Field 2: metadata_pointer

            // Deserialize the tuple using the proper TupleUtils method
            Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);

            // Extract the centroid from the deserialized fields
            float[] floatCentroid = (float[]) fieldValues[1];

            // Convert from float[] to double[]
            double[] doubleCentroid = new double[floatCentroid.length];
            for (int i = 0; i < floatCentroid.length; i++) {
                doubleCentroid[i] = floatCentroid[i];
            }

            return doubleCentroid;

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to extract centroid from interior tuple using TupleUtils.deserializeTuple()", e);
        }
    }

    /**
     * Extract centroid from an interior frame tuple (format: <cid, centroid, child_ptr>).
     */
    private double[] extractCentroidFromInteriorTuple(ITreeIndexTupleReference tuple) {
        // Centroid is the second field in interior frame tuples
        try {
            // Create field serializers array - specify only the centroid field we need
            ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[3];
            fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE; // Field 0: cid
            fieldSerdes[1] = FloatArraySerializerDeserializer.INSTANCE; // Field 1: centroid
            fieldSerdes[2] = IntegerSerializerDeserializer.INSTANCE; // Field 2: metadata_pointer

            // Deserialize the tuple using the proper TupleUtils method
            Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);

            // Extract the centroid from the deserialized fields
            float[] floatCentroid = (float[]) fieldValues[1];

            // Convert from float[] to double[]
            double[] doubleCentroid = new double[floatCentroid.length];
            for (int i = 0; i < floatCentroid.length; i++) {
                doubleCentroid[i] = floatCentroid[i];
            }

            return doubleCentroid;

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to extract centroid from interior tuple using TupleUtils.deserializeTuple()", e);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (isOpen) {
            closeCurrentPage();
        }
        this.isOpen = false;
        this.currentTuple = null;
        this.currentTupleIndex = 0;
        this.tupleCount = 0;
        this.currentDataPageId = -1;
    }

    @Override
    public void destroy() throws HyracksDataException {
        close();
    }
}
