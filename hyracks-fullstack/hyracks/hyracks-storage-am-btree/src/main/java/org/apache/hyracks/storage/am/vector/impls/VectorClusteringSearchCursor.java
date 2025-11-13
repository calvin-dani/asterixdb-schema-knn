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
package org.apache.hyracks.storage.am.vector.impls;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
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
    private double[] queryVector;
    private boolean isOpen;
    private ITupleReference currentTuple;
    private ICachedPage currentPage;
    private IVectorClusteringDataFrame dataFrame;
    private ITreeIndexTupleReference frameTuple;
    private int currentTupleIndex;
    private int tupleCount;
    private IIndexAccessor accessor;
    private IVectorDistanceFunction distanceFunction;

    // Multi-cluster support fields
    private int K;  // Target number of records
    private int recordsCollected;  // Count of records returned so far
    private ClusterSearchResult currentClusterResult;  // Current cluster being scanned
    private boolean exhaustedAllClusters;  // Flag to stop searching for more clusters

    // For finding next clusters (lazy-populated)
    private List<ClusterSearchResult> remainingClusters;  // All clusters sorted by distance
    private int nextClusterIndex;  // Index in remainingClusters

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

    public void setQueryVector(double[] queryVector) {
        this.queryVector = queryVector;
    }

    /**
     * Extract K value from search predicate.
     * Tries to get K from VectorAnnPredicate, or from query tuple via parameters, or defaults to Integer.MAX_VALUE.
     */
    private int extractK(ISearchPredicate searchPred) throws HyracksDataException {
        // Try to extract from VectorAnnPredicate if available
        if (searchPred instanceof VectorAnnPredicate) {
            return ((VectorAnnPredicate) searchPred).getK();
        }

        // Try to extract from query tuple via parameters
        if (searchPred instanceof VectorPointPredicate) {
            VectorPointPredicate vectorPred = (VectorPointPredicate) searchPred;
            ITupleReference queryTuple = vectorPred.getQueryTuple();

            if (queryTuple != null && accessor != null) {
                // Get K field index from parameters
                Object kFieldIndexObj = accessor.getOpContext().getIndexAccessParameters()
                        .getParameters().get(HyracksConstants.VECTOR_K);

                if (kFieldIndexObj instanceof Integer) {
                    int kFieldIndex = (Integer) kFieldIndexObj;
                    try {
                        // Extract K from tuple field
                        byte[] fieldData = queryTuple.getFieldData(kFieldIndex);
                        int fieldStart = queryTuple.getFieldStart(kFieldIndex);
                        int fieldLength = queryTuple.getFieldLength(kFieldIndex);

                        ByteArrayInputStream bais = new ByteArrayInputStream(fieldData, fieldStart, fieldLength);
                        DataInputStream dis = new DataInputStream(bais);
                        return IntegerSerializerDeserializer.read(dis);
                    } catch (Exception e) {
                        // Fall through to default
                    }
                }
            }
        }

        // Default: no limit
        return Integer.MAX_VALUE;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.isOpen = true;
        this.recordsCollected = 0;
        this.exhaustedAllClusters = false;
        this.remainingClusters = null;
        this.nextClusterIndex = 0;

        // Get query vector and other parameters from initial state
        // The query vector is passed via IIndexAccessParameters from the operator layer
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

            this.accessor = vectorState.getIndexAccessor();

            // Get distance function from initial state
            this.distanceFunction = vectorState.getDistanceFunction();
            // Fallback to Euclidean if not provided
            if (this.distanceFunction == null) {
                this.distanceFunction = VectorUtils::calculateEuclideanDistance;
            }
        }

        // Extract K from predicate or parameters
        this.K = extractK(searchPred);

        // If targetMetadataPageId is not set, find the closest cluster first
        if (this.targetMetadataPageId == -1) {
            if (this.queryVector == null) {
                throw HyracksDataException
                        .create(new IllegalArgumentException("Query vector must be provided for centroid finding"));
            }

            // Find closest cluster via tree traversal using the provided distance function
            ClusterSearchResult clusterResult = ((VectorClusteringTree.VectorClusteringTreeAccessor) accessor)
                    .findClosestLeafCentroid(queryVector, this.distanceFunction);
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

        // Current page exhausted, try to move to next data page in same cluster
        if (moveToNextDataPage()) {
            return true;  // Found more data pages in current cluster
        }

        // Current cluster exhausted
        // Check if we have enough records
        if (recordsCollected >= K) {
            return false;  // We have K records, done!
        }

        // Check if we've exhausted all clusters
        if (exhaustedAllClusters) {
            return false;  // No more clusters to scan
        }

        // Need more records: find and open next closest cluster
        ClusterSearchResult nextCluster = findNextClosestCluster();
        if (nextCluster == null) {
            exhaustedAllClusters = true;
            return false;  // No more clusters available
        }

        // Open next cluster and check if it has data
        openCluster(nextCluster);
        return currentTupleIndex < tupleCount;  // Check if new cluster has tuples
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
        recordsCollected++;  // Track how many records we've returned
    }

    @Override
    public ITupleReference getTuple() {
        return currentTuple;
    }

    /**
     * Get the query vector used for this search.
     */
    public double[] getQueryVector() {
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
                return metadataFrame.getDataPagePointer(0);
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
     * Open a specific cluster for scanning.
     * Gets metadata page, then opens first data page.
     */
    private void openCluster(ClusterSearchResult cluster) throws HyracksDataException {
        if (cluster == null) {
            this.currentDataPageId = -1;
            this.tupleCount = 0;
            return;
        }

        this.currentClusterResult = cluster;

        // Get metadata page pointer from leaf frame
        this.targetMetadataPageId = getMetadataPageIdFromCluster(cluster);

        // Get first data page from metadata
        this.currentDataPageId = getFirstDataPageFromMetadata();

        if (this.currentDataPageId != -1) {
            openDataPage(this.currentDataPageId);
        } else {
            // Empty cluster
            this.tupleCount = 0;
        }

        this.currentTupleIndex = 0;
    }

    /**
     * Find the next closest cluster to scan.
     * Lazily populates list of all clusters on first call.
     *
     * @return Next closest cluster, or null if no more clusters
     */
    private ClusterSearchResult findNextClosestCluster() throws HyracksDataException {
        // Lazy initialization: find all clusters sorted by distance
        if (remainingClusters == null) {
            remainingClusters = findAllLeafClustersSorted();
            nextClusterIndex = 0;

            // Skip the first cluster (already scanned in open())
            if (!remainingClusters.isEmpty() &&
                    remainingClusters.get(0).centroidId == currentClusterResult.centroidId) {
                nextClusterIndex = 1;
            }
        }

        // Get next cluster from list
        if (nextClusterIndex >= remainingClusters.size()) {
            return null;  // No more clusters
        }

        ClusterSearchResult next = remainingClusters.get(nextClusterIndex);
        nextClusterIndex++;
        return next;
    }

    /**
     * Find all leaf clusters sorted by distance to query vector.
     * This is called lazily only when we need more clusters.
     */
    private List<ClusterSearchResult> findAllLeafClustersSorted() throws HyracksDataException {
        return ((VectorClusteringTree.VectorClusteringTreeAccessor) accessor)
                .findAllLeafClustersSorted(queryVector);
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
     * Get metadata page ID from cluster search result.
     */
    private long getMetadataPageIdFromCluster(ClusterSearchResult clusterResult) throws HyracksDataException {
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
