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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.am.vector.utils.VCTreeNavigationUtils;
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
    private int K; // Target number of records
    private int recordsCollected; // Count of records returned so far
    private ClusterSearchResult currentClusterResult; // Current cluster being scanned
    private boolean exhaustedAllClusters; // Flag to stop searching for more clusters
    private VCTreeNavigationUtils.NavigationState iteratorState; // DFS navigation state
    private int clustersProbed; // Count of clusters scanned during this search

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
     * VectorPointPredicate now contains the k value for top-K ANN search.
     */
    private int extractK(ISearchPredicate searchPred) {
        if (searchPred instanceof VectorPointPredicate) {
            return ((VectorPointPredicate) searchPred).getK();
        }

        if (searchPred instanceof VectorAnnPredicate) {
            return ((VectorAnnPredicate) searchPred).getK();
        }

        // Fallback: return a large number (scan all clusters)
        return Integer.MAX_VALUE;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.isOpen = true;
        this.recordsCollected = 0;
        this.exhaustedAllClusters = false;
        this.clustersProbed = 0;

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

        // Initialize DFS iterator for multi-cluster search
        if (this.queryVector == null) {
            throw HyracksDataException
                    .create(new IllegalArgumentException("Query vector must be provided for centroid finding"));
        }

        // Create navigation state for iterative DFS
        this.iteratorState = new VCTreeNavigationUtils.NavigationState(bufferCache, fileId, rootPageId,
                interiorFrameFactory, leafFrameFactory, queryVector);

        // Initialize iterator and get first (closest) cluster
        this.currentClusterResult = VCTreeNavigationUtils.initializeClusterIterator(iteratorState, distanceFunction);

        if (this.currentClusterResult == null) {
            // Empty tree
            this.tupleCount = 0;
            this.currentTupleIndex = 0;
            this.exhaustedAllClusters = true;
            return;
        }

        // Get metadata page pointer for first cluster
        this.targetMetadataPageId = getMetadataPageIdFromCluster(currentClusterResult);

        // Start from the first data page of the first cluster
        this.currentDataPageId = getFirstDataPageFromMetadata();

        if (this.currentDataPageId != -1) {
            openDataPage(this.currentDataPageId);
        } else {
            // No data pages in this cluster
            this.tupleCount = 0;
        }
        this.currentTupleIndex = 0;
        this.clustersProbed = 1; // First cluster opened
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (!isOpen) {
            System.err.println("[VectorClusteringSearchCursor.hasNext] Cursor not open, returning false");
            return false;
        }

        // Check if there are more tuples in current data page
        if (currentTupleIndex < tupleCount) {
            return true;
        }

        // Current page exhausted, try to move to next data page in same cluster
        if (moveToNextDataPage()) {
            System.err.println(String.format(
                    "[VectorClusteringSearchCursor.hasNext] Moved to next data page, tupleCount=%d", tupleCount));
            return true; // Found more data pages in current cluster
        }

        System.err.println(String.format(
                "[VectorClusteringSearchCursor.hasNext] Current cluster exhausted | recordsCollected=%d, K=%d, exhaustedAllClusters=%s",
                recordsCollected, K, exhaustedAllClusters));

        // Current cluster exhausted
        // Check if we have enough records
        if (recordsCollected >= K) {
            System.err.println(
                    "[VectorClusteringSearchCursor.hasNext] Collected enough records (K reached), returning false");
            return false; // We have K records, done!
        }

        // Check if we've exhausted all clusters
        if (exhaustedAllClusters) {
            System.err.println("[VectorClusteringSearchCursor.hasNext] All clusters exhausted, returning false");
            return false; // No more clusters to scan
        }

        // Need more records: find and open next closest cluster
        // Loop to skip empty clusters
        while (true) {
            System.err.println("[VectorClusteringSearchCursor.hasNext] Finding next closest cluster...");
            ClusterSearchResult nextCluster =
                    VCTreeNavigationUtils.findNextClosestCluster(iteratorState, distanceFunction);
            if (nextCluster == null) {
                exhaustedAllClusters = true;
                System.err.println(
                        "[VectorClusteringSearchCursor.hasNext] findNextClosestCluster returned null, marking exhausted");
                return false; // No more clusters available
            }

            // Open next cluster
            openCluster(nextCluster);
            boolean hasData = currentTupleIndex < tupleCount;
            System.err.println(String.format(
                    "[VectorClusteringSearchCursor.hasNext] Opened next cluster, hasData=%s, tupleCount=%d", hasData,
                    tupleCount));

            if (hasData) {
                return true; // Found cluster with data
            }

            // Empty cluster, continue to next one
            System.err.println("[VectorClusteringSearchCursor.hasNext] Cluster is empty, skipping to next cluster");
        }
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
        recordsCollected++; // Track how many records we've returned
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
        this.clustersProbed++; // Increment cluster counter

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

        // Log cluster probing
        System.err.println(String.format(
                "[VectorClusteringSearchCursor] Opened cluster %d (centroidId=%d, distance=%.4f) | Total clusters probed: %d | Records collected so far: %d | Target K: %d",
                clustersProbed, cluster.centroidId, cluster.distance, clustersProbed, recordsCollected, K));
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

            // Log final statistics
            System.err.println(String.format(
                    "[VectorClusteringSearchCursor] Search completed | Total clusters probed: %d | Total records returned: %d | Target K: %d | Exhausted all clusters: %s",
                    clustersProbed, recordsCollected, K, exhaustedAllClusters));
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
