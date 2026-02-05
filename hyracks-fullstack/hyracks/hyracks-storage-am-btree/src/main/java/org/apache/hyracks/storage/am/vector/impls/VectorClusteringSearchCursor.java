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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVectorQuantizer;
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
    /* Metadata page for current cluster */
    private long targetMetadataPageId;
    /* Currently opened data page */
    private long currentDataPageId;
    private double[] queryVector;
    private boolean isOpen;
    private ITupleReference currentTuple;
    private ICachedPage currentPage;
    private IVectorClusteringDataFrame dataFrame;
    private ITreeIndexTupleReference frameTuple;
    /* Position in current data page (0-based, next tuple to read) */
    private int currentTupleIndex;
    /* Total tuples in current data page */
    private int tupleCount;
    private IIndexAccessor accessor;
    private IVectorDistanceFunction distanceFunction;

    // Multi-cluster support fields
    /* Total records iterated (before any LSM-layer filtering) */
    private int recordsIterated;
    /* Current cluster being scanned */
    private ClusterSearchResult currentClusterResult;
    /* Flag when no more clusters available */
    private boolean exhaustedAllClusters;
    /* DFS navigation state for query mode */
    private VCTreeNavigationUtils.NavigationState iteratorState;
    /* Count of clusters scanned */
    private int clustersProbed;

    // Two modes:
    // - fullScanMode = false: Query mode - distance-based cluster iteration (closest clusters first)
    // - fullScanMode = true: Merge mode - sequential cluster iteration (0→1→2→...)
    private boolean fullScanMode;
    /* For full-scan: which cluster index we're at (0, 1, 2, ...) */
    private int currentSequentialClusterIndex;
    /* Total number of leaf clusters */
    private int totalLeafClusters;
    /* First directory page ID (cluster 0) */
    private long firstDirectoryPageId;
    /* All directory page IDs collected from all leaf pages */
    private List<Long> allDirectoryPageIds;

    // Shared state from LSM layer (for DFS visited tracking)
    private Set<Integer> sharedVisitedSet; // Shared visited set from LSM layer
    private int nprobe; // Minimum clusters to probe before K-check
    private double epsilon; // Distance threshold for level-wise

    // Quantization state (propagated from VectorCursorInitialState)
    private double[] quantizedQueryVector; // Quantized query vector (null = non-quantized)
    private IVectorQuantizer quantizer; // Quantizer instance (null = non-quantized)

    public VectorClusteringSearchCursor() {
        this.isOpen = false;
        this.currentTupleIndex = 0;
        this.tupleCount = 0;
        this.currentDataPageId = -1;
        this.targetMetadataPageId = -1;
    }

    /**
     * Check if the cursor is open and ready for use.
     */
    public boolean isOpen() {
        return isOpen;
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

    public void setFullScanMode(boolean fullScanMode) {
        this.fullScanMode = fullScanMode;
    }

    /**
     * Set the shared visited set from LSM layer.
     * This allows tracking visited centroids across all LSM components.
     */
    public void setSharedVisitedSet(Set<Integer> visitedSet) {
        this.sharedVisitedSet = visitedSet;
        if (this.iteratorState != null) {
            this.iteratorState.setVisitedSet(visitedSet);
        }
    }

    /**
     * Get the number of clusters probed so far.
     */
    public int getClustersProbed() {
        return clustersProbed;
    }

    /**
     * Reset the clusters probed counter.
     * Used when re-opening cursor to a different first cluster (e.g., level-wise[0] instead of DFS result).
     */
    public void resetClustersProbed() {
        this.clustersProbed = 0;
    }

    /**
     * Get the distance function.
     */
    public IVectorDistanceFunction getDistanceFunction() {
        return this.distanceFunction;
    }

    /**
     * Get the quantized query vector, or null if quantization is not configured.
     */
    public double[] getQuantizedQueryVector() {
        return this.quantizedQueryVector;
    }

    /**
     * Get the vector quantizer, or null if quantization is not configured.
     */
    public IVectorQuantizer getQuantizer() {
        return this.quantizer;
    }

    /**
     * Extract nprobe value from search predicate.
     */
    private int extractNprobe(ISearchPredicate searchPred) {
        if (searchPred instanceof VectorPointPredicate) {
            return ((VectorPointPredicate) searchPred).getNprobe();
        }
        return 1; // Default: probe 1 cluster
    }

    /**
     * Extract epsilon value from search predicate.
     */
    private double extractEpsilon(ISearchPredicate searchPred) {
        if (searchPred instanceof VectorPointPredicate) {
            return ((VectorPointPredicate) searchPred).getEpsilon();
        }
        return 0.0; // Default: no epsilon
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        this.isOpen = true;
        this.recordsIterated = 0;
        this.exhaustedAllClusters = false;
        this.clustersProbed = 0;

        // Get query vector and other parameters from initial state
        // The query vector is passed via IIndexAccessParameters from the operator layer
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

        // Extract quantized state from initial state (null = non-quantized path)
        this.quantizedQueryVector = vectorState.getQuantizedQueryVector();
        this.quantizer = vectorState.getQuantizer();

        // Extract nprobe and epsilon from predicate
        this.nprobe = extractNprobe(searchPred);
        this.epsilon = extractEpsilon(searchPred);

        if (fullScanMode) {
            // Full-scan mode: Navigate to cluster 0 and iterate sequentially
            navigateToFirstCluster();
        } else {
            // Query mode: Find closest cluster using DFS
            // NOTE: Level-wise exploration is handled by LSM layer via openClusterById()
            if (this.queryVector == null) {
                throw HyracksDataException
                        .create(new IllegalArgumentException("Query vector must be provided for centroid finding"));
            }

            // Create navigation state for iterative DFS with shared visited set
            if (sharedVisitedSet != null) {
                this.iteratorState = new VCTreeNavigationUtils.NavigationState(bufferCache, fileId, rootPageId,
                        interiorFrameFactory, leafFrameFactory, queryVector, sharedVisitedSet);
            } else {
                this.iteratorState = new VCTreeNavigationUtils.NavigationState(bufferCache, fileId, rootPageId,
                        interiorFrameFactory, leafFrameFactory, queryVector);
            }

            // Initialize DFS iterator and get first (closest) cluster
            this.currentClusterResult =
                    VCTreeNavigationUtils.initializeClusterIterator(iteratorState, distanceFunction);

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
                "[VectorClusteringSearchCursor.hasNext] Current cluster exhausted | recordsIterated=%d, exhaustedAllClusters=%s",
                recordsIterated, exhaustedAllClusters));

        // Current cluster exhausted - return false
        // Let the LSM layer decide whether to advance to next cluster
        return false;
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
        recordsIterated++; // Track how many records we've iterated through
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
     * Get the current cluster result (the cluster this cursor is currently scanning).
     * Used by LSM layer to mark the first cluster as visited.
     */
    public ClusterSearchResult getCurrentClusterResult() {
        return currentClusterResult;
    }

    /**
     * Navigate to the first cluster and collect ALL directory page IDs for full-scan mode.
     * Scans all leaf pages in overflow chain to build complete directory list.
     * Follows the path: root → first child → interior → first child → leftmost leaf
     * Then: leftmost leaf → next leaf → ... → last leaf (following nextLeaf pointers)
     */
    private void navigateToFirstCluster() throws HyracksDataException {
        System.err.println("[VectorClusteringSearchCursor.navigateToFirstCluster] Starting navigation to cluster 0...");

        // Step 1: Navigate to leftmost leaf page
        int currentPageId = rootPageId;

        // Traverse tree by always taking first child until we reach leaf level
        while (!isLeafPage(currentPageId)) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
            try {
                page.acquireReadLatch();
                IVectorClusteringInteriorFrame interiorFrame = createInteriorFrame();
                interiorFrame.setPage(page);

                // Always take FIRST child to reach leftmost leaf
                if (interiorFrame.getTupleCount() > 0) {
                    currentPageId = interiorFrame.getChildPageId(0);
                    System.err.println(String.format(
                            "[VectorClusteringSearchCursor.navigateToFirstCluster] Interior page, taking first child -> pageId=%d",
                            currentPageId));
                } else {
                    throw HyracksDataException
                            .create(new IllegalStateException("Empty interior page encountered during navigation"));
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        // Step 2: Scan ALL leaf pages and collect ALL directory page IDs
        this.allDirectoryPageIds = new ArrayList<>();
        int leafPageId = currentPageId;
        int totalClusters = 0;

        System.err.println(String.format(
                "[VectorClusteringSearchCursor.navigateToFirstCluster] Starting leaf page scan from pageId=%d",
                leafPageId));

        while (leafPageId != -1) {
            ICachedPage leafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafPageId));
            try {
                leafPage.acquireReadLatch();
                IVectorClusteringLeafFrame leafFrame = createLeafFrame();
                leafFrame.setPage(leafPage);

                int tupleCount = leafFrame.getTupleCount();
                int nextLeafPageId = leafFrame.getNextLeaf();

                System.err.println(String.format(
                        "[VectorClusteringSearchCursor.navigateToFirstCluster] Leaf page %d: tuples=%d, nextLeaf=%d",
                        leafPageId, tupleCount, nextLeafPageId));

                // Collect directory page IDs from THIS leaf page
                for (int i = 0; i < tupleCount; i++) {
                    long dirPageId = leafFrame.getMetadataPagePointer(i);
                    allDirectoryPageIds.add(dirPageId);
                    totalClusters++;
                }

                // Move to next leaf page (follow nextLeaf pointer)
                leafPageId = nextLeafPageId;

            } finally {
                leafPage.releaseReadLatch();
                bufferCache.unpin(leafPage);
            }
        }

        // Step 3: Store collected information
        this.totalLeafClusters = totalClusters;

        if (this.totalLeafClusters == 0) {
            throw HyracksDataException.create(new IllegalStateException("No leaf centroids found - empty index"));
        }

        this.firstDirectoryPageId = allDirectoryPageIds.get(0);

        System.err.println(String.format(
                "[VectorClusteringSearchCursor.navigateToFirstCluster] Scan complete: totalClusters=%d, collected %d directory pages",
                totalLeafClusters, allDirectoryPageIds.size()));

        // Step 4: Open cluster 0
        this.currentSequentialClusterIndex = 0;
        openClusterByDirectoryPage(this.firstDirectoryPageId);
        this.clustersProbed = 1;

        // Create ClusterSearchResult for first cluster (for LSM layer to access)
        this.currentClusterResult = new ClusterSearchResult(-1, // No leaf page ID in full-scan mode
                0, // Cluster index
                null, // No centroid vector
                0.0, // No distance in full-scan mode
                0, // Cluster index as centroid ID
                this.firstDirectoryPageId, // Directory page ID for O(1) access
                Double.NaN // No quantized distance in full-scan mode
        );

        System.err.println(String.format(
                "[VectorClusteringSearchCursor.navigateToFirstCluster] Successfully opened cluster 0, tupleCount=%d",
                tupleCount));
    }

    /**
     * Check if a page is a leaf page by examining its structure.
     * Leaf pages point to metadata pages, interior pages point to child pages.
     */
    private boolean isLeafPage(int pageId) throws HyracksDataException {
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
        try {
            page.acquireReadLatch();
            // Try to interpret as interior frame first
            IVectorClusteringInteriorFrame interiorFrame = createInteriorFrame();
            interiorFrame.setPage(page);

            // Check page-level metadata to determine type
            // Interior frames have a different structure than leaf frames
            // We can check if the page level indicator shows it's a leaf
            byte level = interiorFrame.getLevel();
            return level == 0; // Level 0 = leaf pages
        } finally {
            page.releaseReadLatch();
            bufferCache.unpin(page);
        }
    }

    /**
     * Open a cluster by its directory (metadata) page ID.
     * Used by full-scan mode for sequential cluster iteration.
     */
    private void openClusterByDirectoryPage(long directoryPageId) throws HyracksDataException {
        System.err.println(String.format(
                "[VectorClusteringSearchCursor.openClusterByDirectoryPage] Opening directoryPage=%d", directoryPageId));

        this.targetMetadataPageId = directoryPageId;

        // Pin metadata/directory page
        ICachedPage dirPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) directoryPageId));
        try {
            dirPage.acquireReadLatch();
            IVectorClusteringMetadataFrame metadataFrame = createMetadataFrame();
            metadataFrame.setPage(dirPage);

            int metadataTupleCount = metadataFrame.getTupleCount();
            if (metadataTupleCount == 0) {
                // Empty cluster - no data pages
                System.err.println(
                        "[VectorClusteringSearchCursor.openClusterByDirectoryPage] Empty cluster (no metadata entries)");
                this.currentDataPageId = -1;
                this.tupleCount = 0;
                this.currentTupleIndex = 0;
                return;
            }

            // Get first data page from metadata
            long firstDataPageId = metadataFrame.getDataPagePointer(0);
            this.currentDataPageId = firstDataPageId;

            System.err.println(String.format(
                    "[VectorClusteringSearchCursor.openClusterByDirectoryPage] Metadata has %d entries, firstDataPage=%d",
                    metadataTupleCount, firstDataPageId));
        } finally {
            dirPage.releaseReadLatch();
            bufferCache.unpin(dirPage);
        }

        // Open first data page
        if (this.currentDataPageId != -1) {
            openDataPage(this.currentDataPageId);
        } else {
            this.tupleCount = 0;
        }

        this.currentTupleIndex = 0;
    }

    /**
     * Advance to the next closest cluster.
     * This method is called by the LSM layer when it needs more data.
     * <p>
     * Supports two modes:
     * - Full-scan mode: Sequential iteration (cluster 0 → 1 → 2 → ...)
     * - Query mode: Distance-based iteration (closest clusters first)
     *
     * @return true if successfully moved to next cluster, false if no more clusters available
     */
    public boolean advanceToNextCluster() throws HyracksDataException {
        System.err.println("[VectorClusteringSearchCursor.advanceToNextCluster] Looking for next cluster...");

        if (fullScanMode) {
            // Full-scan mode: Sequential iteration through clusters
            currentSequentialClusterIndex++;

            if (currentSequentialClusterIndex >= totalLeafClusters) {
                exhaustedAllClusters = true;
                System.err.println(String.format(
                        "[VectorClusteringSearchCursor.advanceToNextCluster] Full-scan exhausted all %d clusters",
                        totalLeafClusters));
                return false; // No more clusters
            }

            // Get directory page ID from collected list (handles multiple leaf pages)
            long nextDirectoryPageId = allDirectoryPageIds.get(currentSequentialClusterIndex);
            System.err.println(String.format(
                    "[VectorClusteringSearchCursor.advanceToNextCluster] Full-scan advancing to cluster %d (directoryPage=%d)",
                    currentSequentialClusterIndex, nextDirectoryPageId));

            openClusterByDirectoryPage(nextDirectoryPageId);

            // Create ClusterSearchResult for this sequential cluster
            // In full-scan mode, we don't have centroid info, but we have the directory page
            this.currentClusterResult = new ClusterSearchResult(-1, // No leaf page ID in full-scan mode
                    currentSequentialClusterIndex, // Cluster index
                    null, // No centroid vector
                    0.0, // No distance in full-scan mode
                    currentSequentialClusterIndex, // Use cluster index as centroid ID
                    nextDirectoryPageId, // Directory page ID for O(1) access
                    Double.NaN // No quantized distance in full-scan mode
            );
            this.clustersProbed++;

            return true;

        } else {
            // Query mode: Distance-based iteration using DFS
            // Open next cluster and return immediately (even if empty)
            // Let LSMVCTreeSearchCursor handle cluster synchronization
            ClusterSearchResult nextCluster =
                    VCTreeNavigationUtils.findNextClosestCluster(iteratorState, distanceFunction);

            if (nextCluster == null) {
                exhaustedAllClusters = true;
                System.err.println(
                        "[VectorClusteringSearchCursor.advanceToNextCluster] No more clusters, marking exhausted");
                return false; // No more clusters available
            }

            // Open next cluster (even if it might be empty)
            openCluster(nextCluster);
            boolean hasData = currentTupleIndex < tupleCount;
            System.err.println(String.format(
                    "[VectorClusteringSearchCursor.advanceToNextCluster] Opened cluster %d (centroidId=%d, distance=%.4f), hasData=%s, tupleCount=%d",
                    clustersProbed, nextCluster.centroidId, nextCluster.distance, hasData, tupleCount));

            // Return true even if cluster is empty - let LSMVCTreeSearchCursor handle it
            // This ensures cluster synchronization across all LSM components
            return true;
        }
    }

    /**
     * Check if this cursor has more clusters to scan.
     *
     * @return true if more clusters are available, false if all clusters exhausted
     */
    public boolean hasMoreClusters() {
        return !exhaustedAllClusters;
    }

    /**
     * Open a specific cluster using a ClusterSearchResult that already has directoryPageId.
     * This is O(1) - no tree traversal needed since directoryPageId is already known.
     * Used by LSM layer for efficient cluster advancement.
     *
     * @param cluster the ClusterSearchResult containing directoryPageId
     * @return true if cluster was opened successfully and has data, false otherwise
     */
    public boolean openClusterByResult(ClusterSearchResult cluster) throws HyracksDataException {
        if (cluster == null) {
            return false;
        }

        System.err.println(String.format(
                "[VectorClusteringSearchCursor.openClusterByResult] Opening cluster cid=%d with directoryPageId=%d (O(1) access)",
                cluster.centroidId, cluster.directoryPageId));

        // Use directoryPageId if available (O(1)), otherwise fall back to tree traversal (O(n))
        if (cluster.hasDirectoryPageId()) {
            // O(1) direct access using stored directory page ID
            openClusterByDirectoryPage(cluster.directoryPageId);
            this.currentClusterResult = cluster;
            this.clustersProbed++;
        } else {
            // Fall back to tree traversal (legacy path)
            System.err.println(
                    "[VectorClusteringSearchCursor.openClusterByResult] No directoryPageId, falling back to openCluster()");
            openCluster(cluster);
        }

        // Check if cluster has data
        boolean hasData = currentTupleIndex < tupleCount;
        System.err.println(String.format(
                "[VectorClusteringSearchCursor.openClusterByResult] Opened cluster cid=%d, hasData=%s, tupleCount=%d",
                cluster.centroidId, hasData, tupleCount));

        return hasData;
    }

    /**
     * Find next cluster using DFS (for LSM layer to get cluster ID).
     * Does NOT open the cluster, just returns the result.
     * Skips clusters already in the shared visited set.
     */
    public ClusterSearchResult findNextClusterDFS() throws HyracksDataException {
        if (iteratorState == null) {
            return null;
        }

        // Initialize if needed
        if (!iteratorState.initialized) {
            ClusterSearchResult first =
                    VCTreeNavigationUtils.initializeClusterIterator(iteratorState, distanceFunction);
            if (first != null) {
                return first;
            }
        }

        // Get next from DFS (automatically skips visited via NavigationState)
        return VCTreeNavigationUtils.findNextClosestCluster(iteratorState, distanceFunction);
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
     *
     * @return true if successfully moved to next page, false if no more pages
     */
    private boolean moveToNextDataPage() throws HyracksDataException {
        if (dataFrame == null) {
            return false;
        }

        // After deletion, data pages can become empty but there might
        // be more non-empty pages later in the chain. We must skip empty pages instead
        // of stopping at the first empty page.

        while (true) {
            // Get the next page ID from the current data frame's linked list pointer
            int nextDataPageId = dataFrame.getNextPage();
            if (nextDataPageId == -1) {
                System.err.println("[VectorClusteringSearchCursor.moveToNextDataPage] "
                        + "Reached end of data page chain, no more pages");
                return false; // Reached end of chain
            }

            // Move to the next data page
            this.currentDataPageId = nextDataPageId;
            openDataPage(nextDataPageId);

            // Check if this page has tuples
            if (this.tupleCount > 0) {
                System.err
                        .println(String.format(
                                "[VectorClusteringSearchCursor.moveToNextDataPage] "
                                        + "Found non-empty data page %d with %d tuples",
                                nextDataPageId, this.tupleCount));
                return true; // Found non-empty page
            }

            // Page is empty after deletion - continue to next page
            System.err
                    .println(
                            String.format(
                                    "[VectorClusteringSearchCursor.moveToNextDataPage] "
                                            + "Data page %d is empty (after deletion), skipping to next page",
                                    nextDataPageId));
            // Loop continues to next page
        }
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
                "[VectorClusteringSearchCursor] Opened cluster %d (centroidId=%d, distance=%.4f) | Total clusters probed: %d | Records iterated so far: %d",
                clustersProbed, cluster.centroidId, cluster.distance, clustersProbed, recordsIterated));
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
        // Debug: log who is calling close() to help track unexpected closure
        System.err.println(
                String.format("[VectorClusteringSearchCursor.close] Called on cursor (isOpen=%s, recordsIterated=%d)",
                        isOpen, recordsIterated));

        if (isOpen) {
            closeCurrentPage();

            // Log final statistics
            System.err.println(String.format(
                    "[VectorClusteringSearchCursor] Search completed | Total clusters probed: %d | Total records iterated: %d | Exhausted all clusters: %s",
                    clustersProbed, recordsIterated, exhaustedAllClusters));
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
