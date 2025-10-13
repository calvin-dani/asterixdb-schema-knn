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
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.am.vector.api.*;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Static structure bulk loader for VectorClusteringTree.
 * Builds predetermined hierarchical structure using BFS-order tuple stream.
 *
 * The loader receives tuples in top-down BFS order and builds a multi-level
 * clustering structure with predetermined cluster sizes and hierarchy.
 */
public class VCTreeLoader extends AbstractTreeIndexBulkLoader {

    private static final Logger LOGGER = LogManager.getLogger();
    private final int numLevels;
    private final List<Integer> clustersPerLevel; // List of cluster counts per level
    private final List<List<Integer>> centroidsPerCluster; // List of Lists: level -> [centroids per cluster]
    private final int maxEntriesPerPage;
    private final int maxTupleSize;

    // Loading state
    private int currentLevel;
    private int currentClusterInLevel;
    private int currentCentroidInCluster;

    // Current page being built
    private ICachedPage currentPage;
    private ITreeIndexFrame currentFrame;
    private int entriesInCurrentPage;

    // Helper arrays for computing page IDs
    private int[] totalCentroidsUpToLevel; // Precomputed totals for each level
    private int[] totalPagesUpToLevel; // Precomputed page offsets for each level

    // Track page IDs for debugging
    private final List<List<Integer>> levelPageIds; // level -> list of page IDs

    // Track metadata page IDs for leaf clusters
    private final List<ICachedPage> directoryPages; // list of directory pages

    // Bulk loading state for leaf clusters
    private int currentLeafClusterIndex; // Current leaf cluster being loaded (0-based)
    private ICachedPage currentDirectoryPage; // Current directory page for the cluster
    private ICachedPage currentDataPage; // Current data page being filled
    private ITreeIndexFrame currentDataFrame; // Frame for the current data page
    private ITreeIndexFrame currentDirectoryFrame; // Frame for the current directory page
    private int entriesInCurrentDataPage; // Number of entries in current data page
    private int entriesInCurrentDirectoryPage; // Number of entries in current directory page
    private int currentDataPageId; // Page ID of the current data page

    // Frames for different page types
    private IVectorClusteringInteriorFrame interiorFrame;
    private IVectorClusteringLeafFrame leafFrame;

    private loadState state = loadState.STATIC_STRUCTURE_BUILDING;

    /**
     * Initialize static structure loader with predetermined structure.
     */

    public enum loadState {
        STATIC_STRUCTURE_BUILDING, CLUSTERING, BULKLOADING
    }

    public VCTreeLoader(float fillFactor, IPageWriteCallback callback, VectorClusteringTree vectorTree,
            ITreeIndexFrame leafFrame, ITreeIndexFrame dataFrame, IBufferCacheWriteContext writeContext, int numLevels,
            List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage)
            throws HyracksDataException {
        super(fillFactor, callback, vectorTree, leafFrame, writeContext);

        this.numLevels = numLevels;
        this.clustersPerLevel = new ArrayList<>(clustersPerLevel); // Defensive copy
        this.centroidsPerCluster = new ArrayList<>();
        // Deep copy of nested lists
        for (List<Integer> levelCentroids : centroidsPerCluster) {
            this.centroidsPerCluster.add(new ArrayList<>(levelCentroids));
        }
        this.maxEntriesPerPage = maxEntriesPerPage;
        // Calculate max tuple size based on page size and frame requirements
        this.maxTupleSize = bufferCache.getPageSize() / 2;

        this.currentLevel = 0; // Start from root level (level 0)
        this.currentClusterInLevel = 0;
        this.currentCentroidInCluster = 0;
        this.entriesInCurrentPage = 0;

        this.levelPageIds = new ArrayList<>(numLevels);
        for (int i = 0; i < numLevels; i++) {
            levelPageIds.add(new ArrayList<>());
        }

        this.directoryPages = new ArrayList<>();

        // Initialize frames
        this.interiorFrame = (IVectorClusteringInteriorFrame) vectorTree.getInteriorFrameFactory().createFrame();
        this.leafFrame = (IVectorClusteringLeafFrame) vectorTree.getLeafFrameFactory().createFrame();
        this.currentDirectoryFrame = vectorTree.getMetadataFrameFactory()
                .createFrame();
        this.currentDataFrame = vectorTree.getDataFrameFactory().createFrame();

        // Precompute helper arrays for mathematical page ID calculation
        computeHelperArrays();

        // Calculate total number of clusters (pages) across all levels
        int totalClusters = 0;
        for (int level = 0; level < numLevels; level++) {
            totalClusters += clustersPerLevel.get(level);
        }

        LOGGER.debug("Total clusters in structure: {}", totalClusters);

        // Call freePageManager.takePage() for each cluster to update metadata frame
        for (int i = 0; i < totalClusters; i++) {
            int allocatedPageId = freePageManager.takePage(metaFrame);
            LOGGER.debug("Pre-allocated page {} ({}/{})", allocatedPageId, i + 1, totalClusters);
        }

        // Create first page (root page) - computed page ID 0
        createNewPage(computeCurrentClusterPageId());

        LOGGER.debug("VCTreeStaticStructureLoader initialized");
        LOGGER.debug("numLevels={}, maxEntriesPerPage={}", numLevels, maxEntriesPerPage);
        printStructureInfo();
    }

    /**
     * Precompute cumulative totals for efficient child page ID calculation.
     */
    private void computeHelperArrays() {
        totalCentroidsUpToLevel = new int[numLevels + 1];
        totalPagesUpToLevel = new int[numLevels + 1];

        totalCentroidsUpToLevel[0] = 0;
        totalPagesUpToLevel[0] = 0;

        for (int level = 0; level < numLevels; level++) {
            int centroidsInLevel = 0;
            for (int cluster = 0; cluster < clustersPerLevel.get(level); cluster++) {
                centroidsInLevel += centroidsPerCluster.get(level).get(cluster);
            }
            totalCentroidsUpToLevel[level + 1] = totalCentroidsUpToLevel[level] + centroidsInLevel;
            totalPagesUpToLevel[level + 1] = totalPagesUpToLevel[level] + clustersPerLevel.get(level);
        }

        LOGGER.debug("totalCentroidsUpToLevel: {}", Arrays.toString(totalCentroidsUpToLevel));
        LOGGER.debug("totalPagesUpToLevel: {}", Arrays.toString(totalPagesUpToLevel));
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        if (state == loadState.BULKLOADING) {
            addToLeafCluster(tuple);
            return;
        }

        // Compute child page ID mathematically
        int childPageId = determineChildPageId();

        // Create entry tuple: <centroid_id, embedding, child_page_id>
        ITupleReference entryTuple = createEntryTuple(tuple, childPageId);

        // Check if current page has space
        if (entriesInCurrentPage >= maxEntriesPerPage) {
            // Create overflow page for same cluster
            createOverflowPage();
        }

        // Insert entry into current page
        ((IVectorClusteringFrame) currentFrame).insertSorted(entryTuple);
        entriesInCurrentPage++;

        // Advance position in structure
        advancePosition();
    }

    /**
     * Compute child page ID based on current position and predetermined structure.
     */
    private int determineChildPageId() throws HyracksDataException {
        if (currentLevel == numLevels - 1) {
            // Leaf level - child page ID will be metadata page (created later)
            return -1;
        }

        // Compute which cluster this centroid points to in the next level
        int childClusterIndex = computeChildClusterIndex();

        // Child page ID = offset for next level + cluster index within that level
        int childPageId = totalPagesUpToLevel[currentLevel + 1] + childClusterIndex;

        LOGGER.debug("Centroid at level {} points to cluster {} -> page {}", currentLevel, childClusterIndex, childPageId);

        return childPageId;
    }

    /**
     * Compute which cluster in the next level this centroid points to.
     * This is based on the BFS ordering and predetermined structure.
     */
    private int computeChildClusterIndex() {
        // In BFS order, centroids map sequentially to child clusters
        // The Nth centroid in level L points to cluster N in level L+1

        int centroidsProcessedInCurrentLevel = 0;

        // Count centroids processed in current level so far
        for (int cluster = 0; cluster < currentClusterInLevel; cluster++) {
            centroidsProcessedInCurrentLevel += centroidsPerCluster.get(currentLevel).get(cluster);
        }
        centroidsProcessedInCurrentLevel += currentCentroidInCluster;

        return centroidsProcessedInCurrentLevel;
    }

    /**
     * Compute the page ID for the current cluster being built.
     */
    private int computeCurrentClusterPageId() {
        // Current cluster's page ID = offset for current level + current cluster index
        return totalPagesUpToLevel[currentLevel] + currentClusterInLevel;
    }

    /**
     * Create entry tuple with centroid info and child page pointer.
     */
    private ITupleReference createEntryTuple(ITupleReference tuple, int childPageId) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[2];
        fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE;
        fieldSerdes[1] = FloatArraySerializerDeserializer.INSTANCE;

        // Deserialize the tuple using the proper TupleUtils method
        Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);

        // Extract the vector from the deserialized fields
        int centroidId = (Integer) fieldValues[0];
        float[] embedding = (float[]) fieldValues[1];

        LOGGER.debug("Adding centroid {} at level={}, cluster={}, position={}", centroidId, currentLevel, currentClusterInLevel, currentCentroidInCluster);

        try {
            return TupleUtils.createTuple(
                    new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                            FloatArraySerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE },
                    centroidId, embedding, childPageId);
        } catch (Exception e) {
            throw new HyracksDataException("Failed to create entry tuple", e);
        }
    }

    /**
     * Create a new page with the specified computed page ID.
     */
    private void createNewPage(int computedPageId) throws HyracksDataException {
        // Finish current page if exists
        if (currentPage != null) {
            finishCurrentPage();
        }

        // Use our computed page ID for actual allocation (ensures deterministic structure)
        long dpid = BufferedFileHandle.getDiskPageId(fileId, computedPageId);
        currentPage = bufferCache.confiscatePage(dpid);

        try {
            // Determine frame type based on current level
            if (currentLevel == numLevels - 1) {
                // Leaf level
                currentFrame = leafFrame;
                leafFrame.setPage(currentPage);
                leafFrame.initBuffer((byte) currentLevel);
            } else {
                // Interior/root level
                currentFrame = interiorFrame;
                interiorFrame.setPage(currentPage);
                interiorFrame.initBuffer((byte) currentLevel);
            }

            entriesInCurrentPage = 0;

            // Track page ID for this level (use computed ID)
            levelPageIds.get(currentLevel).add(computedPageId);

            LOGGER.debug("Created new page {} for level {}", computedPageId, currentLevel);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Create overflow page for current cluster.
     */
    private void createOverflowPage() throws HyracksDataException {
        // Compute next overflow page ID
        int overflowPageId = freePageManager.takePage(metaFrame);

        // Set next page pointer in current page
        if (currentLevel == numLevels - 1) {
            // Leaf page - set next leaf pointer
            leafFrame.setNextLeaf(overflowPageId);
            leafFrame.setOverflowFlagBit(true);
        } else {
            // Interior page - set next page pointer
            interiorFrame.setNextPage(overflowPageId);
        }

        // Create the new overflow page
        createNewPage(overflowPageId);

        LOGGER.debug("Created overflow page {} for level {}, cluster {}", overflowPageId, currentLevel, currentClusterInLevel);
    }

    /**
     * Advance position in the predetermined structure.
     */
    private void advancePosition() throws HyracksDataException {
        currentCentroidInCluster++;

        // Check if we finished current cluster
        if (currentCentroidInCluster >= centroidsPerCluster.get(currentLevel).get(currentClusterInLevel)) {
            // Move to next cluster
            currentCentroidInCluster = 0;
            currentClusterInLevel++;

            // Check if we finished current level
            if (currentClusterInLevel >= clustersPerLevel.get(currentLevel)) {
                // Move to next level
                currentLevel++;
                currentClusterInLevel = 0;

                if (currentLevel < numLevels) {
                    LOGGER.debug("Moving to level {}", currentLevel);
                    // Create first page of new level
                    createNewPage(computeCurrentClusterPageId());
                }
            } else {
                // Start new cluster in same level
                LOGGER.debug("Starting cluster {} in level {}", currentClusterInLevel, currentLevel);
                // Create page for new cluster
                createNewPage(computeCurrentClusterPageId());
            }
        }
    }

    /**
     * Finish current page and release resources.
     */
    private void finishCurrentPage() throws HyracksDataException {
        write(currentPage);
    }

    private void endStaticStructure() throws HyracksDataException {
        finishCurrentPage();
        // Set root page ID (always page 0)
        setRootPageId(0);
        LOGGER.debug("Set root page ID: 0");

        // Create metadata pages for leaf clusters
        createMetadataPages();
        createNewDataPage();

        state = loadState.BULKLOADING;
        LOGGER.debug("VCTreeStaticStructureLoader completed successfully");
        printFinalStructure();
    }

    /**
     * Create metadata pages for leaf clusters.
     */
    private void createMetadataPages() throws HyracksDataException {
        int leafLevel = numLevels - 1;
        int leafCentroidStart = totalCentroidsUpToLevel[leafLevel];
        int leafCentroidEnd = totalCentroidsUpToLevel[leafLevel + 1];

        for (int leafCentroidId = leafCentroidStart; leafCentroidId < leafCentroidEnd; leafCentroidId++) {
            // For metadata pages, use freePageManager.takePage() normally
            int metadataPageId = freePageManager.takePage(metaFrame);
            long dpid = BufferedFileHandle.getDiskPageId(fileId, metadataPageId);
            ICachedPage directoryPage = bufferCache.confiscatePage(dpid);

            // Initialize the directory page
            currentDirectoryFrame.setPage(directoryPage);
            currentDirectoryFrame.initBuffer((byte) 0);
            directoryPages.add(directoryPage);

            LOGGER.debug("Created directory page {} for leaf centroid {}", metadataPageId, leafCentroidId);
        }

        // Initialize bulk loading state - start with first leaf cluster
        if (!directoryPages.isEmpty()) {
            entriesInCurrentDirectoryPage = 0;
            // Initialize data page state
            entriesInCurrentDataPage = 0;
            LOGGER.debug("Initialized bulk loading with {} directory pages", directoryPages.size());
        }
    }

    /**
     * ========= Clustering records to leaf centroids =======
     */

    public ClusterResult findCluster(ITupleReference tuple) throws HyracksDataException {
        /*  use static structure to find the closest leaf centroid for the given tuple
            return the leaf centroid ID and the distance
        */
        return new ClusterResult(0, 0.0);
    }


    /**
     * ========= leaf cluster bulk loading methods =========
     */

    private void addToLeafCluster(ITupleReference tuple) throws HyracksDataException {
        try {
            // Calculate space needed for this tuple - following BTreeNSMBulkLoader pattern
            int spaceNeeded = tupleWriter.bytesRequired(tuple) + slotSize;
            int spaceAvailable = currentDataFrame.getTotalFreeSpace();

            // If still full, need to create new data page and update directory
            if (spaceNeeded > spaceAvailable) {
                if (currentDataFrame.getTupleCount() == 0) {
                    // following BTreeNSMBulkLoader pattern
                    bufferCache.returnPage(currentDataPage, false);
                }
                // Write current data page and add entry to directory
                writeDataPageToDirectory();
                // TODO: For now we don't handle large tuples exceeds page size
                // Create new regular data page
                createNewDataPage();
            }
            // TODO: skip verify tuple
            // Insert tuple into current data page (tuples are pre-sorted by distance)
            ((IVectorClusteringDataFrame) currentDataFrame).insertSorted(tuple);
            entriesInCurrentDataPage++;

            LOGGER.debug("Added tuple to leaf cluster {}, data page entries: {}", currentLeafClusterIndex, entriesInCurrentDataPage);
        } catch (HyracksDataException | RuntimeException e) {
            // Log state for debugging - following BTreeNSMBulkLoader pattern
            logDataPageState(tuple, e);
            handleException();
            throw e;
        }
    }

    public void loadToNextLeafCluster() throws HyracksDataException {
        // Finish current data page if it exists and has data
        if (currentDataPage != null && entriesInCurrentDataPage > 0) {
            writeDataPageToDirectory();
        }

        // Move to next leaf cluster
        currentLeafClusterIndex++;

        // Check if we have more clusters to load
        if (currentLeafClusterIndex >= directoryPages.size()) {
            throw new HyracksDataException("No more leaf clusters to load");
        }

        // Set current directory page to the next cluster's directory page
        currentDirectoryPage = directoryPages.get(currentLeafClusterIndex);

        // Initialize directory frame for the new cluster
        currentDirectoryFrame.setPage(currentDirectoryPage);
        currentDirectoryFrame.initBuffer((byte) 0);
        entriesInCurrentDirectoryPage = 0;

        // Reset data page state for new cluster
        entriesInCurrentDataPage = 0;

        LOGGER.debug("Moved to leaf cluster {}", currentLeafClusterIndex);
    }




    /**
     * Get the centroid ID of the first leaf level centroid.
     * This is calculated based on the total number of centroids in all levels before the leaf level.
     * @return the centroid ID of the first leaf centroid
     */
    public int getFirstLeafCentroidId() {
        // The first leaf centroid ID is the total number of centroids processed before the leaf level
        return totalCentroidsUpToLevel[numLevels - 1];
    }

    /**
     * Create a new data page for the current leaf cluster.
     */
    private void createNewDataPage() throws HyracksDataException {
        // Allocate new page ID
        int dataPageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, dataPageId);
        currentDataPage = bufferCache.confiscatePage(dpid);
        currentDataPageId = dataPageId;

        // Initialize data frame
        currentDataFrame.setPage(currentDataPage);
        currentDataFrame.initBuffer((byte) 0);
        entriesInCurrentDataPage = 0;

        LOGGER.debug("Created new data page {} for leaf cluster {}", dataPageId, currentLeafClusterIndex);
    }

    /**
     * Write the current data page information to the directory page.
     */
    private void writeDataPageToDirectory() throws HyracksDataException {
        // Create directory entry tuple with max distance and page ID
        // For now, use a placeholder distance value
        // this would be the maximum distance of tuples in the data page
        // TODO: extract actual max distance from data page's last tuple
        double maxDistance = 6.6; // Placeholder
        
        try {
            ITupleReference directoryEntry = TupleUtils.createTuple(
                new ISerializerDeserializer[] { 
                    DoubleSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE 
                },
                maxDistance, currentDataPageId);

            // Check if directory page has space
            int spaceNeeded = tupleWriter.bytesRequired(directoryEntry) + slotSize;
            int spaceAvailable =  currentDirectoryFrame.getTotalFreeSpace();

            if (spaceNeeded > spaceAvailable) {
                // Directory page is full, need to create overflow directory page
                createOverflowDirectoryPage();
            }

            // Insert directory entry using the frame's insert method
            // Cast to appropriate frame type that has insertSorted method
            ((IVectorClusteringFrame) currentDirectoryFrame).insertSorted(directoryEntry);
            entriesInCurrentDirectoryPage++;

            LOGGER.debug("Added directory entry for data page {} to directory page, entries: {}", currentDataPageId, entriesInCurrentDirectoryPage);

        } catch (Exception e) {
            throw new HyracksDataException("Failed to create directory entry");
        }

        int nextPageId = freePageManager.takePage(metaFrame);
        ((IVectorClusteringDataFrame)currentDataFrame).setNextPage(nextPageId);

        // Write the data page
        write(currentDataPage);

        entriesInCurrentDataPage = 0;
        // Allocate new page ID
        long dpid = BufferedFileHandle.getDiskPageId(fileId, nextPageId);
        currentDataPage = bufferCache.confiscatePage(dpid);
        currentDataPageId = nextPageId;

        // Initialize data frame
        currentDataFrame.setPage(currentDataPage);
        currentDataFrame.initBuffer((byte) 0);
        entriesInCurrentDataPage = 0;

        LOGGER.debug("Created new data page {} for leaf cluster {}", currentDataPageId, currentLeafClusterIndex);
    }

    /**
     * Create overflow directory page when current directory page is full.
     */
    private void createOverflowDirectoryPage() throws HyracksDataException {
        // Allocate new directory page
        int nextDirectoryPageId = freePageManager.takePage(metaFrame);


        // Set next page pointer in current directory page
        // This would depend on the specific directory frame implementation
        // For now, we'll assume it has a setNextPage method similar to leaf frames
        ((IVectorClusteringMetadataFrame)currentDirectoryFrame).setNextPage(nextDirectoryPageId);

        // Write current directory page first
        write(currentDirectoryPage);

        long dpid = BufferedFileHandle.getDiskPageId(fileId, nextDirectoryPageId);
        currentDirectoryPage = bufferCache.confiscatePage(dpid);

        // Initialize new directory frame
        currentDirectoryFrame.setPage(currentDirectoryPage);
        currentDirectoryFrame.initBuffer((byte) 0);
        entriesInCurrentDirectoryPage = 0;

        LOGGER.debug("Created overflow directory page {} for leaf cluster {}", nextDirectoryPageId, currentLeafClusterIndex);
    }

    /**
     * Log the state of the data page for debugging purposes.
     * Similar to BTreeNSMBulkLoader's logState method.
     */
    private void logDataPageState(ITupleReference tuple, Exception e) {
        try {
            if (currentDataFrame != null) {
                int tupleSize = currentDataFrame.getBytesRequiredToWriteTuple(tuple);
                int spaceNeeded = tupleWriter.bytesRequired(tuple) + slotSize;
                int spaceUsed = currentDataFrame.getBuffer().capacity() - currentDataFrame.getTotalFreeSpace();
                
                LOGGER.error("Data page state - tupleSize: {}, spaceNeeded: {}, spaceUsed: {}, entriesInCurrentDataPage: {}",
                             tupleSize, spaceNeeded, spaceUsed, entriesInCurrentDataPage);
            }
        } catch (Throwable t) {
            e.addSuppressed(t);
        }
    }

    /**
     * Only gets called when everything is done.
     */
    @Override
    public void end() throws HyracksDataException {
        super.end();
    }

    @Override
    public void abort() throws HyracksDataException {
        LOGGER.debug("VCTreeStaticStructureLoader aborted");
        super.handleException();
    }

    /**
     * Print structure configuration for debugging.
     */
    private void printStructureInfo() {
        LOGGER.debug("Structure configuration:");
        for (int level = 0; level < numLevels; level++) {
            StringBuilder sb = new StringBuilder();
            sb.append("Level ").append(level).append(": ").append(clustersPerLevel.get(level)).append(" clusters, centroids=[");
            List<Integer> levelCentroids = centroidsPerCluster.get(level);
            for (int cluster = 0; cluster < levelCentroids.size(); cluster++) {
                sb.append(levelCentroids.get(cluster));
                if (cluster < levelCentroids.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            LOGGER.debug(sb.toString());
        }
    }

    /**
     * Print final structure for debugging.
     */
    private void printFinalStructure() {
        LOGGER.debug("Final structure:");
        for (int level = 0; level < numLevels; level++) {
            List<Integer> pageIds = levelPageIds.get(level);
            LOGGER.debug("Level {} pages: {}", level, pageIds);
        }
        LOGGER.debug("First leaf centroid ID: {}", getFirstLeafCentroidId());
    }


    /**
     * Result of cluster search operation.
     */
    public static class ClusterResult {
        final int centroidId;
        final double distance;

        ClusterResult(int centroidId, double distance) {
            this.centroidId = centroidId;
            this.distance = distance;
        }
    }
}
