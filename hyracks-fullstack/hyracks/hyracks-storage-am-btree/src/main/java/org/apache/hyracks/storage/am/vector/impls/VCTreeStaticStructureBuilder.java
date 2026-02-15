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
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VCTreeStaticStructureBuilder extends AbstractTreeIndexBulkLoader {

    private static final Logger LOGGER = LogManager.getLogger();
    private final int numLevels;
    private final List<Integer> clustersPerLevel; // List of cluster counts per level
    private final List<List<Integer>> centroidsPerCluster; // List of Lists: level -> [centroids per cluster]
    private int maxEntriesPerPage = 0;

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

    // Frames for different page types
    private IVectorClusteringInteriorFrame interiorFrame;
    private IVectorClusteringLeafFrame leafFrame;
    private final List<ICachedPage> leafPages = new ArrayList<>();

    public VCTreeStaticStructureBuilder(IPageWriteCallback callback, VectorClusteringTree vectorTree,
            ITreeIndexFrame leafFrame, ITreeIndexFrame dataFrame, int numLevels, List<Integer> clustersPerLevel,
            List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage) throws HyracksDataException {

        super(0, callback, vectorTree, leafFrame, DefaultBufferCacheWriteContext.INSTANCE);

        this.numLevels = numLevels;
        this.clustersPerLevel = new ArrayList<>(clustersPerLevel); // Defensive copy
        this.centroidsPerCluster = new ArrayList<>();
        // Deep copy of nested lists
        for (List<Integer> levelCentroids : centroidsPerCluster) {
            this.centroidsPerCluster.add(new ArrayList<>(levelCentroids));
        }

        this.currentLevel = 0; // Start from root level (level 0)
        this.currentClusterInLevel = 0;
        this.currentCentroidInCluster = 0;
        this.entriesInCurrentPage = 0;
        this.maxEntriesPerPage = maxEntriesPerPage;

        this.levelPageIds = new ArrayList<>(numLevels);
        for (int i = 0; i < numLevels; i++) {
            levelPageIds.add(new ArrayList<>());
        }

        // Initialize frames
        this.interiorFrame = (IVectorClusteringInteriorFrame) vectorTree.getInteriorFrameFactory().createFrame();
        this.leafFrame = (IVectorClusteringLeafFrame) vectorTree.getLeafFrameFactory().createFrame();

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

        // TODO: verify number of levels
        // Create first page (root page) - page ID 1
        createNewPage(1);

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
        //        System.err.println("=== VCTreeStaticStructureBuilder.add ===");
        //        System.err.println("Input tuple field count: " + tuple.getFieldCount());
        //        for (int i = 0; i < tuple.getFieldCount(); i++) {
        //            System.err.println("  Field " + i + ": length=" + tuple.getFieldLength(i));
        //        }

        // Compute child page ID mathematically
        int childPageId = determineChildPageId();
        //        System.err.println("Child page ID: " + childPageId);

        // Create entry tuple: <centroid_id, embedding, child_page_id>
        //        System.err.println("Calling createEntryTuple...");
        ITupleReference entryTuple = createEntryTuple(tuple, childPageId);
        //        System.err.println("createEntryTuple completed successfully");

        // Check if current page has SPACE for this tuple (not just entry count)
        int spaceNeeded = currentFrame.getBytesRequiredToWriteTuple(entryTuple) + slotSize;
        int spaceAvailable = currentFrame.getTotalFreeSpace();
        //        System.err.println("Space check: needed=" + spaceNeeded + ", available=" + spaceAvailable);

        if (spaceNeeded > spaceAvailable) {
            // Create overflow page for same cluster
            //            System.err.println("Creating overflow page (insufficient space)...");
            createOverflowPage();
        }

        // Insert entry into current page
        //        System.err.println("Inserting entry into current page...");
        ((IVectorClusteringFrame) currentFrame).insertSorted(entryTuple);
        entriesInCurrentPage++;
        //        System.err.println("Entry inserted successfully. Total entries in current page: " + entriesInCurrentPage);

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

        LOGGER.debug("Centroid at level {} points to cluster {} -> page {}", currentLevel, childClusterIndex,
                childPageId);

        return childPageId + 1;
    }

    /**
     * Compute which cluster in the next level this centroid points to. This is based on the BFS ordering and
     * predetermined structure.
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
        return totalPagesUpToLevel[currentLevel] + currentClusterInLevel + 1;
    }

    /**
     * Create entry tuple with centroid info and child page pointer.
     * Handles variable field counts:
     * - 2-field input (interior): [centroidId, embedding] -> creates [centroidId, embedding, childPageId]
     * - 3-field input (leaf with quantization): [centroidId, embedding, quantizedBytes] ->
     *   creates [centroidId, embedding, quantizedBytes, childPageId]
     */
    private ITupleReference createEntryTuple(ITupleReference tuple, int childPageId) throws HyracksDataException {
        int inputFieldCount = tuple.getFieldCount();

        // Deserialize centroidId and embedding (always present)
        ISerializerDeserializer[] baseFieldSerdes = new ISerializerDeserializer[2];
        baseFieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE;
        baseFieldSerdes[1] = DoubleArraySerializerDeserializer.INSTANCE;

        Object[] baseFieldValues = TupleUtils.deserializeTuple(tuple, baseFieldSerdes);
        int centroidId = (Integer) baseFieldValues[0];
        double[] embedding = (double[]) baseFieldValues[1];

        LOGGER.debug("Adding centroid {} at level={}, cluster={}, position={}, inputFields={}", centroidId,
                currentLevel, currentClusterInLevel, currentCentroidInCluster, inputFieldCount);

        try {
            if (inputFieldCount == 3) {
                // 3-field input: leaf tuple with quantization [centroidId, embedding, quantizedBytes]
                // Deserialize the quantized bytes
                ISerializerDeserializer[] fullFieldSerdes = new ISerializerDeserializer[3];
                fullFieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE;
                fullFieldSerdes[1] = DoubleArraySerializerDeserializer.INSTANCE;
                fullFieldSerdes[2] = ByteArraySerializerDeserializer.INSTANCE;

                Object[] fullFieldValues = TupleUtils.deserializeTuple(tuple, fullFieldSerdes);
                byte[] quantizedBytes = (byte[]) fullFieldValues[2];

                LOGGER.debug("Leaf tuple with quantization: centroidId={}, embeddingLen={}, quantizedLen={}",
                        centroidId, embedding.length, quantizedBytes.length);

                // Create 4-field output tuple: [centroidId, embedding, quantizedBytes, childPageId]
                // childPageId (metadataPtr for leaf) is always the last field so that
                // getMetadataPagePointer() using getFieldCount()-1 works for both
                // 3-field (non-quantized) and 4-field (quantized) tuples.
                return TupleUtils.createTuple(
                        new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                                DoubleArraySerializerDeserializer.INSTANCE, ByteArraySerializerDeserializer.INSTANCE,
                                IntegerSerializerDeserializer.INSTANCE },
                        centroidId, embedding, quantizedBytes, childPageId);
            } else {
                // 2-field input: interior or non-quantized leaf [centroidId, embedding]
                // Create 3-field output tuple: [centroidId, embedding, childPageId]
                return TupleUtils.createTuple(
                        new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                                DoubleArraySerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE },
                        centroidId, embedding, childPageId);
            }
        } catch (Exception e) {
            System.err.println("ERROR creating entry tuple: " + e.getMessage());
            e.printStackTrace();
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

        // Determine frame type based on current level
        if (currentLevel == numLevels - 1) {
            // Leaf level
            currentFrame = leafFrame;
            leafFrame.setPage(currentPage);
            /* TODO : verify leaf level should be more elegant */
            leafFrame.initBuffer((byte) 0);
            leafFrame.setLevel((byte) 0);
            leafPages.add(currentPage);
        } else {
            // Interior/root level
            currentFrame = interiorFrame;
            interiorFrame.setPage(currentPage);
            interiorFrame.initBuffer((byte) 0);
            interiorFrame.setLevel((byte) 1);
        }

        entriesInCurrentPage = 0;

        // Track page ID for this level (use computed ID)
        levelPageIds.get(currentLevel).add(computedPageId);

        LOGGER.debug("Created new page {} for level {}", computedPageId, currentLevel);
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
            interiorFrame.setOverflowFlagBit(true);
        }

        // Create the new overflow page
        createNewPage(overflowPageId);

        LOGGER.debug("Created overflow page {} for level {}, cluster {}", overflowPageId, currentLevel,
                currentClusterInLevel);
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

                // For leaf level: chain last page of previous cluster to first page of new cluster
                // NOTE: We set nextLeaf but keep overflow=false (overflow only for within-cluster pages)
                if (currentLevel == numLevels - 1 && currentPage != null) {
                    int nextClusterPageId = computeCurrentClusterPageId();
                    leafFrame.setNextLeaf(nextClusterPageId);
                    leafFrame.setOverflowFlagBit(false);
                    LOGGER.debug("Linking leaf page to next cluster: current page -> page {}", nextClusterPageId);
                }

                // Create page for new cluster
                createNewPage(computeCurrentClusterPageId());
            }
        }
    }

    /**
     * Finish current page and release resources.
     */
    private void finishCurrentPage() throws HyracksDataException {
        if (currentPage == null) {
            return;
        }
        /* TODO: not write back for leaf pages at this point */
        write(currentPage);
    }

    private int getNumLeafCentroids() {
        return totalCentroidsUpToLevel[numLevels] - totalCentroidsUpToLevel[numLevels - 1];
    }

    private int getFirstLeafPageId() {
        return totalPagesUpToLevel[numLevels - 1] + 1;
    }

    /**
     * Update all leaf pages to have correct directory page pointers instead of -1.
     */
    private void updateLeafPagesWithDirectoryPointers() throws HyracksDataException {
        if (leafPages.isEmpty()) {
            LOGGER.warn("No leaf pages to update");
            return;
        }

        int nextDirectoryPageId = metaFrame.getMaxPage() + 1;

        LOGGER.debug("Starting directory page ID: {}, processing {} leaf pages", nextDirectoryPageId, leafPages.size());

        // Process each leaf page
        for (int pageIndex = 0; pageIndex < leafPages.size(); pageIndex++) {
            ICachedPage leafPage = leafPages.get(pageIndex);

            // Set up frame to read the leaf page
            leafFrame.setPage(leafPage);

            int tupleCount = leafFrame.getTupleCount();
            LOGGER.debug("Processing leaf page {} with {} tuples", pageIndex, tupleCount);

            // Update each tuple in the leaf page
            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                leafFrame.setMetadataPagePointer(tupleIndex, nextDirectoryPageId);
                // Move to next directory page for next tuple
                nextDirectoryPageId++;
            }
            LOGGER.debug("Completed updating leaf page {} with {} tuples", pageIndex, tupleCount);
        }

        LOGGER.debug("Updated all {} leaf pages with directory page pointers from {} to {}", leafPages.size(),
                metaFrame.getMaxPage() + 1, nextDirectoryPageId - 1);
    }

    /**
     * Only gets called when everything is done.
     */
    @Override
    public void end() throws HyracksDataException {
        // Update leaf pages with correct directory page pointers
        updateLeafPagesWithDirectoryPointers();

        // Write all leaf pages
        for (ICachedPage leafPage : leafPages) {
            write(leafPage);
        }

        metaFrame.put(new MutableArrayValueReference("num_leaf_centroids".getBytes()),
                LongPointable.FACTORY.createPointable(getNumLeafCentroids()));
        metaFrame.put(new MutableArrayValueReference("first_leaf_centroid_id".getBytes()),
                LongPointable.FACTORY.createPointable(totalCentroidsUpToLevel[numLevels - 1]));
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
            sb.append("Level ").append(level).append(": ").append(clustersPerLevel.get(level))
                    .append(" clusters, centroids=[");
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
}