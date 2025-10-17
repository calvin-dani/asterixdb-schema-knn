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

        this.currentLevel = numLevels - 1; // Start from root level (highest level)
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
        System.err.println("=== VCTreeStaticStructureBuilder.add ===");
        System.err.println("Input tuple field count: " + tuple.getFieldCount());
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            System.err.println("  Field " + i + ": length=" + tuple.getFieldLength(i));
        }
        
        // Compute child page ID mathematically
        int childPageId = determineChildPageId();
        System.err.println("Child page ID: " + childPageId);

        // Create entry tuple: <centroid_id, embedding, child_page_id>
        System.err.println("Calling createEntryTuple...");
        ITupleReference entryTuple = createEntryTuple(tuple, childPageId);
        System.err.println("createEntryTuple completed successfully");

        // Check if current page has space
        if (entriesInCurrentPage >= maxEntriesPerPage) {
            // Create overflow page for same cluster
            System.err.println("Creating overflow page...");
            createOverflowPage();
        }

        // Insert entry into current page
        System.err.println("Inserting entry into current page...");
        ((IVectorClusteringFrame) currentFrame).insertSorted(entryTuple);
        entriesInCurrentPage++;
        System.err.println("Entry inserted successfully. Total entries in current page: " + entriesInCurrentPage);

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
        return totalPagesUpToLevel[currentLevel] + currentClusterInLevel + 1;
    }

    /**
     * Create entry tuple with centroid info and child page pointer.
     */
    private ITupleReference createEntryTuple(ITupleReference tuple, int childPageId) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[2];
        fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE;
        fieldSerdes[1] = DoubleArraySerializerDeserializer.INSTANCE; // Use double array for full precision

        // Deserialize the tuple using the proper TupleUtils method
        Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);

        // Extract the vector from the deserialized fields
        int centroidId = (Integer) fieldValues[0];
        double[] embedding = (double[]) fieldValues[1];

        LOGGER.debug("Adding centroid {} at level={}, cluster={}, position={}", centroidId, currentLevel,
                currentClusterInLevel, currentCentroidInCluster);

        try {
            return TupleUtils.createTuple(
                    new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                            DoubleArraySerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE },
                    centroidId, embedding, childPageId);
        } catch (Exception e) {
            System.err.println("ERROR creating entry tuple: " + e.getMessage());
            e.printStackTrace();
            throw new HyracksDataException("Failed to create entry tuple", e);
        }
    }

    /**
     * Extract embedding vector from tuple field using TupleUtils.deserializeTuple with proper field serializers.
     * Expects 3-field tuple format: [centroidId, embedding, childPageId]
     */
    private double[] extractEmbeddingFromTuple(ITupleReference tuple, int fieldIndex) throws HyracksDataException {
        try {
            System.err.println("=== EXTRACTING EMBEDDING FROM FIELD " + fieldIndex + " ===");
            System.err.println("Field data length: " + tuple.getFieldLength(fieldIndex));
            System.err.println("Field start offset: " + tuple.getFieldStart(fieldIndex));
            
            // The input tuple has 3 fields: [centroidId, embedding, childPageId]
            // We need to deserialize the entire tuple to get the embedding from field 1
            ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[3];
            fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE; // centroidId
            fieldSerdes[1] = DoubleArraySerializerDeserializer.INSTANCE; // embedding
            fieldSerdes[2] = IntegerSerializerDeserializer.INSTANCE; // childPageId
            
            // Deserialize the tuple using the proper TupleUtils method
            Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);
            
            // Extract the embedding from field 1 (index 1)
            double[] embedding = (double[]) fieldValues[1];
            
            if (embedding == null || embedding.length == 0) {
                System.err.println("WARNING: Failed to extract embedding from field " + fieldIndex + ", returning empty array");
                return new double[0];
            }
            
            System.err.println("Successfully extracted embedding with " + embedding.length + " dimensions");
            if (embedding.length > 0) {
                System.err.println("First few values: " + Arrays.toString(Arrays.copyOfRange(embedding, 0, Math.min(5, embedding.length))));
            }
            
            return embedding;
            
        } catch (Exception e) {
            System.err.println("ERROR extracting embedding from field " + fieldIndex + ": " + e.getMessage());
            e.printStackTrace();
            return new double[0];
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
        if (currentLevel == 0) {
            // Leaf level (level 0)
            currentFrame = leafFrame;
            leafFrame.setPage(currentPage);
            /* TODO : verify leaf level should be more elegant */
            leafFrame.initBuffer((byte) 0);
            leafFrame.setLevel((byte) 0);
        } else {
            // Interior/root level (level > 0)
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
        if (currentLevel == 0) {
            // Leaf page - set next leaf pointer
            leafFrame.setNextLeaf(overflowPageId);
            leafFrame.setOverflowFlagBit(true);
        } else {
            // Interior page - set next page pointer
            interiorFrame.setNextPage(overflowPageId);
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
                // Move to next level (decrement since we start from highest level)
                currentLevel--;
                currentClusterInLevel = 0;

                if (currentLevel >= 0) {
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
        if (currentPage == null) {
            return;
        }
        write(currentPage);
    }

    private int getNumLeafCentroids() {
        return totalCentroidsUpToLevel[numLevels] - totalCentroidsUpToLevel[numLevels - 1];
    }

    /**
     * Only gets called when everything is done.
     */
    @Override
    public void end() throws HyracksDataException {
        // Write existing metadata
        metaFrame.put(new MutableArrayValueReference("num_leaf_centroids".getBytes()),
                LongPointable.FACTORY.createPointable(getNumLeafCentroids()));
        metaFrame.put(new MutableArrayValueReference("first_leaf_centroid_id".getBytes()),
                LongPointable.FACTORY.createPointable(totalCentroidsUpToLevel[numLevels - 1]));

        // Write root page ID (always 0 for VCTreeStaticStructureBuilder)
        metaFrame.put(new MutableArrayValueReference("root_page_id".getBytes()),
                LongPointable.FACTORY.createPointable(0));

        // Write structure configuration for navigator
        writeStructureMetadata();

        super.end();
    }

    /**
     * Write structure metadata for navigator use.
     */
    private void writeStructureMetadata() throws HyracksDataException {
        // Write number of levels
        metaFrame.put(new MutableArrayValueReference("num_levels".getBytes()),
                LongPointable.FACTORY.createPointable(numLevels));

        // Write clusters per level as comma-separated string
        StringBuilder clustersPerLevelStr = new StringBuilder();
        for (int i = 0; i < clustersPerLevel.size(); i++) {
            if (i > 0)
                clustersPerLevelStr.append(",");
            clustersPerLevelStr.append(clustersPerLevel.get(i));
        }
        metaFrame.put(new MutableArrayValueReference("clusters_per_level".getBytes()),
                LongPointable.FACTORY.createPointable(clustersPerLevelStr.toString().hashCode()));

        LOGGER.debug("Wrote structure metadata: numLevels={}, clustersPerLevel={}", numLevels,
                clustersPerLevelStr.toString());
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
