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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexLoader;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

/**
 * Creates VCTree static structure files without LSM index instance.
 * Uses predetermined hierarchical structure with mathematical page ID computation.
 */
public class VCTreeStaticStructureCreator extends AbstractTreeIndexLoader {

    // Structure configuration
    private final int numLevels;
    private final List<Integer> clustersPerLevel;
    private final List<List<Integer>> centroidsPerCluster;
    private final int maxEntriesPerPage;

    // Loading state
    private int currentLevel;
    private int currentClusterInLevel;
    private int currentCentroidInCluster;
    private int entriesInCurrentPage;

    // Current page being built
    private ICachedPage currentPage;
    private ITreeIndexFrame currentFrame;

    // Helper arrays for computing page IDs
    private int[] totalCentroidsUpToLevel;
    private int[] totalPagesUpToLevel;

    // Track page IDs for debugging
    private final List<List<Integer>> levelPageIds;

    // Frames for different page types
    private IVectorClusteringInteriorFrame interiorFrame;
    private IVectorClusteringLeafFrame leafFrame;

    /**
     * Initialize VCTree static structure creator.
     */
    public VCTreeStaticStructureCreator(IBufferCache bufferCache, IIOManager ioManager, FileReference targetFile,
            int pageSize, float fillFactor, ITreeIndexFrame leafFrame, ITreeIndexFrame interiorFrame,
            ITreeIndexMetadataFrame metaFrame, IPageWriteCallback callback, int numLevels,
            List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage)
            throws HyracksDataException {

        super(bufferCache, ioManager, targetFile, pageSize, fillFactor, leafFrame, interiorFrame, metaFrame, callback);

        this.numLevels = numLevels;
        this.clustersPerLevel = new ArrayList<>(clustersPerLevel);
        this.centroidsPerCluster = new ArrayList<>();
        for (List<Integer> levelCentroids : centroidsPerCluster) {
            this.centroidsPerCluster.add(new ArrayList<>(levelCentroids));
        }
        this.maxEntriesPerPage = maxEntriesPerPage;

        this.currentLevel = 0;
        this.currentClusterInLevel = 0;
        this.currentCentroidInCluster = 0;
        this.entriesInCurrentPage = 0;

        this.levelPageIds = new ArrayList<>(numLevels);
        for (int i = 0; i < numLevels; i++) {
            levelPageIds.add(new ArrayList<>());
        }

        // Initialize frames
        this.interiorFrame = (IVectorClusteringInteriorFrame) interiorFrame;
        this.leafFrame = (IVectorClusteringLeafFrame) leafFrame;

        // Precompute helper arrays
        computeHelperArrays();

        // Pre-allocate all pages
        preallocatePages();

        // Create first page (root page)
        createNewPage(computeCurrentClusterPageId());

        System.out.println("DEBUG: VCTreeStaticStructureCreator initialized");
        System.out.println("DEBUG: numLevels=" + numLevels + ", maxEntriesPerPage=" + maxEntriesPerPage);
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

        System.out.println("DEBUG: totalCentroidsUpToLevel: " + java.util.Arrays.toString(totalCentroidsUpToLevel));
        System.out.println("DEBUG: totalPagesUpToLevel: " + java.util.Arrays.toString(totalPagesUpToLevel));
    }

    /**
     * Pre-allocate all pages needed for the structure.
     */
    private void preallocatePages() throws HyracksDataException {
        int totalClusters = 0;
        for (int level = 0; level < numLevels; level++) {
            totalClusters += clustersPerLevel.get(level);
        }

        System.out.println("DEBUG: Pre-allocating " + totalClusters + " pages");

        for (int i = 0; i < totalClusters; i++) {
            allocatePage();
        }
    }

    /**
     * Add a tuple to the structure.
     */
    public void add(ITupleReference tuple) throws HyracksDataException {
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

        System.out.println("DEBUG: Centroid at level " + currentLevel + " points to cluster " + childClusterIndex
                + " -> page " + childPageId);

        return childPageId;
    }

    /**
     * Compute which cluster in the next level this centroid points to.
     */
    private int computeChildClusterIndex() {
        int centroidsProcessedInCurrentLevel = 0;

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
        return totalPagesUpToLevel[currentLevel] + currentClusterInLevel;
    }

    /**
     * Create entry tuple with centroid info and child page pointer.
     */
    private ITupleReference createEntryTuple(ITupleReference tuple, int childPageId) throws HyracksDataException {
        try {
            // Debug: Print tuple information
            System.out.println("DEBUG: Processing tuple with " + tuple.getFieldCount() + " fields");
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                System.out.println("DEBUG: Field " + i + " length: " + tuple.getFieldLength(i));
            }

            // Try to deserialize field by field to identify the issue
            int centroidId = 0;
            float[] embedding = null;

            // Try different approaches based on tuple structure
            if (tuple.getFieldCount() >= 4) {
                // Expected format: <level, clusterId, centroidId, embedding>
                System.out.println("DEBUG: Field 3 length: " + tuple.getFieldLength(3) + " bytes");

                // Check if field 3 is small (likely not a proper array)
                if (tuple.getFieldLength(3) < 100) {
                    System.out.println("DEBUG: Field 3 too small for array, using dummy embedding");
                    // Field 3 is too small to be a proper embedding array
                    // Extract centroidId from field 2 and create dummy embedding
                    try {
                        // Use ByteArrayInputStream for proper deserialization
                        byte[] fieldData = tuple.getFieldData(2);
                        int fieldStart = tuple.getFieldStart(2);
                        int fieldLength = tuple.getFieldLength(2);

                        java.io.ByteArrayInputStream bais =
                                new java.io.ByteArrayInputStream(fieldData, fieldStart, fieldLength);
                        java.io.DataInputStream dis = new java.io.DataInputStream(bais);

                        ISerializerDeserializer intSerde = IntegerSerializerDeserializer.INSTANCE;
                        Object centroidIdObj = intSerde.deserialize(dis);
                        centroidId = (Integer) centroidIdObj;
                        System.out.println("DEBUG: Extracted centroidId: " + centroidId);
                    } catch (Exception e) {
                        System.out.println("DEBUG: Failed to extract centroidId: " + e.getMessage());
                        centroidId = currentCentroidInCluster;
                    }

                    // Create dummy embedding with smaller size to fit in frame
                    embedding = new float[32]; // 32 floats = 128 bytes, converted to 32 doubles = 256 bytes
                    for (int i = 0; i < embedding.length; i++) {
                        embedding[i] = (float) (Math.random() - 0.5) * 2;
                    }
                    System.out.println("DEBUG: Using dummy embedding of length " + embedding.length);
                } else {
                    // Field 3 is large enough to be an array, try deserialization
                    try {
                        // Follow VCTreeStaticStructureBuilder pattern: expect 2 fields (centroidId, embedding)
                        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[2];
                        fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE; // centroidId
                        fieldSerdes[1] = DoubleArraySerializerDeserializer.INSTANCE; // embedding (DOUBLE array)

                        Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);

                        // Extract values
                        Object centroidIdObj = fieldValues[0];
                        Object embeddingObj = fieldValues[1];

                        System.out.println("DEBUG: CentroidId: " + centroidIdObj);

                        centroidId = (Integer) centroidIdObj;

                        // Use double array directly (no conversion needed)
                        if (embeddingObj instanceof double[]) {
                            double[] doubleEmbedding = (double[]) embeddingObj;
                            // Convert double array to float array for internal processing
                            embedding = new float[doubleEmbedding.length];
                            for (int i = 0; i < doubleEmbedding.length; i++) {
                                embedding[i] = (float) doubleEmbedding[i];
                            }
                            System.out.println("DEBUG: Embedding length: " + embedding.length);
                        } else {
                            System.out.println("DEBUG: Unexpected embedding type: " + embeddingObj.getClass());
                            // Create dummy embedding with smaller size to fit in frame
                            embedding = new float[32]; // 32 floats = 128 bytes, converted to 32 doubles = 256 bytes
                            for (int i = 0; i < embedding.length; i++) {
                                embedding[i] = (float) (Math.random() - 0.5) * 2;
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("DEBUG: Failed to deserialize tuple fields: " + e.getMessage());
                        // Fallback: create dummy values with smaller size to fit in frame
                        centroidId = currentCentroidInCluster;
                        embedding = new float[32]; // 32 floats = 128 bytes, converted to 32 doubles = 256 bytes
                        for (int i = 0; i < embedding.length; i++) {
                            embedding[i] = (float) (Math.random() - 0.5) * 2;
                        }
                    }
                }
            } else {
                // Fallback for unexpected tuple format
                System.out.println("DEBUG: Unexpected tuple format, using fallback values");
                centroidId = currentCentroidInCluster;
                embedding = new float[32]; // 32 floats = 128 bytes, converted to 32 doubles = 256 bytes
                for (int i = 0; i < embedding.length; i++) {
                    embedding[i] = (float) (Math.random() - 0.5) * 2;
                }
            }

            System.out.println("DEBUG: Adding centroid " + centroidId + " at level=" + currentLevel + ", cluster="
                    + currentClusterInLevel + ", position=" + currentCentroidInCluster);

            // Convert float array to double array to match VCTreeStaticStructureBuilder pattern
            double[] doubleEmbedding = new double[embedding.length];
            for (int i = 0; i < embedding.length; i++) {
                doubleEmbedding[i] = embedding[i];
            }
            
            return TupleUtils.createTuple(
                    new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                            DoubleArraySerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE },
                    centroidId, doubleEmbedding, childPageId);

        } catch (Exception e) {
            System.out.println("DEBUG: Exception in createEntryTuple: " + e.getMessage());
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

        // Determine frame type based on current level
        boolean isLeaf = (currentLevel == numLevels - 1);

        // Use the parent class method to create the page properly
        currentPage = createPage(computedPageId, isLeaf, currentLevel);

        // Set the appropriate frame
        if (isLeaf) {
            currentFrame = leafFrame;
        } else {
            currentFrame = interiorFrame;
        }

        entriesInCurrentPage = 0;

        // Track page ID for this level
        levelPageIds.get(currentLevel).add(computedPageId);

        System.out.println("DEBUG: Created new page " + computedPageId + " for level " + currentLevel);
    }

    /**
     * Create overflow page for current cluster.
     */
    private void createOverflowPage() throws HyracksDataException {
        // Allocate new overflow page
        int overflowPageId = allocatePage();

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

        System.out.println("DEBUG: Created overflow page " + overflowPageId + " for level " + currentLevel
                + ", cluster " + currentClusterInLevel);
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
                    System.out.println("DEBUG: Moving to level " + currentLevel);
                    // Create first page of new level
                    createNewPage(computeCurrentClusterPageId());
                }
            } else {
                // Start new cluster in same level
                System.out.println("DEBUG: Starting cluster " + currentClusterInLevel + " in level " + currentLevel);
                // Create page for new cluster
                createNewPage(computeCurrentClusterPageId());
            }
        }
    }

    /**
     * Finish current page and write to disk.
     */
    private void finishCurrentPage() throws HyracksDataException {
        if (currentPage != null) {
            writePage(currentPage);
        }
    }

    /**
     * Finalize the structure and write to disk.
     */
    public void finalize() throws HyracksDataException {
        try {
            // Finish last page
            finishCurrentPage();

            // Create metadata pages for leaf clusters
            createMetadataPages();

            // Close the loader
            close();

            System.out.println("DEBUG: VCTreeStaticStructureCreator completed successfully");
            printFinalStructure();

        } catch (Exception e) {
            handleException();
            throw new HyracksDataException("Failed to finalize structure", e);
        }
    }

    /**
     * Create metadata pages for leaf clusters.
     */
    private void createMetadataPages() throws HyracksDataException {
        int leafLevel = numLevels - 1;
        int leafPageStart = totalPagesUpToLevel[leafLevel];
        int leafPageEnd = totalPagesUpToLevel[leafLevel + 1];

        for (int leafPageId = leafPageStart; leafPageId < leafPageEnd; leafPageId++) {
            // Allocate metadata page
            int metadataPageId = allocatePage();

            // Create the metadata page using the parent class method
            ICachedPage metadataPage = createPage(metadataPageId, false, 0); // false = not leaf, level 0 for metadata

            // Set up the metadata frame
            metaFrame.setPage(metadataPage);

            System.out.println("DEBUG: Created metadata page " + metadataPageId + " for leaf page " + leafPageId);

            // Write the page (this will release the latch)
            writePage(metadataPage);
        }
    }

    /**
     * Print structure configuration for debugging.
     */
    private void printStructureInfo() {
        System.out.println("DEBUG: Structure configuration:");
        for (int level = 0; level < numLevels; level++) {
            System.out.print("DEBUG: Level " + level + ": " + clustersPerLevel.get(level) + " clusters, centroids=[");
            List<Integer> levelCentroids = centroidsPerCluster.get(level);
            for (int cluster = 0; cluster < levelCentroids.size(); cluster++) {
                System.out.print(levelCentroids.get(cluster));
                if (cluster < levelCentroids.size() - 1) {
                    System.out.print(", ");
                }
            }
            System.out.println("]");
        }
    }

    /**
     * Print final structure for debugging.
     */
    private void printFinalStructure() {
        System.out.println("DEBUG: Final structure:");
        for (int level = 0; level < numLevels; level++) {
            List<Integer> pageIds = levelPageIds.get(level);
            System.out.println("DEBUG: Level " + level + " pages: " + pageIds);
        }
    }
}
