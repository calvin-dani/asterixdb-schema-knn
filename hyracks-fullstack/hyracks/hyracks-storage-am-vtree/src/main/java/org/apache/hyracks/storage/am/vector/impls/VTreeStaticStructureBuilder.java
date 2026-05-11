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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.vector.api.IVTreeFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VTreeStaticStructureBuilder extends PageWriteFailureCallback implements IIndexBulkLoader {

    private static final Logger LOGGER = LogManager.getLogger();

    // Fields previously inherited from AbstractTreeIndexBulkLoader
    private final IBufferCache bufferCache;
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private final AbstractTreeIndex treeIndex;
    private final int fileId;
    private final int slotSize;
    private final IFIFOPageWriter pageWriter;
    private final ICompressedPageWriter compressedPageWriter;

    // Page buffering for sequential writes at end()
    private final TreeMap<Integer, ICachedPage> allPages = new TreeMap<>();
    private int currentPageId = -1;

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
    private IVTreeInteriorFrame interiorFrame;
    private IVTreeLeafFrame leafFrame;
    private final List<ICachedPage> leafPages = new ArrayList<>();

    public VTreeStaticStructureBuilder(IPageWriteCallback callback, VTree vectorTree, int numLevels,
            List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage)
            throws HyracksDataException {

        this.bufferCache = vectorTree.getBufferCache();
        this.freePageManager = vectorTree.getPageManager();
        this.fileId = vectorTree.getFileId();
        this.treeIndex = vectorTree;
        this.metaFrame = freePageManager.createMetadataFrame();

        // Initialize frames
        this.interiorFrame = (IVTreeInteriorFrame) vectorTree.getInteriorFrameFactory().createFrame();
        this.leafFrame = (IVTreeLeafFrame) vectorTree.getLeafFrameFactory().createFrame();
        this.slotSize = ((ITreeIndexFrame) leafFrame).getSlotSize();

        this.pageWriter = bufferCache.createFIFOWriter(callback, this, DefaultBufferCacheWriteContext.INSTANCE);
        this.compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);

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

        // Precompute helper arrays for mathematical page ID calculation
        computeHelperArrays();

        // Calculate total number of clusters (pages) across all levels
        int totalClusters = 0;
        for (int level = 0; level < numLevels; level++) {
            totalClusters += clustersPerLevel.get(level);
        }

        LOGGER.log(Level.TRACE, "Total clusters in structure: {}", totalClusters);

        // Call freePageManager.takePage() for each cluster to update metadata frame
        for (int i = 0; i < totalClusters; i++) {
            int allocatedPageId = freePageManager.takePage(metaFrame);
            LOGGER.log(Level.TRACE, "Pre-allocated page {} ({}/{})", allocatedPageId, i + 1, totalClusters);
        }

        // Create first page (root page) - page ID 0
        createNewPage(0);

        LOGGER.log(Level.TRACE, "VTreeStaticStructureLoader initialized");
        LOGGER.log(Level.TRACE, "numLevels={}, maxEntriesPerPage={}", numLevels, maxEntriesPerPage);
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

        LOGGER.log(Level.TRACE, "totalCentroidsUpToLevel: {}", Arrays.toString(totalCentroidsUpToLevel));
        LOGGER.log(Level.TRACE, "totalPagesUpToLevel: {}", Arrays.toString(totalPagesUpToLevel));
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        // Compute child page ID mathematically
        int childPageId = determineChildPageId();
        ITupleReference entryTuple = createEntryTuple(tuple, childPageId);

        // Check if current page has SPACE for this tuple (not just entry count)
        int spaceNeeded = currentFrame.getBytesRequiredToWriteTuple(entryTuple) + slotSize;
        int spaceAvailable = currentFrame.getTotalFreeSpace();

        if (spaceNeeded > spaceAvailable) {
            createOverflowPage();
        }

        ((IVTreeFrame) currentFrame).insertSorted(entryTuple);
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
        return totalPagesUpToLevel[currentLevel + 1] + childClusterIndex;
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
        return totalPagesUpToLevel[currentLevel] + currentClusterInLevel;
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

        LOGGER.log(Level.TRACE, "Adding centroid {} at level={}, cluster={}, position={}, inputFields={}", centroidId,
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

                LOGGER.log(Level.TRACE, "Leaf tuple with quantization: centroidId={}, embeddingLen={}, quantizedLen={}",
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
            LOGGER.log(Level.TRACE, "Error creating entry tuple: {}", e.getMessage());
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

        // Track the page ID before confiscating
        currentPageId = computedPageId;

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

        LOGGER.log(Level.TRACE, "Created new page {} for level {}", computedPageId, currentLevel);
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

        LOGGER.log(Level.TRACE, "Created overflow page {} for level {}, cluster {}", overflowPageId, currentLevel,
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
                    LOGGER.log(Level.TRACE, "Moving to level {}", currentLevel);
                    // Create first page of new level
                    createNewPage(computeCurrentClusterPageId());
                }
            } else {
                // Start new cluster in same level
                LOGGER.log(Level.TRACE, "Starting cluster {} in level {}", currentClusterInLevel, currentLevel);

                // For leaf level: chain last page of previous cluster to first page of new cluster
                // NOTE: We set nextLeaf but keep overflow=false (overflow only for within-cluster pages)
                if (currentLevel == numLevels - 1 && currentPage != null) {
                    int nextClusterPageId = computeCurrentClusterPageId();
                    leafFrame.setNextLeaf(nextClusterPageId);
                    leafFrame.setOverflowFlagBit(false);
                    LOGGER.log(Level.TRACE, "Linking leaf page to next cluster: current page -> page {}",
                            nextClusterPageId);
                }

                // Create page for new cluster
                createNewPage(computeCurrentClusterPageId());
            }
        }
    }

    /**
     * Buffer current page for later sequential write.
     */
    private void finishCurrentPage() throws HyracksDataException {
        if (currentPage == null) {
            return;
        }
        allPages.put(currentPageId, currentPage);
    }

    private void write(ICachedPage cPage) throws HyracksDataException {
        compressedPageWriter.prepareWrite(cPage);
        pageWriter.write(cPage);
    }

    private int getNumLeafCentroids() {
        return totalCentroidsUpToLevel[numLevels] - totalCentroidsUpToLevel[numLevels - 1];
    }

    /**
     * Update all leaf pages to have correct directory page pointers instead of -1.
     */
    private void updateLeafPagesWithDirectoryPointers() throws HyracksDataException {
        if (leafPages.isEmpty()) {
            LOGGER.log(Level.TRACE, "No leaf pages to update");
            return;
        }

        int nextDirectoryPageId = metaFrame.getMaxPage() + 1;

        LOGGER.log(Level.TRACE, "Starting directory page ID: {}, processing {} leaf pages", nextDirectoryPageId,
                leafPages.size());

        // Process each leaf page
        for (int pageIndex = 0; pageIndex < leafPages.size(); pageIndex++) {
            ICachedPage leafPage = leafPages.get(pageIndex);

            // Set up frame to read the leaf page
            leafFrame.setPage(leafPage);

            int tupleCount = leafFrame.getTupleCount();
            LOGGER.log(Level.TRACE, "Processing leaf page {} with {} tuples", pageIndex, tupleCount);

            // Update each tuple in the leaf page
            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                leafFrame.setMetadataPagePointer(tupleIndex, nextDirectoryPageId);
                // Move to next directory page for next tuple
                nextDirectoryPageId++;
            }
            LOGGER.log(Level.TRACE, "Completed updating leaf page {} with {} tuples", pageIndex, tupleCount);
        }

        LOGGER.log(Level.TRACE, "Updated all {} leaf pages with directory page pointers from {} to {}",
                leafPages.size(), metaFrame.getMaxPage() + 1, nextDirectoryPageId - 1);
    }

    /**
     * Build a flat (single-level) copy of all leaf centroids for A/B experiments.
     * Reads tuples from the hierarchical leaf pages and writes them into new flat leaf pages
     * with the same metadata page pointers.
     *
     * @return the page ID of the flat root page
     */
    private int buildFlatStructure() throws HyracksDataException {
        int flatRootPageId = freePageManager.takePage(metaFrame);

        IVTreeLeafFrame srcFrame = (IVTreeLeafFrame) treeIndex.getLeafFrameFactory().createFrame();
        IVTreeLeafFrame dstFrame = (IVTreeLeafFrame) treeIndex.getLeafFrameFactory().createFrame();
        ITreeIndexTupleReference tupleRef = ((IVTreeFrame) srcFrame).createTupleReference();

        // Create first flat page
        int currentFlatPageId = flatRootPageId;
        long dpid = BufferedFileHandle.getDiskPageId(fileId, currentFlatPageId);
        ICachedPage flatPage = bufferCache.confiscatePage(dpid);
        dstFrame.setPage(flatPage);
        dstFrame.initBuffer((byte) 0);
        dstFrame.setLevel((byte) 0);

        int totalCopied = 0;

        for (int pageIdx = 0; pageIdx < leafPages.size(); pageIdx++) {
            ICachedPage srcPage = leafPages.get(pageIdx);
            srcFrame.setPage(srcPage);
            int tupleCount = ((ITreeIndexFrame) srcFrame).getTupleCount();

            for (int i = 0; i < tupleCount; i++) {
                tupleRef.resetByTupleIndex((ITreeIndexFrame) srcFrame, i);
                if (totalCopied == 0) {
                    System.err.println("[buildFlatStructure] First tuple: fieldCount=" + tupleRef.getFieldCount()
                            + ", tupleSize=" + tupleRef.getTupleSize() + ", field0len=" + tupleRef.getFieldLength(0)
                            + ", field1len=" + tupleRef.getFieldLength(1));
                }

                int spaceNeeded = ((ITreeIndexFrame) dstFrame).getBytesRequiredToWriteTuple(tupleRef) + slotSize;
                int spaceAvailable = ((ITreeIndexFrame) dstFrame).getTotalFreeSpace();

                if (spaceNeeded > spaceAvailable) {
                    // Save current flat page and create overflow
                    allPages.put(currentFlatPageId, flatPage);
                    int overflowPageId = freePageManager.takePage(metaFrame);
                    dstFrame.setNextLeaf(overflowPageId);
                    dstFrame.setOverflowFlagBit(true);

                    currentFlatPageId = overflowPageId;
                    dpid = BufferedFileHandle.getDiskPageId(fileId, currentFlatPageId);
                    flatPage = bufferCache.confiscatePage(dpid);
                    dstFrame.setPage(flatPage);
                    dstFrame.initBuffer((byte) 0);
                    dstFrame.setLevel((byte) 0);
                }

                ((IVTreeFrame) dstFrame).insertSorted(tupleRef);
                totalCopied++;
            }
        }

        // Save last flat page
        allPages.put(currentFlatPageId, flatPage);

        System.err.println("FLAT BUILD: flatRootPageId=" + flatRootPageId + ", centroids=" + totalCopied);
        return flatRootPageId;
    }

    /**
     * Only gets called when everything is done.
     */
    @Override
    public void end() throws HyracksDataException {
        // Save the last page being built
        if (currentPage != null) {
            allPages.put(currentPageId, currentPage);
            currentPage = null;
        }

        // Update leaf pages with correct directory page pointers
        updateLeafPagesWithDirectoryPointers();

        // Log tree structure summary
        StringBuilder structLog = new StringBuilder();
        structLog.append("TREE STRUCTURE: numLevels=").append(numLevels);
        structLog.append(", clustersPerLevel=").append(clustersPerLevel);
        structLog.append(", totalPages=").append(allPages.size());
        structLog.append(", leafPages=").append(leafPages.size());
        for (int lvl = 0; lvl < numLevels; lvl++) {
            int centroidsInLevel = 0;
            for (int c : centroidsPerCluster.get(lvl)) {
                centroidsInLevel += c;
            }
            structLog.append(String.format(", L%d: %d clusters, %d centroids, %d pages, distribution=%s", lvl,
                    clustersPerLevel.get(lvl), centroidsInLevel, levelPageIds.get(lvl).size(),
                    centroidsPerCluster.get(lvl)));
        }
        System.err.println(structLog.toString());

        // Build flat structure (all leaf centroids in a single-level chain) for A/B experiments
        int flatRootPageId = buildFlatStructure();

        // Store metadata key-values
        metaFrame.put(new MutableArrayValueReference("num_leaf_centroids".getBytes()),
                LongPointable.FACTORY.createPointable(getNumLeafCentroids()));
        metaFrame.put(new MutableArrayValueReference("first_leaf_centroid_id".getBytes()),
                LongPointable.FACTORY.createPointable(totalCentroidsUpToLevel[numLevels - 1]));
        metaFrame.put(new MutableArrayValueReference("flat_root_page_id".getBytes()),
                LongPointable.FACTORY.createPointable(flatRootPageId));

        // Set root page ID (page 0)
        freePageManager.setRootPageId(0);

        // Check for prior write failures
        if (hasFailed()) {
            throw HyracksDataException.create(getFailure());
        }

        // Write ALL pages in ascending page-ID order (0, 1, 2, 3, ...)
        for (Map.Entry<Integer, ICachedPage> entry : allPages.entrySet()) {
            LOGGER.log(Level.TRACE, "Writing page {} sequentially", entry.getKey());
            write(entry.getValue());
        }
        allPages.clear();
    }

    @Override
    public void abort() throws HyracksDataException {
        LOGGER.log(Level.TRACE, "VTreeStaticStructureLoader aborted");
        compressedPageWriter.abort();
        // Return all buffered pages
        for (ICachedPage page : allPages.values()) {
            if (page != null && page.confiscated()) {
                bufferCache.returnPage(page, false);
            }
        }
        if (currentPage != null && currentPage.confiscated()) {
            bufferCache.returnPage(currentPage, false);
        }
        allPages.clear();
        freePageManager.returnAllPages();
    }

    @Override
    public void force() throws HyracksDataException {
        bufferCache.force(fileId, false);
    }

    /**
     * Print structure configuration for debugging.
     */
    private void printStructureInfo() {
        LOGGER.log(Level.TRACE, "Structure configuration:");
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
            LOGGER.log(Level.TRACE, sb.toString());
        }
    }
}
