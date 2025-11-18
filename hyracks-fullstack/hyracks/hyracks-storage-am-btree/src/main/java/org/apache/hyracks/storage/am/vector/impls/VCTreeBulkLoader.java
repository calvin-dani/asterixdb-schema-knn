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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VCTreeBulkLoader extends AbstractTreeIndexBulkLoader {
    private static final Logger LOGGER = LogManager.getLogger();
    private int firstLeafCentroidId; // ID of the first leaf centroid
    private int numLeafCentroid; // Total number of leaf centroids

    // Bulk loading state for leaf clusters
    private final List<ICachedPage> directoryPages; // Directory pages for each leaf cluster
    private int currentLeafClusterIndex; // Current leaf cluster being loaded (0-based)
    private ICachedPage currentDirectoryPage; // Current directory page for the cluster
    private ICachedPage currentDataPage; // Current data page being filled
    private final ITreeIndexFrame currentDataFrame; // Frame for the current data page
    private final ITreeIndexFrame currentDirectoryFrame; // Frame for the current directory page
    private int entriesInCurrentDataPage; // Number of entries in current data page
    private int entriesInCurrentDirectoryPage; // Number of entries in current directory page
    private int currentDataPageId; // Page ID of the current data page
    private ISerializerDeserializer[] dataFrameSerds;
    private ITreeIndexTupleWriter directoryFrameTupleWriter;
    private ITreeIndexTupleWriter dataFrameTupleWriter;
    private final VectorClusteringTree vcTreeIndex;
    private int firstDirectoryPageId;
    private int currentCentroidId;
    private int counter = 0;

    public VCTreeBulkLoader(float fillFactor, IPageWriteCallback callback, VectorClusteringTree vectorTree,
            ITreeIndexFrame leafFrame, ITreeIndexFrame dataFrame, IBufferCacheWriteContext writeContext,
            ISerializerDeserializer[] dataFrameSerds, ITreeIndexAccessor staticAccessor) throws HyracksDataException {
        super(0, callback, vectorTree, leafFrame, writeContext);
        //        this.numLeafCentroid = numLeafCentroid;
        //        this.firstLeafCentroidId = firstLeafCentroidId;

        this.directoryPages = new ArrayList<>();
        // Initialize frames
        this.interiorFrame = vectorTree.getInteriorFrameFactory().createFrame();
        this.leafFrame = vectorTree.getLeafFrameFactory().createFrame();
        this.currentDirectoryFrame = vectorTree.getMetadataFrameFactory().createFrame();
        this.currentDataFrame = vectorTree.getDataFrameFactory().createFrame();
        this.dataFrameSerds = dataFrameSerds;
        this.vcTreeIndex = vectorTree;
        this.dataFrameTupleWriter = currentDataFrame.getTupleWriter();
        this.directoryFrameTupleWriter = currentDirectoryFrame.getTupleWriter();
        this.currentLeafClusterIndex = 0;
        this.currentCentroidId = -1;
        VectorClusteringTree.VectorClusteringTreeAccessor vcTreeAccessor =
                (VectorClusteringTree.VectorClusteringTreeAccessor) staticAccessor;
        VectorClusteringTree vctree = vcTreeAccessor.getIndex();
        ITreeIndexMetadataFrame metaFrame = (vcTreeAccessor).getOpContext().getMetaFrame();
        int maxPageId = vctree.getPageManager().getMaxPageId(metaFrame);
        MutableArrayValueReference key1 = new MutableArrayValueReference("num_leaf_centroids".getBytes());
        LongPointable value1 = LongPointable.FACTORY.createPointable();
        MutableArrayValueReference key2 = new MutableArrayValueReference("first_leaf_centroid_id".getBytes());
        LongPointable value2 = LongPointable.FACTORY.createPointable();
        metaFrame.get(key1, value1);
        metaFrame.get(key2, value2);
        this.numLeafCentroid = value1.intValue();
        this.firstLeafCentroidId = value2.intValue();

        // Simple bulk load - just copy all pages

        for (int pageId = 1; pageId <= maxPageId; pageId++) {
            ICachedPage sourcePage = vcTreeAccessor.getCachedPage(pageId);
            copyPage(sourcePage);
            vcTreeAccessor.releasePage(sourcePage);
        }
    }

    /**
     * Copy static structure from existing file into this bulk loader.
     * 
     * @param staticStructureFileName Name of the static structure file (relative to index file directory)
     * @throws HyracksDataException if file doesn't exist or copying fails
     */
    private void copyStaticStructure(String staticStructureFileName) throws HyracksDataException {
        try {
            // Derive static structure file path from index file

            FileReference indexFileRef = treeIndex.getFileReference();
            FileReference staticStructureFileRef = indexFileRef.getParent().getChild(staticStructureFileName);

            // Check if file exists
            File staticStructureFile = new File(staticStructureFileRef.getAbsolutePath());
            if (!staticStructureFile.exists()) {
                throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.FILE_DOES_NOT_EXIST,
                        "Static structure file does not exist: " + staticStructureFileRef.getAbsolutePath());
            }

            LOGGER.debug("Copying static structure from: {}", staticStructureFileRef.getAbsolutePath());

            // Open static structure file
            int staticStructureFileId = bufferCache.openFile(staticStructureFileRef);

            try {
                // Copy pages 1-8 in level order
                // TODO : CALVIN DANI
                for (int pageId = 1; pageId <= 8; pageId++) {
                    ICachedPage sourcePage =
                            bufferCache.pin(BufferedFileHandle.getDiskPageId(staticStructureFileId, pageId));
                    try {
                        copyPage(sourcePage);
                    } finally {
                        bufferCache.unpin(sourcePage);
                    }
                }

                LOGGER.debug("Successfully copied static structure pages 1-8");
            } finally {
                // Close static structure file
                bufferCache.closeFile(staticStructureFileId);
            }

        } catch (HyracksDataException e) {
            throw e;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public void copyPage(ICachedPage sourcePage) throws HyracksDataException {
        // Copy page from source to target
        int targetPageId = freePageManager.takePage(metaFrame);
        ICachedPage targetPage =
                bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(treeIndex.getFileId(), targetPageId));
        // Copy entire page content
        targetPage.setDiskPageId(BufferedFileHandle.getDiskPageId(treeIndex.getFileId(), targetPageId));
        System.arraycopy(sourcePage.getBuffer().array(), 0, targetPage.getBuffer().array(), 0,
                sourcePage.getBuffer().capacity());

        write(targetPage);
        LOGGER.debug("Copied page {} ", targetPage);
    }

    /**
     * Create metadata pages for leaf clusters.
     */
    private void createFirstDirectoryPages() throws HyracksDataException {
        for (int leafCentroidId = 0; leafCentroidId < numLeafCentroid; leafCentroidId++) {
            // For metadata pages, use freePageManager.takePage() normally
            int metadataPageId = freePageManager.takePage(metaFrame);
            if (leafCentroidId == 0) {
                // set the first directory page ID
                firstDirectoryPageId = metadataPageId;
            }
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
            currentDirectoryPage = directoryPages.getFirst();
            currentDirectoryFrame.setPage(currentDirectoryPage);
            LOGGER.debug("Initialized bulk loading with {} directory pages", directoryPages.size());
        }
    }

    /**
     * ========= Clustering records to leaf centroids =======
     */

    //    public ClusterSearchResult findCluster(ITupleReference tuple) throws HyracksDataException {
    //        /*  use static structure to find the closest leaf centroid for the given tuple
    //            return the leaf centroid ID and the distance
    //        */
    //        VectorClusteringTree.VectorClusteringTreeAccessor accessor =
    //                (VectorClusteringTree.VectorClusteringTreeAccessor) vcTreeIndex
    //                        .createAccessor(NoOpIndexAccessParameters.INSTANCE);
    //
    //        double[] vectorEmbedding = extractVector(tuple);
    //        return accessor.findClosestLeafCentroid(vectorEmbedding); // Placeholder
    //    }

    private double extractDistance(ITupleReference tuple) throws HyracksDataException {
        // Assuming the distance is stored in the first field of the tuple
        Object[] fieldValues = TupleUtils.deserializeTuple(tuple, dataFrameSerds);
        return (Double) fieldValues[0];
    }

    private double[] extractVector(ITupleReference tuple) throws HyracksDataException {
        // Assuming the vector is stored in the second field of the tuple
        Object[] fieldValues = TupleUtils.deserializeTuple(tuple, dataFrameSerds);
        return (double[]) fieldValues[2];
    }

    private int extractCentroidId(ITupleReference tuple) throws HyracksDataException {
        return IntegerPointable.getInteger(tuple.getFieldData(1), tuple.getFieldStart(1) + 1);
    }

    /**
     * ========= leaf cluster bulk loading methods =========
     */
    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        int tupleCentroidId = extractCentroidId(tuple); // just to verify tuple format
        if (currentCentroidId == -1) {
            // First tuple being added
            LOGGER.debug("Starting bulk load with first centroid cluster: {}", tupleCentroidId);
            currentCentroidId = tupleCentroidId;
        } else if (currentCentroidId != tupleCentroidId) {
            // Moved to a new centroid cluster
            LOGGER.debug("Switching from centroid {} to centroid {}", currentCentroidId, tupleCentroidId);
            currentCentroidId = tupleCentroidId;
            // Calculate target cluster index from centroid ID (handle gaps in centroid IDs)
            int targetClusterIndex = tupleCentroidId - firstLeafCentroidId;
            loadToNextLeafCluster(targetClusterIndex);
        }
        if (directoryPages.isEmpty()) {
            createFirstDirectoryPages();
            createNewDataPage();
        }
        try {
            // Calculate space needed for this tuple - following BTreeNSMBulkLoader pattern
            int spaceNeeded = dataFrameTupleWriter.bytesRequired(tuple) + slotSize;
            int spaceAvailable = currentDataFrame.getTotalFreeSpace();

            // If still full, need to create new data page and update directory
            if (spaceNeeded > spaceAvailable) {
                if (currentDataFrame.getTupleCount() == 0) {
                    // following BTreeNSMBulkLoader pattern
                    bufferCache.returnPage(currentDataPage, false);
                }
                // Write current data page and add entry to directory
                writeDataPageToDirectory(false);
                // TODO: For now we don't handle large tuples exceeds page size
            }
            // TODO: skip verify tuple
            // Insert tuple into current data page (tuples are pre-sorted by distance)
            ((IVectorClusteringDataFrame) currentDataFrame).insertSorted(tuple);
            entriesInCurrentDataPage++;

            LOGGER.debug("Added tuple to leaf cluster {}, data page entries: {}", currentLeafClusterIndex,
                    entriesInCurrentDataPage);
        } catch (HyracksDataException | RuntimeException e) {
            // Log state for debugging - following BTreeNSMBulkLoader pattern
            logDataPageState(tuple, e);
            handleException();
            throw e;
        }
    }

    /**
     * Load to a specific leaf cluster by index.
     * Handles gaps in centroid IDs by jumping directly to the target cluster.
     *
     * @param targetClusterIndex Target cluster index (0-based) calculated from centroid ID
     * @throws HyracksDataException if cluster index is out of bounds
     */
    public void loadToNextLeafCluster(int targetClusterIndex) throws HyracksDataException {
        // Validate target cluster index
        if (targetClusterIndex < 0 || targetClusterIndex >= directoryPages.size()) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Target cluster index out of bounds: " + targetClusterIndex + " (valid range: 0-"
                            + (directoryPages.size() - 1) + ")");
        }

        // Skip if already at target cluster
        if (currentLeafClusterIndex == targetClusterIndex) {
//            LOGGER.debug("Already at target cluster {}, skipping", targetClusterIndex);
            return;
        }

        // Finish current data page if it exists and has data
        if (currentDataPage != null && entriesInCurrentDataPage > 0) {
            writeDataPageToDirectory(true);
        }

        // Write current directory page before switching
        if (currentDirectoryPage != null) {
            write(currentDirectoryPage);
        }

        // Log gap if skipping clusters
//        if (targetClusterIndex > currentLeafClusterIndex + 1) {
////            LOGGER.info("Skipping empty clusters from {} to {} (gap detected)", currentLeafClusterIndex + 1,
////                    targetClusterIndex - 1);
//        }

        // Move to target leaf cluster
        currentLeafClusterIndex = targetClusterIndex;

        // Set current directory page to the target cluster's directory page
        currentDirectoryPage = directoryPages.get(currentLeafClusterIndex);

        // Initialize directory frame for the new cluster
        currentDirectoryFrame.setPage(currentDirectoryPage);
        currentDirectoryFrame.initBuffer((byte) 0);
        entriesInCurrentDirectoryPage = 0;

        // Reset data page state for new cluster
        entriesInCurrentDataPage = 0;

        LOGGER.debug("Moved to leaf cluster {} (centroid ID: {})", currentLeafClusterIndex,
                firstLeafCentroidId + currentLeafClusterIndex);
    }

    /**
     * Get the centroid ID of the first leaf level centroid.
     * This is calculated based on the total number of centroids in all levels before the leaf level.
     * @return the centroid ID of the first leaf centroid
     */
    public int getFirstLeafCentroidId() {
        // The first leaf centroid ID is the total number of centroids processed before the leaf level
        return firstLeafCentroidId;
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
    private void writeDataPageToDirectory(boolean lastPage) throws HyracksDataException {
        // Create directory entry tuple with max distance and page ID
        // For now, use a placeholder distance value
        // this would be the maximum distance of tuples in the data page

        int tupleCount = currentDataFrame.getTupleCount();
        double maxDistance = ((IVectorClusteringDataFrame) currentDataFrame).getDistanceToCentroid(tupleCount - 1);

        try {
            ITupleReference directoryEntry =
                    TupleUtils.createTuple(new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                            IntegerSerializerDeserializer.INSTANCE }, maxDistance, currentDataPageId);

            // Check if directory page has space
            int spaceNeeded = directoryFrameTupleWriter.bytesRequired(directoryEntry) + slotSize;
            int spaceAvailable = currentDirectoryFrame.getTotalFreeSpace();

            if (spaceNeeded > spaceAvailable) {
                // Directory page is full, need to create overflow directory page
                createOverflowDirectoryPage();
            }

            // Insert directory entry using the frame's insert method
            // Cast to appropriate frame type that has insertSorted method
            ((IVectorClusteringFrame) currentDirectoryFrame).insertSorted(directoryEntry);
            entriesInCurrentDirectoryPage++;

            LOGGER.debug("Added directory entry for data page {} to directory page, entries: {}", currentDataPageId,
                    entriesInCurrentDirectoryPage);

        } catch (Exception e) {
            throw new HyracksDataException("Failed to create directory entry");
        }

        int nextPageId = freePageManager.takePage(metaFrame);

        if (lastPage) {
            ((IVectorClusteringDataFrame) currentDataFrame).setNextPage(-1);
        } else {
            ((IVectorClusteringDataFrame) currentDataFrame).setNextPage(nextPageId);
        }

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
        ((IVectorClusteringMetadataFrame) currentDirectoryFrame).setNextPage(nextDirectoryPageId);

        // Write current directory page first
        write(currentDirectoryPage);

        long dpid = BufferedFileHandle.getDiskPageId(fileId, nextDirectoryPageId);
        currentDirectoryPage = bufferCache.confiscatePage(dpid);

        // Initialize new directory frame
        currentDirectoryFrame.setPage(currentDirectoryPage);
        currentDirectoryFrame.initBuffer((byte) 0);
        entriesInCurrentDirectoryPage = 0;

        LOGGER.debug("Created overflow directory page {} for leaf cluster {}", nextDirectoryPageId,
                currentLeafClusterIndex);
    }

    /**
     * Log the state of the data page for debugging purposes.
     * Similar to BTreeNSMBulkLoader's logState method.
     */
    private void logDataPageState(ITupleReference tuple, Exception e) {
        try {
            if (currentDataFrame != null) {
                int tupleSize = currentDataFrame.getBytesRequiredToWriteTuple(tuple);
                int spaceNeeded = dataFrameTupleWriter.bytesRequired(tuple) + slotSize;
                int spaceUsed = currentDataFrame.getBuffer().capacity() - currentDataFrame.getTotalFreeSpace();

                LOGGER.error(
                        "Data page state - tupleSize: {}, spaceNeeded: {}, spaceUsed: {}, entriesInCurrentDataPage: {}",
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
        // Write the final data page if it has entries
        // BUG FIX: Previously, we called write(currentDataPage) directly,
        // which wrote the page but didn't create a directory entry.
        // This made the last records inaccessible during search.
        if (entriesInCurrentDataPage > 0) {
            writeDataPageToDirectory(true); // Creates directory entry AND writes page
        }

        // Write the directory page
        write(currentDirectoryPage);

        metaFrame.put(new MutableArrayValueReference("num_leaf_centroids".getBytes()),
                LongPointable.FACTORY.createPointable(numLeafCentroid));
        metaFrame.put(new MutableArrayValueReference("first_leaf_centroid_id".getBytes()),
                LongPointable.FACTORY.createPointable(firstLeafCentroidId));
        metaFrame.put(new MutableArrayValueReference("first_directory_page_id".getBytes()),
                LongPointable.FACTORY.createPointable(firstDirectoryPageId));
        super.end();
    }

    @Override
    public void abort() throws HyracksDataException {
        LOGGER.debug("VCTreeStaticStructureLoader aborted");
        super.handleException();
    }
}
