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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
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

public class VCTreeBulkLoder extends AbstractTreeIndexBulkLoader {
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

    public VCTreeBulkLoder(float fillFactor, IPageWriteCallback callback, VectorClusteringTree vectorTree,
            ITreeIndexFrame leafFrame, ITreeIndexFrame dataFrame, IBufferCacheWriteContext writeContext,
            int numLeafCentroid, int firstLeafCentroidId, ISerializerDeserializer[] dataFrameSerds)
            throws HyracksDataException {
        super(fillFactor, callback, vectorTree, leafFrame, writeContext);
        this.numLeafCentroid = numLeafCentroid;
        this.firstLeafCentroidId = firstLeafCentroidId;

        this.directoryPages = new ArrayList<>();
        // Initialize frames
        this.interiorFrame = vectorTree.getInteriorFrameFactory().createFrame();
        this.leafFrame = vectorTree.getLeafFrameFactory().createFrame();
        this.currentDirectoryFrame = vectorTree.getMetadataFrameFactory().createFrame();
        this.currentDataFrame = vectorTree.getDataFrameFactory().createFrame();
        this.dataFrameSerds = dataFrameSerds;
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
    private void createMetadataPages() throws HyracksDataException {
        for (int leafCentroidId = 0; leafCentroidId < numLeafCentroid; leafCentroidId++) {
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

    private double extractDistance(ITupleReference tuple) throws HyracksDataException {
        // Assuming the distance is stored in the first field of the tuple
        Object[] fieldValues = TupleUtils.deserializeTuple(tuple, dataFrameSerds);
        return (Double) fieldValues[0];
    }

    /**
     * ========= leaf cluster bulk loading methods =========
     */

    public void add(ITupleReference tuple) throws HyracksDataException {
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

            LOGGER.debug("Added tuple to leaf cluster {}, data page entries: {}", currentLeafClusterIndex,
                    entriesInCurrentDataPage);
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
    private void writeDataPageToDirectory() throws HyracksDataException {
        // Create directory entry tuple with max distance and page ID
        // For now, use a placeholder distance value
        // this would be the maximum distance of tuples in the data page
        // TODO: extract actual max distance from data page's last tuple
        double maxDistance = 6.6; // Placeholder

        try {
            ITupleReference directoryEntry =
                    TupleUtils.createTuple(new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                            IntegerSerializerDeserializer.INSTANCE }, maxDistance, currentDataPageId);

            // Check if directory page has space
            int spaceNeeded = tupleWriter.bytesRequired(directoryEntry) + slotSize;
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
        ((IVectorClusteringDataFrame) currentDataFrame).setNextPage(nextPageId);

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
                int spaceNeeded = tupleWriter.bytesRequired(tuple) + slotSize;
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
        super.end();
    }

    @Override
    public void abort() throws HyracksDataException {
        LOGGER.debug("VCTreeStaticStructureLoader aborted");
        super.handleException();
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
