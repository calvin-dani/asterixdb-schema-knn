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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VCTreeBulkLoader extends PageWriteFailureCallback implements IIndexBulkLoader {
    private static final Logger LOGGER = LogManager.getLogger();

    // Fields replacing inherited ones from AbstractTreeIndexBulkLoader
    private final IBufferCache bufferCache;
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private final AbstractTreeIndex treeIndex;
    private final int fileId;
    private final int slotSize;
    private final IFIFOPageWriter pageWriter;
    private final ICompressedPageWriter compressedPageWriter;

    // Static structure data (saved byte arrays for copying at end)
    private final List<byte[]> staticPageContents;
    private final int numStaticPages;

    private int firstLeafCentroidId;
    private int numLeafCentroid;

    // Per-cluster directory page tracking: clusterIndex -> first dir page ID
    private final int[] clusterFirstDirPageId;

    // Bulk loading state for leaf clusters
    private int currentLeafClusterIndex;
    private ICachedPage currentDirectoryPage;
    private int currentDirectoryPageId;
    private ICachedPage currentDataPage;
    private final ITreeIndexFrame currentDataFrame;
    private final ITreeIndexFrame currentDirectoryFrame;
    private int entriesInCurrentDataPage;
    private int entriesInCurrentDirectoryPage;
    private int currentDataPageId;
    private ITreeIndexTupleWriter directoryFrameTupleWriter;
    private ITreeIndexTupleWriter dataFrameTupleWriter;
    private int currentCentroidId;

    // Buffered data pages for current batch (flushed when directory page fills or cluster ends)
    private final List<ICachedPage> bufferedDataPages = new ArrayList<>();

    public VCTreeBulkLoader(IPageWriteCallback callback, VectorClusteringTree vectorTree,
            ITreeIndexAccessor staticAccessor) throws HyracksDataException {

        this.bufferCache = vectorTree.getBufferCache();
        this.freePageManager = vectorTree.getPageManager();
        this.fileId = vectorTree.getFileId();
        this.treeIndex = vectorTree;
        this.metaFrame = freePageManager.createMetadataFrame();

        // Initialize frames
        this.currentDirectoryFrame = vectorTree.getMetadataFrameFactory().createFrame();
        this.currentDataFrame = vectorTree.getDataFrameFactory().createFrame();
        this.dataFrameTupleWriter = currentDataFrame.getTupleWriter();
        this.directoryFrameTupleWriter = currentDirectoryFrame.getTupleWriter();
        this.slotSize = currentDataFrame.getSlotSize();
        this.currentLeafClusterIndex = 0;
        this.currentCentroidId = -1;

        this.pageWriter = bufferCache.createFIFOWriter(callback, this, DefaultBufferCacheWriteContext.INSTANCE);
        this.compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);

        VectorClusteringTree.VectorClusteringTreeAccessor vcTreeAccessor =
                (VectorClusteringTree.VectorClusteringTreeAccessor) staticAccessor;
        VectorClusteringTree vctree = vcTreeAccessor.getIndex();
        ITreeIndexMetadataFrame staticMetaFrame = vcTreeAccessor.getOpContext().getMetaFrame();
        int maxPageId = vctree.getPageManager().getMaxPageId(staticMetaFrame);

        MutableArrayValueReference key1 = new MutableArrayValueReference("num_leaf_centroids".getBytes());
        LongPointable value1 = LongPointable.FACTORY.createPointable();
        MutableArrayValueReference key2 = new MutableArrayValueReference("first_leaf_centroid_id".getBytes());
        LongPointable value2 = LongPointable.FACTORY.createPointable();
        staticMetaFrame.get(key1, value1);
        staticMetaFrame.get(key2, value2);
        this.numLeafCentroid = value1.intValue();
        this.firstLeafCentroidId = value2.intValue();

        // Save static page contents as byte arrays (do NOT write to disk yet)
        staticPageContents = new ArrayList<>();
        for (int pageId = 0; pageId <= maxPageId; pageId++) {
            ICachedPage sourcePage = vcTreeAccessor.getCachedPage(pageId);
            byte[] content = new byte[sourcePage.getBuffer().capacity()];
            System.arraycopy(sourcePage.getBuffer().array(), 0, content, 0, content.length);
            staticPageContents.add(content);
            vcTreeAccessor.releasePage(sourcePage);
        }
        numStaticPages = staticPageContents.size();

        // Initialize per-cluster directory page tracking
        clusterFirstDirPageId = new int[numLeafCentroid];
        for (int i = 0; i < numLeafCentroid; i++) {
            clusterFirstDirPageId[i] = -1;
        }

        LOGGER.debug("VCTreeBulkLoader initialized: numLeafCentroid={}, firstLeafCentroidId={}, numStaticPages={}",
                numLeafCentroid, firstLeafCentroidId, numStaticPages);
    }

    /**
     * Create and confiscate directory page for a specific leaf cluster on-demand.
     */
    private void createDirectoryPageForCluster(int clusterIndex) throws HyracksDataException {
        int dirPageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, dirPageId);
        currentDirectoryPage = bufferCache.confiscatePage(dpid);
        currentDirectoryFrame.setPage(currentDirectoryPage);
        currentDirectoryFrame.initBuffer((byte) 0);
        currentDirectoryPageId = dirPageId;
        entriesInCurrentDirectoryPage = 0;

        // Record first directory page for this cluster
        if (clusterFirstDirPageId[clusterIndex] == -1) {
            clusterFirstDirPageId[clusterIndex] = dirPageId;
        }

        LOGGER.debug("Created directory page {} for cluster {}", dirPageId, clusterIndex);
    }

    private int extractCentroidId(ITupleReference tuple) throws HyracksDataException {
        return IntegerPointable.getInteger(tuple.getFieldData(1), tuple.getFieldStart(1));
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        int tupleCentroidId = extractCentroidId(tuple);
        if (currentCentroidId == -1) {
            // First tuple being added - initialize for first cluster
            LOGGER.debug("Starting bulk load with first centroid cluster: {}", tupleCentroidId);
            currentCentroidId = tupleCentroidId;
            int targetClusterIndex = tupleCentroidId - firstLeafCentroidId;
            currentLeafClusterIndex = targetClusterIndex;
            createDirectoryPageForCluster(targetClusterIndex);
            createNewDataPage();
        } else if (currentCentroidId != tupleCentroidId) {
            // Moved to a new centroid cluster
            LOGGER.debug("Switching from centroid {} to centroid {}", currentCentroidId, tupleCentroidId);
            currentCentroidId = tupleCentroidId;
            int targetClusterIndex = tupleCentroidId - firstLeafCentroidId;
            loadToNextLeafCluster(targetClusterIndex);
        }
        try {
            int spaceNeeded = dataFrameTupleWriter.bytesRequired(tuple) + slotSize;
            int spaceAvailable = currentDataFrame.getTotalFreeSpace();

            if (spaceNeeded > spaceAvailable) {
                if (currentDataFrame.getTupleCount() == 0) {
                    bufferCache.returnPage(currentDataPage, false);
                }
                writeDataPageToDirectory(false);
            }
            ((IVectorClusteringDataFrame) currentDataFrame).insertSorted(tuple);
            entriesInCurrentDataPage++;

            LOGGER.debug("Added tuple to leaf cluster {}, data page entries: {}", currentLeafClusterIndex,
                    entriesInCurrentDataPage);
        } catch (HyracksDataException | RuntimeException e) {
            logDataPageState(tuple, e);
            handleException();
            throw e;
        }
    }

    /**
     * Load to a specific leaf cluster by index.
     */
    public void loadToNextLeafCluster(int targetClusterIndex) throws HyracksDataException {
        if (targetClusterIndex < 0 || targetClusterIndex >= numLeafCentroid) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Target cluster index out of bounds: " + targetClusterIndex + " (valid range: 0-"
                            + (numLeafCentroid - 1) + ")");
        }

        if (currentLeafClusterIndex == targetClusterIndex) {
            return;
        }

        // Finish current data page if it has data
        if (currentDataPage != null && entriesInCurrentDataPage > 0) {
            writeDataPageToDirectory(true);
        }

        // Flush remaining batch for current cluster
        if (currentDirectoryPage != null) {
            flushBatch();
        }

        // Move to target leaf cluster
        currentLeafClusterIndex = targetClusterIndex;
        createDirectoryPageForCluster(targetClusterIndex);
        createNewDataPage();

        LOGGER.debug("Moved to leaf cluster {} (centroid ID: {})", currentLeafClusterIndex,
                firstLeafCentroidId + currentLeafClusterIndex);
    }

    public int getFirstLeafCentroidId() {
        return firstLeafCentroidId;
    }

    /**
     * Create a new data page for the current leaf cluster.
     */
    private void createNewDataPage() throws HyracksDataException {
        int dataPageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, dataPageId);
        currentDataPage = bufferCache.confiscatePage(dpid);
        currentDataPageId = dataPageId;

        currentDataFrame.setPage(currentDataPage);
        currentDataFrame.initBuffer((byte) 0);
        entriesInCurrentDataPage = 0;

        LOGGER.debug("Created new data page {} for leaf cluster {}", dataPageId, currentLeafClusterIndex);
    }

    /**
     * Write the current data page information to the directory page.
     * When lastPage is true, the data page next-pointer is set to -1 and no new data page is allocated.
     */
    private void writeDataPageToDirectory(boolean lastPage) throws HyracksDataException {
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
                // Directory page is full - flush current batch before creating overflow
                flushBatch();
                createOverflowDirectoryPage();
            }

            ((IVectorClusteringFrame) currentDirectoryFrame).insertSorted(directoryEntry);
            entriesInCurrentDirectoryPage++;

            LOGGER.debug("Added directory entry for data page {} to directory page, entries: {}", currentDataPageId,
                    entriesInCurrentDirectoryPage);

        } catch (HyracksDataException e) {
            throw e;
        } catch (Exception e) {
            throw new HyracksDataException("Failed to create directory entry", e);
        }

        // Set next-page pointer on data page
        if (lastPage) {
            ((IVectorClusteringDataFrame) currentDataFrame).setNextPage(-1);
        } else {
            int nextDataPageId = freePageManager.takePage(metaFrame);
            ((IVectorClusteringDataFrame) currentDataFrame).setNextPage(nextDataPageId);

            // Buffer the current data page
            bufferedDataPages.add(currentDataPage);

            // Create new data page with the pre-allocated ID
            currentDataPageId = nextDataPageId;
            long dpid = BufferedFileHandle.getDiskPageId(fileId, currentDataPageId);
            currentDataPage = bufferCache.confiscatePage(dpid);
            currentDataFrame.setPage(currentDataPage);
            currentDataFrame.initBuffer((byte) 0);
            entriesInCurrentDataPage = 0;

            LOGGER.debug("Created new data page {} for leaf cluster {}", currentDataPageId, currentLeafClusterIndex);
            return;
        }

        // For last page, just buffer it (no new page created)
        bufferedDataPages.add(currentDataPage);
        currentDataPage = null;
        entriesInCurrentDataPage = 0;
    }

    /**
     * Flush the current batch: write directory page first, then all buffered data pages.
     * This ensures sequential page ID ordering (directory page ID < data page IDs).
     */
    private void flushBatch() throws HyracksDataException {
        // Write directory page first (it has the smallest page ID in this batch)
        if (currentDirectoryPage != null) {
            write(currentDirectoryPage);
            currentDirectoryPage = null;
        }

        // Write all buffered data pages in order
        for (ICachedPage dataPage : bufferedDataPages) {
            write(dataPage);
        }
        bufferedDataPages.clear();
    }

    /**
     * Create overflow directory page when current directory page is full.
     */
    private void createOverflowDirectoryPage() throws HyracksDataException {
        int nextDirectoryPageId = freePageManager.takePage(metaFrame);

        // Set next page pointer in current directory page
        ((IVectorClusteringMetadataFrame) currentDirectoryFrame).setNextPage(nextDirectoryPageId);

        // Note: The current directory page was already written in flushBatch() before this call

        long dpid = BufferedFileHandle.getDiskPageId(fileId, nextDirectoryPageId);
        currentDirectoryPage = bufferCache.confiscatePage(dpid);
        currentDirectoryPageId = nextDirectoryPageId;

        currentDirectoryFrame.setPage(currentDirectoryPage);
        currentDirectoryFrame.initBuffer((byte) 0);
        entriesInCurrentDirectoryPage = 0;

        LOGGER.debug("Created overflow directory page {} for leaf cluster {}", nextDirectoryPageId,
                currentLeafClusterIndex);
    }

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

    private void handleException() {
        compressedPageWriter.abort();
        for (ICachedPage page : bufferedDataPages) {
            if (page != null && page.confiscated()) {
                bufferCache.returnPage(page, false);
            }
        }
        if (currentDirectoryPage != null && currentDirectoryPage.confiscated()) {
            bufferCache.returnPage(currentDirectoryPage, false);
        }
        if (currentDataPage != null && currentDataPage.confiscated()) {
            bufferCache.returnPage(currentDataPage, false);
        }
        bufferedDataPages.clear();
        freePageManager.returnAllPages();
    }

    @Override
    public void end() throws HyracksDataException {
        // Flush last cluster's remaining pages
        if (entriesInCurrentDataPage > 0) {
            writeDataPageToDirectory(true);
        }
        if (currentDirectoryPage != null) {
            flushBatch();
        }

        // --- Copy static pages to end of file ---
        int staticBasePageId = freePageManager.takePage(metaFrame);
        // Allocate remaining S-1 pages
        for (int i = 1; i < numStaticPages; i++) {
            freePageManager.takePage(metaFrame);
        }

        // Create frames for pointer adjustment
        IVectorClusteringInteriorFrame intFrame =
                (IVectorClusteringInteriorFrame) treeIndex.getInteriorFrameFactory().createFrame();
        IVectorClusteringLeafFrame lfFrame = (IVectorClusteringLeafFrame) treeIndex.getLeafFrameFactory().createFrame();

        int centroidIndex = 0;
        TreeMap<Integer, ICachedPage> staticPages = new TreeMap<>();

        for (int i = 0; i < numStaticPages; i++) {
            int newPageId = staticBasePageId + i;
            long dpid = BufferedFileHandle.getDiskPageId(fileId, newPageId);
            ICachedPage page = bufferCache.confiscatePage(dpid);

            // Copy content from saved byte array
            System.arraycopy(staticPageContents.get(i), 0, page.getBuffer().array(), 0,
                    staticPageContents.get(i).length);

            // Determine page type via level field and adjust pointers
            ((ITreeIndexFrame) intFrame).setPage(page);
            byte level = ((ITreeIndexFrame) intFrame).getLevel();

            if (level > 0) {
                // Interior page: offset child pointers by staticBasePageId
                for (int t = 0; t < ((ITreeIndexFrame) intFrame).getTupleCount(); t++) {
                    int oldChildId = intFrame.getChildPageId(t);
                    intFrame.setChildPageId(t, oldChildId + staticBasePageId);
                }
                // Offset next-page (overflow) pointer if present
                if (intFrame.getOverflowFlagBit()) {
                    intFrame.setNextPage(intFrame.getNextPage() + staticBasePageId);
                }
            } else {
                // Leaf page: set metadata pointers to actual dir page IDs
                ((ITreeIndexFrame) lfFrame).setPage(page);
                for (int t = 0; t < ((ITreeIndexFrame) lfFrame).getTupleCount(); t++) {
                    if (centroidIndex < numLeafCentroid) {
                        lfFrame.setMetadataPagePointer(t, clusterFirstDirPageId[centroidIndex]);
                        centroidIndex++;
                    }
                }
                // Offset next-leaf pointer (overflow or sibling chain)
                int oldNextLeaf = lfFrame.getNextLeaf();
                if (oldNextLeaf >= 0) {
                    lfFrame.setNextLeaf(oldNextLeaf + staticBasePageId);
                }
            }

            staticPages.put(newPageId, page);
        }

        // Write all static pages sequentially
        for (Map.Entry<Integer, ICachedPage> entry : staticPages.entrySet()) {
            write(entry.getValue());
        }

        // Set root page and metadata
        int rootPageId = staticBasePageId; // Root was page 0 in static structure
        ((VectorClusteringTree) treeIndex).setRootPageId(rootPageId);
        freePageManager.setRootPageId(rootPageId);

        metaFrame.put(new MutableArrayValueReference("num_leaf_centroids".getBytes()),
                LongPointable.FACTORY.createPointable(numLeafCentroid));
        metaFrame.put(new MutableArrayValueReference("first_leaf_centroid_id".getBytes()),
                LongPointable.FACTORY.createPointable(firstLeafCentroidId));

        if (hasFailed()) {
            throw HyracksDataException.create(getFailure());
        }
    }

    private void write(ICachedPage cPage) throws HyracksDataException {
        compressedPageWriter.prepareWrite(cPage);
        pageWriter.write(cPage);
    }

    @Override
    public void abort() throws HyracksDataException {
        LOGGER.debug("VCTreeBulkLoader aborted");
        handleException();
    }

    @Override
    public void force() throws HyracksDataException {
        bufferCache.force(fileId, false);
    }
}
