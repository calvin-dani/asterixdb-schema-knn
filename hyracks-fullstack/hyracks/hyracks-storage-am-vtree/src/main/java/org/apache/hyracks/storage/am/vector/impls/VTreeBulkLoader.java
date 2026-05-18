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
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.ISampler;
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

public class VTreeBulkLoader extends PageWriteFailureCallback implements IIndexBulkLoader {
    private static final Logger LOGGER = LogManager.getLogger();

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
    // Root page id within the static structure's page id space. After the static
    // pages are copied into this data component starting at staticBasePageId, the
    // root of the copied structure sits at (staticBasePageId + staticStructureRootPage).
    private final int staticStructureRootPage;

    private int firstLeafCentroidId;
    private int numLeafCentroid;

    // Per-cluster directory page tracking: clusterIndex -> first dir page ID
    private final int[] clusterFirstDirPageId;

    // Current cluster state
    private int currentLeafClusterIndex;
    private int currentCentroidId;

    // Current data page (only one in memory at a time — written immediately when full)
    private ICachedPage currentDataPage;
    private int currentDataPageId;
    private final ITreeIndexFrame currentDataFrame;
    private final ITreeIndexTupleWriter dataFrameTupleWriter;
    private int entriesInCurrentDataPage;

    // Directory pages for current cluster. Confiscated with INVALID_DPID and kept in memory
    // until the cluster is finalized, at which point they receive real page IDs, get chained
    // via nextPage pointers, and are written to disk. Typically only 1 page per cluster in
    // production (one 32KB directory page holds ~2000 entries).
    private final ITreeIndexFrame currentDirectoryFrame;
    private final ITreeIndexTupleWriter directoryFrameTupleWriter;
    private final List<ICachedPage> pendingDirectoryPages = new ArrayList<>();
    private ICachedPage currentDirectoryPage;
    private final ISampler sampler;

    public VTreeBulkLoader(IPageWriteCallback callback, VTree vectorTree, ITreeIndexAccessor staticAccessor,
            ISampler sampler) throws HyracksDataException {

        this.sampler = sampler;
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
        this.currentLeafClusterIndex = -1;
        this.currentCentroidId = -1;

        this.pageWriter = bufferCache.createFIFOWriter(callback, this, DefaultBufferCacheWriteContext.INSTANCE);
        this.compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);

        VTree.VTreeAccessor vTreeAccessor = (VTree.VTreeAccessor) staticAccessor;
        VTree vtree = vTreeAccessor.getIndex();
        ITreeIndexMetadataFrame staticMetaFrame = vTreeAccessor.getOpContext().getMetaFrame();
        int maxPageId = vtree.getPageManager().getMaxPageId(staticMetaFrame);

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
            ICachedPage sourcePage = vTreeAccessor.getCachedPage(pageId);
            byte[] content = new byte[sourcePage.getBuffer().capacity()];
            System.arraycopy(sourcePage.getBuffer().array(), 0, content, 0, content.length);
            staticPageContents.add(content);
            vTreeAccessor.releasePage(sourcePage);
        }
        numStaticPages = staticPageContents.size();
        // VTreeStaticStructureBuilder writes the root at the highest page id (bottom-up).
        // Capture it now so we can translate when the static pages are copied into this
        // data component with an offset.
        this.staticStructureRootPage = vtree.getRootPageId();

        // Initialize per-cluster directory page tracking
        clusterFirstDirPageId = new int[numLeafCentroid];
        for (int i = 0; i < numLeafCentroid; i++) {
            clusterFirstDirPageId[i] = -1;
        }

        LOGGER.log(Level.TRACE,
                "VTreeBulkLoader initialized: numLeafCentroid={}, firstLeafCentroidId={}, numStaticPages={}",
                numLeafCentroid, firstLeafCentroidId, numStaticPages);
    }

    /**
     * Create a directory page confiscated with INVALID_DPID.
     * Directory pages are kept in memory until the cluster is finalized,
     * at which point they receive real page IDs.
     */
    private void createDirectoryPage() throws HyracksDataException {
        currentDirectoryPage = bufferCache.confiscatePage(IBufferCache.INVALID_DPID);
        currentDirectoryFrame.setPage(currentDirectoryPage);
        currentDirectoryFrame.initBuffer((byte) 0);

        LOGGER.log(Level.TRACE, "Created directory page (in-memory) for cluster {}", currentLeafClusterIndex);
    }

    private int extractCentroidId(ITupleReference tuple) {
        return IntegerPointable.getInteger(tuple.getFieldData(1), tuple.getFieldStart(1));
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        sampler.addTuple(tuple);
        int tupleCentroidId = extractCentroidId(tuple);
        if (currentCentroidId == -1) {
            // First tuple being added - initialize for first cluster
            LOGGER.log(Level.TRACE, "Starting bulk load with first centroid cluster: {}", tupleCentroidId);
            currentCentroidId = tupleCentroidId;
            currentLeafClusterIndex = tupleCentroidId - firstLeafCentroidId;
            createDirectoryPage();
            createNewDataPage();
        } else if (currentCentroidId != tupleCentroidId) {
            // Moved to a new centroid cluster
            LOGGER.log(Level.TRACE, "Switching from centroid {} to centroid {}", currentCentroidId, tupleCentroidId);
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
                // Data page full - write it to disk immediately
                finishCurrentDataPage(false);
            }
            ((IVTreeDataFrame) currentDataFrame).insertSorted(tuple);
            entriesInCurrentDataPage++;

            LOGGER.log(Level.TRACE, "Added tuple to leaf cluster {}, data page entries: {}", currentLeafClusterIndex,
                    entriesInCurrentDataPage);
        } catch (HyracksDataException | RuntimeException e) {
            logDataPageState(tuple, e);
            handleException();
            throw e;
        }
    }

    /**
     * Switch to a specific leaf cluster. Finishes the current data page,
     * finalizes the current cluster's directory pages, then starts the new cluster.
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
            finishCurrentDataPage(true);
        }

        // Finalize directory pages for current cluster (assign IDs, chain, write)
        finalizeClusterDirectory();

        // Start new cluster
        currentLeafClusterIndex = targetClusterIndex;
        createDirectoryPage();
        createNewDataPage();

        LOGGER.log(Level.TRACE, "Moved to leaf cluster {} (centroid ID: {})", currentLeafClusterIndex,
                firstLeafCentroidId + currentLeafClusterIndex);
    }

    public int getFirstLeafCentroidId() {
        return firstLeafCentroidId;
    }

    /**
     * Create a new data page with a real page ID.
     * Data pages get real IDs immediately so they can be written to disk right away.
     */
    private void createNewDataPage() throws HyracksDataException {
        currentDataPageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, currentDataPageId);
        currentDataPage = bufferCache.confiscatePage(dpid);
        currentDataFrame.setPage(currentDataPage);
        currentDataFrame.initBuffer((byte) 0);
        entriesInCurrentDataPage = 0;

        LOGGER.log(Level.TRACE, "Created new data page {} for leaf cluster {}", currentDataPageId,
                currentLeafClusterIndex);
    }

    /**
     * Finish the current data page: set the next-page pointer, write to disk immediately,
     * and add a directory entry for it.
     *
     * @param lastPage true if this is the last data page for the current cluster
     */
    private void finishCurrentDataPage(boolean lastPage) throws HyracksDataException {
        int tupleCount = currentDataFrame.getTupleCount();
        if (tupleCount == 0) {
            return;
        }

        double maxDistance = ((IVTreeDataFrame) currentDataFrame).getDistanceToCentroid(tupleCount - 1);
        int writtenDataPageId = currentDataPageId;

        if (lastPage) {
            // Last data page in cluster - no next page
            ((IVTreeDataFrame) currentDataFrame).setNextPage(-1);
            write(currentDataPage);
            currentDataPage = null;
            entriesInCurrentDataPage = 0;
        } else {
            // Allocate next data page ID and set forward pointer before writing
            int nextDataPageId = freePageManager.takePage(metaFrame);
            ((IVTreeDataFrame) currentDataFrame).setNextPage(nextDataPageId);

            // Write current data page to disk immediately
            write(currentDataPage);

            // Create new data page with the pre-allocated ID
            currentDataPageId = nextDataPageId;
            long dpid = BufferedFileHandle.getDiskPageId(fileId, currentDataPageId);
            currentDataPage = bufferCache.confiscatePage(dpid);
            currentDataFrame.setPage(currentDataPage);
            currentDataFrame.initBuffer((byte) 0);
            entriesInCurrentDataPage = 0;

            LOGGER.log(Level.TRACE, "Created new data page {} for leaf cluster {}", currentDataPageId,
                    currentLeafClusterIndex);
        }

        // Add directory entry for the written data page
        addDirectoryEntry(maxDistance, writtenDataPageId);
    }

    /**
     * Add a directory entry <maxDistance, dataPageId> to the current directory page.
     * If the directory page is full, move it to the pending list and create a new overflow.
     */
    private void addDirectoryEntry(double maxDistance, int dataPageId) throws HyracksDataException {
        try {
            ITupleReference directoryEntry =
                    TupleUtils.createTuple(new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                            IntegerSerializerDeserializer.INSTANCE }, maxDistance, dataPageId);

            // Check if directory page has space
            int spaceNeeded = directoryFrameTupleWriter.bytesRequired(directoryEntry) + slotSize;
            int spaceAvailable = currentDirectoryFrame.getTotalFreeSpace();

            if (spaceNeeded > spaceAvailable) {
                // Directory page full - keep in pending list and create overflow
                pendingDirectoryPages.add(currentDirectoryPage);
                createDirectoryPage();

                LOGGER.log(Level.TRACE, "Directory page full for cluster {}, created overflow",
                        currentLeafClusterIndex);
            }

            ((IVTreeFrame) currentDirectoryFrame).insertSorted(directoryEntry);

            LOGGER.log(Level.TRACE, "Added directory entry for data page {} (maxDist={}) to directory, cluster {}",
                    dataPageId, maxDistance, currentLeafClusterIndex);

        } catch (HyracksDataException e) {
            throw e;
        } catch (Exception e) {
            throw new HyracksDataException("Failed to create directory entry", e);
        }
    }

    /**
     * Finalize directory pages for the current cluster:
     * 1. Assign real sequential page IDs to all pending directory pages
     * 2. Set nextPage chain (dir0 -> dir1 -> ... -> -1)
     * 3. Write all directory pages in ascending ID order
     * 4. Record clusterFirstDirPageId for leaf frame pointer assignment
     *
     * Since directory page IDs are allocated after all data pages have been written,
     * the overall write order is: data pages (lower IDs) then directory pages (higher IDs),
     * which naturally maintains strict FIFO ordering.
     */
    private void finalizeClusterDirectory() throws HyracksDataException {
        // Add current directory page to the pending list
        if (currentDirectoryPage != null) {
            pendingDirectoryPages.add(currentDirectoryPage);
            currentDirectoryPage = null;
        }

        if (pendingDirectoryPages.isEmpty()) {
            return;
        }

        // Allocate real page IDs for all directory pages
        int numDirPages = pendingDirectoryPages.size();
        int[] dirPageIds = new int[numDirPages];
        for (int i = 0; i < numDirPages; i++) {
            dirPageIds[i] = freePageManager.takePage(metaFrame);
        }

        // Set disk page IDs, nextPage chain, and write
        for (int i = 0; i < numDirPages; i++) {
            ICachedPage dirPage = pendingDirectoryPages.get(i);

            // Assign real disk page ID
            dirPage.setDiskPageId(BufferedFileHandle.getDiskPageId(fileId, dirPageIds[i]));

            // Set nextPage chain
            currentDirectoryFrame.setPage(dirPage);
            if (i < numDirPages - 1) {
                ((IVTreeMetadataFrame) currentDirectoryFrame).setNextPage(dirPageIds[i + 1]);
            } else {
                ((IVTreeMetadataFrame) currentDirectoryFrame).setNextPage(-1);
            }

            write(dirPage);
        }

        // Record first directory page ID for this cluster
        clusterFirstDirPageId[currentLeafClusterIndex] = dirPageIds[0];
        pendingDirectoryPages.clear();

        LOGGER.log(Level.TRACE, "Finalized directory for cluster {}: {} pages, first dir page = {}",
                currentLeafClusterIndex, numDirPages, dirPageIds[0]);
    }

    private void logDataPageState(ITupleReference tuple, Exception e) {
        try {
            if (currentDataFrame != null) {
                int tupleSize = currentDataFrame.getBytesRequiredToWriteTuple(tuple);
                int spaceNeeded = dataFrameTupleWriter.bytesRequired(tuple) + slotSize;
                int spaceUsed = currentDataFrame.getBuffer().capacity() - currentDataFrame.getTotalFreeSpace();

                LOGGER.log(Level.TRACE,
                        "Data page state - tupleSize: {}, spaceNeeded: {}, spaceUsed: {}, entriesInCurrentDataPage: {}",
                        tupleSize, spaceNeeded, spaceUsed, entriesInCurrentDataPage);
            }
        } catch (Throwable t) {
            e.addSuppressed(t);
        }
    }

    private void handleException() {
        compressedPageWriter.abort();
        // Return pending directory pages (confiscated with INVALID_DPID or real IDs)
        for (ICachedPage page : pendingDirectoryPages) {
            if (page != null && page.confiscated()) {
                bufferCache.returnPage(page, false);
            }
        }
        pendingDirectoryPages.clear();
        if (currentDirectoryPage != null && currentDirectoryPage.confiscated()) {
            bufferCache.returnPage(currentDirectoryPage, false);
            currentDirectoryPage = null;
        }
        if (currentDataPage != null && currentDataPage.confiscated()) {
            bufferCache.returnPage(currentDataPage, false);
            currentDataPage = null;
        }
        freePageManager.returnAllPages();
    }

    @Override
    public void end() throws HyracksDataException {
        // Finish last cluster's remaining data page
        if (currentDataPage != null && entriesInCurrentDataPage > 0) {
            finishCurrentDataPage(true);
        }

        // Finalize last cluster's directory pages
        finalizeClusterDirectory();

        // --- Copy static pages to end of file ---
        int staticBasePageId = freePageManager.takePage(metaFrame);
        // Allocate remaining S-1 pages
        for (int i = 1; i < numStaticPages; i++) {
            freePageManager.takePage(metaFrame);
        }

        // Create frames for pointer adjustment
        IVTreeInteriorFrame intFrame = (IVTreeInteriorFrame) treeIndex.getInteriorFrameFactory().createFrame();
        IVTreeLeafFrame lfFrame = (IVTreeLeafFrame) treeIndex.getLeafFrameFactory().createFrame();

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
            int tupleCount = ((ITreeIndexFrame) intFrame).getTupleCount();

            if (level > 0) {
                // Interior page: offset child pointers by staticBasePageId
                for (int t = 0; t < tupleCount; t++) {
                    int oldChildId = intFrame.getChildPageId(t);
                    intFrame.setChildPageId(t, oldChildId + staticBasePageId);
                }
                // Offset next-page (overflow) pointer if present
                if (intFrame.getOverflowFlagBit()) {
                    intFrame.setNextPage(intFrame.getNextPage() + staticBasePageId);
                }
            } else {
                // Leaf page: set metadata pointers to actual dir page IDs
                // Use centroidId from each tuple to compute correct cluster index,
                // because page-ID order does NOT match centroid BFS order when
                // overflow pages exist (overflow pages have higher IDs than
                // subsequent clusters' main pages).
                ((ITreeIndexFrame) lfFrame).setPage(page);
                int leafTupleCount = ((ITreeIndexFrame) lfFrame).getTupleCount();
                for (int t = 0; t < leafTupleCount; t++) {
                    int centroidId = lfFrame.getCentroidId(t);
                    int clusterIndex = centroidId - firstLeafCentroidId;
                    if (clusterIndex >= 0 && clusterIndex < numLeafCentroid) {
                        lfFrame.setMetadataPagePointer(t, clusterFirstDirPageId[clusterIndex]);
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

        // Set root page and metadata. In bottom-up static structures the root sits at
        // the highest page id; staticStructureRootPage captures that offset within the
        // static page id space, and we translate it into this data component's space
        // by adding staticBasePageId.
        int rootPageId = staticBasePageId + staticStructureRootPage;
        ((VTree) treeIndex).setRootPageId(rootPageId);
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
        LOGGER.log(Level.TRACE, "VTreeBulkLoader aborted");
        handleException();
    }

    @Override
    public void force() throws HyracksDataException {
        bufferCache.force(fileId, false);
    }
}
