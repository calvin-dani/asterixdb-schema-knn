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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.VTreeStaticTupleConstants;
import org.apache.hyracks.storage.am.vector.utils.VTreeLeafNeighborList;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataKeys;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Flush loader for VTree that writes memory component (VBC) pages
 * to a disk component using identity mapping (VBC page N -> disk page N),
 * then appends the static structure pages at the end with pointer adjustment.
 */
public class VTreeFlushLoader extends PageWriteFailureCallback implements IIndexBulkLoader {

    private final IBufferCache bufferCache;
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private final AbstractTreeIndex treeIndex;
    private final int fileId;
    private final IFIFOPageWriter pageWriter;
    private final ICompressedPageWriter compressedPageWriter;

    // Source memory component info (for directory page identification during static structure copy)
    private final VTree sourceMemoryTree;

    public VTreeFlushLoader(IPageWriteCallback callback, VTree diskTree, VTree sourceMemoryTree) {
        this.bufferCache = diskTree.getBufferCache();
        this.freePageManager = diskTree.getPageManager();
        this.metaFrame = freePageManager.createMetadataFrame();
        this.fileId = diskTree.getFileId();
        this.treeIndex = diskTree;
        this.sourceMemoryTree = sourceMemoryTree;
        this.pageWriter = bufferCache.createFIFOWriter(callback, this, DefaultBufferCacheWriteContext.INSTANCE);
        this.compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException("Use copyPage() instead");
    }

    /**
     * Copy a VBC page to disk with identity mapping (VBC page N -> disk page N).
     */
    public void copyPage(ICachedPage sourcePage) throws HyracksDataException {
        int diskPageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, diskPageId);
        ICachedPage targetPage = bufferCache.confiscatePage(dpid);
        System.arraycopy(sourcePage.getBuffer().array(), 0, targetPage.getBuffer().array(), 0,
                sourcePage.getBuffer().capacity());
        write(targetPage);
    }

    /**
     * Copy static structure pages to end of file with pointer adjustment.
     * Interior child pointers are offset by staticBasePageId.
     * Leaf metadata pointers are set from the source memory tree's centroidDirPageMap
     * (identity mapping: VBC page IDs = disk page IDs).
     * Leaf next-page pointers are offset by staticBasePageId.
     *
     * @param staticAccessor accessor to the static structure disk component
     * @return the root page ID (staticBasePageId) for the flushed component
     */
    public int copyStaticStructure(VTree.VTreeAccessor staticAccessor) throws HyracksDataException {

        VTree staticTree = staticAccessor.getIndex();
        ITreeIndexMetadataFrame staticMeta = staticAccessor.getOpContext().getMetaFrame();
        int maxStaticPageId = staticTree.getPageManager().getMaxPageId(staticMeta);
        int numStaticPages = maxStaticPageId + 1;

        // Save static page contents as byte arrays
        List<byte[]> staticPageContents = new ArrayList<>();
        for (int pageId = 0; pageId <= maxStaticPageId; pageId++) {
            ICachedPage sourcePage = staticAccessor.getCachedPage(pageId);
            byte[] content = new byte[sourcePage.getBuffer().capacity()];
            System.arraycopy(sourcePage.getBuffer().array(), 0, content, 0, content.length);
            staticPageContents.add(content);
            staticAccessor.releasePage(sourcePage);
        }

        // Allocate disk pages for static structure
        int staticBasePageId = freePageManager.takePage(metaFrame);
        for (int i = 1; i < numStaticPages; i++) {
            freePageManager.takePage(metaFrame);
        }

        // Create frames for pointer adjustment
        IVTreeInteriorFrame intFrame = (IVTreeInteriorFrame) treeIndex.getInteriorFrameFactory().createFrame();
        IVTreeLeafFrame lfFrame = (IVTreeLeafFrame) treeIndex.getLeafFrameFactory().createFrame();

        int[] centroidDirPageMap = sourceMemoryTree.getCentroidDirPageMap();
        int numLeafCentroid = sourceMemoryTree.getNumLeafCentroidMem();
        int firstLeafCid = sourceMemoryTree.getFirstLeafCentroidIdMem();
        TreeMap<Integer, ICachedPage> staticPages = new TreeMap<>();

        for (int i = 0; i < numStaticPages; i++) {
            int newPageId = staticBasePageId + i;
            long dpid = BufferedFileHandle.getDiskPageId(fileId, newPageId);
            ICachedPage page = bufferCache.confiscatePage(dpid);

            // Copy content from saved byte array
            System.arraycopy(staticPageContents.get(i), 0, page.getBuffer().array(), 0,
                    staticPageContents.get(i).length);

            // Determine page type via level field and adjust pointers
            intFrame.setPage(page);
            byte level = intFrame.getLevel();

            if (level > 0) {
                // Interior page: offset child pointers by staticBasePageId
                for (int t = 0; t < intFrame.getTupleCount(); t++) {
                    int oldChildId = intFrame.getChildPageId(t);
                    intFrame.setChildPageId(t, oldChildId + staticBasePageId);
                }
                // Offset next-page (overflow) pointer if present
                if (intFrame.getOverflowFlagBit()) {
                    intFrame.setNextPage(intFrame.getNextPage() + staticBasePageId);
                }
            } else {
                // Leaf page: set metadata pointers to VBC directory page IDs
                // (identity mapping means VBC page IDs = disk page IDs). Index
                // centroidDirPageMap by the slot's centroid_id (cid - firstLeafCid), not by
                // traversal order, since physical page-id order need not match the nextLeaf chain.
                lfFrame.setPage(page);
                for (int t = 0; t < lfFrame.getTupleCount(); t++) {
                    int cid = lfFrame.getCentroidId(t);
                    int idx = cid - firstLeafCid;
                    if (idx >= 0 && idx < numLeafCentroid) {
                        lfFrame.setMetadataPagePointer(t, centroidDirPageMap[idx]);
                    }
                }
                // Offset next-leaf pointer (overflow or sibling chain)
                int oldNextLeaf = lfFrame.getNextLeaf();
                if (oldNextLeaf >= 0) {
                    lfFrame.setNextLeaf(oldNextLeaf + staticBasePageId);
                }
                // Offset resolved graph-neighbor pointers by staticBasePageId (slot unchanged), the same
                // shift applied to next-leaf/metadata pointers — the scaffold carries resolved pointers.
                offsetLeafNeighborPointers(lfFrame, staticBasePageId);
            }

            staticPages.put(newPageId, page);
        }

        // Write all static pages sequentially
        for (Map.Entry<Integer, ICachedPage> entry : staticPages.entrySet()) {
            write(entry.getValue());
        }

        return staticBasePageId;
    }

    /**
     * Shift each resolved graph-neighbor pointer on the given leaf page (already set on {@code lfFrame})
     * by {@code staticBasePageId}, translating scaffold-space page ids into this flushed component's
     * space. The slot is unchanged (pages are copied byte-for-byte). Leaf layouts without a neighbor
     * field, and any still-provisional entry, are left untouched.
     */
    private void offsetLeafNeighborPointers(IVTreeLeafFrame lfFrame, int staticBasePageId) throws HyracksDataException {
        int tupleCount = lfFrame.getTupleCount();
        ITreeIndexTupleReference frameTuple = lfFrame.createTupleReference();
        for (int t = 0; t < tupleCount; t++) {
            frameTuple.resetByTupleIndex(lfFrame, t);
            if (frameTuple.getFieldCount() <= VTreeStaticTupleConstants.LEAF_NEIGHBOR_LIST_FIELD + 1) {
                continue;
            }
            byte[] fieldData = frameTuple.getFieldData(VTreeStaticTupleConstants.LEAF_NEIGHBOR_LIST_FIELD);
            int fieldStart = frameTuple.getFieldStart(VTreeStaticTupleConstants.LEAF_NEIGHBOR_LIST_FIELD);
            int contentLen = ByteArrayPointable.getContentLength(fieldData, fieldStart);
            int contentStart = fieldStart + ByteArrayPointable.getNumberBytesToStoreMeta(contentLen);
            int numEntries = contentLen / VTreeLeafNeighborList.ENTRY_SIZE;
            for (int e = 0; e < numEntries; e++) {
                if (!VTreeLeafNeighborList.isResolved(fieldData, contentStart, e)) {
                    continue;
                }
                int pageId = VTreeLeafNeighborList.readPageId(fieldData, contentStart, e);
                int slot = VTreeLeafNeighborList.readSlot(fieldData, contentStart, e);
                VTreeLeafNeighborList.writeResolved(fieldData, contentStart, e, pageId + staticBasePageId, slot);
            }
        }
    }

    /**
     * Finalize the flushed disk component with correct metadata.
     */
    public void end(int numLeafCentroid, int firstLeafCentroidId, int rootPageId) throws HyracksDataException {
        ((VTree) treeIndex).setRootPageId(rootPageId);
        freePageManager.setRootPageId(rootPageId);

        metaFrame.put(new MutableArrayValueReference(VTreeMetadataKeys.NUM_LEAF_CENTROIDS.getBytes()),
                LongPointable.FACTORY.createPointable(numLeafCentroid));
        metaFrame.put(new MutableArrayValueReference(VTreeMetadataKeys.FIRST_LEAF_CENTROID_ID.getBytes()),
                LongPointable.FACTORY.createPointable(firstLeafCentroidId));

        if (hasFailed()) {
            throw HyracksDataException.create(getFailure());
        }
    }

    @Override
    public void end() throws HyracksDataException {
        throw new UnsupportedOperationException("Use end(numLeafCentroid, firstLeafCentroidId, rootPageId) instead");
    }

    @Override
    public void abort() throws HyracksDataException {
        compressedPageWriter.abort();
        freePageManager.returnAllPages();
    }

    @Override
    public void force() throws HyracksDataException {
        bufferCache.force(fileId, false);
    }

    private void write(ICachedPage cPage) throws HyracksDataException {
        compressedPageWriter.prepareWrite(cPage);
        pageWriter.write(cPage);
    }
}
