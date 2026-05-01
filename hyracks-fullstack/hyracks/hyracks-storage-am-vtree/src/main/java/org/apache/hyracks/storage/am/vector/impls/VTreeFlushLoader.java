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
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Flush loader for VTree that writes memory component (VBC) pages
 * to a disk component using identity mapping (VBC page N -> disk page N),
 * then appends the static structure pages at the end with pointer adjustment.
 */
public class VTreeFlushLoader extends PageWriteFailureCallback implements IIndexBulkLoader {

    private static final Logger LOGGER = LogManager.getLogger();

    private final IBufferCache bufferCache;
    private final IPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private final AbstractTreeIndex treeIndex;
    private final int fileId;
    private final IFIFOPageWriter pageWriter;
    private final ICompressedPageWriter compressedPageWriter;

    // Source memory component info (for directory page identification during static structure copy)
    private final VTree sourceMemoryTree;

    public VTreeFlushLoader(IPageWriteCallback callback, VTree diskTree, VTree sourceMemoryTree)
            throws HyracksDataException {
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
                // Leaf page: set metadata pointers to VBC directory page IDs
                // (identity mapping means VBC page IDs = disk page IDs)
                ((ITreeIndexFrame) lfFrame).setPage(page);
                for (int t = 0; t < ((ITreeIndexFrame) lfFrame).getTupleCount(); t++) {
                    if (centroidIndex < numLeafCentroid) {
                        lfFrame.setMetadataPagePointer(t, centroidDirPageMap[centroidIndex]);
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

        return staticBasePageId;
    }

    /**
     * Finalize the flushed disk component with correct metadata.
     */
    public void end(int numLeafCentroid, int firstLeafCentroidId, int rootPageId) throws HyracksDataException {
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
