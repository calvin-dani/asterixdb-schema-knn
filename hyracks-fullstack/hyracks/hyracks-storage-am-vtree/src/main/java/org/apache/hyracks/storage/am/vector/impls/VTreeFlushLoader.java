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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
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
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Flush loader for VTree that writes memory component (VBC) pages to a disk component,
 * then appends the static structure pages at the end.
 * <p>
 * The identity mapping (VBC page N -&gt; disk page N) applies only to {@link #copyPage} — the
 * data/directory pages copied verbatim. {@link #copyStaticStructure} is NOT identity: it appends
 * the static pages at an offset and rewrites their pointers accordingly.
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
     * <p>
     * Bounded memory: each source page is pinned, copied into one confiscated destination page,
     * patched, written, and released before the next page is touched — no up-front snapshot of
     * the whole structure and no simultaneous confiscation of every destination page.
     *
     * @param staticAccessor accessor to the static structure disk component
     * @return the root page ID of the flushed component: staticBasePageId offset by the static
     *         tree's own root page id (bottom-up layout puts leaves first and the root at the
     *         highest static page id, so the first copied page is a LEAF, not the root)
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Fix: flush recorded first static (leaf) page as component root")
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Bounded-memory static-structure copy (review feedback): one source/destination page pair at a time instead of a full byte[] snapshot plus all-pages confiscation")
    public int copyStaticStructure(VTree.VTreeAccessor staticAccessor) throws HyracksDataException {

        VTree staticTree = staticAccessor.getIndex();
        ITreeIndexMetadataFrame staticMeta = staticAccessor.getOpContext().getMetaFrame();
        int maxStaticPageId = staticTree.getPageManager().getMaxPageId(staticMeta);
        int numStaticPages = maxStaticPageId + 1;

        // Allocate disk pages for static structure. Destination page ids are deterministic:
        // sequential from staticBasePageId, in source page-id order.
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

        // Copy one source/destination page pair at a time: pin source, copy into the confiscated
        // destination page, release the source, patch pointers, write, then move on.
        for (int i = 0; i < numStaticPages; i++) {
            int newPageId = staticBasePageId + i;
            long dpid = BufferedFileHandle.getDiskPageId(fileId, newPageId);
            ICachedPage page = bufferCache.confiscatePage(dpid);

            ICachedPage sourcePage = staticAccessor.getCachedPage(i);
            try {
                System.arraycopy(sourcePage.getBuffer().array(), 0, page.getBuffer().array(), 0,
                        sourcePage.getBuffer().capacity());
            } finally {
                staticAccessor.releasePage(sourcePage);
            }

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

            write(page);
        }

        // The static structure's root sits at its own root page id within the copied range
        // (mirrors VTreeBulkLoader#end: rootPageId = staticBasePageId + staticStructureRootPage).
        // Returning staticBasePageId itself would persist the first copied LEAF page as root.
        return staticBasePageId + staticTree.getRootPageId();
    }

    /**
     * Shift each resolved graph-neighbor pointer on the given leaf page (already set on {@code lfFrame})
     * by {@code staticBasePageId}, translating scaffold-space page ids into this flushed component's
     * space. The slot is unchanged (pages are copied byte-for-byte). Leaf layouts without a neighbor
     * field, and any still-provisional entry, are left untouched.
     */
    private void offsetLeafNeighborPointers(IVTreeLeafFrame lfFrame, int staticBasePageId) throws HyracksDataException {
        VTreeLeafNeighborList.forEachLeafNeighborEntry(lfFrame, (fieldData, contentStart, e) -> {
            if (!VTreeLeafNeighborList.isResolved(fieldData, contentStart, e)) {
                return;
            }
            int pageId = VTreeLeafNeighborList.readPageId(fieldData, contentStart, e);
            int slot = VTreeLeafNeighborList.readSlot(fieldData, contentStart, e);
            VTreeLeafNeighborList.writeResolved(fieldData, contentStart, e, pageId + staticBasePageId, slot);
        });
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
