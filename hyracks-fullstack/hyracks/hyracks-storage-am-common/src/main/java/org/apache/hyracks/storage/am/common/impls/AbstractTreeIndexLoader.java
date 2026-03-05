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
package org.apache.hyracks.storage.am.common.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.PageWriteFailureCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Abstract base class for creating tree index structures without an index instance.
 * Provides infrastructure for file-based page creation and disk writing.
 */
public abstract class AbstractTreeIndexLoader extends PageWriteFailureCallback {

    protected final IBufferCache bufferCache;
    protected final IIOManager ioManager;
    protected final FileReference targetFile;
    protected final int fileId;
    protected final int pageSize;
    protected final float fillFactor;

    // Page management
    protected final List<ICachedPage> allocatedPages = new ArrayList<>();
    protected final List<Integer> pageIds = new ArrayList<>();
    protected int nextPageId = 0;

    // Frame infrastructure
    protected final ITreeIndexFrame leafFrame;
    protected final ITreeIndexFrame interiorFrame;
    protected final ITreeIndexMetadataFrame metaFrame;
    protected final ITreeIndexTupleWriter tupleWriter;

    // Page writing
    protected final IFIFOPageWriter pageWriter;
    protected final IBufferCacheWriteContext writeContext;

    protected AbstractTreeIndexLoader(IBufferCache bufferCache, IIOManager ioManager, FileReference targetFile,
            int pageSize, float fillFactor, ITreeIndexFrame leafFrame, ITreeIndexFrame interiorFrame,
            ITreeIndexMetadataFrame metaFrame, IPageWriteCallback callback) throws HyracksDataException {
        super();

        this.bufferCache = bufferCache;
        this.ioManager = ioManager;
        this.targetFile = targetFile;
        this.pageSize = pageSize;
        this.fillFactor = fillFactor;
        this.leafFrame = leafFrame;
        this.interiorFrame = interiorFrame;
        this.metaFrame = metaFrame;
        this.tupleWriter = leafFrame.getTupleWriter();

        // Initialize file
        this.fileId = initializeFile();

        // Initialize page writer
        this.writeContext = DefaultBufferCacheWriteContext.INSTANCE;
        this.pageWriter = bufferCache.createFIFOWriter(callback, this, writeContext);

        System.out.println("DEBUG: AbstractTreeIndexLoader initialized");
        System.out.println("DEBUG: Target file: " + targetFile.getAbsolutePath());
        System.out.println("DEBUG: File ID: " + fileId);
        System.out.println("DEBUG: Page size: " + pageSize);
    }

    /**
     * Initialize the target file and return file ID.
     */
    private int initializeFile() throws HyracksDataException {
        try {
            // Create parent directories if needed
            FileReference parentDir = targetFile.getParent();
            if (parentDir != null && !ioManager.exists(parentDir)) {
                ioManager.makeDirectories(parentDir);
            }

            // Create the file
            if (ioManager.exists(targetFile)) {
                ioManager.delete(targetFile);
            }
            ioManager.create(targetFile);

            // Get file ID from buffer cache
            return bufferCache.openFile(targetFile);

        } catch (Exception e) {
            throw new HyracksDataException("Failed to initialize file: " + targetFile.getAbsolutePath(), e);
        }
    }

    /**
     * Allocate a new page and return its ID.
     */
    protected int allocatePage() throws HyracksDataException {
        int pageId = nextPageId++;
        pageIds.add(pageId);

        long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
        ICachedPage page = bufferCache.pin(dpid,
                org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider.NEW);
        allocatedPages.add(page);

        System.out.println("DEBUG: Allocated page " + pageId + " (total: " + pageIds.size() + ")");
        return pageId;
    }

    /**
     * Get a page by ID for writing.
     */
    protected ICachedPage getPage(int pageId) throws HyracksDataException {
        int index = pageIds.indexOf(pageId);
        if (index == -1) {
            throw new HyracksDataException("Page " + pageId + " not found");
        }
        return allocatedPages.get(index);
    }

    /**
     * Create a new page with specified ID and frame type.
     */
    protected ICachedPage createPage(int pageId, boolean isLeaf, int level) throws HyracksDataException {
        ICachedPage page = getPage(pageId);

        try {
            page.acquireWriteLatch();

            ITreeIndexFrame frame = isLeaf ? leafFrame : interiorFrame;
            frame.setPage(page);
            frame.initBuffer((byte) level);

            System.out.println(
                    "DEBUG: Created page " + pageId + " (level " + level + ", " + (isLeaf ? "leaf" : "interior") + ")");

        } catch (Exception e) {
            bufferCache.unpin(page);
            throw new HyracksDataException("Failed to create page " + pageId, e);
        }

        return page;
    }

    /**
     * Write a page to disk.
     */
    protected void writePage(ICachedPage page) throws HyracksDataException {
        try {
            page.releaseWriteLatch(true);
            pageWriter.write(page);
        } catch (Exception e) {
            bufferCache.unpin(page);
            throw new HyracksDataException("Failed to write page", e);
        }
    }

    /**
     * Create tuple reference for the current frame type.
     */
    protected ITreeIndexTupleReference createTupleReference() {
        return leafFrame.createTupleReference();
    }

    /**
     * Force all pages to disk.
     */
    public void force() throws HyracksDataException {
        bufferCache.force(fileId, false);
        System.out.println("DEBUG: Forced " + allocatedPages.size() + " pages to disk");
    }

    /**
     * Close the loader and release resources.
     */
    public void close() throws HyracksDataException {
        try {
            // Write all remaining pages
            for (ICachedPage page : allocatedPages) {
                if (page.confiscated()) {
                    writePage(page);
                }
            }

            // Force to disk
            force();

            // Close file
            bufferCache.closeFile(fileId);

            System.out.println("DEBUG: AbstractTreeIndexLoader closed successfully");

        } catch (Exception e) {
            throw new HyracksDataException("Failed to close loader", e);
        }
    }

    /**
     * Handle cleanup on failure.
     */
    protected void handleException() {
        try {
            // Return all confiscated pages
            for (ICachedPage page : allocatedPages) {
                if (page.confiscated()) {
                    bufferCache.returnPage(page, false);
                }
            }

            // Close file if opened
            if (fileId >= 0) {
                bufferCache.closeFile(fileId);
            }

        } catch (Exception e) {
            System.err.println("ERROR: Failed to cleanup on exception: " + e.getMessage());
        }
    }

    /**
     * Get the file ID.
     */
    public int getFileId() {
        return fileId;
    }

    /**
     * Get the target file reference.
     */
    public FileReference getTargetFile() {
        return targetFile;
    }

    /**
     * Get the number of allocated pages.
     */
    public int getPageCount() {
        return pageIds.size();
    }
}
