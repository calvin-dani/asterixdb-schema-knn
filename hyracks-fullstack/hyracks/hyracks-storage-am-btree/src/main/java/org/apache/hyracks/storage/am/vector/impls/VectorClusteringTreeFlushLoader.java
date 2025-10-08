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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class VectorClusteringTreeFlushLoader extends AbstractTreeIndexBulkLoader {

    public VectorClusteringTreeFlushLoader(float fillFactor, VectorClusteringTree treeIndex,
            IPageWriteCallback callback) throws HyracksDataException {
        super(fillFactor, callback, treeIndex);
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        // Not used - we bulk load entire pages, not individual tuples
        throw new UnsupportedOperationException("Use bulkLoadFromTree() instead");
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

        // WRITE PAGE TO DISK
        write(targetPage);
        System.out.println("DEBUG: Copied page " + targetPageId + " to " + targetPageId);
    }

    /**
     * Simple bulk load: iterate through all pages and write them to disk
     */
    public void bulkLoadFromTree() throws HyracksDataException {
        // Get all page IDs from source tree
        int maxPageId = freePageManager.getMaxPageId(metaFrame);

        // Copy each page from source to target
        for (int id = 0; id <= maxPageId; id++) {
            ICachedPage sourcePage =
                    treeIndex.getBufferCache().pin(BufferedFileHandle.getDiskPageId(treeIndex.getFileId(), id));
            try {
                write(sourcePage);
            } finally {
                treeIndex.getBufferCache().unpin(sourcePage);
            }
        }
    }

    /**
     * Copy a single page from source tree to target tree.
     */
    private void copyPage(int sourcePageId) throws HyracksDataException {
        // Pin source page
        ICachedPage sourcePage =
                treeIndex.getBufferCache().pin(BufferedFileHandle.getDiskPageId(treeIndex.getFileId(), sourcePageId));

        try {
            sourcePage.acquireReadLatch();

            // Get new page in target tree
            int targetPageId = freePageManager.takePage(metaFrame);
            ICachedPage targetPage =
                    bufferCache.confiscatePage(BufferedFileHandle.getDiskPageId(treeIndex.getFileId(), targetPageId));

            try {
                // Copy entire page content
                System.arraycopy(sourcePage.getBuffer().array(), 0, targetPage.getBuffer().array(), 0,
                        sourcePage.getBuffer().capacity());

                // WRITE PAGE TO DISK
                write(targetPage);

                System.out.println("DEBUG: Copied page " + sourcePageId + " to " + targetPageId);

            } finally {
                // targetPage is handled by write() method
            }

        } finally {
            sourcePage.releaseReadLatch();
            treeIndex.getBufferCache().unpin(sourcePage);
        }
    }

    @Override
    public void end() throws HyracksDataException {
        try {
            // Update root page ID in disk component
            super.end();
        } catch (HyracksDataException e) {
            handleException();
            throw e;
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        super.handleException();
    }
}