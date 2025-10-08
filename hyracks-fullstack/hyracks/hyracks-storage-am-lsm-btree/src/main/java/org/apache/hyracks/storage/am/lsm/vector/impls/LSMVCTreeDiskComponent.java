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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.ChainedLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeFlushLoader;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

/**
 * LSM disk component for Vector Clustering Trees.
 * 
 * This class represents a disk-based component in the LSM Vector Clustering Tree structure.
 * Each disk component contains a materialized VectorClusteringTree that has been flushed
 * from memory to persistent storage.
 */
public class LSMVCTreeDiskComponent extends AbstractLSMDiskComponent {

    private final VectorClusteringTree vcTree;

    public LSMVCTreeDiskComponent(AbstractLSMIndex lsmIndex, VectorClusteringTree vcTree, ILSMComponentFilter filter) {
        super(lsmIndex, getMetadataPageManager(vcTree), filter);
        this.vcTree = vcTree;
    }

    @Override
    public VectorClusteringTree getIndex() {
        return vcTree;
    }

    @Override
    public VectorClusteringTree getMetadataHolder() {
        return vcTree;
    }

    @Override
    public long getComponentSize() {
        return getComponentSize(vcTree);
    }

    @Override
    public int getFileReferenceCount() {
        return getFileReferenceCount(vcTree);
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        return getFiles(vcTree);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + vcTree.getFileReference().getRelativePath();
    }

    @Override
    public void validate() throws HyracksDataException {
        vcTree.validate();
    }

    static IMetadataPageManager getMetadataPageManager(VectorClusteringTree vcTree) {
        return (IMetadataPageManager) vcTree.getPageManager();
    }

    static long getComponentSize(VectorClusteringTree vcTree) {
        return vcTree.getFileReference().getFile().length();
    }

    static int getFileReferenceCount(VectorClusteringTree vcTree) {
        return vcTree.getBufferCache().getFileReferenceCount(vcTree.getFileId());
    }

    static Set<String> getFiles(VectorClusteringTree vcTree) {
        Set<String> files = new HashSet<>();
        files.add(vcTree.getFileReference().getFile().getAbsolutePath());
        return files;
    }

    public ILSMDiskComponentBulkLoader createFlushLoader(NCConfig storageConfig, ILSMIOOperation operation,
            boolean cleanupEmptyComponent, IPageWriteCallback callback) throws HyracksDataException {
        LSMVCTreeDiskComponentLoader diskComponentLoader =
                new LSMVCTreeDiskComponentLoader(operation, this, cleanupEmptyComponent);
        diskComponentLoader.setFlushLoader(
                (VectorClusteringTreeFlushLoader) (getIndex().createFlushLoader(0, callback)));
        callback.initialize(diskComponentLoader);
        return diskComponentLoader;
    }
}
