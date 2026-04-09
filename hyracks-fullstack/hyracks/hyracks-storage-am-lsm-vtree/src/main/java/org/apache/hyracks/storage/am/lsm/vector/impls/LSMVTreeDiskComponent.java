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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.ChainedLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexBulkLoader;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.ISampler;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;

/**
 * LSM disk component for Vector Clustering Trees.
 * 
 * This class represents a disk-based component in the LSM Vector Clustering Tree structure.
 * Each disk component contains a materialized VTree that has been flushed
 * from memory to persistent storage.
 */
public class LSMVTreeDiskComponent extends AbstractLSMDiskComponent {

    private final VTree vTree;
    private boolean isStaticStructure = false;

    public LSMVTreeDiskComponent(AbstractLSMIndex lsmIndex, VTree vTree, ILSMComponentFilter filter) {
        super(lsmIndex, getMetadataPageManager(vTree), filter);
        this.vTree = vTree;
    }

    @Override
    public VTree getIndex() {
        return vTree;
    }

    @Override
    public VTree getMetadataHolder() {
        return vTree;
    }

    @Override
    public long getComponentSize() {
        return getComponentSize(vTree);
    }

    @Override
    public int getFileReferenceCount() {
        return getFileReferenceCount(vTree);
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles() {
        return getFiles(vTree);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + vTree.getFileReference().getRelativePath();
    }

    @Override
    public void validate() throws HyracksDataException {
        vTree.validate();
    }

    @Override
    public ISampler getThetaSampler() {
        return super.getThetaSampler();
    }

    /**
     * Creates a bulk loader for VTree with static structure support.
     * 
     * This method follows the LSMBTree pattern by creating a chained bulk loader
     * that can handle both regular bulk loading and static structure loading
     * based on the operation parameters.
     */
    @Override
    public ChainedLSMDiskComponentBulkLoader createBulkLoader(NCConfig storageConfig, ILSMIOOperation operation,
            float fillFactor, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent, IPageWriteCallback callback) throws HyracksDataException {

        ChainedLSMDiskComponentBulkLoader chainedBulkLoader =
                new ChainedLSMDiskComponentBulkLoader(operation, this, cleanupEmptyComponent);

        // Add filter bulk loader if needed
        if (withFilter && getLsmIndex().getFilterFields() != null) {
            chainedBulkLoader.addBulkLoader(createFilterBulkLoader());
        }

        // Check if this is a static structure bulk load operation
        Map<String, Object> parameters = operation.getParameters();
        boolean isStaticStructureLoad = parameters.containsKey("numLevels")
                && parameters.containsKey("clustersPerLevel") && parameters.containsKey("centroidsPerCluster");
        IChainedComponentBulkLoader indexBulkLoader;
        if (isStaticStructureLoad) {
            indexBulkLoader = createVTreeStaticStructureBulkLoader(storageConfig, operation, fillFactor, verifyInput,
                    numElementsHint, checkIfEmptyIndex, callback);
        } else {
            indexBulkLoader = createVTreeBulkLoader(storageConfig, operation, fillFactor, verifyInput, numElementsHint,
                    checkIfEmptyIndex, callback);
        }

        chainedBulkLoader.addBulkLoader(indexBulkLoader);
        callback.initialize(chainedBulkLoader);

        return chainedBulkLoader;
    }

    /**
     * Creates a VTreeStaticStructureLoader with real hierarchical structure parameters.
     * 
     * This method extracts the structure information from the operation parameters
     * and creates a VTreeStaticStructureLoader configured with the actual
     * hierarchical k-means structure.
     */
    private IChainedComponentBulkLoader createVTreeStaticStructureBulkLoader(NCConfig storageConfig,
            ILSMIOOperation operation, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {

        Map<String, Object> parameters = operation.getParameters();

        // Extract structure parameters with defaults
        int numLevels = (Integer) parameters.getOrDefault("numLevels", 2);
        @SuppressWarnings("unchecked")
        List<Integer> clustersPerLevel =
                (List<Integer>) parameters.getOrDefault("clustersPerLevel", Arrays.asList(5, 10));
        @SuppressWarnings("unchecked")
        List<List<Integer>> centroidsPerCluster = (List<List<Integer>>) parameters.getOrDefault("centroidsPerCluster",
                Arrays.asList(Arrays.asList(1, 1, 1, 1, 1), Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1)));
        int maxEntriesPerPage = (Integer) parameters.getOrDefault("maxEntriesPerPage", 100);
        try {
            // Create VTreeStaticStructureLoader with real structure
            IIndexBulkLoader ssbuilder = getIndex().createStaticStructureBulkLoader(numLevels, clustersPerLevel,
                    centroidsPerCluster, maxEntriesPerPage, callback);
            // Wrap VTreeStaticStructureLoader in LSMIndexBulkLoader to implement IChainedComponentBulkLoader
            return new LSMIndexBulkLoader(ssbuilder, getMetadata(), getThetaSampler());

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Creates a VTreeStaticStructureLoader with real hierarchical structure parameters.
     *
     * This method extracts the structure information from the operation parameters
     * and creates a VTreeStaticStructureLoader configured with the actual
     * hierarchical k-means structure.
     */
    private IChainedComponentBulkLoader createVTreeBulkLoader(NCConfig storageConfig, ILSMIOOperation operation,
            float fillFactor, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex,
            IPageWriteCallback callback) throws HyracksDataException {

        Map<String, Object> parameters = operation.getParameters();

        LSMVTreeDiskComponent staticComponent =
                parameters.getOrDefault("static_structure_component", null) instanceof LSMVTreeDiskComponent comp
                        ? comp : null;
        if (staticComponent == null) {
            throw new HyracksDataException(
                    "static_structure_component must be provided in parameters for data loading");
        }
        ITreeIndexAccessor staticAccessor =
                staticComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);

        try {
            IIndexBulkLoader builder =
                    getIndex().createComponentBulkLoader(NoOpPageWriteCallback.INSTANCE, staticAccessor,
                            getThetaSampler());
            return new LSMIndexBulkLoader(builder, getMetadata(), getThetaSampler());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    static IMetadataPageManager getMetadataPageManager(VTree vTree) {
        return (IMetadataPageManager) vTree.getPageManager();
    }

    static long getComponentSize(VTree vTree) {
        return vTree.getFileReference().getFile().length();
    }

    static int getFileReferenceCount(VTree vTree) {
        return vTree.getBufferCache().getFileReferenceCount(vTree.getFileId());
    }

    static Set<String> getFiles(VTree vTree) {
        Set<String> files = new HashSet<>();
        files.add(vTree.getFileReference().getFile().getAbsolutePath());
        return files;
    }

    // createFlushLoader removed - flush logic moved directly to LSMVTree.doFlush()
    // using VTreeFlushLoader directly

    public boolean isStaticStructure() {
        return isStaticStructure;
    }

    public void setStaticStructure(boolean isStaticStructure) {
        this.isStaticStructure = isStaticStructure;
    }

    public void setInitialized() throws HyracksDataException {
        vTree.setStaticStructureInitialized();
        int rootPageId = vTree.getPageManager().getRootPageId();
        vTree.setRootPageId(rootPageId);
    }
}
