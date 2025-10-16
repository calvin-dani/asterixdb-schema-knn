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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
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
import org.apache.hyracks.storage.am.vector.impls.*;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexBulkLoader;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeFlushLoader;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;

/**
 * LSM disk component for Vector Clustering Trees.
 * 
 * This class represents a disk-based component in the LSM Vector Clustering Tree structure.
 * Each disk component contains a materialized VectorClusteringTree that has been flushed
 * from memory to persistent storage.
 */
public class LSMVCTreeDiskComponent extends AbstractLSMDiskComponent {

    private final VectorClusteringTree vcTree;
    private boolean isStaticStructure = false;

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

    /**
     * Creates a bulk loader for VCTree with static structure support.
     * 
     * This method follows the LSMBTree pattern by creating a chained bulk loader
     * that can handle both regular bulk loading and static structure loading
     * based on the operation parameters.
     */
    @Override
    public ChainedLSMDiskComponentBulkLoader createBulkLoader(NCConfig storageConfig, ILSMIOOperation operation,
            float fillFactor, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent, IPageWriteCallback callback) throws HyracksDataException {

        System.err.println("=== LSMVCTreeDiskComponent.createBulkLoader ===");
        System.err.println("Operation type: " + operation.getIOOperationType());
        System.err.println("Parameters: " + operation.getParameters());

        ChainedLSMDiskComponentBulkLoader chainedBulkLoader =
                new ChainedLSMDiskComponentBulkLoader(operation, this, cleanupEmptyComponent);

        // Add filter bulk loader if needed
        if (withFilter && getLsmIndex().getFilterFields() != null) {
            System.err.println("Adding filter bulk loader");
            chainedBulkLoader.addBulkLoader(createFilterBulkLoader());
        }

        // Check if this is a static structure bulk load operation
        Map<String, Object> parameters = operation.getParameters();
        boolean isStaticStructureLoad = parameters.containsKey("numLevels")
                && parameters.containsKey("clustersPerLevel") && parameters.containsKey("centroidsPerCluster");

        IChainedComponentBulkLoader indexBulkLoader;
        if (isStaticStructureLoad) {
            System.err.println("Creating VCTreeStaticStructureLoader with real structure parameters");
            indexBulkLoader = createVCTreeStaticStructureBulkLoader(storageConfig, operation, fillFactor, verifyInput,
                    numElementsHint, checkIfEmptyIndex, callback);
        } else {
            System.err.println("ERROR: Static structure parameters not found - VCTree requires hierarchical structure");
            System.err.println("Expected parameters: numLevels, clustersPerLevel, centroidsPerCluster");
            System.err.println("Available parameters: " + parameters.keySet());
            throw new HyracksDataException("VCTreeStaticStructureLoader requires hierarchical structure parameters");
        }

        chainedBulkLoader.addBulkLoader(indexBulkLoader);
        callback.initialize(chainedBulkLoader);

        System.err.println("VCTree bulk loader created successfully");
        return chainedBulkLoader;
    }

    /**
     * Creates a VCTreeStaticStructureLoader with real hierarchical structure parameters.
     * 
     * This method extracts the structure information from the operation parameters
     * and creates a VCTreeStaticStructureLoader configured with the actual
     * hierarchical k-means structure.
     */
    private IChainedComponentBulkLoader createVCTreeStaticStructureBulkLoader(NCConfig storageConfig,
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

        System.err.println("VCTreeStaticStructureLoader parameters:");
        System.err.println("  - numLevels: " + numLevels);
        System.err.println("  - clustersPerLevel: " + clustersPerLevel);
        System.err.println("  - centroidsPerCluster: " + centroidsPerCluster);
        System.err.println("  - maxEntriesPerPage: " + maxEntriesPerPage);
        System.err.println("  - fillFactor: " + fillFactor);

        try {
            // Create VCTreeStaticStructureLoader with real structure
            VCTreeLoader staticLoader = new VCTreeLoader(fillFactor, callback, vcTree,
                    vcTree.getLeafFrameFactory().createFrame(), vcTree.getDataFrameFactory().createFrame(),
                    DefaultBufferCacheWriteContext.INSTANCE, numLevels,
                    clustersPerLevel, centroidsPerCluster, maxEntriesPerPage);

            System.err.println("VCTreeStaticStructureLoader created successfully with real structure");

            // Wrap VCTreeStaticStructureLoader in LSMIndexBulkLoader to implement IChainedComponentBulkLoader
            LSMIndexBulkLoader wrappedLoader = new LSMIndexBulkLoader(staticLoader);
            return wrappedLoader;

        } catch (Exception e) {
            System.err.println("ERROR: Failed to create VCTreeStaticStructureLoader: " + e.getMessage());
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
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
        diskComponentLoader
                .setFlushLoader((VectorClusteringTreeFlushLoader) (getIndex().createFlushLoader(0, callback)));
        callback.initialize(diskComponentLoader);
        return diskComponentLoader;
    }

    /* TODO - Hongyu: simplified version */
    public VCTreeStaticStructureBuilder createStaticStructureBuilder(NCConfig storageConfig, int numLevels,
            List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage,
            NoOpPageWriteCallback instance) throws HyracksDataException {
        return getIndex().createStaticStructureBuilder(numLevels, clustersPerLevel, centroidsPerCluster,
                maxEntriesPerPage, instance);
    }

    public VCTreeBulkLoder createBulkLoader(int numLeafCentroid, int firstLeafCentroidId,
            ISerializerDeserializer[] dataFrameSerdes, IPageWriteCallback callback) throws HyracksDataException {
        return getIndex().createBulkLoader((NoOpPageWriteCallback) callback, numLeafCentroid, firstLeafCentroidId,
                dataFrameSerdes);
    }

    public boolean isStaticStructure() {
        return  isStaticStructure;
    }

    public void setStaticStructure(boolean isStaticStructure) {
        this.isStaticStructure = isStaticStructure;
    }
}
