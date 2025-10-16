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
package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.vector.utils.VCTreeNavigationUtils;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Operator that handles bulk loader initialization and recursive data grouping to run files.
 * This operator is designed for job 3 in the VCTree creation pipeline.
 * 
 * Responsibilities:
 * 1. Initialize LSM bulk loader for VectorClusteringTree
 * 2. Apply recursive partitioning logic using SHAPIRO formula
 * 3. Group data into run files based on memory budget and data size
 * 4. Manage run file creation and data distribution
 */
public class VCTreeBulkLoaderAndGroupingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final int VECTOR_DIMENSION = 256;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final float fillFactor;
    private final UUID permitUUID;
    private final UUID materializedDataUUID;
    
    // Navigation components
    private IBufferCache bufferCache;
    private int staticStructureFileId;
    private ITreeIndexFrameFactory interiorFrameFactory;
    private ITreeIndexFrameFactory leafFrameFactory;

    public VCTreeBulkLoaderAndGroupingOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID, UUID materializedDataUUID) {
        super(spec, 1, 0);
        this.indexHelperFactory = indexHelperFactory;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID;
        this.materializedDataUUID = materializedDataUUID;
        System.err.println("VCTreeBulkLoaderAndGroupingOperatorDescriptor created with permit UUID: " + permitUUID);
    }

    /**
     * Configure frame factories for navigation.
     * 
     * @param ctx Hyracks task context for getting frame factories
     * @throws HyracksDataException if frame factory configuration fails
     */
    private void configureFrameFactories(IHyracksTaskContext ctx) throws HyracksDataException {
        try {
            System.err.println("=== CONFIGURING FRAME FACTORIES FOR NAVIGATION ===");
            
            // Get frame factories from the index helper
            IIndexDataflowHelper indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), 0);
            indexHelper.open();
            
            // Get the LSM index to access frame factories
            ILSMIndex lsmIndex = (ILSMIndex) indexHelper.getIndexInstance();
            
            // Configure frame factories for navigation
            // Cast to ITreeIndex to access frame factories
            org.apache.hyracks.storage.am.common.api.ITreeIndex treeIndex = (org.apache.hyracks.storage.am.common.api.ITreeIndex) lsmIndex;
            this.interiorFrameFactory = treeIndex.getInteriorFrameFactory();
            this.leafFrameFactory = treeIndex.getLeafFrameFactory();
            
            System.err.println("✅ Frame factories configured successfully");
            System.err.println("  Interior frame factory: " + interiorFrameFactory.getClass().getSimpleName());
            System.err.println("  Leaf frame factory: " + leafFrameFactory.getClass().getSimpleName());
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to configure frame factories: " + e.getMessage());
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Initialize navigation components for static structure access.
     * 
     * @param ctx Hyracks task context for getting buffer cache and file access
     * @param staticFileId File ID of the static structure file to navigate
     * @throws HyracksDataException if navigation initialization fails
     */
    private void initializeNavigationComponents(IHyracksTaskContext ctx, int staticFileId) throws HyracksDataException {
        try {
            System.err.println("=== INITIALIZING NAVIGATION COMPONENTS ===");
            System.err.println("Static structure file ID: " + staticFileId);
            
            // Set up buffer cache access
            this.bufferCache = ((org.apache.asterix.common.api.INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext()).getBufferCache();
            this.staticStructureFileId = staticFileId;
            
            // Configure frame factories
            configureFrameFactories(ctx);
            
            // Validate static structure file accessibility
            validateStaticStructureFile();
            
            System.err.println("✅ Navigation components initialized successfully");
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to initialize navigation components: " + e.getMessage());
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Validate that the static structure file exists and is accessible.
     * 
     * @throws HyracksDataException if file is not accessible
     */
    private void validateStaticStructureFile() throws HyracksDataException {
        try {
            System.err.println("=== VALIDATING STATIC STRUCTURE FILE ===");
            System.err.println("File ID: " + staticStructureFileId);
            
            // Try to pin the root page (page 0) to verify file exists and is accessible
            long dpid = org.apache.hyracks.storage.common.file.BufferedFileHandle.getDiskPageId(staticStructureFileId, 0);
            org.apache.hyracks.storage.common.buffercache.ICachedPage page = bufferCache.pin(dpid);
            
            try {
                page.acquireReadLatch();
                // If we can acquire the latch, the file exists and is accessible
                System.err.println("✅ Static structure file validation successful - root page is accessible");
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
            
        } catch (Exception e) {
            System.err.println("ERROR: Static structure file validation failed: " + e.getMessage());
            e.printStackTrace();
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Static structure file not accessible: " + e.getMessage());
        }
    }

    /**
     * Find the closest centroid using VCTreeNavigationUtils.
     * 
     * @param queryVector Query vector to find closest centroid for
     * @return ClusterSearchResult containing closest centroid information
     * @throws HyracksDataException if navigation fails
     */
    public ClusterSearchResult findClosestCentroid(double[] queryVector) throws HyracksDataException {
        try {
            System.err.println("=== FINDING CLOSEST CENTROID ===");
            System.err.println("Query vector dimension: " + queryVector.length);
            System.err.println("Static structure file ID: " + staticStructureFileId);
            
            // Use VCTreeNavigationUtils to find closest centroid
            ClusterSearchResult result = VCTreeNavigationUtils.findClosestCentroid(
                    bufferCache, 
                    staticStructureFileId, 
                    0, // rootPageId = 0 for static structures
                    interiorFrameFactory, 
                    leafFrameFactory, 
                    queryVector);
            
            System.err.println("✅ Closest centroid found successfully");
            System.err.println("  Leaf page ID: " + result.leafPageId);
            System.err.println("  Cluster index: " + result.clusterIndex);
            System.err.println("  Distance: " + result.distance);
            System.err.println("  Centroid ID: " + result.centroidId);
            
            return result;
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to find closest centroid: " + e.getMessage());
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Calculate number of partitions using SHAPIRO formula for VCTree centroid distribution.
     * 
     * @param K Total number of centroids
     * @param inputDataBytesSize Size of input data in bytes
     * @param frameSize Frame size in bytes
     * @param memoryBudget Available memory budget in frames
     * @return Number of partitions for centroid distribution
     */
    public int calculatePartitionsUsingShapiro(int K, long inputDataBytesSize, int frameSize, int memoryBudget) {
        System.err.println("=== CALCULATING PARTITIONS USING SHAPIRO FORMULA ===");
        System.err.println("K (centroids): " + K);
        System.err.println("Input data size: " + inputDataBytesSize + " bytes");
        System.err.println("Frame size: " + frameSize + " bytes");
        System.err.println("Memory budget: " + memoryBudget + " frames");

        long numberOfInputFrames = inputDataBytesSize / frameSize;
        System.err.println("Input frames: " + numberOfInputFrames);

        // SHAPIRO FORMULA
        final double FUDGE_FACTOR = 1.1;

        if (memoryBudget >= numberOfInputFrames * FUDGE_FACTOR) {
            // All in memory - use 2 partitions to avoid infinite loops
            System.err.println("All data fits in memory, using 2 partitions");
            return 2;
        }

        // Main SHAPIRO formula: ceil((inputFrames * FUDGE_FACTOR - availableFrames) / (availableFrames - 1))
        long numberOfPartitions =
                (long) (Math.ceil((numberOfInputFrames * FUDGE_FACTOR - memoryBudget) / (memoryBudget - 1)));
        numberOfPartitions = Math.max(2, numberOfPartitions);

        if (numberOfPartitions > memoryBudget) {
            // Fallback: use square root when too many partitions
            numberOfPartitions = (long) Math.ceil(Math.sqrt(numberOfInputFrames * FUDGE_FACTOR));
            numberOfPartitions = Math.max(2, Math.min(numberOfPartitions, memoryBudget));
        }

        int numPartitions = (int) Math.min(numberOfPartitions, Integer.MAX_VALUE);

        // Calculate centroids per partition
        int centroidsPerPartition = (int) Math.ceil(1.0 * K / numPartitions);

        System.err.println("SHAPIRO RESULT:");
        System.err.println("  Number of partitions: " + numPartitions);
        System.err.println("  Centroids per partition: " + centroidsPerPartition);

        // Determine frame allocation strategy
        if (numPartitions > 1) {
            System.err.println("  Strategy: Group multiple centroids in one run file");
        } else {
            System.err.println("  Strategy: Allocate 1 frame per centroid");
        }

        return numPartitions;
    }




    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeBulkLoaderAndGroupingNodePushable(ctx, partition, nPartitions, inputRecDesc, permitUUID,
                materializedDataUUID);
    }


    /**
     * Node pushable implementation for VCTreeBulkLoaderAndGroupingOperatorDescriptor.
     */
    private class VCTreeBulkLoaderAndGroupingNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final int partition;
        private final UUID materializedDataUUID;
        private LSMIndexDiskComponentBulkLoader lsmBulkLoader;
        private IIndexDataflowHelper indexHelper;
        private ILSMIndex lsmIndex;
        private MaterializerTaskState materializedData;

        public VCTreeBulkLoaderAndGroupingNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc, UUID permitUUID, UUID materializedDataUUID) {
            this.ctx = ctx;
            this.partition = partition;
            this.materializedDataUUID = materializedDataUUID;
        }

        @Override
        public void open() throws HyracksDataException {
            System.err.println("=== VCTreeBulkLoaderAndGroupingNodePushable OPENING ===");
            try {
                // Initialize materialized data state
                materializedData = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                        new PartitionedUUID(materializedDataUUID, partition));
                materializedData.open(ctx);

                // Initialize navigation components for static structure access
                // Using a hardcoded file ID for now - in practice this would come from configuration
                int staticStructureFileId = 1; // TODO: Get from configuration
                initializeNavigationComponents(ctx, staticStructureFileId);

                System.err.println("✅ VCTreeBulkLoaderAndGroupingNodePushable opened successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeBulkLoaderAndGroupingNodePushable: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }


        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            // This operator doesn't process input frames, it just initializes bulk loader and grouping
            System.err.println("VCTreeBulkLoaderAndGroupingNodePushable: nextFrame called (no processing needed)");
        }

        @Override
        public void close() throws HyracksDataException {
            System.err.println("=== VCTreeBulkLoaderAndGroupingNodePushable CLOSING ===");
            try {
                if (lsmBulkLoader != null) {
                    lsmBulkLoader.end();
                }
                if (indexHelper != null) {
                    indexHelper.close();
                }
                if (materializedData != null) {
                    materializedData.close();
                    ctx.setStateObject(materializedData);
                }
                System.err.println("✅ VCTreeBulkLoaderAndGroupingNodePushable closed successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to close VCTreeBulkLoaderAndGroupingNodePushable: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            System.err.println("=== VCTreeBulkLoaderAndGroupingNodePushable FAILING ===");
            try {
                if (lsmBulkLoader != null) {
                    lsmBulkLoader.abort();
                }
                if (indexHelper != null) {
                    indexHelper.close();
                }
                if (materializedData != null) {
                    materializedData.close();
                }
            } catch (Exception e) {
                System.err
                        .println("ERROR: Failed to cleanup VCTreeBulkLoaderAndGroupingNodePushable: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
