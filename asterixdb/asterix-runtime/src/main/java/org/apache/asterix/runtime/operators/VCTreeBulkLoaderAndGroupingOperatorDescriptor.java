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

import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.operators.KMeansUtils;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
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
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.LocalResource;

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
    private final float fillFactor; // TODO: Use fillFactor in future bulk loading operations
    private final UUID permitUUID;
    private final UUID materializedDataUUID;
    private final IScalarEvaluatorFactory args;
    private final RecordDescriptor inputRecDesc;
    
    // Navigation components
    private IBufferCache bufferCache;
    private int staticStructureFileId;
    private ITreeIndexFrameFactory interiorFrameFactory;
    private ITreeIndexFrameFactory leafFrameFactory;

    public VCTreeBulkLoaderAndGroupingOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID, UUID materializedDataUUID,
            IScalarEvaluatorFactory args) {
        super(spec, 1, 0);
        this.indexHelperFactory = indexHelperFactory;
        this.fillFactor = fillFactor;
        this.inputRecDesc = inputRecordDescriptor;
        this.permitUUID = permitUUID;
        this.materializedDataUUID = materializedDataUUID;
        this.args = args;
        System.err.println("VCTreeBulkLoaderAndGroupingOperatorDescriptor created with permit UUID: " + permitUUID);
    }

    /**
     * Configure frame factories for VCTree navigation.
     * Creates the correct VCTree frame factories that implement IVectorClusteringLeafFrame
     * and IVectorClusteringInteriorFrame interfaces required by VCTreeNavigationUtils.
     * 
     * @param ctx Hyracks task context for getting frame factories
     * @throws HyracksDataException if frame factory configuration fails
     */
    private void configureFrameFactories(IHyracksTaskContext ctx) throws HyracksDataException {
        try {
            System.err.println("=== CONFIGURING VCTREE FRAME FACTORIES FOR NAVIGATION ===");
            
            // Create tuple writers with proper type traits for VCTree
            // Tuple format: [centroidId (int), embedding (float[]), childPageId (int)]
            ITypeTraits[] typeTraits = new ITypeTraits[3];
            typeTraits[0] = IntegerPointable.TYPE_TRAITS; // centroidId
            typeTraits[1] = VarLengthTypeTrait.INSTANCE; // embedding (float array) - variable length
            typeTraits[2] = IntegerPointable.TYPE_TRAITS; // childPageId

            VectorClusteringLeafTupleWriterFactory leafTupleWriterFactory =
                    new VectorClusteringLeafTupleWriterFactory(typeTraits, null, null);
            VectorClusteringInteriorTupleWriterFactory interiorTupleWriterFactory =
                    new VectorClusteringInteriorTupleWriterFactory(typeTraits, null, null);

            // Create VCTree frame factories (these implement the correct interfaces)
            this.leafFrameFactory = new VectorClusteringLeafFrameFactory(
                    leafTupleWriterFactory.createTupleWriter(), VECTOR_DIMENSION);
            this.interiorFrameFactory = new VectorClusteringInteriorFrameFactory(
                    interiorTupleWriterFactory.createTupleWriter(), VECTOR_DIMENSION);
            
            System.err.println("✅ VCTree frame factories configured successfully");
            System.err.println("  Interior frame factory: " + interiorFrameFactory.getClass().getSimpleName());
            System.err.println("  Leaf frame factory: " + leafFrameFactory.getClass().getSimpleName());
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to configure VCTree frame factories: " + e.getMessage());
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
            
            // Validate input vector
            if (queryVector == null) {
                throw new IllegalArgumentException("Query vector cannot be null");
            }
            
            if (queryVector.length == 0) {
                throw new IllegalArgumentException("Query vector cannot be empty");
            }
            
            // Validate vector dimensions
            if (queryVector.length != VECTOR_DIMENSION) {
                System.err.println("WARNING: Query vector dimension (" + queryVector.length + 
                                 ") does not match expected dimension (" + VECTOR_DIMENSION + ")");
                // Continue processing but log the mismatch
            }
            
            System.err.println("Query vector dimension: " + queryVector.length);
            System.err.println("Static structure file ID: " + staticStructureFileId);
            
            // Validate navigation components are initialized
            if (bufferCache == null) {
                throw new IllegalStateException("Buffer cache not initialized");
            }
            
            if (interiorFrameFactory == null || leafFrameFactory == null) {
                throw new IllegalStateException("Frame factories not initialized");
            }
            
            // Use VCTreeNavigationUtils to find closest centroid
            ClusterSearchResult result = VCTreeNavigationUtils.findClosestCentroid(
                    bufferCache, 
                    staticStructureFileId, 
                    0, // rootPageId = 0 for static structures
                    interiorFrameFactory, 
                    leafFrameFactory, 
                    queryVector);
            
            if (result == null) {
                System.err.println("WARNING: No closest centroid found for query vector");
                return null;
            }
            
            System.err.println("✅ Closest centroid found successfully");
            System.err.println("  Leaf page ID: " + result.leafPageId);
            System.err.println("  Cluster index: " + result.clusterIndex);
            System.err.println("  Distance: " + result.distance);
            System.err.println("  Centroid ID: " + result.centroidId);
            
            return result;
            
        } catch (IllegalArgumentException | IllegalStateException e) {
            System.err.println("ERROR: Invalid input or state for closest centroid search: " + e.getMessage());
            throw e;
        } catch (Exception e) {
            System.err.println("ERROR: Failed to find closest centroid: " + e.getMessage());
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Extract embedding from input tuple using IScalarEvaluator and KMeansUtils.
     * This method follows the same pattern as HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.
     * 
     * @param tuple Input tuple containing vector data
     * @param ctx Hyracks task context for evaluator creation
     * @return Extracted double array embedding
     * @throws HyracksDataException if extraction fails
     */
    public double[] extractEmbeddingFromTuple(ITupleReference tuple, IHyracksTaskContext ctx) throws HyracksDataException {
        try {
            System.err.println("=== EXTRACTING EMBEDDING FROM TUPLE ===");
            
            // Validate input parameters
            if (tuple == null) {
                throw new IllegalArgumentException("Tuple cannot be null");
            }
            
            if (ctx == null) {
                throw new IllegalArgumentException("Context cannot be null");
            }
            
            if (args == null) {
                throw new IllegalStateException("Scalar evaluator factory not initialized");
            }
            
            // Create evaluator for extracting vector data
            IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
            IPointable inputVal = new VoidPointable();
            
            // Create KMeansUtils for proper vector parsing
            KMeansUtils kMeansUtils = new KMeansUtils(new VoidPointable(), new ArrayBackedValueStorage());
            ListAccessor listAccessorConstant = new ListAccessor();
            
            // Extract vector data from tuple
            // Cast ITupleReference to IFrameTupleReference for evaluator
            eval.evaluate((org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference) tuple, inputVal);
            
            // Validate evaluation result
            if (inputVal.getLength() == 0) {
                System.err.println("WARNING: Empty evaluation result from tuple");
                return null;
            }
            
            // Check if it's a list type (required for vector data)
            if (!EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                    .isListType()) {
                System.err.println("WARNING: Tuple does not contain list type data, skipping");
                return null;
            }
            
            // Parse the vector data using proper AsterixDB parsing
            listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
            double[] embedding = kMeansUtils.createPrimitveList(listAccessorConstant);
            
            // Validate extracted embedding
            if (embedding == null) {
                System.err.println("WARNING: KMeansUtils returned null embedding");
                return null;
            }
            
            if (embedding.length == 0) {
                System.err.println("WARNING: Extracted embedding is empty");
                return null;
            }
            
            // Validate embedding dimensions
            if (embedding.length != VECTOR_DIMENSION) {
                System.err.println("WARNING: Extracted embedding dimension (" + embedding.length + 
                                 ") does not match expected dimension (" + VECTOR_DIMENSION + ")");
                // Continue processing but log the mismatch
            }
            
            System.err.println("✅ Successfully extracted embedding with " + embedding.length + " dimensions");
            return embedding;
            
        } catch (IllegalArgumentException | IllegalStateException e) {
            System.err.println("ERROR: Invalid input or state for embedding extraction: " + e.getMessage());
            throw e;
        } catch (Exception e) {
            System.err.println("ERROR: Failed to extract embedding from tuple: " + e.getMessage());
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
        private ILSMIndex lsmIndex; // TODO: Use lsmIndex in future bulk loading operations
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
                // Get the correct file ID by opening the .static_structure_vctree file
                int staticStructureFileId = openStaticStructureFile(ctx);
                initializeNavigationComponents(ctx, staticStructureFileId);

                System.err.println("✅ VCTreeBulkLoaderAndGroupingNodePushable opened successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeBulkLoaderAndGroupingNodePushable: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Open the .static_structure_vctree file and return its file ID.
         * This follows the same approach as VCTreeStaticStructureCreatorOperatorDescriptor.
         * 
         * @param ctx Hyracks task context for getting buffer cache and file access
         * @return File ID of the opened static structure file
         * @throws HyracksDataException if file cannot be opened
         */
        private int openStaticStructureFile(IHyracksTaskContext ctx) throws HyracksDataException {
            try {
                System.err.println("=== OPENING STATIC STRUCTURE FILE ===");
                
                // Get buffer cache
                IBufferCache bufferCache = ((org.apache.asterix.common.api.INcApplicationContext) 
                    ctx.getJobletContext().getServiceContext().getApplicationContext()).getBufferCache();
                
                // Get index path (same approach as VCTreeStaticStructureCreatorOperatorDescriptor)
                FileReference indexPathRef = getIndexFilePath();
                if (indexPathRef == null) {
                    throw new HyracksDataException("Could not determine index path");
                }
                System.err.println("Index path: " + indexPathRef);

                // Create static structure file path
                FileReference staticStructureFile = indexPathRef.getChild(".static_structure_vctree");
                System.err.println("Static structure file path: " + staticStructureFile);

                // Open the static structure file
                System.err.println("Opening static structure file...");
                int fileId;
                try {
                    // Check if file exists in the file system
                    IIOManager ioManager = ctx.getIoManager();
                    if (ioManager.exists(staticStructureFile)) {
                        System.err.println("Static structure file exists, opening it...");
                        fileId = bufferCache.openFile(staticStructureFile);
                    } else {
                        throw new HyracksDataException("Static structure file does not exist: " + staticStructureFile);
                    }
                    System.err.println("Static structure file opened with ID: " + fileId);
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to open static structure file: " + e.getMessage());
                    throw HyracksDataException.create(e);
                }

                return fileId;
                
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open static structure file: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Get the index file path for accessing the static structure file.
         * This follows the same approach as VCTreeStaticStructureCreatorOperatorDescriptor.
         * 
         * @return FileReference to the index directory
         */
        private FileReference getIndexFilePath() {
            try {
                // Get the LSM index file manager to determine the correct component directory
                IIndexDataflowHelper indexHelper = indexHelperFactory.create(
                    ctx.getJobletContext().getServiceContext(), partition);
                
                // Get the resource path from the index helper
                LocalResource resource = indexHelper.getResource();
                String resourcePath = resource.getPath();

                // Resolve the file reference using the IO manager
                IIOManager ioManager = ctx.getIoManager();
                return ioManager.resolve(resourcePath);
                
            } catch (Exception e) {
                System.err.println("ERROR: Failed to get index file path: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            System.err.println("=== VCTreeBulkLoaderAndGroupingNodePushable nextFrame ===");
            System.err.println("Processing input frame for centroid extraction and query processing");
            
            try {
                // Create frame tuple accessor
                FrameTupleAccessor fta = new FrameTupleAccessor(inputRecDesc);
                fta.reset(buffer);
                
                int tupleCount = fta.getTupleCount();
                System.err.println("Processing " + tupleCount + " tuples from input frame");
                
                if (tupleCount == 0) {
                    System.err.println("No tuples found in input frame");
                    return;
                }
                
                int successfulExtractions = 0;
                int successfulQueries = 0;
                final int MAX_DETAILED_LOGS = 5; // Log detailed info for first 5 records
                
                // Process each tuple in the frame
                for (int i = 0; i < tupleCount; i++) {
                    FrameTupleReference tuple = new FrameTupleReference();
                    tuple.reset(fta, i);
                    
                    try {
                        // Extract embedding from tuple
                        double[] embedding = extractEmbeddingFromTuple(tuple, ctx);
                        
                        if (embedding != null && embedding.length > 0) {
                            successfulExtractions++;
                            
                            // Find closest centroid using the extracted embedding
                            ClusterSearchResult result = findClosestCentroid(embedding);
                            
                            if (result != null) {
                                successfulQueries++;
                                
                                // Detailed logging for first few records
                                if (successfulQueries <= MAX_DETAILED_LOGS) {
                                    System.err.println("=== DETAILED QUERY RESULT " + successfulQueries + " ===");
                                    System.err.println("📊 Query Vector Details:");
                                    System.err.println("  - Vector dimension: " + embedding.length);
                                    System.err.println("  - First 5 values: [" + 
                                        String.format("%.4f", embedding[0]) + ", " +
                                        String.format("%.4f", embedding[1]) + ", " +
                                        String.format("%.4f", embedding[2]) + ", " +
                                        String.format("%.4f", embedding[3]) + ", " +
                                        String.format("%.4f", embedding[4]) + "...]");
                                    
                                    System.err.println("🎯 Closest Centroid Details:");
                                    System.err.println("  - Centroid ID: " + result.centroidId);
                                    System.err.println("  - Distance: " + String.format("%.6f", result.distance));
                                    System.err.println("  - Leaf page ID: " + result.leafPageId);
                                    System.err.println("  - Cluster index: " + result.clusterIndex);
                                    
                                    if (result.centroid != null) {
                                        System.err.println("  - Centroid first 5 values: [" + 
                                            String.format("%.4f", result.centroid[0]) + ", " +
                                            String.format("%.4f", result.centroid[1]) + ", " +
                                            String.format("%.4f", result.centroid[2]) + ", " +
                                            String.format("%.4f", result.centroid[3]) + ", " +
                                            String.format("%.4f", result.centroid[4]) + "...]");
                                    }
                                    System.err.println("=== END DETAILED RESULT " + successfulQueries + " ===");
                                } else if (successfulQueries == MAX_DETAILED_LOGS + 1) {
                                    System.err.println("📝 Detailed logging completed for first " + MAX_DETAILED_LOGS + " records. Continuing with summary logging...");
                                }
                                
                                // Summary logging for all records
                                if (successfulQueries % 10 == 0 || successfulQueries <= MAX_DETAILED_LOGS) {
                                    System.err.println("✅ Query " + successfulQueries + " completed - Centroid ID: " + 
                                        result.centroidId + ", Distance: " + String.format("%.6f", result.distance));
                                }
                            } else {
                                System.err.println("❌ Failed to find closest centroid for query " + (i + 1));
                            }
                        } else {
                            System.err.println("⚠️ Skipping tuple " + (i + 1) + " - no valid embedding extracted");
                        }
                        
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to process tuple " + (i + 1) + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                }
                
                System.err.println("=== FRAME PROCESSING COMPLETE ===");
                System.err.println("Total tuples processed: " + tupleCount);
                System.err.println("Successful extractions: " + successfulExtractions);
                System.err.println("Successful queries: " + successfulQueries);
                System.err.println("Success rate: " + String.format("%.1f", (double) successfulQueries / tupleCount * 100) + "%");
                
            } catch (Exception e) {
                System.err.println("ERROR: Failed to process input frame: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
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
