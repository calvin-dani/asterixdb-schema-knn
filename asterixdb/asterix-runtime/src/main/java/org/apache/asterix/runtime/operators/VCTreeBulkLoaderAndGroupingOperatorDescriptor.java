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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunction;
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTreeDiskComponent;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.util.string.UTF8StringUtil;

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
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final float fillFactor; // TODO: Use fillFactor in future bulk loading operations
    private final UUID permitUUID;
    private final UUID materializedDataUUID;
    private final IScalarEvaluatorFactory args;
    private final RecordDescriptor inputRecDesc;
    private final RecordDescriptor outputRecDesc;
    private final String distanceMetric;
    private final int vectorDimension;

    // Partitioning components
    private VCTreePartitioner partitioner;

    // Distance function constants
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2 = UTF8StringPointable.generateUTF8Pointable("l2");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
            UTF8StringPointable.generateUTF8Pointable("euclidean");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("l2_squared");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("euclidean_squared");
    private static final UTF8StringPointable MANHATTAN_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("manhattan distance");
    private static final UTF8StringPointable COSINE_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("cosine similarity");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot");

    // Serializable distance function implementations
    private static class ManhattanDistanceFunction implements DistanceFunction, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.manhattan(a, b);
        }
    }

    private static class EuclideanDistanceFunction implements DistanceFunction, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclidean(a, b);
        }
    }

    private static class EuclideanSquaredDistanceFunction implements DistanceFunction, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclidean_squared(a, b);
        }
    }

    private static class CosineDistanceFunction implements DistanceFunction, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.cosine(a, b);
        }
    }

    private static class DotProductDistanceFunction implements DistanceFunction, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.dot(a, b);
        }
    }

    // Distance function hash map
    private static final java.util.Map<Integer, DistanceFunction> DISTANCE_MAP =
            java.util.Map.of(MANHATTAN_FORMAT.hash(), new ManhattanDistanceFunction(), EUCLIDEAN_DISTANCE.hash(),
                    new EuclideanDistanceFunction(), EUCLIDEAN_DISTANCE_L2.hash(), new EuclideanDistanceFunction(),
                    EUCLIDEAN_DISTANCE_SQUARED.hash(), new EuclideanSquaredDistanceFunction(),
                    EUCLIDEAN_DISTANCE_L2_SQUARED.hash(), new EuclideanSquaredDistanceFunction(), COSINE_FORMAT.hash(),
                    new CosineDistanceFunction(), DOT_PRODUCT_FORMAT.hash(), new DotProductDistanceFunction());

    /**
     * Convert distance metric string to DistanceFunction implementation.
     * 
     * @param distanceType Distance metric string (e.g., "euclidean", "cosine similarity", etc.)
     * @return DistanceFunction implementation
     * @throws IllegalArgumentException if distance type is not supported
     */
    private static DistanceFunction getDistanceFunction(String distanceType) {
        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(distanceType.toLowerCase());
        DistanceFunction func = DISTANCE_MAP
                .get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(), formatPointable.getStartOffset()));
        if (func == null) {
            // Default to Euclidean if not found
            System.err.println("WARNING: Unsupported distance function: " + distanceType + ", defaulting to euclidean");
            return new EuclideanDistanceFunction();
        }
        return func;
    }

    /**
     * Convert DistanceFunction to IVectorDistanceFunction for use in Hyracks modules.
     * 
     * @param distanceFunction AsterixDB DistanceFunction
     * @return IVectorDistanceFunction wrapper
     */
    private static IVectorDistanceFunction wrapDistanceFunction(DistanceFunction distanceFunction) {
        return distanceFunction::apply;
    }

    public VCTreeBulkLoaderAndGroupingOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, RecordDescriptor outputRecordDescriptor, UUID permitUUID,
            UUID materializedDataUUID, IScalarEvaluatorFactory args, String distanceMetric, int vectorDimension) {
        super(spec, 1, 1); // Changed from (1, 0) to (1, 1) - now has 1 output
        this.indexHelperFactory = indexHelperFactory;
        this.fillFactor = fillFactor;
        this.inputRecDesc = inputRecordDescriptor;
        this.outputRecDesc = outputRecordDescriptor;
        this.permitUUID = permitUUID;
        this.materializedDataUUID = materializedDataUUID;
        this.args = args;
        this.distanceMetric = distanceMetric != null ? distanceMetric : "euclidean";
        this.vectorDimension = vectorDimension > 0 ? vectorDimension : 384; // Default to 384 if invalid

        // Set output record descriptor in the parent class array
        this.outRecDescs[0] = outputRecordDescriptor;

        System.err.println("VCTreeBulkLoaderAndGroupingOperatorDescriptor created with permit UUID: " + permitUUID);
        System.err.println("Output record descriptor set: " + outputRecordDescriptor);
        System.err.println("Distance metric: " + this.distanceMetric);
        System.err.println("Vector dimension: " + this.vectorDimension);
    }

    /**
     * Create transformed tuple with centroidId, distance, and all original fields.
     * Uses TupleUtils.createTuple() with proper serializers from RecordDescriptor.
     * 
     * @param originalTuple Input tuple with original fields to preserve
     * @param searchResult ClusterSearchResult containing all needed values
     * @return Transformed tuple with format [centroidId, distance, ...original fields...]
     * @throws HyracksDataException if tuple creation fails
     */
    public ITupleReference createTransformedTuple(ITupleReference originalTuple, ClusterSearchResult searchResult)
            throws HyracksDataException {
        try {
            // Get serializers for original fields from input record descriptor
            ISerializerDeserializer<?>[] originalFieldSerdes = inputRecDesc.getFields();

            // Create combined serializers: [new fields] + [original fields]
            int totalFields = 2 + originalTuple.getFieldCount() - 1; // 2 new fields + all original fields - embedding
            ISerializerDeserializer<?>[] combinedSerdes = new ISerializerDeserializer<?>[totalFields];

            // Set serializers for new fields
            ISerializerDeserializer<?>[] outputFieldSerdes = outputRecDesc.getFields();
            combinedSerdes[1] = AInt32SerializerDeserializer.INSTANCE;; // centroidId
            combinedSerdes[0] = ADoubleSerializerDeserializer.INSTANCE; // distance

            // Set serializers for original fields
            for (int i = 1; i < originalTuple.getFieldCount(); i++) {
                combinedSerdes[2 + i - 1] = originalFieldSerdes[i];
            }

            // Deserialize original fields to get their values
            Object[] originalFieldValues = TupleUtils.deserializeTuple(originalTuple, originalFieldSerdes);

            // Create combined field values: [new field values] + [original field values]
            Object[] combinedValues = new Object[totalFields];
            //            combinedValues[0] = searchResult.centroidId; // centroidId
            //            combinedValues[1] = searchResult.distance;   // distance
            combinedValues[0] = new ADouble(searchResult.distance);
            combinedValues[1] = new AInt32(searchResult.centroidId); // Wrap in AInt32

            // Add original field values
            for (int i = 1; i < originalFieldValues.length; i++) {
                combinedValues[2 + i - 1] = originalFieldValues[i];
            }

            // Use TupleUtils.createTuple() with combined serializers and values
            ITupleReference result = TupleUtils.createTuple(outputFieldSerdes, combinedValues);
            //            System.err.println("=== TRANSFORMED TUPLE DEBUG ===");
            //            System.err.println("OutputFieldSerdes length: " + outputFieldSerdes.length);
            //            System.err.println("CombinedValues length: " + combinedValues.length);
            //            System.err.println("Result field count: " + result.getFieldCount());
            //            System.err.println("CentroidId: " + searchResult.centroidId + " (type: "
            //                    + combinedValues[0].getClass().getSimpleName() + ")");
            //            System.err.println("Distance: " + searchResult.distance + " (type: "
            //                    + combinedValues[1].getClass().getSimpleName() + ")");

            return result;

        } catch (Exception e) {
            System.err.println("ERROR: Failed to create transformed tuple: " + e.getMessage());
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
    public double[] extractEmbeddingFromTuple(ITupleReference tuple, IHyracksTaskContext ctx)
            throws HyracksDataException {
        try {

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
                return null;
            }

            // Check if it's a list type (required for vector data)
            if (!EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                    .isListType()) {
                return null;
            }

            // Parse the vector data using proper AsterixDB parsing
            listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
            double[] embedding = kMeansUtils.createPrimitveList(listAccessorConstant);

            // Validate extracted embedding
            if (embedding == null) {
                return null;
            }

            if (embedding.length == 0) {
                return null;
            }

            // Validate embedding dimensions
            if (embedding.length != vectorDimension) {
                return null;
            }

            return embedding;

        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Initialize VCTreePartitioner for recursive partitioning.
     * 
     * @param ctx Hyracks task context for file operations
     * @param memoryBudget Available memory budget in frames
     * @param frameSize Frame size in bytes
     */
    public void initializePartitioner(IHyracksTaskContext ctx, int memoryBudget, int frameSize) {
        //        System.err.println("=== INITIALIZING VCTreePartitioner ===");
        //        System.err.println("Memory budget: " + memoryBudget + " frames");
        //        System.err.println("Frame size: " + frameSize + " bytes");

        this.partitioner = new VCTreePartitioner(ctx, memoryBudget, frameSize);
        //        System.err.println(" VCTreePartitioner initialized successfully");
    }

    /**
     * Process data using VCTreePartitioner for recursive partitioning with real data.
     * 
     * @param inputTuples List of input tuples to partition
     * @param K Number of centroids
     * @param centroidIdColumn Column index containing centroid ID (0 for first, -1 for last)
     * @return Map of centroid ID to file reference
     * @throws HyracksDataException if partitioning fails
     */
    public Map<Integer, FileReference> processDataWithPartitioner(List<ITupleReference> inputTuples, int K,
            int centroidIdColumn) throws HyracksDataException {
        System.err.println("=== PROCESSING DATA WITH VCTreePartitioner (REAL DATA) ===");
        System.err.println("Input tuples: " + inputTuples.size());
        System.err.println("K (centroids): " + K);
        System.err.println("Centroid ID column: " + centroidIdColumn);

        if (partitioner == null) {
            throw new IllegalStateException("VCTreePartitioner not initialized. Call initializePartitioner() first.");
        }

        // Use VCTreePartitioner for recursive partitioning with real data
        partitioner.partitionData(inputTuples, K, centroidIdColumn);
        Map<Integer, FileReference> centroidFiles = partitioner.getCentroidFiles();

        System.err.println("✅ VCTreePartitioner processing complete");
        System.err.println("Created " + centroidFiles.size() + " centroid files");

        return centroidFiles;
    }

    /**
     * Process data using VCTreePartitioner for recursive partitioning (legacy method).
     * 
     * @param K Number of centroids
     * @param estimatedDataSize Estimated data size in bytes
     * @return Map of centroid ID to file reference
     * @throws HyracksDataException if partitioning fails
     */
    public Map<Integer, FileReference> processDataWithPartitioner(int K, long estimatedDataSize)
            throws HyracksDataException {
        System.err.println("=== PROCESSING DATA WITH VCTreePartitioner ===");
        System.err.println("K (centroids): " + K);
        System.err.println("Estimated data size: " + estimatedDataSize + " bytes");

        if (partitioner == null) {
            throw new IllegalStateException("VCTreePartitioner not initialized. Call initializePartitioner() first.");
        }

        // Use VCTreePartitioner for recursive partitioning
        partitioner.partitionData(K, estimatedDataSize);
        Map<Integer, FileReference> centroidFiles = partitioner.getCentroidFiles();

        System.err.println("VCTreePartitioner processing complete");
        System.err.println("Created " + centroidFiles.size() + " centroid files");

        return centroidFiles;
    }

    /**
     * Close VCTreePartitioner and cleanup resources.
     * 
     * @throws HyracksDataException if cleanup fails
     */
    public void closePartitioner() throws HyracksDataException {
        if (partitioner != null) {
            //            System.err.println("=== CLOSING VCTreePartitioner ===");
            partitioner.closeAllFiles();
            //            System.err.println("✅ VCTreePartitioner closed successfully");
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
        VCTreeBulkLoaderAndGroupingNodePushable pushable = new VCTreeBulkLoaderAndGroupingNodePushable(ctx, partition,
                nPartitions, inputRecDesc, permitUUID, materializedDataUUID);

        // Set output record descriptor for the pushable
        pushable.setOutputRecordDescriptor(outputRecDesc);

        return pushable;
    }

    /**
     * Node pushable implementation for VCTreeBulkLoaderAndGroupingOperatorDescriptor.
     */
    private class VCTreeBulkLoaderAndGroupingNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final int partition;
        private final UUID materializedDataUUID;
        private LSMIndexDiskComponentBulkLoader lsmBulkLoader;
        private IIndexDataflowHelper indexHelper;
        private ILSMIndex lsmIndex; // TODO: Use lsmIndex in future bulk loading operations
        private LSMVCTree lsmVCTree;
        private VectorClusteringTree.VectorClusteringTreeAccessor vcTreeAccessor;
        private MaterializerTaskState materializedData;
        int successfulQueries = 0;
        int totalTuplesProcessed = 0;

        // Output infrastructure for transformed tuples
        private FrameTupleAppender outputAppender;
        private ArrayTupleBuilder outputTupleBuilder;
        private ArrayTupleReference outputTupleRef;
        private RecordDescriptor outputRecDesc;
        private DistanceFunction distanceFunction;
        private IVectorDistanceFunction hyracksDistanceFunction;

        public VCTreeBulkLoaderAndGroupingNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc, UUID permitUUID, UUID materializedDataUUID) {
            this.ctx = ctx;
            this.partition = partition;
            this.materializedDataUUID = materializedDataUUID;
        }

        /**
         * Set the output record descriptor for transformed tuples.
         * 
         * @param outputRecDesc Record descriptor for output tuples
         */
        public void setOutputRecordDescriptor(RecordDescriptor outputRecDesc) {
            this.outputRecDesc = outputRecDesc;
        }

        @Override
        public void open() throws HyracksDataException {
            try {
                // Initialize materialized data state
                materializedData = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                        new PartitionedUUID(materializedDataUUID, partition));
                materializedData.open(ctx);

                // Initialize output infrastructure for transformed tuples
                initializeOutputInfrastructure();

                // Convert distance metric string to DistanceFunction
                distanceFunction = getDistanceFunction(distanceMetric);
                // Wrap for use in Hyracks modules
                hyracksDistanceFunction = wrapDistanceFunction(distanceFunction);
                //                System.err.println("Initialized distance function for metric: " + distanceMetric);

                // Open the output writer
                if (writer != null) {
                    writer.open();
                }
                // Initialize VCTreePartitioner for recursive partitioning
                int memoryBudget = 32; // frames - typical value from other operators
                int frameSize = 32768; // 32KB frame size
                initializePartitioner(ctx, memoryBudget, frameSize);

                // Pre-initialize partitioning strategy with known K
                int knownK = 10; // K = 10 centroids (0-9)
                if (partitioner != null) {
                    partitioner.preInitializePartitioning(knownK, memoryBudget, frameSize);
                }

                // Initialize index helper to access static structure via LSM index system
                //                System.err.println("=== INITIALIZING INDEX-BASED STATIC STRUCTURE ACCESS ===");
                indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                indexHelper.open();

                // Get LSMVCTree instance
                org.apache.hyracks.storage.common.IIndex indexInstance = indexHelper.getIndexInstance();
                //                System.err.println("Index instance type: "
                //                        + (indexInstance != null ? indexInstance.getClass().getName() : "null"));

                if (!(indexInstance instanceof ILSMIndex)) {
                    throw new HyracksDataException("Index is not an ILSMIndex instance, got: "
                            + (indexInstance != null ? indexInstance.getClass().getName() : "null"));
                }
                ILSMIndex lsmIndex = (ILSMIndex) indexInstance;

                if (!(lsmIndex instanceof LSMVCTree)) {
                    throw new HyracksDataException(
                            "Index is not an LSMVCTree instance, got: " + lsmIndex.getClass().getName());
                }
                lsmVCTree = (LSMVCTree) lsmIndex;
                //                System.err.println("LSMVCTree instance obtained successfully");

                // Get static structure and create accessor
                LSMVCTreeDiskComponent staticStructure = lsmVCTree.getStaticStructure();
                IIndexAccessor accessor = staticStructure.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
                vcTreeAccessor = (VectorClusteringTree.VectorClusteringTreeAccessor) accessor;
                //                System.err.println("✅ VectorClusteringTreeAccessor created successfully");

            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initialize output infrastructure for transformed tuples.
         * 
         * @throws HyracksDataException if initialization fails
         */
        private void initializeOutputInfrastructure() throws HyracksDataException {
            try {
                //                System.err.println("=== INITIALIZING OUTPUT INFRASTRUCTURE ===");

                // Initialize tuple building components
                outputTupleBuilder = new ArrayTupleBuilder(outputRecDesc.getFieldCount());
                outputTupleRef = new ArrayTupleReference();

                // Initialize frame appender for output with a frame
                org.apache.hyracks.api.comm.VSizeFrame outputFrame = new org.apache.hyracks.api.comm.VSizeFrame(ctx);
                outputAppender = new FrameTupleAppender(outputFrame);

                //                System.err.println("Output infrastructure initialized successfully");
            } catch (Exception e) {
                //                System.err.println("ERROR: Failed to initialize output infrastructure: " + e.getMessage());
                //                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Find the closest centroid using VectorClusteringTreeAccessor.
         * This follows the same approach as VectorTreeTestUtils.clusterRecords().
         * 
         * @param queryVector Query vector to find closest centroid for
         * @return ClusterSearchResult containing closest centroid information
         * @throws HyracksDataException if search fails
         */
        private ClusterSearchResult findClosestCentroid(double[] queryVector) throws HyracksDataException {
            try {
                // Validate input vector
                if (queryVector == null) {
                    throw new IllegalArgumentException("Query vector cannot be null");
                }

                if (queryVector.length == 0) {
                    throw new IllegalArgumentException("Query vector cannot be empty");
                }

                // Validate vector dimensions
                if (queryVector.length != vectorDimension) {
                    System.err.println("WARNING: Query vector dimension (" + queryVector.length
                            + ") does not match expected dimension (" + vectorDimension + ")");
                }

                // Validate accessor is initialized
                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VectorClusteringTreeAccessor not initialized");
                }

                // Validate distance function is initialized
                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunction not initialized");
                }

                ClusterSearchResult result =
                        vcTreeAccessor.findClosestLeafCentroid(queryVector, hyracksDistanceFunction);

                if (result == null) {
                    System.err.println("WARNING: No closest centroid found for query vector");
                    return null;
                }

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
         * Find the closest centroid using VectorClusteringTreeAccessor.
         * This follows the same approach as VectorTreeTestUtils.clusterRecords().
         *
         * @param queryVector Query vector to find closest centroid for
         * @return ClusterSearchResult containing closest centroid information
         * @throws HyracksDataException if search fails
         */
        private List<ClusterSearchResult> findCloseLeafCentroid(double[] queryVector, double epi)
                throws HyracksDataException {
            try {
                // Validate input vector
                if (queryVector == null) {
                    throw new IllegalArgumentException("Query vector cannot be null");
                }

                if (queryVector.length == 0) {
                    throw new IllegalArgumentException("Query vector cannot be empty");
                }

                // Validate vector dimensions
                if (queryVector.length != vectorDimension) {
                    System.err.println("WARNING: Query vector dimension (" + queryVector.length
                            + ") does not match expected dimension (" + vectorDimension + ")");
                }

                // Validate accessor is initialized
                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VectorClusteringTreeAccessor not initialized");
                }

                // Validate distance function is initialized
                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunction not initialized");
                }

                List<ClusterSearchResult> result =
                        vcTreeAccessor.findCloseLeafCentroid(queryVector, hyracksDistanceFunction, epi);

                if (result == null) {
                    System.err.println("WARNING: No closest centroid found for query vector");
                    return null;
                }

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
         * Find the closest centroid using VectorClusteringTreeAccessor.
         * This follows the same approach as VectorTreeTestUtils.clusterRecords().
         *
         * @param queryVector Query vector to find closest centroid for
         * @return ClusterSearchResult containing closest centroid information
         * @throws HyracksDataException if search fails
         */
        private List<ClusterSearchResult> findCloseCentroidsFrontier(double[] queryVector, double epi)
                throws HyracksDataException {
            try {
                // Validate input vector
                if (queryVector == null) {
                    throw new IllegalArgumentException("Query vector cannot be null");
                }

                if (queryVector.length == 0) {
                    throw new IllegalArgumentException("Query vector cannot be empty");
                }

                // Validate vector dimensions
                if (queryVector.length != vectorDimension) {
                    System.err.println("WARNING: Query vector dimension (" + queryVector.length
                            + ") does not match expected dimension (" + vectorDimension + ")");
                }

                // Validate accessor is initialized
                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VectorClusteringTreeAccessor not initialized");
                }

                // Validate distance function is initialized
                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunction not initialized");
                }

                List<ClusterSearchResult> result =
                        vcTreeAccessor.findCloseCentroidsFrontier(queryVector, hyracksDistanceFunction, epi);

                if (result == null) {
                    System.err.println("WARNING: No closest centroid found for query vector");
                    return null;
                }

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
         * Find the closest centroid using VectorClusteringTreeAccessor.
         * This follows the same approach as VectorTreeTestUtils.clusterRecords().
         *
         * @param queryVector Query vector to find closest centroid for
         * @return ClusterSearchResult containing closest centroid information
         * @throws HyracksDataException if search fails
         */
        private List<ClusterSearchResult> findCloseCentroidsLevelWise(double[] queryVector, double epi)
                throws HyracksDataException {
            try {
                // Validate input vector
                if (queryVector == null) {
                    throw new IllegalArgumentException("Query vector cannot be null");
                }

                if (queryVector.length == 0) {
                    throw new IllegalArgumentException("Query vector cannot be empty");
                }

                // Validate vector dimensions
                if (queryVector.length != vectorDimension) {
                    System.err.println("WARNING: Query vector dimension (" + queryVector.length
                            + ") does not match expected dimension (" + vectorDimension + ")");
                }

                // Validate accessor is initialized
                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VectorClusteringTreeAccessor not initialized");
                }

                // Validate distance function is initialized
                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunction not initialized");
                }

                List<ClusterSearchResult> result =
                        vcTreeAccessor.findCloseCentroidsLevelWise(queryVector, hyracksDistanceFunction, epi);

                if (result == null) {
                    System.err.println("WARNING: No closest centroid found for query vector");
                    return null;
                }

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

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

            try {
                // Create frame tuple accessor
                FrameTupleAccessor fta = new FrameTupleAccessor(inputRecDesc);
                fta.reset(buffer);

                int tupleCount = fta.getTupleCount();
                totalTuplesProcessed += tupleCount;

                if (tupleCount == 0) {
                    return;
                }

                // Process each tuple in the frame
                for (int i = 0; i < tupleCount; i++) {
                    FrameTupleReference tuple = new FrameTupleReference();
                    tuple.reset(fta, i);

                    try {
                        // Extract embedding from tuple
                        double[] embedding = extractEmbeddingFromTuple(tuple, ctx);

                        if (embedding != null && embedding.length > 0) {
                            // Find closest centroid using the extracted embedding
                            // Use accessor to find closest leaf centroid with distance function
                            boolean crossPollinate = true; // Do not cross partition boundaries
                            boolean leafPollinate = true;
                            boolean interiorPollinate = false;
                            if (!crossPollinate) {
                                ClusterSearchResult result = findClosestCentroid(embedding);
                                if (result != null) {
                                    successfulQueries++;

                                    // Create transformed tuple with [centroidId, distance, ...original fields...]
                                    ITupleReference transformedTuple = createTransformedTuple(tuple, result);

                                    // Output the transformed tuple to downstream operators
                                    outputTransformedTuple(transformedTuple);

                                } else {
                                    System.err.println("Failed to find closest centroid for query " + (i + 1));
                                }
                            } else if (crossPollinate && leafPollinate) {
                                // FUTURE: Implement cross-partition centroid search
                                List<ClusterSearchResult> result = findCloseLeafCentroid(embedding, 0.1);
                                if (result != null) {
                                    successfulQueries++;
                                    for (ClusterSearchResult res : result) {
                                        // Create transformed tuple with [centroidId, distance, ...original fields...]
                                        ITupleReference transformedTuple = createTransformedTuple(tuple, res);

                                        // Output the transformed tuple to downstream operators
                                        outputTransformedTuple(transformedTuple);
                                    }
                                } else {
                                    System.err.println("Failed to find closest centroid for query " + (i + 1));
                                }

                            } else if (crossPollinate && interiorPollinate) {
                                // FUTURE: Implement cross-partition centroid search
                                List<ClusterSearchResult> result = findCloseCentroidsLevelWise(embedding, 0.1);
                                if (result != null) {
                                    successfulQueries++;
                                    for (ClusterSearchResult res : result) {
                                        // Create transformed tuple with [centroidId, distance, ...original fields...]
                                        ITupleReference transformedTuple = createTransformedTuple(tuple, res);

                                        // Output the transformed tuple to downstream operators
                                        outputTransformedTuple(transformedTuple);
                                    }
                                } else {
                                    System.err.println("Failed to find closest centroid for query " + (i + 1));
                                }

                            } else {
                                // FUTURE: Implement cross-partition centroid search
                                List<ClusterSearchResult> result = findCloseCentroidsFrontier(embedding, 0.1);
                                if (result != null) {
                                    successfulQueries++;
                                    for (ClusterSearchResult res : result) {
                                        // Create transformed tuple with [centroidId, distance, ...original fields...]
                                        ITupleReference transformedTuple = createTransformedTuple(tuple, res);

                                        // Output the transformed tuple to downstream operators
                                        outputTransformedTuple(transformedTuple);
                                    }
                                } else {
                                    System.err.println("Failed to find closest centroid for query " + (i + 1));
                                }

                            }

                        } else {
                            System.err.println("Skipping tuple " + (i + 1) + " - no valid embedding extracted");
                        }

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to process tuple " + (i + 1) + ": " + e.getMessage());
                        throw HyracksDataException.create(e);
                    }
                }

            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Output transformed tuple to downstream operators.
         * 
         * @param transformedTuple Tuple with [centroidId, distance, ...original fields...]
         * @throws HyracksDataException if output fails
         */
        private void outputTransformedTuple(ITupleReference transformedTuple) throws HyracksDataException {
            try {

                if (transformedTuple == null) {
                    return;
                }

                if (writer != null && outputAppender != null) {
                    // Append tuple to output frame

                    if (!outputAppender.append(transformedTuple)) {
                        FrameUtils.flushFrame(outputAppender.getBuffer(), writer);
                        outputAppender.reset(new VSizeFrame(ctx), true);
                        outputAppender.append(transformedTuple);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void flush() throws HyracksDataException {
            try {
                if (writer != null && outputAppender != null) {
                    outputAppender.flush(writer);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            //            System.err.println("Total tuples processed: " + totalTuplesProcessed);
            //            System.err.println("Successful extractions: " + successfulQueries);

            try {
                // CRITICAL: Write any remaining output data before closing
                // This ensures the downstream sort operator receives all data
                if (writer != null && outputAppender != null) {
                    //                    System.err.println("Writing final output data to downstream sort operator...");
                    outputAppender.write(writer, false); // false = don't clear frame, just write remaining data
                    //                    System.err.println("Final output data written successfully");
                }

                // Finalize partitioning after all data is processed
                //                if (partitioner != null) {
                //                    Map<Integer, FileReference> centroidFiles = partitioner.finalizePartitioning();
                //                    System.err.println("Finalized partitioning with " + centroidFiles.size() + " centroid files");
                //
                //                    // Stream data from run files in centroid ID order
                //                    streamRunFilesInOrder(centroidFiles);
                //                }

                // Close VCTreePartitioner
                closePartitioner();

                if (lsmBulkLoader != null) {
                    lsmBulkLoader.end();
                }
                if (vcTreeAccessor != null) {
                    // Accessor doesn't need explicit close, but we can set to null for cleanup
                    vcTreeAccessor = null;
                }
                if (indexHelper != null) {
                    indexHelper.close();
                }
                if (materializedData != null) {
                    materializedData.close();
                    ctx.setStateObject(materializedData);
                }

                // Close the writer AFTER flushing all data
                if (writer != null) {
                    writer.close();
                    //                    System.err.println("Writer closed after flushing all data");
                }

            } catch (Exception e) {
                //                System.err.println("ERROR: Failed to close VCTreeBulkLoaderAndGroupingNodePushable: " + e.getMessage());
                //                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Stream data from run files in centroid ID order (lowest to highest).
         * 
         * @param centroidFiles Map of centroid ID to file reference
         * @throws HyracksDataException if streaming fails
         */
        private void streamRunFilesInOrder(Map<Integer, FileReference> centroidFiles) throws HyracksDataException {
            try {

                if (centroidFiles.isEmpty()) {
                    System.err.println("No centroid files to stream");
                    return;
                }

                // Sort centroid IDs to ensure order (lowest to highest)
                List<Integer> sortedCentroidIds = new ArrayList<>(centroidFiles.keySet());
                sortedCentroidIds.sort(Integer::compareTo);

                System.err.println("Streaming centroids in order: " + sortedCentroidIds);

                long totalTuplesStreamed = 0;

                // Stream each run file in centroid ID order
                for (int centroidId : sortedCentroidIds) {
                    FileReference runFile = centroidFiles.get(centroidId);
                    if (runFile != null) {
                        long tuplesInFile = streamSingleRunFile(runFile, centroidId);
                        totalTuplesStreamed += tuplesInFile;
                    } else {
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Stream data from a single run file.
         * 
         * @param runFile File reference to the run file
         * @param centroidId Centroid ID for logging
         * @return Number of tuples streamed from this file
         * @throws HyracksDataException if streaming fails
         */
        private long streamSingleRunFile(FileReference runFile, int centroidId) throws HyracksDataException {
            long tuplesStreamed = 0;

            try {
                // Create run file reader
                org.apache.hyracks.dataflow.common.io.RunFileReader reader =
                        new org.apache.hyracks.dataflow.common.io.RunFileReader(runFile, ctx.getIoManager(), 0, false);
                reader.open();

                try {
                    // Read frames from the run file
                    org.apache.hyracks.api.comm.IFrame frame = new org.apache.hyracks.api.comm.VSizeFrame(ctx);

                    while (reader.nextFrame(frame)) {
                        ByteBuffer frameBuffer = frame.getBuffer();

                        // Process the frame and stream tuples
                        tuplesStreamed += processAndStreamFrame(frameBuffer, centroidId);
                    }

                } finally {
                    reader.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }

            return tuplesStreamed;
        }

        /**
         * Process a frame and stream its tuples to the output.
         * 
         * @param frameBuffer Frame buffer containing tuples
         * @param centroidId Centroid ID for logging
         * @return Number of tuples processed from this frame
         * @throws HyracksDataException if processing fails
         */
        private long processAndStreamFrame(ByteBuffer frameBuffer, int centroidId) throws HyracksDataException {
            long tuplesProcessed = 0;

            try {
                // Reset frame buffer for reading
                frameBuffer.rewind();

                // Create frame tuple accessor with the CORRECT record descriptor
                // The run files contain tuples written by VCTreePartitioner, so we need to use
                // the same record descriptor that VCTreePartitioner uses for reading/writing
                RecordDescriptor partitionerRecDesc = partitioner.getTupleRecordDescriptor();
                FrameTupleAccessor fta = new FrameTupleAccessor(partitionerRecDesc);
                fta.reset(frameBuffer);

                int tupleCount = fta.getTupleCount();
                tuplesProcessed = tupleCount;

                // Debug: Check if the frame has any data
                if (tupleCount == 0) {

                    // Try to read a few bytes to see what's in the buffer
                    if (frameBuffer.remaining() > 0) {
                        byte[] sample = new byte[Math.min(32, frameBuffer.remaining())];
                        frameBuffer.get(sample);
                    }
                }

                // Process each tuple in the frame
                for (int i = 0; i < tupleCount; i++) {
                    FrameTupleReference tuple = new FrameTupleReference();
                    tuple.reset(fta, i);

                    // Debug: Log tuple field count

                    // Stream the tuple to output
                    if (writer != null && outputAppender != null) {
                        outputAppender.append(tuple);
                    }
                }

                // Output the frame if we have a writer
                if (writer != null && outputAppender != null) {
                    outputAppender.flush(writer);
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }

            return tuplesProcessed;
        }

        @Override
        public void fail() throws HyracksDataException {
            try {
                // Close VCTreePartitioner
                closePartitioner();

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

                e.printStackTrace();
            }
        }
    }
}
