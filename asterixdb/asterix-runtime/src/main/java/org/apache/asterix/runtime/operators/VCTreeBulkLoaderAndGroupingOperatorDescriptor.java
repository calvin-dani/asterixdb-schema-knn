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

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.storage.OptimizedScalarQuantizationSampleFile;
import org.apache.asterix.common.storage.ScalarVectorQuantizer;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunctionDouble;
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
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
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
import org.apache.hyracks.storage.am.lsm.vector.dataflow.LSMVTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeDiskComponent;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Operator that handles bulk loader initialization and recursive data grouping to run files.
 * This operator is designed for job 3 in the VCTree creation pipeline.
 * <p>
 * Responsibilities:
 * 1. Initialize LSM bulk loader for VTree
 * 2. Apply recursive partitioning logic using SHAPIRO formula
 * 3. Group data into run files based on memory budget and data size
 * 4. Manage run file creation and data distribution
 */
public class VCTreeBulkLoaderAndGroupingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final float fillFactor; // TODO: Use fillFactor in future bulk loading operations
    private final UUID permitUUID;
    private final UUID materializedDataUUID;
    private final IScalarEvaluatorFactory args;
    private final RecordDescriptor inputRecDesc;
    private final RecordDescriptor outputRecDesc;
    private final String distanceMetric;
    private final int vectorDimension;
    private final int numPrimaryKeys;
    private final int numIncludeFields;
    private final boolean isQuantized;

    /** Epsilon for level-wise centroid search ({@code findCloseCentroidsLevelWiseGlobalSort}); from index WITH clause. */
    private final double levelwiseEpsilon;

    /**
     * Cross-pollination factor: each record is replicated into the M closest leaf centroids at bulk-load,
     * not just the closest one. M=1 reproduces the legacy (no cross-pollination) behavior.
     */
    private final int crossPollinationM;

    // Maps task (compute) partition to storage partition(s) for index resource lookup
    private final int[][] partitionsMap;

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
            UTF8StringPointable.generateUTF8Pointable("manhattan_distance");
    private static final UTF8StringPointable COSINE_FORMAT = UTF8StringPointable.generateUTF8Pointable("cosine");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot");

    // Serializable distance function implementations
    private static class ManhattanDistanceFunctionDouble implements DistanceFunctionDouble, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.manhattan(a, b);
        }
    }

    private static class EuclideanDistanceFunctionDouble implements DistanceFunctionDouble, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclidean(a, b);
        }
    }

    private static class EuclideanSquaredDistanceFunctionDouble
            implements DistanceFunctionDouble, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclideanSquared(a, b);
        }
    }

    private static class CosineDistanceFunctionDouble implements DistanceFunctionDouble, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.cosineDistance(a, b);
        }
    }

    private static class DotProductDistanceFunctionDouble implements DistanceFunctionDouble, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        /** Returns -dot(a,b) so that minimizing "distance" equals maximizing dot product (MIPS). */
        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return -VectorDistanceArrCalculation.dot(a, b);
        }
    }

    // Distance function hash map
    private static final java.util.Map<Integer, DistanceFunctionDouble> DISTANCE_MAP = java.util.Map.of(
            EUCLIDEAN_DISTANCE.hash(), new EuclideanDistanceFunctionDouble(), EUCLIDEAN_DISTANCE_L2.hash(),
            new EuclideanDistanceFunctionDouble(), EUCLIDEAN_DISTANCE_SQUARED.hash(),
            new EuclideanSquaredDistanceFunctionDouble(), EUCLIDEAN_DISTANCE_L2_SQUARED.hash(),
            new EuclideanSquaredDistanceFunctionDouble(), COSINE_FORMAT.hash(), new CosineDistanceFunctionDouble(),
            DOT_PRODUCT_FORMAT.hash(), new DotProductDistanceFunctionDouble());

    /**
     * Convert distance metric string to DistanceFunctionDouble implementation.
     *
     * @param distanceType Distance metric string (e.g., "euclidean", "cosine similarity", etc.)
     * @return DistanceFunctionDouble implementation
     * @throws IllegalArgumentException if distance type is not supported
     */
    private static DistanceFunctionDouble getDistanceFunctionDouble(String distanceType) {
        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(distanceType.toLowerCase());
        DistanceFunctionDouble func = DISTANCE_MAP
                .get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(), formatPointable.getStartOffset()));
        //        if (func == null) {
        //            // Default to Euclidean if not found
        //            System.err.println("WARNING: Unsupported distance function: " + distanceType + ", defaulting to euclidean");
        //            return new EuclideanDistanceFunctionDouble();
        //        }
        return func;
    }

    /**
     * Convert DistanceFunctionDouble to IVTreeDistanceFunction for use in Hyracks modules.
     *
     * @param DistanceFunctionDouble AsterixDB DistanceFunctionDouble
     * @return IVTreeDistanceFunction wrapper
     */
    private static IVTreeDistanceFunction wrapDistanceFunctionDouble(DistanceFunctionDouble distanceFunction) {
        return distanceFunction::apply;
    }

    public VCTreeBulkLoaderAndGroupingOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, RecordDescriptor outputRecordDescriptor, UUID permitUUID,
            UUID materializedDataUUID, IScalarEvaluatorFactory args, String distanceMetric, int vectorDimension,
            int numPrimaryKeys, int numIncludeFields, boolean isQuantized, int[][] partitionsMap,
            double levelwiseEpsilon, int crossPollinationM) {
        super(spec, 1, 1); // Changed from (1, 0) to (1, 1) - now has 1 output
        this.indexHelperFactory = indexHelperFactory;
        this.fillFactor = fillFactor;
        this.inputRecDesc = inputRecordDescriptor;
        this.outputRecDesc = outputRecordDescriptor;
        this.permitUUID = permitUUID;
        this.materializedDataUUID = materializedDataUUID;
        this.args = args;
        this.distanceMetric = distanceMetric;
        this.vectorDimension = vectorDimension; // Default to 384 if invalid
        this.numPrimaryKeys = numPrimaryKeys;
        this.numIncludeFields = numIncludeFields;
        this.isQuantized = isQuantized;
        this.partitionsMap = partitionsMap;
        this.levelwiseEpsilon = levelwiseEpsilon > 0.0 && Double.isFinite(levelwiseEpsilon) ? levelwiseEpsilon : 0.3;
        this.crossPollinationM = Math.max(1, crossPollinationM);

        // Set output record descriptor in the parent class array
        this.outRecDescs[0] = outputRecordDescriptor;

    }

    /**
     * Create transformed tuple with distance, centroidId, PKs, and include fields.
     * Uses TupleUtils.createTuple() with proper serializers from RecordDescriptor.
     * <p>
     * Input tuple format from CastAssign: [embedding, include_fields..., pk...]
     * <p>
     * Non-quantized output: [distance, centroidId, pk..., include_fields...]
     * Quantized output:     [distance, centroidId, quantized_distance, quantized_embedding, pk..., include_fields...]
     * <p>
     * IMPORTANT: centroidId MUST be at field index 1 in both formats so that
     * sortFields={1,0} and extractCentroidId(field[1]) work unchanged.
     *
     * @param originalTuple   Input tuple with original fields to preserve
     * @param searchResult    ClusterSearchResult containing all needed values
     * @param quantizedVector QuantizedVector from quantization (nullable — only used for quantized indexes)
     * @return Transformed tuple
     * @throws HyracksDataException if tuple creation fails
     */
    public ITupleReference createTransformedTuple(ITupleReference originalTuple, ClusterSearchResult searchResult,
            OptimizedScalarQuantizationSampleFile.QuantizedVector quantizedVector) throws HyracksDataException {
        try {
            // Get serializers for original fields from input record descriptor
            ISerializerDeserializer<?>[] originalFieldSerdes = inputRecDesc.getFields();

            // Number of secondary fields depends on quantization
            int numSecondaryFields = isQuantized ? 4 : 2;
            int totalFields = numSecondaryFields + numPrimaryKeys + numIncludeFields;

            // Get output serializers
            ISerializerDeserializer<?>[] outputFieldSerdes = outputRecDesc.getFields();

            // Deserialize original fields to get their values
            // Original tuple format: [embedding(0), include_fields(1 to numIncludeFields), pk(numIncludeFields+1 onwards)]
            Object[] originalFieldValues = TupleUtils.deserializeTuple(originalTuple, originalFieldSerdes);

            // Create combined field values with reordered fields
            Object[] combinedValues = new Object[totalFields];
            combinedValues[0] = searchResult.distance; // raw double

            if (isQuantized) {
                // Quantized format: [distance, centroidId, quantized_distance, quantized_embedding, pk..., includes...]
                combinedValues[1] = searchResult.centroidId; // raw int — MUST be at index 1

                // Fill quantized_distance from navigation result
                combinedValues[2] = searchResult.hasQuantizedDistance() ? searchResult.quantizedDistance : 0.0;

                // Fill quantized_embedding from quantization result
                if (quantizedVector != null && quantizedVector.quantizedBytes != null) {
                    Object qBytes = quantizedVector.quantizedBytes;
                    if (qBytes instanceof byte[]) {
                        combinedValues[3] = qBytes;
                    } else if (qBytes instanceof short[]) {
                        // Convert short[] to byte[] for serialization
                        short[] shorts = (short[]) qBytes;
                        byte[] bytes = new byte[shorts.length * 2];
                        java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).asShortBuffer()
                                .put(shorts);
                        combinedValues[3] = bytes;
                    } else if (qBytes instanceof int[]) {
                        // Convert int[] to byte[] for serialization
                        int[] ints = (int[]) qBytes;
                        byte[] bytes = new byte[ints.length * 4];
                        java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(ints);
                        combinedValues[3] = bytes;
                    } else {
                        combinedValues[3] = new byte[0]; // fallback
                    }
                } else {
                    combinedValues[3] = new byte[0]; // fallback
                }
            } else {
                // Non-quantized format: [distance, centroidId, pk..., includes...]
                combinedValues[1] = searchResult.centroidId; // raw int
            }

            // Add primary key fields (they are at positions numIncludeFields+1 onwards in original tuple)
            for (int i = 0; i < numPrimaryKeys; i++) {
                int originalPkIndex = 1 + numIncludeFields + i; // Skip embedding(0) and include fields
                combinedValues[numSecondaryFields + i] = originalFieldValues[originalPkIndex];
            }

            // Add include fields (they are at positions 1 to numIncludeFields in original tuple)
            for (int i = 0; i < numIncludeFields; i++) {
                int originalIncludeIndex = 1 + i; // Skip embedding(0)
                combinedValues[numSecondaryFields + numPrimaryKeys + i] = originalFieldValues[originalIncludeIndex];
            }

            // Use TupleUtils.createTuple() with output serializers and reordered values
            return TupleUtils.createTuple(outputFieldSerdes, combinedValues);

        } catch (Exception e) {
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Legacy overload for backwards compatibility (no quantized vector).
     */
    public ITupleReference createTransformedTuple(ITupleReference originalTuple, ClusterSearchResult searchResult)
            throws HyracksDataException {
        return createTransformedTuple(originalTuple, searchResult, null);
    }

    /**
     * Extract embedding from input tuple using IScalarEvaluator and KMeansUtils.
     * This method follows the same pattern as HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.
     *
     * @param tuple Input tuple containing vector data
     * @param ctx   Hyracks task context for evaluator creation
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

            // Validate embedding dimensions — return empty array as sentinel for mismatch
            // (null = missing field/silent, empty = dimension mismatch/warn)
            if (embedding.length != vectorDimension) {
                return new double[0];
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
     * @param ctx          Hyracks task context for file operations
     * @param memoryBudget Available memory budget in frames
     * @param frameSize    Frame size in bytes
     */
    public void initializePartitioner(IHyracksTaskContext ctx, int memoryBudget, int frameSize) {
        //        System.err.println("=== INITIALIZING VCTreePartitioner ===");
        //        System.err.println("Memory budget: " + memoryBudget + " frames");
        //        System.err.println("Frame size: " + frameSize + " bytes");

        this.partitioner = new VCTreePartitioner(ctx, memoryBudget, frameSize);
        //        System.err.println(" VCTreePartitioner initialized successfully");
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
        /** Storage partition for index resource lookup (from compute-storage map). */
        private final int storagePartition;
        private final UUID materializedDataUUID;
        private LSMIndexDiskComponentBulkLoader lsmBulkLoader;
        private IIndexDataflowHelper indexHelper;
        private ILSMIndex lsmIndex; // TODO: Use lsmIndex in future bulk loading operations
        private LSMVTree LSMVTree;
        private VTree.VTreeAccessor vcTreeAccessor;
        private MaterializerTaskState materializedData;
        int successfulQueries = 0;
        int totalTuplesProcessed = 0;
        int dimensionMismatchCount = 0;

        // Output infrastructure for transformed tuples
        private FrameTupleAppender outputAppender;
        private ArrayTupleBuilder outputTupleBuilder;
        private ArrayTupleReference outputTupleRef;
        private RecordDescriptor outputRecDesc;
        private DistanceFunctionDouble distanceFunction;
        private IVTreeDistanceFunction hyracksDistanceFunctionDouble;
        private OptimizedScalarQuantizationSampleFile.Params quantizationParams;
        private ScalarVectorQuantizer quantizer; // nullable — created only for quantized indexes

        public VCTreeBulkLoaderAndGroupingNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc, UUID permitUUID, UUID materializedDataUUID) {
            this.ctx = ctx;
            this.partition = partition;
            this.storagePartition = resolveStoragePartition(partition);
            this.materializedDataUUID = materializedDataUUID;
        }

        /** Resolve storage partition from compute-storage map; fallback to task partition if map absent. */
        private int resolveStoragePartition(int taskPartition) {
            if (partitionsMap != null && taskPartition < partitionsMap.length && partitionsMap[taskPartition] != null
                    && partitionsMap[taskPartition].length > 0) {
                return partitionsMap[taskPartition][0];
            }
            return taskPartition;
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

                // Convert distance metric string to DistanceFunctionDouble
                distanceFunction = getDistanceFunctionDouble(distanceMetric);
                // Wrap for use in Hyracks modules
                hyracksDistanceFunctionDouble = wrapDistanceFunctionDouble(distanceFunction);
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
                // Use storage partition so the correct resource is opened when compute and storage partitions differ
                //                System.err.println("=== INITIALIZING INDEX-BASED STATIC STRUCTURE ACCESS ===");
                indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), storagePartition);
                indexHelper.open();

                // Get LSMVTree instance
                org.apache.hyracks.storage.common.IIndex indexInstance = indexHelper.getIndexInstance();
                //                System.err.println("Index instance type: "
                //                        + (indexInstance != null ? indexInstance.getClass().getName() : "null"));

                if (!(indexInstance instanceof ILSMIndex)) {
                    throw new HyracksDataException("Index is not an ILSMIndex instance, got: "
                            + (indexInstance != null ? indexInstance.getClass().getName() : "null"));
                }
                ILSMIndex lsmIndex = (ILSMIndex) indexInstance;

                if (!(lsmIndex instanceof LSMVTree)) {
                    throw new HyracksDataException(
                            "Index is not an LSMVTree instance, got: " + lsmIndex.getClass().getName());
                }
                LSMVTree = (LSMVTree) lsmIndex;
                //                System.err.println("LSMVTree instance obtained successfully");

                // Get static structure and create accessor
                LSMVTreeDiskComponent staticStructure = LSMVTree.getStaticStructure();
                IIndexAccessor accessor = staticStructure.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
                vcTreeAccessor = (VTree.VTreeAccessor) accessor;
                //                System.err.println("✅ VTreeAccessor created successfully");

                // Read quantization parameters from metadata file
                quantizationParams = readQuantizationParamsFromMetadata(indexHelper, vectorDimension);

                // Create quantizer for quantized indexes
                if (isQuantized && quantizationParams != null) {
                    OptimizedScalarQuantizationSampleFile.SimilarityFunction simFunc =
                            OptimizedScalarQuantizationSampleFile.fromDistanceMetric(distanceMetric);
                    this.quantizer = new ScalarVectorQuantizer(quantizationParams, simFunc);
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Reads quantization parameters from the metadata file.
         * Falls back to default values if parameters are not available.
         *
         * @param indexHelper     The index dataflow helper to get the resource
         * @param vectorDimension The vector dimension (used as fallback)
         * @return OptimizedScalarQuantizationSampleFile.Params with values from metadata or defaults
         * @throws HyracksDataException if reading fails
         */
        private OptimizedScalarQuantizationSampleFile.Params readQuantizationParamsFromMetadata(
                org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper indexHelper, int vectorDimension)
                throws HyracksDataException {
            try {
                // Get LocalResource from index helper
                LocalResource localResource = indexHelper.getResource();
                if (localResource == null) {
                    return createDefaultQuantizationParams(vectorDimension);
                }

                // Extract LSMVTreeLocalResource (handle DatasetLocalResource wrapper)
                IResource resource = localResource.getResource();
                LSMVTreeLocalResource vcResource = null;
                DatasetLocalResource datasetWrapper = null;

                if (resource instanceof DatasetLocalResource) {
                    datasetWrapper = (DatasetLocalResource) resource;
                    IResource wrappedResource = datasetWrapper.getResource();
                    if (wrappedResource instanceof LSMVTreeLocalResource) {
                        vcResource = (LSMVTreeLocalResource) wrappedResource;
                    }
                } else if (resource instanceof LSMVTreeLocalResource) {
                    vcResource = (LSMVTreeLocalResource) resource;
                }

                if (vcResource == null) {
                    return createDefaultQuantizationParams(vectorDimension);
                }

                // Read quantization parameters using public getter methods
                Integer bits = vcResource.getBits();
                Float confidenceInterval = vcResource.getConfidenceInterval();
                Float minQuantile = vcResource.getMinQuantile();
                Float maxQuantile = vcResource.getMaxQuantile();
                Float alpha = vcResource.getAlpha();
                Integer sampleCount = vcResource.getSampleCount();

                // Use defaults if any required parameter is missing
                if (!vcResource.hasQuantizationParams()) {
                    return createDefaultQuantizationParams(vectorDimension);
                }

                // Use provided sampleCount or default
                int finalSampleCount = (sampleCount != null) ? sampleCount : 20000;

                return new OptimizedScalarQuantizationSampleFile.Params(bits, vectorDimension, finalSampleCount,
                        confidenceInterval, minQuantile, maxQuantile, alpha);

            } catch (Exception e) {
                e.printStackTrace();
                return createDefaultQuantizationParams(vectorDimension);
            }
        }

        /**
         * Creates default quantization parameters as fallback.
         */
        private OptimizedScalarQuantizationSampleFile.Params createDefaultQuantizationParams(int vectorDimension) {
            return new OptimizedScalarQuantizationSampleFile.Params(7, // bits
                    vectorDimension, // vectorDimensions
                    20000, // sampleCount
                    0.99f, // confidenceInterval
                    -10.0f, // minQuantile
                    10.0f, // maxQuantile
                    6.35f // alpha: (127)/(10.0-(-10.0)) = 6.35
            );
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
         * Find the closest centroid using VTreeAccessor.
         * This follows the same approach as VectorTreeTestUtils.clusterRecords().
         *
         * @param queryVector Query vector to find closest centroid for
         * @return ClusterSearchResult containing closest centroid information
         * @throws HyracksDataException if search fails
         */
        private ClusterSearchResult findClosestCentroid(double[] queryVector) throws HyracksDataException {
            return findClosestCentroid(queryVector, null);
        }

        /**
         * Find the closest centroid, optionally computing quantized distance.
         *
         * @param queryVector Query vector to find closest centroid for
         * @param quantizedQueryVector Quantized form of queryVector (nullable)
         * @return ClusterSearchResult containing closest centroid information (with quantizedDistance if quantizer set)
         * @throws HyracksDataException if search fails
         */
        private ClusterSearchResult findClosestCentroid(double[] queryVector, double[] quantizedQueryVector)
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
                }

                // Validate accessor is initialized
                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VTreeAccessor not initialized");
                }

                // Validate distance function is initialized
                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunctionDouble not initialized");
                }

                // Use accessor to find closest leaf centroid with distance function
                // Pass quantized data through for quantized distance computation
                ClusterSearchResult result = vcTreeAccessor.findClosestLeafCentroid(queryVector,
                        hyracksDistanceFunctionDouble, quantizedQueryVector, quantizer);

                if (result == null) {
                    return null;
                }

                return result;

            } catch (IllegalArgumentException | IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Find the closest centroid using VTreeAccessor.
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
                }

                // Validate accessor is initialized
                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VTreeAccessor not initialized");
                }

                // Validate distance function is initialized
                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunctionDouble not initialized");
                }

                List<ClusterSearchResult> result =
                        vcTreeAccessor.findCloseLeafCentroid(queryVector, hyracksDistanceFunctionDouble, epi);

                if (result == null) {
                    return null;
                }

                return result;

            } catch (IllegalArgumentException | IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Find the closest centroid using VTreeAccessor.
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
                }

                // Validate accessor is initialized
                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VTreeAccessor not initialized");
                }

                // Validate distance function is initialized
                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunctionDouble not initialized");
                }

                List<ClusterSearchResult> result =
                        vcTreeAccessor.findCloseCentroidsFrontier(queryVector, hyracksDistanceFunctionDouble, epi);

                if (result == null) {
                    return null;
                }

                return result;

            } catch (IllegalArgumentException | IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Quantizes a vector using optimized scalar quantization with similarity-function awareness.
         * Only {@code quantizedBytes} from the result are stored; corrective multiplier is not persisted.
         *
         * @param embedding      The input embedding vector (double array)
         * @param params         Quantization parameters
         * @param distanceMetric Distance metric string to determine similarity function
         * @return QuantizedVector containing per-dimension quantized bytes (and metadata)
         */
        private OptimizedScalarQuantizationSampleFile.QuantizedVector quantizeVector(double[] embedding,
                OptimizedScalarQuantizationSampleFile.Params params, String distanceMetric) {
            if (embedding == null || params == null) {
                return null;
            }

            // Convert distance metric string to SimilarityFunction enum
            OptimizedScalarQuantizationSampleFile.SimilarityFunction similarityFunction =
                    OptimizedScalarQuantizationSampleFile.fromDistanceMetric(distanceMetric);

            // Use the new quantizeVector API with similarity function awareness
            return OptimizedScalarQuantizationSampleFile.quantizeVector(embedding, params, similarityFunction);
        }

        /**
         * Find the closest centroid using VTreeAccessor.
         * This follows the same approach as VectorTreeTestUtils.clusterRecords().
         *
         * @param queryVector Query vector to find closest centroid for
         * @return ClusterSearchResult containing closest centroid information
         * @throws HyracksDataException if search fails
         */
        private List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(double[] queryVector, double epi)
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
                }

                // Validate accessor is initialized
                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VTreeAccessor not initialized");
                }

                // Validate distance function is initialized
                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunctionDouble not initialized");
                }

                List<ClusterSearchResult> result = vcTreeAccessor.findCloseCentroidsLevelWiseGlobalSort(queryVector,
                        hyracksDistanceFunctionDouble, epi);

                if (result == null) {
                    return null;
                }

                return result;

            } catch (IllegalArgumentException | IllegalStateException e) {
                throw e;
            } catch (Exception e) {
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

                        // Dimension mismatch: returns empty array (length 0) vs missing field: returns null
                        if (embedding != null && embedding.length == 0) {
                            dimensionMismatchCount++;
                        }

                        if (embedding != null && embedding.length > 0) {
                            // Cross-pollination: assign this record to the top-M closest leaf centroids.
                            // M = 1 reproduces legacy single-centroid behavior. M > 1 emits M copies of the
                            // record (same vector, different (centroidId, distance) per copy). The downstream
                            // ExternalSort on (centroidId, distance) then groups them per cluster.
                            List<ClusterSearchResult> closeResults =
                                    findCloseCentroidsLevelWiseGlobalSort(embedding, levelwiseEpsilon);

                            if (closeResults != null && !closeResults.isEmpty()) {
                                int assignCount = Math.min(crossPollinationM, closeResults.size());

                                // Pre-compute the quantized embedding once per record (same for every copy).
                                double[] quantizedEmbedding =
                                        (quantizer != null) ? quantizer.quantize(embedding) : null;

                                // Quantize the persisted vector (qEmbed in the data tuple) once per record.
                                OptimizedScalarQuantizationSampleFile.QuantizedVector quantizedVector = null;
                                if (isQuantized) {
                                    quantizedVector = quantizeVector(embedding, quantizationParams, distanceMetric);
                                }

                                successfulQueries++;
                                for (int m = 0; m < assignCount; m++) {
                                    ClusterSearchResult result = closeResults.get(m);

                                    // For quantized indexes, ensure the stored D(x, C) matches the query-time
                                    // convention: distanceFunction(quantize(x), quantize(C)). Done per-centroid.
                                    if (isQuantized && quantizer != null && result.centroid != null
                                            && !result.hasQuantizedDistance()) {
                                        try {
                                            double[] qEmb = quantizedEmbedding != null ? quantizedEmbedding
                                                    : quantizer.quantize(embedding);
                                            double[] qCen = quantizer.quantize(result.centroid);
                                            double qDist = hyracksDistanceFunctionDouble.apply(qEmb, qCen);
                                            result = ClusterSearchResult.create(result.leafPageId, result.clusterIndex,
                                                    result.centroid, result.distance, result.centroidId,
                                                    result.directoryPageId, qDist);
                                        } catch (Exception qex) {
                                            System.err.println("WARNING: failed to compute quantized D(x,C): " + qex);
                                        }
                                    }

                                    ITupleReference transformedTuple =
                                            createTransformedTuple(tuple, result, quantizedVector);
                                    outputTransformedTuple(transformedTuple);
                                }
                            }
                        }

                    } catch (Exception e) {
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
            // Emit client-facing warning for dimension mismatches
            if (dimensionMismatchCount > 0) {
                LOGGER.warn("Vector index build: {} records skipped due to embedding dimension mismatch (expected {})",
                        dimensionMismatchCount, vectorDimension);
                IWarningCollector warningCollector = ctx.getWarningCollector();
                if (warningCollector.shouldWarn()) {
                    warningCollector
                            .warn(Warning.of(null, org.apache.asterix.common.exceptions.ErrorCode.COMPILATION_ERROR,
                                    String.format(
                                            "Vector index build: %d records skipped due to embedding dimension mismatch (expected %d)",
                                            dimensionMismatchCount, vectorDimension)));
                }
            }

            try {
                // Write any remaining output data before closing so the downstream sort
                // operator receives all data.
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
                    return;
                }

                // Sort centroid IDs to ensure order (lowest to highest)
                List<Integer> sortedCentroidIds = new ArrayList<>(centroidFiles.keySet());
                sortedCentroidIds.sort(Integer::compareTo);

                long totalTuplesStreamed = 0;

                // Stream each run file in centroid ID order
                for (int centroidId : sortedCentroidIds) {
                    FileReference runFile = centroidFiles.get(centroidId);
                    if (runFile != null) {
                        long tuplesInFile = streamSingleRunFile(runFile, centroidId);
                        totalTuplesStreamed += tuplesInFile;
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
         * @param runFile    File reference to the run file
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
         * @param centroidId  Centroid ID for logging
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
