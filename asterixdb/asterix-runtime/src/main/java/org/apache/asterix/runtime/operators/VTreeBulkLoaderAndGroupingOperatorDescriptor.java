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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.storage.OptimizedScalarQuantizationSampleFile;
import org.apache.asterix.common.storage.ScalarVectorQuantizer;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
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
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.LSMVTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeDiskComponent;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.utils.RngAcceptanceFilter;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Operator that assigns vectors to centroids from the VCTree static structure and emits
 * transformed tuples for downstream bulk load.
 * <p>
 * Responsibilities:
 * 1. Open the VCTree static structure via the LSM index
 * 2. For each input tuple, find the nearest centroid (level-wise search)
 * 3. Emit tuples with distance, centroid id, and optional quantization fields
 */
public class VTreeBulkLoaderAndGroupingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final IIndexDataflowHelperFactory indexHelperFactory;
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
     * Cross-pollination factor: each record is written into the M closest accepted leaf centroids at
     * bulk-load (M=1 = legacy single-closest). From the index WITH clause (cross_pollination_m).
     */
    private final int crossPollinationM;

    /**
     * SPTAG-style RNG diversity factor applied to the eps-filtered candidate list before replicating
     * (see {@link RngAcceptanceFilter}). From the index WITH clause (rng_factor); 1.0 = canonical SPTAG.
     */
    private final double rngFactor;

    // Maps task (compute) partition to storage partition(s) for index resource lookup
    private final int[][] partitionsMap;

    // Distance function constants
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2 = UTF8StringPointable.generateUTF8Pointable("l2");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
            UTF8StringPointable.generateUTF8Pointable("euclidean");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("l2_squared");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("euclidean_squared");
    private static final UTF8StringPointable COSINE_FORMAT = UTF8StringPointable.generateUTF8Pointable("cosine");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot");

    // Serializable distance function implementations
    private static class EuclideanDistanceFunction implements IVTreeDistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceCalculation.euclidean(a, b);
        }
    }

    private static class EuclideanSquaredDistanceFunction implements IVTreeDistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceCalculation.euclideanSquared(a, b);
        }
    }

    private static class CosineDistanceFunction implements IVTreeDistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceCalculation.cosineDistance(a, b);
        }
    }

    private static class DotProductDistanceFunction implements IVTreeDistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        /** Returns -dot(a,b) so that minimizing "distance" equals maximizing dot product (MIPS). */
        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return -VectorDistanceCalculation.dotProduct(a, b);
        }
    }

    // Distance function hash map
    private static final Map<Integer, IVTreeDistanceFunction> DISTANCE_MAP = Map.of(EUCLIDEAN_DISTANCE.hash(),
            new EuclideanDistanceFunction(), EUCLIDEAN_DISTANCE_L2.hash(), new EuclideanDistanceFunction(),
            EUCLIDEAN_DISTANCE_SQUARED.hash(), new EuclideanSquaredDistanceFunction(),
            EUCLIDEAN_DISTANCE_L2_SQUARED.hash(), new EuclideanSquaredDistanceFunction(), COSINE_FORMAT.hash(),
            new CosineDistanceFunction(), DOT_PRODUCT_FORMAT.hash(), new DotProductDistanceFunction());

    /**
     * Convert distance metric string to IVTreeDistanceFunction implementation.
     *
     * @param distanceType Distance metric string (e.g., "euclidean", "cosine similarity", etc.)
     * @return IVTreeDistanceFunction implementation
     * @throws IllegalArgumentException if distance type is not supported
     */
    private static IVTreeDistanceFunction getDistanceFunction(String distanceType) {
        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(distanceType.toLowerCase());
        IVTreeDistanceFunction func = DISTANCE_MAP
                .get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(), formatPointable.getStartOffset()));
        return func;
    }

    /**
     * Convert IVTreeDistanceFunction to IVTreeDistanceFunction for use in Hyracks modules.
     *
     * @param distanceFunction AsterixDB IVTreeDistanceFunction
     * @return IVTreeDistanceFunction wrapper
     */
    private static IVTreeDistanceFunction wrapDistanceFunction(IVTreeDistanceFunction distanceFunction) {
        return distanceFunction::apply;
    }

    public VTreeBulkLoaderAndGroupingOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, RecordDescriptor inputRecordDescriptor,
            RecordDescriptor outputRecordDescriptor, IScalarEvaluatorFactory args, String distanceMetric,
            int vectorDimension, int numPrimaryKeys, int numIncludeFields, boolean isQuantized, int[][] partitionsMap,
            double levelwiseEpsilon, int crossPollinationM, double rngFactor) {
        super(spec, 1, 1); // one input activity, one output activity (forwards transformed tuples downstream)
        this.indexHelperFactory = indexHelperFactory;
        this.inputRecDesc = inputRecordDescriptor;
        this.outputRecDesc = outputRecordDescriptor;
        this.args = args;
        this.distanceMetric = distanceMetric;
        this.vectorDimension = vectorDimension; // Default to 384 if invalid
        this.numPrimaryKeys = numPrimaryKeys;
        this.numIncludeFields = numIncludeFields;
        this.isQuantized = isQuantized;
        this.partitionsMap = partitionsMap;
        this.levelwiseEpsilon = levelwiseEpsilon > 0.0 && Double.isFinite(levelwiseEpsilon) ? levelwiseEpsilon : 0.3;
        this.crossPollinationM = Math.max(1, crossPollinationM);
        this.rngFactor = rngFactor;

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
                        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asShortBuffer().put(shorts);
                        combinedValues[3] = bytes;
                    } else if (qBytes instanceof int[]) {
                        // Convert int[] to byte[] for serialization
                        int[] ints = (int[]) qBytes;
                        byte[] bytes = new byte[ints.length * 4];
                        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(ints);
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
            eval.evaluate((IFrameTupleReference) tuple, inputVal);

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
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        VCTreeBulkLoaderAndGroupingNodePushable pushable = new VCTreeBulkLoaderAndGroupingNodePushable(ctx, partition);

        // Set output record descriptor for the pushable
        pushable.setOutputRecordDescriptor(outputRecDesc);

        return pushable;
    }

    /**
     * Node pushable implementation for VTreeBulkLoaderAndGroupingOperatorDescriptor.
     */
    private class VCTreeBulkLoaderAndGroupingNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        /** Storage partition for index resource lookup (from compute-storage map). */
        private final int storagePartition;
        private IIndexDataflowHelper indexHelper;
        private LSMVTree LSMVTree;
        private VTree.VTreeAccessor vcTreeAccessor;
        int dimensionMismatchCount = 0;

        // Output infrastructure for transformed tuples
        private FrameTupleAppender outputAppender;
        private RecordDescriptor outputRecDesc;
        private IVTreeDistanceFunction distanceFunction;
        private IVTreeDistanceFunction hyracksDistanceFunction;
        private OptimizedScalarQuantizationSampleFile.Params quantizationParams;
        private ScalarVectorQuantizer quantizer; // nullable — created only for quantized indexes

        public VCTreeBulkLoaderAndGroupingNodePushable(IHyracksTaskContext ctx, int partition) {
            this.ctx = ctx;
            this.storagePartition = resolveStoragePartition(partition);
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
                // Initialize output infrastructure for transformed tuples
                initializeOutputInfrastructure();

                // Convert distance metric string to IVTreeDistanceFunction
                distanceFunction = getDistanceFunction(distanceMetric);
                // Wrap for use in Hyracks modules
                hyracksDistanceFunction = wrapDistanceFunction(distanceFunction);

                // Open the output writer
                if (writer != null) {
                    writer.open();
                }

                // Initialize index helper to access static structure via LSM index system
                // Use storage partition so the correct resource is opened when compute and storage partitions differ
                indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), storagePartition);
                indexHelper.open();

                // Get LSMVTree instance
                IIndex indexInstance = indexHelper.getIndexInstance();

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

                // Get static structure and create accessor
                LSMVTreeDiskComponent staticStructure = LSMVTree.getStaticStructure();
                IIndexAccessor accessor = staticStructure.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
                vcTreeAccessor = (VTree.VTreeAccessor) accessor;

                // Read quantization parameters from metadata file
                quantizationParams = readQuantizationParamsFromMetadata(indexHelper, vectorDimension);

                // Create quantizer for quantized indexes
                if (isQuantized) {
                    OptimizedScalarQuantizationSampleFile.SimilarityFunction simFunc =
                            OptimizedScalarQuantizationSampleFile.fromDistanceMetric(distanceMetric);
                    this.quantizer = new ScalarVectorQuantizer(quantizationParams, simFunc);
                }

            } catch (Exception e) {
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
                IIndexDataflowHelper indexHelper, int vectorDimension) throws HyracksDataException {
            try {
                // Get LocalResource from index helper
                LocalResource localResource = indexHelper.getResource();
                if (localResource == null) {
                    return createDefaultQuantizationParams(vectorDimension);
                }

                // Extract LSMVTreeLocalResource (handle DatasetLocalResource wrapper)
                IResource resource = localResource.getResource();
                LSMVTreeLocalResource vcResource = null;

                if (resource instanceof DatasetLocalResource) {
                    IResource wrappedResource = ((DatasetLocalResource) resource).getResource();
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
                return createDefaultQuantizationParams(vectorDimension);
            }
        }

        /**
         * Creates default quantization parameters as fallback.
         */
        private OptimizedScalarQuantizationSampleFile.Params createDefaultQuantizationParams(int vectorDimension) {
            return new OptimizedScalarQuantizationSampleFile.Params(8, // bits
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
                VSizeFrame outputFrame = new VSizeFrame(ctx);
                outputAppender = new FrameTupleAppender(outputFrame);
            } catch (Exception e) {
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

            OptimizedScalarQuantizationSampleFile.SimilarityFunction similarityFunction =
                    OptimizedScalarQuantizationSampleFile.fromDistanceMetric(distanceMetric);

            return OptimizedScalarQuantizationSampleFile.quantizeVector(embedding, params, similarityFunction);
        }

        private List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(double[] queryVector, double epi)
                throws HyracksDataException {
            try {
                if (queryVector == null) {
                    throw new IllegalArgumentException("Query vector cannot be null");
                }

                if (queryVector.length == 0) {
                    throw new IllegalArgumentException("Query vector cannot be empty");
                }

                if (vcTreeAccessor == null) {
                    throw new IllegalStateException("VTreeAccessor not initialized");
                }

                if (distanceFunction == null) {
                    throw new IllegalStateException("DistanceFunction not initialized");
                }

                return vcTreeAccessor.findCloseCentroidsLevelWiseGlobalSort(queryVector, hyracksDistanceFunction, epi);

            } catch (IllegalArgumentException | IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {

            try {
                FrameTupleAccessor fta = new FrameTupleAccessor(inputRecDesc);
                fta.reset(buffer);

                int tupleCount = fta.getTupleCount();

                if (tupleCount == 0) {
                    return;
                }

                for (int i = 0; i < tupleCount; i++) {
                    FrameTupleReference tuple = new FrameTupleReference();
                    tuple.reset(fta, i);

                    try {
                        double[] embedding = extractEmbeddingFromTuple(tuple, ctx);

                        if (embedding != null && embedding.length == 0) {
                            dimensionMismatchCount++;
                        }

                        if (embedding != null && embedding.length > 0) {
                            double[] quantizedEmbedding = null;
                            if (quantizer != null) {
                                quantizedEmbedding = quantizer.quantize(embedding);
                            }

                            List<ClusterSearchResult> closeResults =
                                    findCloseCentroidsLevelWiseGlobalSort(embedding, levelwiseEpsilon);
                            // Cross-pollination: accept up to crossPollinationM closest centroids, thinned
                            // by the SPTAG-style RNG diversity rule, and write one replica of the record per
                            // accepted centroid. M=1 reproduces the legacy single-closest behavior.
                            List<ClusterSearchResult> acceptedResults = RngAcceptanceFilter.accept(closeResults,
                                    hyracksDistanceFunction, rngFactor, crossPollinationM, null);
                            for (ClusterSearchResult result : acceptedResults) {
                                OptimizedScalarQuantizationSampleFile.QuantizedVector quantizedVector = null;
                                if (isQuantized) {
                                    quantizedVector = quantizeVector(embedding, quantizationParams, distanceMetric);

                                    if (quantizer != null && result.centroid != null
                                            && !result.hasQuantizedDistance()) {
                                        try {
                                            double[] qEmb = quantizedEmbedding != null ? quantizedEmbedding
                                                    : quantizer.quantize(embedding);
                                            double[] qCen = quantizer.quantize(result.centroid);
                                            double qDist = hyracksDistanceFunction.apply(qEmb, qCen);
                                            result = ClusterSearchResult.create(result.leafPageId, result.clusterIndex,
                                                    result.centroid, result.distance, result.centroidId,
                                                    result.directoryPageId, qDist);
                                        } catch (Exception qex) {
                                        }
                                    }
                                }
                                ITupleReference transformedTuple =
                                        createTransformedTuple(tuple, result, quantizedVector);
                                outputTransformedTuple(transformedTuple);
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
                    if (!outputAppender.append(transformedTuple)) {
                        FrameUtils.flushFrame(outputAppender.getBuffer(), writer);
                        outputAppender.reset(new VSizeFrame(ctx), true);
                        outputAppender.append(transformedTuple);
                    }
                }
            } catch (Exception e) {
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
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            if (dimensionMismatchCount > 0) {
                LOGGER.warn("Vector index build: {} records skipped due to embedding dimension mismatch (expected {})",
                        dimensionMismatchCount, vectorDimension);
                IWarningCollector warningCollector = ctx.getWarningCollector();
                if (warningCollector.shouldWarn()) {
                    warningCollector.warn(Warning.of(null, ErrorCode.COMPILATION_ERROR,
                            String.format(
                                    "Vector index build: %d records skipped due to embedding dimension mismatch (expected %d)",
                                    dimensionMismatchCount, vectorDimension)));
                }
            }

            try {
                if (writer != null && outputAppender != null) {
                    outputAppender.write(writer, false);
                }

                if (vcTreeAccessor != null) {
                    vcTreeAccessor = null;
                }
                if (indexHelper != null) {
                    indexHelper.close();
                }

                if (writer != null) {
                    writer.close();
                }

            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            try {
                if (indexHelper != null) {
                    indexHelper.close();
                }
            } catch (Exception e) {
            }
        }
    }
}
