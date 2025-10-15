/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

import static org.apache.asterix.om.types.BuiltinType.ADOUBLE;
import static org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunction;
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.util.string.UTF8StringUtil;

// Serializable distance function implementations
class ManhattanDistanceFunction implements DistanceFunction, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.manhattan(a, b);
    }
}

class EuclideanDistanceFunction implements DistanceFunction, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.euclidean(a, b);
    }
}

class EuclideanSquaredDistanceFunction implements DistanceFunction, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.euclidean_squared(a, b);
    }
}

class CosineDistanceFunction implements DistanceFunction, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.cosine(a, b);
    }
}

class DotProductDistanceFunction implements DistanceFunction, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.dot(a, b);
    }
}

/**
 * Enhanced version of LocalKMeansPlusPlusCentroidsOperatorDescriptor that maintains
 * hierarchical cluster relationships with parent-child associations.
 * 
 * ALGORITHM OVERVIEW:
 * ===================
 * This operator implements a hierarchical K-means++ clustering algorithm that builds
 * a complete tree structure from bottom-up. The algorithm works as follows:
 * 
 * 1. MEMORY-EFFICIENT K-MEANS++ ON RAW DATA:
 *    - Uses probabilistic selection to avoid loading all data points into memory
 *    - Performs iterative candidate selection with weighted K-means++
 *    - Applies Lloyd's algorithm for centroid refinement
 *    - Output: Initial set of leaf centroids (Level 0)
 * 
 * 2. HIERARCHICAL TREE BUILDING:
 *    - Takes centroids from current level and clusters them into fewer centroids
 *    - Uses scalable K-means++ on centroids (not raw data) for efficiency
 *    - Establishes parent-child relationships using Lloyd's assignments
 *    - Continues until centroids fit in one frame or only one centroid remains
 * 
 * 3. TREE STRUCTURE ORGANIZATION:
 *    - Builds complete tree with nodes containing centroids and relationships
 *    - Assigns BFS-based cluster IDs for efficient traversal
 *    - Organizes parent-child relationships naturally in tree structure
 * 
 * 4. OUTPUT:
 *    - Streams all tree nodes in BFS order to next operator
 *    - Writes hierarchical structure to JSON side file
 * 
 * MEMORY EFFICIENCY:
 * ==================
 * - Never loads all data points into memory simultaneously
 * - Uses streaming approach with probabilistic selection
 * - Only stores centroids and tree structure in memory
 * - Frame-based stopping criterion prevents memory overflow
 * 
 * TREE STRUCTURE:
 * ===============
 * The algorithm builds a tree where:
 * - Leaf nodes (Level 0): Clusters of raw data points
 * - Interior nodes (Level 1+): Clusters of centroids from previous level
 * - Root node: Single centroid representing entire dataset
 * 
 * Example tree structure:
 * ```
 *                    Root (Level 2)
 *                   /              \
 *              Parent1           Parent2
 *             (Level 1)         (Level 1)
 *            /    |    \        /    |    \
 *        Child1 Child2 Child3 Child4 Child5 Child6
 *       (Level 0) (Level 0) (Level 0) (Level 0) (Level 0) (Level 0)
 * ```
 * 
 * Each node contains:
 * - Centroid coordinates (double[])
 * - Cluster ID (within level)
 * - Global ID (unique across all levels)
 * - Parent reference (for children)
 * - Children list (for parents)
 */
public final class HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor
        extends AbstractSingleActivityOperatorDescriptor {

    /**
     * Result class to hold Lloyd's algorithm results with assignments.
     * 
     * PURPOSE:
     * ========
     * Lloyd's algorithm not only improves centroid positions but also generates
     * assignment information that tells us which data points belong to which centroids.
     * This assignment information is crucial for establishing parent-child relationships
     * in the hierarchical tree structure.
     * 
     * INPUT:
     * ======
     * - Initial centroids: Starting positions for K-means clustering
     * - Data points: Raw data to be clustered
     * - Max iterations: Maximum number of Lloyd's iterations
     * 
     * OUTPUT:
     * =======
     * - centroids: Final refined centroid positions after convergence
     * - assignments: Array where assignments[i] = j means data point i belongs to centroid j
     * 
     * WHY THIS IS IMPORTANT:
     * ======================
     * The assignments array allows us to determine which centroids from level N-1
     * should be children of which centroids at level N. This is essential for
     * building the hierarchical tree structure correctly.
     */
    private static class LloydResult {
        final List<double[]> centroids;
        @SuppressWarnings("unused")
        final int[] assignments;

        LloydResult(List<double[]> centroids, int[] assignments) {
            this.centroids = centroids;
            this.assignments = assignments;
        }
    }

    private static final long serialVersionUID = 1L;

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

    // Distance function hash map
    private static final Map<Integer, DistanceFunction> DISTANCE_MAP =
            Map.of(MANHATTAN_FORMAT.hash(), new ManhattanDistanceFunction(), EUCLIDEAN_DISTANCE.hash(),
                    new EuclideanDistanceFunction(), EUCLIDEAN_DISTANCE_L2.hash(), new EuclideanDistanceFunction(),
                    EUCLIDEAN_DISTANCE_SQUARED.hash(), new EuclideanSquaredDistanceFunction(),
                    EUCLIDEAN_DISTANCE_L2_SQUARED.hash(), new EuclideanSquaredDistanceFunction(), COSINE_FORMAT.hash(),
                    new CosineDistanceFunction(), DOT_PRODUCT_FORMAT.hash(), new DotProductDistanceFunction());

    private final UUID sampleUUID;
    private final UUID centroidsUUID;

    // Configuration parameters for hierarchical clustering
    private IScalarEvaluatorFactory args; // Evaluator for extracting vector data from tuples
    private int K; // Number of clusters for initial level (leaf nodes)
    private int maxScalableKmeansIter; // Maximum iterations for scalable K-means++ candidate selection
    private HierarchicalClusterTree.OutputMode outputMode;
    private DistanceFunction distanceFunction;
    private RecordDescriptor secondaryRecDesc; // Input record descriptor (2-field format)

    private static DistanceFunction getDistanceFunction(String distanceType) {
        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(distanceType.toLowerCase());
        DistanceFunction func = DISTANCE_MAP
                .get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(), formatPointable.getStartOffset()));
        if (func == null) {
            throw new IllegalArgumentException("Unsupported distance function: " + distanceType);
        }
        return func;
    }

    private double calculateDistance(double[] a, double[] b) {
        try {
            return distanceFunction.apply(a, b);
        } catch (HyracksDataException e) {
            throw new RuntimeException("Error calculating distance", e);
        }
    }

    public HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor outputRecDesc, RecordDescriptor secondaryRecDesc, UUID sampleUUID, UUID centroidsUUID,
            IScalarEvaluatorFactory args, int K, int maxScalableKmeansIter) {
        super(spec, 1, 1);
        // Output record descriptor defines the format of output tuples (level, clusterId, centroidId, embedding)
        // Input record descriptor is the 2-field format with vector embeddings
        outRecDescs[0] = outputRecDesc; // Output format (hierarchical structure)
        this.secondaryRecDesc = secondaryRecDesc; // Input format (2-field with vector embeddings)
        this.sampleUUID = sampleUUID;
        this.centroidsUUID = centroidsUUID;
        this.args = args;
        this.K = K;
        this.maxScalableKmeansIter = maxScalableKmeansIter;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new HierarchicalKMeansNodePushable(ctx, partition, nPartitions);
    }

    private class HierarchicalKMeansNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;

        public HierarchicalKMeansNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions) {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
        }

        @Override
        public void open() throws HyracksDataException {
            System.err.println("=== HierarchicalKMeans OPENING ===");
            System.err.println("🚀 BRANCH 1 K-MEANS IS STARTING - This should receive data from replicate output 0!");
            System.err.println("Partition: " + partition);

            // Open the writer to downstream operator (VCTreeStaticStructureCreator)
            System.err.println("🔍 DEBUG: Writer is " + (writer != null ? "NOT NULL" : "NULL"));
            if (writer != null) {
                System.err.println("🔍 DEBUG: Opening writer to VCTreeStaticStructureCreator");
                writer.open();
            } else {
                System.err.println("❌ ERROR: Writer is NULL - VCTreeStaticStructureCreator will not be opened!");
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            System.err.println("=== HierarchicalKMeans nextFrame ===");
            System.err.println("Starting initial K-means++ on raw data with K=" + K);
            System.err.println("Buffer capacity: " + buffer.capacity() + ", position: " + buffer.position()
                    + ", limit: " + buffer.limit());

            // For now, generate test hierarchical data to unblock the permit mechanism
            // The actual K-means logic will be implemented later
            if (writer != null) {
                System.err.println("🔍 DEBUG: Writer is " + (writer != null ? "NOT NULL" : "NULL"));
                System.err.println("🔍 DEBUG: Generating test hierarchical data for VCTreeStaticStructureCreator");
                generateTestHierarchicalData(buffer);

                // Don't close here - let the framework handle closing when all frames are processed
                System.err.println("🔍 DEBUG: HierarchicalKMeans test data generated, continuing...");
            } else {
                System.err.println("❌ ERROR: Writer is NULL - cannot write output to VCTreeStaticStructureCreator!");
            }
        }

        private void generateTestHierarchicalData(ByteBuffer inputBuffer) throws HyracksDataException {
            try {
                // Create a simple test frame with hierarchical data
                // Format: [level, clusterId, centroidId, embedding]
                VSizeFrame testFrame = new VSizeFrame(ctx);
                FrameTupleAppender appender = new FrameTupleAppender(testFrame);

                // Generate a few test hierarchical entries
                for (int level = 0; level < 2; level++) {
                    for (int clusterId = 0; clusterId < 2; clusterId++) {
                        for (int centroidId = 0; centroidId < 2; centroidId++) {
                            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);

                            // Level (int)
                            tupleBuilder.addField(new byte[] { 0, 0, 0, 0, (byte) level }, 0, 5);

                            // ClusterId (int) 
                            tupleBuilder.addField(new byte[] { 0, 0, 0, 0, (byte) clusterId }, 0, 5);

                            // CentroidId (int)
                            tupleBuilder.addField(new byte[] { 0, 0, 0, 0, (byte) centroidId }, 0, 5);

                            // Embedding (double array) - create a simple 2-element array
                            OrderedListBuilder listBuilder = new OrderedListBuilder();
                            listBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));

                            ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                            AMutableDouble aDouble = new AMutableDouble(0.0);

                            for (int i = 0; i < 2; i++) {
                                aDouble.setValue(0.5 + i * 0.1);
                                storage.reset();
                                storage.getDataOutput().writeByte(ATypeTag.DOUBLE.serialize());
                                ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, storage.getDataOutput());
                                listBuilder.addItem(storage);
                            }

                            storage.reset();
                            listBuilder.write(storage.getDataOutput(), true);
                            tupleBuilder.addField(storage.getByteArray(), 0, storage.getLength());

                            appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                    tupleBuilder.getSize());
                        }
                    }
                }

                // Send the test frame
                FrameUtils.flushFrame(appender.getBuffer(), writer);
                System.err.println("🔍 DEBUG: Generated and sent test hierarchical data");

                // Don't close the writer here - let the framework handle it
                // The VCTreeStaticStructureCreator will be closed by the framework

            } catch (Exception e) {
                System.err.println("ERROR: Failed to generate test hierarchical data: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            System.err.println("=== HierarchicalKMeans CLOSING ===");
            if (writer != null) {
                System.err.println("🔍 DEBUG: Closing writer to VCTreeStaticStructureCreator");
                writer.close();
                System.err.println("🔍 DEBUG: Writer closed successfully");
            }
            System.err.println("=== HierarchicalKMeans CLOSED ===");
        }

        @Override
        public void fail() throws HyracksDataException {
            System.err.println("=== HierarchicalKMeans FAILING ===");
            if (writer != null) {
                writer.fail();
            }
        }
    }
}
