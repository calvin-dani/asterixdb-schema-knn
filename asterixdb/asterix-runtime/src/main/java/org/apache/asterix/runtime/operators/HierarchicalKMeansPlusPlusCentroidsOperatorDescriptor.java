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
import static org.apache.asterix.om.types.BuiltinType.AFLOAT;
import static org.apache.asterix.om.types.BuiltinType.AINT32;
import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;
import static org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunction;
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;

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
public final class HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor extends AbstractOperatorDescriptor {

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
        outRecDescs[0] = outputRecDesc;       // Output format (hierarchical structure)
        this.secondaryRecDesc = secondaryRecDesc; // Input format (2-field with vector embeddings)
        this.sampleUUID = sampleUUID;
        this.centroidsUUID = centroidsUUID;
        this.args = args;
        this.K = K;
        this.maxScalableKmeansIter = maxScalableKmeansIter;
    }



    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StoreCentroidsActivity sa = new StoreCentroidsActivity(new ActivityId(odId, 0));
        FindCandidatesActivity ca = new FindCandidatesActivity(new ActivityId(odId, 1));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ca);
        builder.addTargetEdge(0, ca, 0);

        builder.addBlockingEdge(sa, ca);
        builder.addTargetEdge(0, ca, 0);
    }

    /**
     * First activity in the hierarchical clustering pipeline.
     * 
     * PURPOSE:
     * ========
     * This activity processes the input data stream and stores the first data point
     * as the initial centroid. It also materializes all data points to disk for
     * subsequent processing by the FindCandidatesActivity.
     * 
     * WHY WE NEED THIS:
     * ================
     * 1. MEMORY EFFICIENCY: We can't load all data points into memory at once
     * 2. STREAMING PROCESSING: Data comes in frames, we need to process it incrementally
     * 3. INITIAL CENTROID: K-means++ needs at least one centroid to start the process
     * 4. DATA PERSISTENCE: We need to store data for multiple passes (cost calculation, selection, Lloyd's)
     * 
     * DATA FLOW:
     * ==========
     * Input: Stream of tuples containing vector data
     * Output: 
     *   - First data point stored as initial centroid in CentroidsState
     *   - All data points materialized to disk via MaterializerTaskState
     * 
     * ALGORITHM:
     * ==========
     * 1. Take first tuple from input stream
     * 2. Extract vector data using evaluator
     * 3. Store as initial centroid in CentroidsState
     * 4. Materialize all subsequent tuples to disk for later processing
     */
    protected class StoreCentroidsActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private FrameTupleAccessor fta;
        private FrameTupleReference tuple;

        protected StoreCentroidsActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                IScalarEvaluator eval;
                IPointable inputVal;
                CentroidsState state;
                boolean first;
                private MaterializerTaskState materializedSample;
                KMeansUtils kMeansUtils;

                @Override
                public void open() throws HyracksDataException {
                    // Initialize data persistence for multiple passes over the data
                    materializedSample = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(sampleUUID, partition));
                    materializedSample.open(ctx);

                    // Initialize centroid storage for the first centroid
                    state = new CentroidsState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(centroidsUUID, partition));

                    // Initialize evaluator for extracting vector data from tuples
                    eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                    inputVal = new VoidPointable();
                    // Use secondaryRecDesc directly for now (2-field format with vector embeddings)
                    RecordDescriptor inputRecDesc = secondaryRecDesc;
                    fta = new FrameTupleAccessor(inputRecDesc);
                    tuple = new FrameTupleReference();
                    first = true;
                    kMeansUtils = new KMeansUtils(new VoidPointable(), new ArrayBackedValueStorage());
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    fta.reset(buffer);
                    int tupleCount = fta.getTupleCount();

                    if (first) {
                        // CRITICAL: Perform initial K-means++ on raw data to generate K centroids
                        // This is essential for hierarchical clustering - we need multiple centroids
                        // to start the hierarchical tree building process
                        System.err.println("Starting initial K-means++ on raw data with K=" + K);

                        // Perform K-means++ on the raw data
                        Random rand = new Random();
                        int maxKMeansIterations = 20;
                        List<double[]> initialCentroids = performInitialKMeansPlusPlus(buffer, fta, tuple, eval,
                                inputVal, kMeansUtils, K, rand, maxKMeansIterations);

                        // Store all generated centroids
                        for (double[] centroid : initialCentroids) {
                            state.addCentroid(centroid);
                        }

                        System.err.println("Generated " + initialCentroids.size()
                                + " initial centroids for hierarchical clustering");
                        first = false;
                    }
                    // Materialize all data to disk for subsequent processing passes
                    // This allows us to make multiple passes over the data without loading it all into memory
                    materializedSample.appendFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (state != null) {
                        ctx.setStateObject(state);
                    }
                    if (materializedSample != null) {
                        materializedSample.close();
                        ctx.setStateObject(materializedSample);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }

        /**
         * Performs initial K-means++ on raw data to generate K centroids
         * 
         * FIX: This method was added to replace the previous broken approach where only the first
         * data point was stored as a centroid. The original code was only generating 1 centroid
         * instead of K centroids, which caused the hierarchical clustering to fail.
         * 
         * The method implements:
         * 1. K-means++ initialization (weighted random selection of initial centroids)
         * 2. Lloyd's algorithm for centroid refinement and convergence
         * 3. Proper handling of empty datasets and edge cases
         * 
         * This is used by StoreCentroidsActivity to generate the initial set of centroids
         * for hierarchical clustering
         */
        private List<double[]> performInitialKMeansPlusPlus(ByteBuffer buffer, FrameTupleAccessor fta,
                FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal, KMeansUtils kMeansUtils, int k,
                Random rand, int maxIterations) throws HyracksDataException {

            if (k <= 0) {
                return new ArrayList<>();
            }

            List<double[]> centroids = new ArrayList<>();
            List<double[]> allPoints = new ArrayList<>();

            // First pass: collect all data points
            fta.reset(buffer);
            int tupleCount = fta.getTupleCount();

            for (int i = 0; i < tupleCount; i++) {
                tuple.reset(fta, i);
                eval.evaluate(tuple, inputVal);
                ListAccessor listAccessorConstant = new ListAccessor();
                
                if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                        .isListType()) {
                    continue; // Skip unsupported types
                }
                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                try {
                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);
                    allPoints.add(point);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            if (allPoints.isEmpty()) {
                return centroids;
            }

            System.err.println(
                    "performInitialKMeansPlusPlus: collected " + allPoints.size() + " data points, target k = " + k);

            // K-means++ initialization
            // 1. Choose first centroid randomly
            int firstIdx = rand.nextInt(allPoints.size());
            centroids.add(Arrays.copyOf(allPoints.get(firstIdx), allPoints.get(firstIdx).length));

            // 2. Choose remaining centroids using weighted selection
            for (int i = 1; i < k; i++) {
                double[] distances = new double[allPoints.size()];
                double totalDistance = 0.0;

                // Calculate minimum distance to existing centroids for each point
                for (int j = 0; j < allPoints.size(); j++) {
                    double minDist = Double.POSITIVE_INFINITY;
                    for (double[] centroid : centroids) {
                        double dist = euclidean_squared(allPoints.get(j), centroid);
                        minDist = Math.min(minDist, dist);
                    }
                    distances[j] = minDist;
                    totalDistance += minDist;
                }


                // Weighted random selection
                double r = rand.nextDouble() * totalDistance;
                double cumulativeDistance = 0.0;
                int selectedIdx = 0;
                for (int j = 0; j < allPoints.size(); j++) {
                    cumulativeDistance += distances[j];
                    if (cumulativeDistance >= r) {
                        selectedIdx = j;
                        break;
                    }
                }

                centroids.add(Arrays.copyOf(allPoints.get(selectedIdx), allPoints.get(selectedIdx).length));
            }

            // 3. Lloyd's algorithm for refinement
            for (int iter = 0; iter < maxIterations; iter++) {
                // Assign points to closest centroids
                int[] assignments = new int[allPoints.size()];
                for (int i = 0; i < allPoints.size(); i++) {
                    double minDist = Double.POSITIVE_INFINITY;
                    int closestCentroid = 0;
                    for (int j = 0; j < centroids.size(); j++) {
                        double dist = euclidean_squared(allPoints.get(i), centroids.get(j));
                        if (dist < minDist) {
                            minDist = dist;
                            closestCentroid = j;
                        }
                    }
                    assignments[i] = closestCentroid;
                }

                // Update centroids
                double[][] newCentroids = new double[k][allPoints.get(0).length];
                int[] counts = new int[k];

                for (int i = 0; i < allPoints.size(); i++) {
                    int centroidIdx = assignments[i];
                    for (int d = 0; d < allPoints.get(i).length; d++) {
                        newCentroids[centroidIdx][d] += allPoints.get(i)[d];
                    }
                    counts[centroidIdx]++;
                }

                // Check for convergence
                boolean converged = true;
                for (int i = 0; i < k; i++) {
                    if (counts[i] > 0) {
                        for (int d = 0; d < newCentroids[i].length; d++) {
                            newCentroids[i][d] /= counts[i];
                        }
                        // Check if centroid moved significantly
                        double dist = euclidean_squared(centroids.get(i), newCentroids[i]);
                        if (dist > 1e-4) {
                            converged = false;
                        }
                        centroids.set(i, newCentroids[i]);
                    }
                }

                if (converged) {
                    break;
                }
            }

            System.err.println("=== INITIAL K-MEANS++ COMPLETE ===");
            System.err.println("Generated " + centroids.size() + " centroids (target was " + k + ")");
            System.err.println("Success rate: " + String.format("%.1f", (double)centroids.size() / k * 100) + "%");
            System.err.println("=== END INITIAL K-MEANS++ ===");

            return centroids;
        }
    }

    /**
     * Main activity that performs the complete hierarchical clustering algorithm.
     * 
     * PURPOSE:
     * ========
     * This is the core activity that implements the entire hierarchical K-means++ algorithm.
     * It processes the materialized data from StoreCentroidsActivity and builds a complete
     * hierarchical tree structure with parent-child relationships.
     * 
     * ALGORITHM STEPS:
     * ================
     * 1. MEMORY-EFFICIENT K-MEANS++ ON RAW DATA:
     *    - Uses probabilistic selection to avoid loading all data into memory
     *    - Performs iterative candidate selection with weighted K-means++
     *    - Applies Lloyd's algorithm for centroid refinement
     *    - Result: Initial set of leaf centroids (Level 0)
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
     * 4. OUTPUT GENERATION:
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
     * TREE BUILDING PROCESS:
     * ======================
     * The algorithm builds a tree from bottom-up:
     * 
     * Level 0 (Leaf nodes): Raw data points clustered using K-means++
     * Level 1 (Interior): Level 0 centroids clustered into fewer centroids
     * Level 2 (Interior): Level 1 centroids clustered into even fewer centroids
     * ...
     * Level N (Root): Single centroid representing entire dataset
     * 
     * Each level uses the centroids from the previous level as "data points"
     * for the next level of clustering.
     */
    protected class FindCandidatesActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected FindCandidatesActivity(ActivityId id) {
            super(id);
        }

        @Override
        @SuppressWarnings("squid:S1188")
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    System.err.println("=== HIERARCHICAL K-MEANS INITIALIZE CALLED ===");
                    System.err.println("=== PARTITION: " + partition + " ===");
                    System.err.println("=== ACTIVITY: FindCandidatesActivity ===");
                    System.err.println("=== THREAD: " + Thread.currentThread().getName() + " ===");
                    System.err.println("=== TIMESTAMP: " + System.currentTimeMillis() + " ===");
                    
                    // Get file reader for written samples
                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader in = sampleState.creatReader();
                    try {

                        FrameTupleAccessor fta;
                        FrameTupleReference tuple;
                        IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                        IPointable inputVal = new VoidPointable();
                        IPointable tempVal = new VoidPointable();
                        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                        KMeansUtils KMeansUtils = new KMeansUtils(tempVal, storage);
                        // Use secondaryRecDesc directly for now (2-field format with vector embeddings)
                        RecordDescriptor inputRecDesc = secondaryRecDesc;
                        fta = new FrameTupleAccessor(inputRecDesc);
                        tuple = new FrameTupleReference();
                        VSizeFrame vSizeFrame = new VSizeFrame(ctx);
                        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                        ListAccessor listAccessorConstant = new ListAccessor();

                        writer.open();

                        // Create hierarchical cluster index writer
                        HierarchicalClusterIndexWriter indexWriter = new HierarchicalClusterIndexWriter(ctx, partition);

                    // Build complete tree clustering with parent-child relationships
                    System.err.println("=== CALLING buildCompleteTreeClustering ===");
                    System.err.println("=== PARTITION: " + partition + " ===");
                    buildCompleteTreeClustering(ctx, in, fta, tuple, eval, inputVal, listAccessorConstant,
                            KMeansUtils, vSizeFrame, appender, partition, indexWriter);
                    System.err.println("=== COMPLETED buildCompleteTreeClustering ===");

                        // Write the hierarchical cluster index to side file
                        indexWriter.writeIndexToSideFile();

                        // Also write to static location for manual management
                        indexWriter.writeIndex();

                        // Print simple summary
                        System.out.println("Hierarchical cluster index created with "
                                + indexWriter.getClusterLevels().size() + " levels");

                        FrameUtils.flushFrame(appender.getBuffer(), writer);

                    } catch (Throwable e) {
                        writer.fail();
                        throw new RuntimeException(e);
                    } finally {
                        in.close();
                        writer.close();
                    }
                }

                /**
                 * Builds the complete hierarchical clustering tree and outputs it.
                 * 
                 * ALGORITHM OVERVIEW:
                 * ===================
                 * This method implements the complete hierarchical clustering algorithm that builds
                 * a tree structure from bottom-up, establishing parent-child relationships at each level.
                 * 
                 * TREE BUILDING PROCESS:
                 * =====================
                 * The algorithm builds a tree from bottom-up using the following process:
                 * 
                 * 1. LEAF LEVEL (Level 0):
                 *    - Uses memory-efficient K-means++ on raw data
                 *    - Creates initial leaf nodes (clusters of raw data points)
                 *    - These become the children of the next level
                 * 
                 * 2. HIERARCHICAL LEVELS (Level 1+):
                 *    - Takes centroids from previous level as "data points"
                 *    - Uses scalable K-means++ to cluster these centroids
                 *    - Establishes parent-child relationships using Lloyd's assignments
                 *    - Continues until only one centroid remains (root)
                 * 
                 * 3. TREE ORGANIZATION:
                 *    - Builds complete tree with nodes containing centroids and relationships
                 *    - Assigns BFS-based cluster IDs for efficient traversal
                 *    - Organizes parent-child relationships naturally in tree structure
                 * 
                 * TREE STRUCTURE EXAMPLE:
                 * ======================
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
                 * INPUT:
                 * ======
                 * - ctx: Hyracks task context for state management
                 * - in: File reader for materialized data
                 * - fta, tuple, eval, inputVal: Data processing components
                 * - kMeansUtils: Utilities for K-means operations
                 * - vSizeFrame: Frame for data processing
                 * - appender: Frame appender for output
                 * - partition: Partition ID
                 * - indexWriter: Writer for JSON side file
                 * 
                 * OUTPUT:
                 * =======
                 * - Complete hierarchical tree with parent-child relationships
                 * - All tree nodes streamed to next operator in BFS order
                 * - JSON side file with tree structure
                 * 
                 * MEMORY EFFICIENCY:
                 * ==================
                 * - Never loads all data points into memory simultaneously
                 * - Uses streaming approach with probabilistic selection
                 * - Only stores centroids and tree structure in memory
                 * - Frame-based stopping criterion prevents memory overflow
                 * 
                 * WHY BFS-BASED ID ASSIGNMENT:
                 * ============================
                 * BFS-based ID assignment is more efficient than sorting because:
                 * 1. No expensive sorting operations needed
                 * 2. Natural tree traversal order
                 * 3. Consistent ID assignment across runs
                 * 4. Efficient for tree-based queries and operations
                 */
                private void buildCompleteTreeClustering(IHyracksTaskContext ctx, GeneratedRunFileReader in,
                        FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, VSizeFrame vSizeFrame,
                        FrameTupleAppender appender, int partition, HierarchicalClusterIndexWriter indexWriter)
                        throws HyracksDataException, IOException {

                    System.err.println("=== HIERARCHICAL CLUSTERING PHASE ===");
                    System.err.println("Starting hierarchical clustering with K=" + K);

                    // Create tree structure
                    HierarchicalClusterTree tree = new HierarchicalClusterTree();

                    // Build hierarchy levels using the original memory-efficient approach
                    int currentLevel = 0;
                    int currentK = K;
                    Random rand = new Random();
                    int maxKMeansIterations = 20;

                    // Start with the original data (not loaded into memory)
                    List<double[]> currentLevelCentroids = new ArrayList<>();

                    // First level: Use original probabilistic K-means++ on data
                    currentLevelCentroids = performMemoryEfficientKMeansPlusPlus(ctx, in, fta, tuple, eval, inputVal,
                            listAccessorConstant, kMeansUtils, vSizeFrame, currentK, rand, maxKMeansIterations,
                            partition);

                    System.err.println("Level 0: Generated " + currentLevelCentroids.size() + " centroids");

                    if (currentLevelCentroids.isEmpty()) {
                        System.err.println("No centroids available for tree building");
                        return;
                    }

                    // Add Level 0 centroids (children) to tree - these are the leaf nodes
                    // Set the first centroid as root, then add the rest as children

                    // Set first centroid as root
                    tree.setRoot(currentLevelCentroids.get(0));
                    List<HierarchicalClusterTree.TreeNode> currentLevelNodes = new ArrayList<>();
                    currentLevelNodes.add(tree.getRoot());

                    // Add remaining centroids as children of root
                    System.err.println("Adding " + (currentLevelCentroids.size() - 1)
                            + " additional centroids as children of root");
                    for (int i = 1; i < currentLevelCentroids.size(); i++) {
                        HierarchicalClusterTree.TreeNode childNode =
                                tree.addChild(tree.getRoot(), currentLevelCentroids.get(i), currentLevel);
                        currentLevelNodes.add(childNode);
                        System.err.println("Added child " + i + " with global_id " + childNode.getGlobalId());
                    }

                    System.err.println("Starting tree building with " + currentLevelCentroids.size()
                            + " centroids at level " + currentLevel);

                    currentLevel++;
                    currentK = Math.max(1, currentK / 2);

                    // Limit to maximum 5 levels to prevent excessive hierarchy
                    int maxLevels = 5;

                    System.err.println("Starting hierarchical levels. Current level: " + currentLevel + ", K: "
                            + currentK + ", centroids: " + currentLevelCentroids.size());
                    System.err.println("While loop condition: centroids.size() > 1 = "
                            + (currentLevelCentroids.size() > 1) + ", K > 1 = " + (currentK > 1) + ", level < maxLevels = " + (currentLevel < maxLevels));
                    System.err.println("=== CLUSTERING PROGRESS ===");

                    // Debug: print all centroids
                    for (int i = 0; i < currentLevelCentroids.size(); i++) {
                        System.err.println("Level " + currentLevel + " Centroid " + i + ": "
                                + Arrays.toString(Arrays.copyOf(currentLevelCentroids.get(i),
                                        Math.min(3, currentLevelCentroids.get(i).length))));
                    }

                    // Build subsequent levels until centroids fit into one frame or only one centroid remains
                    while (currentLevelCentroids.size() > 1 && currentK > 1 && currentLevel < maxLevels) {
                        // Use scalable K-means++ on centroids from previous level
                        List<double[]> nextLevelCentroids = performScalableKMeansPlusPlusOnCentroids(
                                currentLevelCentroids, currentK, rand, kMeansUtils, maxKMeansIterations);

                        System.err.println("Level " + currentLevel + ": " + currentLevelCentroids.size() + " → " + nextLevelCentroids.size() + " centroids");

                        // Use Lloyd's algorithm approach to assign current level centroids (children) to next level centroids (parents)
                        int[] parentAssignments = assignCentroidsToParents(currentLevelCentroids, nextLevelCentroids);

                        // Add next level centroids (parents) to tree
                        List<HierarchicalClusterTree.TreeNode> nextLevelNodes = new ArrayList<>();
                        for (int i = 0; i < nextLevelCentroids.size(); i++) {
                            HierarchicalClusterTree.TreeNode parentNode =
                                    tree.addNode(nextLevelCentroids.get(i), currentLevel);
                            nextLevelNodes.add(parentNode);
                        }

                        // FIX: Connect parent nodes to the tree structure
                        // The addNode() method creates standalone nodes that are not connected to the tree.
                        // Without this connection, toFlatList() cannot find the parent nodes, causing
                        // the hierarchical structure to appear broken.
                        // 
                        // Solution: Set the first parent as the new root, and add others as its children
                        if (!nextLevelNodes.isEmpty()) {
                            HierarchicalClusterTree.TreeNode newRoot = nextLevelNodes.get(0);
                            tree.setRoot(newRoot);

                            // Add remaining parent nodes as children of the new root
                            for (int i = 1; i < nextLevelNodes.size(); i++) {
                                tree.getRoot().getChildren().add(nextLevelNodes.get(i));
                            }
                        }

                        // Move current level centroids (children) to their assigned parent nodes
                        for (int j = 0; j < currentLevelCentroids.size(); j++) {
                            int parentIndex = parentAssignments[j];
                            HierarchicalClusterTree.TreeNode childNode = currentLevelNodes.get(j);
                            HierarchicalClusterTree.TreeNode parentNode = nextLevelNodes.get(parentIndex);
                            // Move existing child node to new parent
                            tree.moveChildToParent(childNode, parentNode);
                        }

                        // Test if all centroids from this level can fit in one frame
                        boolean canFitInOneFrame = testTreeLevelFitsInFrame(ctx, nextLevelNodes, appender);

                        if (canFitInOneFrame) {
                            System.err.println("Frame capacity reached at level " + currentLevel + " - stopping");
                            break;
                        }

                        // Prepare for next iteration
                        currentLevelCentroids = nextLevelCentroids;
                        currentLevelNodes = nextLevelNodes;
                        currentLevel++;
                        currentK = Math.max(1, currentK / 2);
                    }

                    System.err.println("Hierarchical clustering complete: " + (currentLevel + 1) + " levels, " + tree.getTotalNodes() + " total nodes");

                    // Assign BFS-based IDs to all nodes (no sorting needed!)
                    tree.assignBFSIds();

                    // Output all nodes in BFS order
                    System.err.println("=== CALLING outputCompleteTree ===");
                    System.err.println("=== TREE NODES: " + tree.getTotalNodes() + " ===");
                    outputCompleteTree(tree, appender, indexWriter, partition);
                    System.err.println("=== COMPLETED outputCompleteTree ===");

                    System.err.println("Tree output complete: " + tree.getTotalNodes() + " nodes");
                }

                /**
                 * Performs memory-efficient K-means++ on the original data using probabilistic selection.
                 * 
                 * ALGORITHM OVERVIEW:
                 * ===================
                 * This is the core K-means++ algorithm adapted for big data scenarios where we cannot
                 * load all data points into memory simultaneously. It uses probabilistic selection
                 * to choose centroids without storing all data points.
                 * 
                 * WHY MEMORY EFFICIENCY IS CRITICAL:
                 * =================================
                 * 1. BIG DATA SCALE: Datasets can have millions or billions of data points
                 * 2. MEMORY CONSTRAINTS: Loading all data would cause out-of-memory errors
                 * 3. STREAMING PROCESSING: Data comes in frames, not all at once
                 * 4. MULTIPLE PASSES: We need to make multiple passes over the data
                 * 
                 * K-MEANS++ ALGORITHM:
                 * ===================
                 * 1. INITIALIZATION: Start with first data point as first centroid
                 * 2. PROBABILISTIC SELECTION: For each remaining centroid:
                 *    a. Calculate minimum distance from each data point to existing centroids
                 *    b. Use these distances as weights for probabilistic selection
                 *    c. Select next centroid with probability proportional to squared distance
                 * 3. LLOYD'S REFINEMENT: Iteratively improve centroids using assignments
                 * 
                 * PROBABILISTIC SELECTION PROCESS:
                 * ===============================
                 * 1. First pass: Calculate minimum distances for all data points
                 * 2. Second pass: Use distances as weights for selection
                 * 3. Selection: Choose data point with probability ∝ distance²
                 * 4. Repeat: Until we have K centroids
                 * 
                 * INPUT:
                 * ======
                 * - ctx: Hyracks task context for state management
                 * - in: File reader for materialized data
                 * - fta, tuple, eval, inputVal: Data processing components
                 * - kMeansUtils: Utilities for K-means operations
                 * - vSizeFrame: Frame for data processing
                 * - k: Number of centroids to select
                 * - rand: Random number generator for probabilistic selection
                 * - maxIterations: Maximum iterations for Lloyd's algorithm
                 * - partition: Partition ID
                 * 
                 * OUTPUT:
                 * =======
                 * - List of K centroids selected using K-means++ algorithm
                 * - Centroids are refined using Lloyd's algorithm
                 * 
                 * MEMORY EFFICIENCY TECHNIQUES:
                 * =============================
                 * 1. STREAMING: Process data one frame at a time
                 * 2. PROBABILISTIC: Don't store all data points, use weighted selection
                 * 3. ITERATIVE: Make multiple passes over data without storing it
                 * 4. FRAME-BASED: Use Hyracks frames for efficient data transfer
                 * 
                 * COMPLEXITY:
                 * ===========
                 * - Time: O(k * n * d) where n = number of data points, d = dimension
                 * - Space: O(k * d) - only stores centroids, not all data points
                 * - Passes: 2 passes over data for each centroid selection
                 */
                private List<double[]> performMemoryEfficientKMeansPlusPlus(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, int k, Random rand, int maxIterations,
                        int partition) throws HyracksDataException, IOException {

                    if (k <= 0) {
                        return new ArrayList<>();
                    }

                    // Get the first centroid that was stored in StoreCentroidsActivity
                    CentroidsState centroidsState =
                            (CentroidsState) ctx.getStateObject(new PartitionedUUID(centroidsUUID, partition));
                    List<double[]> existingCentroids = centroidsState.getCentroids();

                    if (existingCentroids.isEmpty()) {
                        System.err.println("No existing centroids found, cannot perform K-means++");
                        return new ArrayList<>();
                    }

                    System.err.println("Found " + existingCentroids.size()
                            + " existing centroids, starting K-means++ with " + k + " target centroids");

                    // If we already have enough centroids from StoreCentroidsActivity, just return them
                    if (existingCentroids.size() >= k) {
                        System.err.println(
                                "Already have " + existingCentroids.size() + " centroids, returning first " + k);
                        return existingCentroids.subList(0, k);
                    }

                    // Initialize candidate set with existing centroids
                    List<double[]> currentCandidates = new ArrayList<>();
                    for (double[] centroid : existingCentroids) {
                        currentCandidates.add(centroid);
                    }

                    // SCALABLE KMEANS ++ to select candidates (iterative approach like original)
                    for (int step = 0; step < maxScalableKmeansIter; step++) {

                        List<Double> preCosts = new ArrayList<>();

                        // First pass: Calculate costs for all points (memory efficient - stream through data)
                        vSizeFrame.reset();
                        in.open();

                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                tuple.reset(fta, tupleIndex);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    continue;
                                }
                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Calculate minimum cost to current candidates
                                double minCost = Double.POSITIVE_INFINITY;
                                for (double[] candidate : currentCandidates) {
                                    double cost = euclidean_squared(point, candidate);
                                    if (cost < minCost) {
                                        minCost = cost;
                                    }
                                }
                                preCosts.add(minCost);
                            }
                        }

                        // Compute sumCosts
                        double sumCosts = 0.0;
                        for (double c : preCosts) {
                            sumCosts += c;
                        }

                        // Second pass: Probabilistic selection
                        List<double[]> chosen = new ArrayList<>();
                        int globalTupleIndex = 0;
                        double l = k * 5; // oversampling factor

                        vSizeFrame.reset();
                        in.seek(0);
                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                                double prob = l * preCosts.get(globalTupleIndex) / sumCosts;
                                if (prob > 1.0)
                                    prob = 1.0;

                                if (rand.nextDouble() < prob) {
                                    tuple.reset(fta, tupleIndex);
                                    eval.evaluate(tuple, inputVal);
                                    if (!ATYPETAGDESERIALIZER
                                            .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                            .isListType()) {
                                        continue;
                                    }
                                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);
                                    chosen.add(point);
                                }
                            }
                        }

                        // Add chosen candidates to current set
                        for (double[] c : chosen) {
                            currentCandidates.add(c);
                        }
                    }

                    // Now perform weighted K-means++ on ALL accumulated candidates
                    List<double[]> weightedCentroids =
                            performWeightedKMeansPlusPlus(currentCandidates, k, rand, kMeansUtils, maxIterations);

                    // Run Lloyd's algorithm with assignments to improve the centroids and get parent-child relationships
                    LloydResult lloydResult = performLloydsAlgorithmWithAssignments(in, fta, tuple, eval, inputVal,
                            listAccessorConstant, kMeansUtils, vSizeFrame, weightedCentroids, k, maxIterations);

                    // Store assignments for parent-child relationships (we'll use this later)
                    // For now, just return the centroids
                    return lloydResult.centroids;
                }

                /**
                 * Performs Lloyd's algorithm to improve centroids by iteratively assigning points to nearest centroids
                 * This is the same as the original implementation
                 */
                @SuppressWarnings("unused")
                private List<double[]> performLloydsAlgorithm(GeneratedRunFileReader in, FrameTupleAccessor fta,
                        FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, VSizeFrame vSizeFrame,
                        List<double[]> initialCentroids, int k, int maxIterations)
                        throws HyracksDataException, IOException {

                    if (initialCentroids.isEmpty() || k <= 0) {
                        return initialCentroids;
                    }

                    int dim = initialCentroids.get(0).length;
                    double[][] centers = new double[k][dim];
                    for (int i = 0; i < k && i < initialCentroids.size(); i++) {
                        centers[i] = Arrays.copyOf(initialCentroids.get(i), dim);
                    }

                    double[][] newCenters = new double[k][dim];
                    int[] counts = new int[k];
                    double epsilon = 1e-4; // convergence threshold
                    int step = 0;
                    boolean converged = false;

                    while (!converged && step < maxIterations) {
                        // Reset accumulators
                        for (int i = 0; i < k; i++) {
                            counts[i] = 0;
                            for (int d = 0; d < dim; d++) {
                                newCenters[i][d] = 0.0;
                            }
                        }

                        // Assign points to nearest centroid and sum
                        vSizeFrame.reset();
                        in.seek(0);
                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                tuple.reset(fta, tupleIndex);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    continue;
                                }
                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Assign to nearest centroid
                                int bestIdx = 0;
                                double minDist = Double.POSITIVE_INFINITY;
                                for (int cIdx = 0; cIdx < k; cIdx++) {
                                    double dist = euclidean_squared(point, centers[cIdx]);
                                    if (dist < minDist) {
                                        minDist = dist;
                                        bestIdx = cIdx;
                                    }
                                }
                                for (int d = 0; d < dim; d++) {
                                    newCenters[bestIdx][d] += point[d];
                                }
                                counts[bestIdx]++;
                            }
                        }

                        // Update centroids & check for convergence
                        converged = true;
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            if (counts[cIdx] > 0) {
                                for (int d = 0; d < dim; d++) {
                                    newCenters[cIdx][d] /= counts[cIdx];
                                }
                            } else {
                                // Optionally keep old center or set to zero
                                newCenters[cIdx] = Arrays.copyOf(centers[cIdx], dim);
                            }
                            // Check movement
                            double dist = euclidean_squared(centers[cIdx], newCenters[cIdx]);
                            if (dist > epsilon) {
                                converged = false;
                            }
                        }

                        // Copy newCenters to centers for next iteration
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            System.arraycopy(newCenters[cIdx], 0, centers[cIdx], 0, dim);
                        }

                        step++;
                    }

                    // Build final centroids
                    List<double[]> finalCentroids = new ArrayList<>(k);
                    for (int cIdx = 0; cIdx < k; cIdx++) {
                        double[] centroid = new double[dim];
                        System.arraycopy(centers[cIdx], 0, centroid, 0, dim);
                        finalCentroids.add(centroid);
                    }

                    return finalCentroids;
                }

                /**
                 * Performs Lloyd's algorithm with assignment tracking for parent-child relationships
                 * This eliminates the need for expensive parent finding later
                 */
                private LloydResult performLloydsAlgorithmWithAssignments(GeneratedRunFileReader in,
                        FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, VSizeFrame vSizeFrame,
                        List<double[]> initialCentroids, int k, int maxIterations)
                        throws HyracksDataException, IOException {

                    if (initialCentroids.isEmpty() || k <= 0) {
                        return new LloydResult(initialCentroids, new int[0]);
                    }

                    int dim = initialCentroids.get(0).length;
                    double[][] centers = new double[k][dim];
                    for (int i = 0; i < k && i < initialCentroids.size(); i++) {
                        centers[i] = Arrays.copyOf(initialCentroids.get(i), dim);
                    }

                    double[][] newCenters = new double[k][dim];
                    int[] counts = new int[k];
                    double epsilon = 1e-4; // convergence threshold
                    int step = 0;
                    boolean converged = false;

                    // Track assignments for parent-child relationships
                    List<Integer> assignments = new ArrayList<>();

                    while (!converged && step < maxIterations) {
                        // Reset accumulators
                        for (int i = 0; i < k; i++) {
                            counts[i] = 0;
                            for (int d = 0; d < dim; d++) {
                                newCenters[i][d] = 0.0;
                            }
                        }

                        // Clear assignments for this iteration
                        assignments.clear();

                        // Assign points to nearest centroid and sum
                        vSizeFrame.reset();
                        in.seek(0);
                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                tuple.reset(fta, tupleIndex);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    continue;
                                }
                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Assign to nearest centroid
                                int bestIdx = 0;
                                double minDist = Double.POSITIVE_INFINITY;
                                for (int cIdx = 0; cIdx < k; cIdx++) {
                                    double dist = euclidean_squared(point, centers[cIdx]);
                                    if (dist < minDist) {
                                        minDist = dist;
                                        bestIdx = cIdx;
                                    }
                                }

                                // Store assignment for parent-child relationships
                                assignments.add(bestIdx);

                                for (int d = 0; d < dim; d++) {
                                    newCenters[bestIdx][d] += point[d];
                                }
                                counts[bestIdx]++;
                            }
                        }

                        // Update centroids & check for convergence
                        converged = true;
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            if (counts[cIdx] > 0) {
                                for (int d = 0; d < dim; d++) {
                                    newCenters[cIdx][d] /= counts[cIdx];
                                }
                            } else {
                                // Optionally keep old center or set to zero
                                newCenters[cIdx] = Arrays.copyOf(centers[cIdx], dim);
                            }
                            // Check movement
                            double dist = euclidean_squared(centers[cIdx], newCenters[cIdx]);
                            if (dist > epsilon) {
                                converged = false;
                            }
                        }

                        // Copy newCenters to centers for next iteration
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            System.arraycopy(newCenters[cIdx], 0, centers[cIdx], 0, dim);
                        }

                        step++;
                    }

                    // Build final centroids
                    List<double[]> finalCentroids = new ArrayList<>(k);
                    for (int cIdx = 0; cIdx < k; cIdx++) {
                        double[] centroid = new double[dim];
                        System.arraycopy(centers[cIdx], 0, centroid, 0, dim);
                        finalCentroids.add(centroid);
                    }

                    // Convert assignments to array
                    int[] assignmentArray = new int[assignments.size()];
                    for (int i = 0; i < assignments.size(); i++) {
                        assignmentArray[i] = assignments.get(i);
                    }

                    return new LloydResult(finalCentroids, assignmentArray);
                }

                /**
                 * Performs weighted K-means++ on a set of candidate points
                 * Uses the efficient approach from LocalKMeansPlusPlusCentroidsOperatorDescriptor
                 */
                private List<double[]> performWeightedKMeansPlusPlus(List<double[]> candidates, int k, Random rand,
                        KMeansUtils kMeansUtils, int maxIterations) {
                    if (candidates.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    int dim = candidates.get(0).length;
                    int n = candidates.size();
                    double[][] centers = new double[k][dim];

                    // Assume equal weights for candidates (since we don't have centroidCounts here)
                    int[] candidateCounts = new int[n];
                    Arrays.fill(candidateCounts, 1); // Each candidate has weight 1

                    // Pick first center using weighted selection
                    int idx = kMeansUtils.pickWeightedIndex(rand, candidates, candidateCounts);
                    centers[0] = Arrays.copyOf(candidates.get(idx), dim);

                    // Initialize costArray efficiently
                    double[] costArray = new double[n];
                    for (int i = 0; i < n; i++) {
                        costArray[i] = euclidean_squared(candidates.get(i), centers[0]);
                    }

                    // Weighted K-means++ initialization (efficient approach)
                    for (int i = 1; i < k; i++) {
                        double sum = 0.0;
                        for (int j = 0; j < n; j++) {
                            sum += costArray[j] * candidateCounts[j]; // Weighted sum
                        }
                        double r = rand.nextDouble() * sum;
                        double cumulativeScore = 0.0;
                        int j = 0;
                        while (j < n && cumulativeScore < r) {
                            cumulativeScore += candidateCounts[j] * costArray[j]; // Weighted selection
                            j++;
                        }
                        if (j == 0) {
                            centers[i] = Arrays.copyOf(candidates.get(0), dim);
                        } else {
                            centers[i] = Arrays.copyOf(candidates.get(j - 1), dim);
                        }

                        // Update costArray efficiently (only 2 nested loops)
                        for (int p = 0; p < n; p++) {
                            costArray[p] = Math.min(euclidean_squared(candidates.get(p), centers[i]), costArray[p]);
                        }
                    }

                    // Weighted Lloyd's algorithm
                    int[] oldClosest = new int[n];
                    Arrays.fill(oldClosest, -1);
                    int iteration = 0;
                    boolean moved = true;

                    while (moved && iteration < maxIterations) {
                        moved = false;
                        double[] counts = new double[k];
                        double[][] sums = new double[k][dim];

                        // Assign points to closest centroids with weights
                        for (int i = 0; i < n; i++) {
                            int index = kMeansUtils.findClosest(centers, candidates.get(i));
                            kMeansUtils.addWeighted(sums[index], candidates.get(i), candidateCounts[i]);
                            counts[index] += candidateCounts[i];
                            if (index != oldClosest[i]) {
                                moved = true;
                                oldClosest[i] = index;
                            }
                        }

                        // Update centers with weighted averages
                        for (int j = 0; j < k; j++) {
                            if (counts[j] == 0.0) {
                                // Handle empty cluster - select random point
                                int selectedIdx = rand.nextInt(n);
                                centers[j] = Arrays.copyOf(candidates.get(selectedIdx), dim);
                            } else {
                                kMeansUtils.scale(sums[j], 1.0 / counts[j]);
                                centers[j] = Arrays.copyOf(sums[j], dim);
                            }
                        }
                        iteration++;
                    }

                    // Convert to list
                    List<double[]> result = new ArrayList<>();
                    for (double[] center : centers) {
                        result.add(center);
                    }
                    return result;
                }

                /**
                 * Performs scalable K-means++ on centroids from previous level.
                 * 
                 * ALGORITHM OVERVIEW:
                 * ===================
                 * This method implements K-means++ clustering on centroids from the previous level,
                 * rather than on raw data points. This is much more efficient because:
                 * 1. Fewer data points to process (centroids vs raw data)
                 * 2. Centroids are already in memory (no disk I/O)
                 * 3. No need for probabilistic selection (can load all centroids)
                 * 
                 * WHY THIS IS MORE EFFICIENT:
                 * ===========================
                 * 1. MEMORY EFFICIENT: Centroids are much fewer than raw data points
                 * 2. NO DISK I/O: Centroids are already in memory
                 * 3. SIMPLER ALGORITHM: Can use standard K-means++ without probabilistic selection
                 * 4. FASTER CONVERGENCE: Centroids are already well-separated
                 * 
                 * ALGORITHM STEPS:
                 * ================
                 * 1. INITIALIZATION: Select first centroid randomly
                 * 2. ITERATIVE SELECTION: For each remaining centroid:
                 *    a. Calculate minimum distance from each centroid to existing centroids
                 *    b. Select next centroid with probability proportional to squared distance
                 * 3. LLOYD'S REFINEMENT: Iteratively improve centroids using assignments
                 * 
                 * INPUT:
                 * ======
                 * - centroids: List of centroids from previous level (already in memory)
                 * - k: Number of centroids to select for current level
                 * - rand: Random number generator for selection
                 * - kMeansUtils: Utilities for K-means operations
                 * - maxIterations: Maximum iterations for Lloyd's algorithm
                 * 
                 * OUTPUT:
                 * =======
                 * - List of K centroids selected using K-means++ algorithm
                 * - Centroids are refined using Lloyd's algorithm
                 * 
                 * COMPLEXITY:
                 * ===========
                 * - Time: O(k * m * d) where m = number of centroids, d = dimension
                 * - Space: O(k * d) - only stores selected centroids
                 * - Note: m << n (number of raw data points)
                 * 
                 * TREE BUILDING CONTEXT:
                 * ======================
                 * This method is used in the hierarchical tree building process:
                 * - Level 0: Raw data → K centroids (using performMemoryEfficientKMeansPlusPlus)
                 * - Level 1: K centroids → K/2 centroids (using this method)
                 * - Level 2: K/2 centroids → K/4 centroids (using this method)
                 * - ... and so on until only one centroid remains (root)
                 */
                private List<double[]> performScalableKMeansPlusPlusOnCentroids(List<double[]> centroids, int k,
                        Random rand, KMeansUtils kMeansUtils, int maxIterations) {
                    if (centroids.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    // Initialize candidate set with first centroid
                    List<double[]> currentCandidates = new ArrayList<>();
                    currentCandidates.add(centroids.get(0));

                    // SCALABLE KMEANS ++ to select candidates (iterative approach like original)
                    for (int stepMultilevel = 1; stepMultilevel < maxScalableKmeansIter; stepMultilevel++) {
                        List<Double> preCosts = new ArrayList<>();

                        int tupleCount = centroids.size();
                        for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                            double[] point = centroids.get(tupleIndex);

                            double minCost = Double.POSITIVE_INFINITY;
                            for (double[] center : currentCandidates) {
                                double cost = euclidean_squared(point, center);
                                if (cost < minCost)
                                    minCost = cost;
                            }
                            preCosts.add(minCost);
                        }

                        // Compute sumCosts
                        double sumCosts = 0.0;
                        for (double c : preCosts)
                            sumCosts += c;

                        // Oversampling factor
                        double l = k * 5; // expected number of candidates per round
                        List<double[]> chosen = new ArrayList<>();

                        int pointCount = centroids.size();
                        for (int tupleIndex = 0; tupleIndex < pointCount; tupleIndex++) {
                            double prob = l * preCosts.get(tupleIndex) / sumCosts;
                            if (prob > 1.0)
                                prob = 1.0; // cap at 1

                            if (rand.nextDouble() < prob) {
                                double[] point = centroids.get(tupleIndex);
                                chosen.add(point);
                            }
                        }

                        for (double[] c : chosen) {
                            currentCandidates.add(c);
                        }
                    }

                    // Calculate weights for each candidate
                    int[] centroidCounts = new int[currentCandidates.size()];
                    for (int i = 0; i < currentCandidates.size(); i++) {
                        double[] candidate = currentCandidates.get(i);
                        int closestIdx = -1;
                        double minCost = Double.POSITIVE_INFINITY;
                        for (int j = 0; j < centroids.size(); j++) {
                            double cost = euclidean_squared(candidate, centroids.get(j));
                            if (cost < minCost) {
                                minCost = cost;
                                closestIdx = j;
                            }
                        }
                        if (closestIdx >= 0) {
                            centroidCounts[i]++;
                        }
                    }

                    // Now perform weighted K-means++ on the chosen candidates
                    System.err.println("performScalableKMeansPlusPlusOnCentroids: input centroids=" + centroids.size()
                            + ", target k=" + k + ", candidates=" + currentCandidates.size());
                    List<double[]> weightedCentroids = performWeightedKMeansPlusPlusWithCounts(currentCandidates, k,
                            rand, kMeansUtils, maxIterations, centroidCounts);

                    System.err.println("Weighted K-means++ generated " + weightedCentroids.size()
                            + " centroids (target k = " + k + ")");

                    // Run Lloyd's algorithm to improve the centroids (like original implementation)
                    List<double[]> finalCentroids =
                            performLloydsAlgorithmOnCentroids(centroids, weightedCentroids, k, maxIterations);

                    System.err.println("Lloyd's algorithm returned " + finalCentroids.size() + " centroids");

                    return finalCentroids;
                }

                /**
                 * Performs Lloyd's algorithm on centroids (for subsequent levels)
                 * This is similar to the original but works on centroids instead of raw data
                 */
                private List<double[]> performLloydsAlgorithmOnCentroids(List<double[]> originalCentroids,
                        List<double[]> initialCentroids, int k, int maxIterations) {
                    if (initialCentroids.isEmpty() || k <= 0) {
                        return initialCentroids;
                    }

                    int dim = initialCentroids.get(0).length;
                    double[][] centers = new double[k][dim];
                    for (int i = 0; i < k && i < initialCentroids.size(); i++) {
                        centers[i] = Arrays.copyOf(initialCentroids.get(i), dim);
                    }

                    double[][] newCenters = new double[k][dim];
                    int[] counts = new int[k];
                    double epsilon = 1e-4; // convergence threshold
                    int step = 0;
                    boolean converged = false;

                    while (!converged && step < maxIterations) {
                        // Reset accumulators
                        for (int i = 0; i < k; i++) {
                            counts[i] = 0;
                            for (int d = 0; d < dim; d++) {
                                newCenters[i][d] = 0.0;
                            }
                        }

                        // Assign centroids to nearest center and sum
                        for (double[] centroid : originalCentroids) {
                            // Assign to nearest center
                            int bestIdx = 0;
                            double minDist = Double.POSITIVE_INFINITY;
                            for (int cIdx = 0; cIdx < k; cIdx++) {
                                double dist = euclidean_squared(centroid, centers[cIdx]);
                                if (dist < minDist) {
                                    minDist = dist;
                                    bestIdx = cIdx;
                                }
                            }
                            for (int d = 0; d < dim; d++) {
                                newCenters[bestIdx][d] += centroid[d];
                            }
                            counts[bestIdx]++;
                        }

                        // Update centers & check for convergence
                        converged = true;
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            if (counts[cIdx] > 0) {
                                for (int d = 0; d < dim; d++) {
                                    newCenters[cIdx][d] /= counts[cIdx];
                                }
                            } else {
                                // Optionally keep old center or set to zero
                                newCenters[cIdx] = Arrays.copyOf(centers[cIdx], dim);
                            }
                            // Check movement
                            double dist = euclidean_squared(centers[cIdx], newCenters[cIdx]);
                            if (dist > epsilon) {
                                converged = false;
                            }
                        }

                        // Copy newCenters to centers for next iteration
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            System.arraycopy(newCenters[cIdx], 0, centers[cIdx], 0, dim);
                        }

                        step++;
                    }

                    // Build final centroids
                    List<double[]> finalCentroids = new ArrayList<>(k);
                    for (int cIdx = 0; cIdx < k; cIdx++) {
                        double[] centroid = new double[dim];
                        System.arraycopy(centers[cIdx], 0, centroid, 0, dim);
                        finalCentroids.add(centroid);
                    }

                    return finalCentroids;
                }

                /**
                 * Performs weighted K-means++ with provided counts (for centroids)
                 */
                private List<double[]> performWeightedKMeansPlusPlusWithCounts(List<double[]> candidates, int k,
                        Random rand, KMeansUtils kMeansUtils, int maxIterations, int[] candidateCounts) {
                    if (candidates.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    int dim = candidates.get(0).length;
                    int n = candidates.size();
                    double[][] centers = new double[k][dim];

                    // Pick first center using weighted selection
                    int idx = kMeansUtils.pickWeightedIndex(rand, candidates, candidateCounts);
                    centers[0] = Arrays.copyOf(candidates.get(idx), dim);

                    // Initialize costArray efficiently
                    double[] costArray = new double[n];
                    for (int i = 0; i < n; i++) {
                        costArray[i] = euclidean_squared(candidates.get(i), centers[0]);
                    }

                    // Weighted K-means++ initialization (efficient approach)
                    for (int i = 1; i < k; i++) {
                        double sum = 0.0;
                        for (int j = 0; j < n; j++) {
                            sum += costArray[j] * candidateCounts[j]; // Weighted sum
                        }
                        double r = rand.nextDouble() * sum;
                        double cumulativeScore = 0.0;
                        int j = 0;
                        while (j < n && cumulativeScore < r) {
                            cumulativeScore += candidateCounts[j] * costArray[j]; // Weighted selection
                            j++;
                        }
                        if (j == 0) {
                            centers[i] = Arrays.copyOf(candidates.get(0), dim);
                        } else {
                            centers[i] = Arrays.copyOf(candidates.get(j - 1), dim);
                        }

                        // Update costArray efficiently (only 2 nested loops)
                        for (int p = 0; p < n; p++) {
                            costArray[p] = Math.min(euclidean_squared(candidates.get(p), centers[i]), costArray[p]);
                        }
                    }

                    // Weighted Lloyd's algorithm
                    int[] oldClosest = new int[n];
                    Arrays.fill(oldClosest, -1);
                    int iteration = 0;
                    boolean moved = true;

                    while (moved && iteration < maxIterations) {
                        moved = false;
                        double[] counts = new double[k];
                        double[][] sums = new double[k][dim];

                        // Assign points to closest centroids with weights
                        for (int i = 0; i < n; i++) {
                            int index = kMeansUtils.findClosest(centers, candidates.get(i));
                            kMeansUtils.addWeighted(sums[index], candidates.get(i), candidateCounts[i]);
                            counts[index] += candidateCounts[i];
                            if (index != oldClosest[i]) {
                                moved = true;
                                oldClosest[i] = index;
                            }
                        }

                        // Update centers with weighted averages
                        for (int j = 0; j < k; j++) {
                            if (counts[j] == 0.0) {
                                // Handle empty cluster - select random point
                                int selectedIdx = rand.nextInt(n);
                                centers[j] = Arrays.copyOf(candidates.get(selectedIdx), dim);
                            } else {
                                kMeansUtils.scale(sums[j], 1.0 / counts[j]);
                                centers[j] = Arrays.copyOf(sums[j], dim);
                            }
                        }
                        iteration++;
                    }

                    // Convert to list
                    List<double[]> result = new ArrayList<>();
                    for (double[] center : centers) {
                        result.add(center);
                    }
                    return result;
                }

                /**
                 * Finds the closest parent centroid for establishing parent-child relationships
                 */
                @SuppressWarnings("unused")
                private HierarchicalClusterId findClosestParentCentroid(double[] childCentroid,
                        HierarchicalCentroidsState hierarchicalState, int parentLevel) {
                    List<HierarchicalCentroidsState.HierarchicalCentroid> parentCentroids =
                            hierarchicalState.getCentroidsAtLevel(parentLevel);

                    if (parentCentroids.isEmpty()) {
                        return null;
                    }

                    double minDistance = Double.POSITIVE_INFINITY;
                    HierarchicalClusterId closestParent = null;

                    for (HierarchicalCentroidsState.HierarchicalCentroid parent : parentCentroids) {
                        double distance = euclidean_squared(childCentroid, parent.getCentroid());
                        if (distance < minDistance) {
                            minDistance = distance;
                            closestParent = parent.getClusterId();
                        }
                    }

                    return closestParent;
                }

                /**
                 * Assigns child centroids to parent centroids using Lloyd's algorithm approach.
                 * 
                 * ALGORITHM OVERVIEW:
                 * ===================
                 * This method establishes parent-child relationships between centroids at different
                 * levels of the hierarchical tree. It uses the assignments generated by Lloyd's
                 * algorithm to determine which child centroids belong to which parent centroids.
                 * 
                 * WHY THIS IS EFFICIENT:
                 * =====================
                 * 1. REUSES LLOYD'S WORK: Lloyd's algorithm already computed assignments
                 * 2. NO EXTRA DISTANCE CALCULATIONS: Uses existing assignment information
                 * 3. DIRECT MAPPING: Simple array lookup for parent-child relationships
                 * 4. BOTTOM-UP APPROACH: Natural for hierarchical tree building
                 * 
                 * ALGORITHM STEPS:
                 * ================
                 * 1. INPUT: Child centroids (from level N-1) and parent centroids (from level N)
                 * 2. LLOYD'S ASSIGNMENT: Use Lloyd's algorithm to assign children to parents
                 * 3. MAPPING: Create assignment array where assignments[i] = j means child i belongs to parent j
                 * 4. OUTPUT: Assignment array for building parent-child relationships
                 * 
                 * TREE BUILDING CONTEXT:
                 * ======================
                 * This method is used in the hierarchical tree building process:
                 * 
                 * ```
                 * Level N (Parents)     Level N-1 (Children)
                 *      P1  ←─────────────── C1, C2, C3
                 *      P2  ←─────────────── C4, C5
                 *      P3  ←─────────────── C6, C7, C8
                 * ```
                 * 
                 * The assignment array would be: [0, 0, 0, 1, 1, 2, 2, 2]
                 * Meaning: C1,C2,C3 → P1, C4,C5 → P2, C6,C7,C8 → P3
                 * 
                 * INPUT:
                 * ======
                 * - childCentroids: List of centroids from level N-1 (children)
                 * - parentCentroids: List of centroids from level N (parents)
                 * 
                 * OUTPUT:
                 * =======
                 * - int[] assignments: Array where assignments[i] = j means child i belongs to parent j
                 * 
                 * COMPLEXITY:
                 * ===========
                 * - Time: O(m * k * d) where m = number of children, k = number of parents, d = dimension
                 * - Space: O(m) - only stores assignment array
                 * 
                 * MEMORY EFFICIENCY:
                 * ==================
                 * - No additional data structures needed
                 * - Reuses existing centroid data
                 * - Simple array for assignments
                 */
                private int[] assignCentroidsToParents(List<double[]> childCentroids, List<double[]> parentCentroids) {
                    if (childCentroids.isEmpty() || parentCentroids.isEmpty()) {
                        return new int[0];
                    }

                    int[] assignments = new int[childCentroids.size()];

                    // For each child centroid, find its closest parent
                    for (int i = 0; i < childCentroids.size(); i++) {
                        double[] childCentroid = childCentroids.get(i);
                        int closestParentIndex = 0;
                        double minDistance = Double.POSITIVE_INFINITY;

                        for (int j = 0; j < parentCentroids.size(); j++) {
                            double distance = euclidean_squared(childCentroid, parentCentroids.get(j));
                            if (distance < minDistance) {
                                minDistance = distance;
                                closestParentIndex = j;
                            }
                        }

                        assignments[i] = closestParentIndex;
                    }

                    return assignments;
                }

                /**
                 * Finds the closest parent index for a child centroid
                 */
                @SuppressWarnings("unused")
                private int findClosestParentIndex(double[] childCentroid, List<double[]> parentCentroids) {
                    if (parentCentroids.isEmpty()) {
                        return 0;
                    }

                    int closestIndex = 0;
                    double minDistance = Double.POSITIVE_INFINITY;

                    for (int i = 0; i < parentCentroids.size(); i++) {
                        double distance = euclidean_squared(childCentroid, parentCentroids.get(i));
                        if (distance < minDistance) {
                            minDistance = distance;
                            closestIndex = i;
                        }
                    }

                    return closestIndex;
                }

                /**
                 * Finds parent node by index (for Lloyd's assignments)
                 */
                @SuppressWarnings("unused")
                private HierarchicalClusterTree.TreeNode findParentNodeByIndex(int parentIndex,
                        HierarchicalClusterTree.TreeNode currentLevelNodes) {
                    if (currentLevelNodes == null) {
                        return null;
                    }

                    // Search through all nodes at the same level as currentLevelNodes
                    Queue<HierarchicalClusterTree.TreeNode> queue = new LinkedList<>();
                    queue.offer(currentLevelNodes);

                    int currentIndex = 0;
                    while (!queue.isEmpty()) {
                        HierarchicalClusterTree.TreeNode current = queue.poll();

                        // Check if this node is at the parent level
                        if (current.level == currentLevelNodes.level) {
                            if (currentIndex == parentIndex) {
                                return current;
                            }
                            currentIndex++;
                        }

                        // Add children to queue
                        for (HierarchicalClusterTree.TreeNode child : current.getChildren()) {
                            queue.offer(child);
                        }
                    }

                    // If not found, return the first node as fallback
                    return currentLevelNodes;
                }

                /**
                 * Finds the closest parent node for tree-based clustering
                 */
                @SuppressWarnings("unused")
                private HierarchicalClusterTree.TreeNode findClosestParentNode(double[] childCentroid,
                        HierarchicalClusterTree.TreeNode parentNode) {
                    if (parentNode == null) {
                        return null;
                    }

                    double minDistance = Double.POSITIVE_INFINITY;
                    HierarchicalClusterTree.TreeNode closestParent = parentNode;

                    // Search through all nodes at the same level as parentNode
                    Queue<HierarchicalClusterTree.TreeNode> queue = new LinkedList<>();
                    queue.offer(parentNode);

                    while (!queue.isEmpty()) {
                        HierarchicalClusterTree.TreeNode current = queue.poll();

                        // Check if this node is at the parent level
                        if (current.level == parentNode.level) {
                            double distance = euclidean_squared(childCentroid, current.getCentroid());
                            if (distance < minDistance) {
                                minDistance = distance;
                                closestParent = current;
                            }
                        }

                        // Add children to queue
                        for (HierarchicalClusterTree.TreeNode child : current.getChildren()) {
                            queue.offer(child);
                        }
                    }

                    return closestParent;
                }

                /**
                 * Tests if all nodes at a tree level can fit in one frame
                 */
                @SuppressWarnings("deprecation")
                private boolean testTreeLevelFitsInFrame(IHyracksTaskContext ctx,
                        List<HierarchicalClusterTree.TreeNode> nodes, FrameTupleAppender appender) {
                    try {
                        if (nodes.isEmpty()) {
                            return true;
                        }

                        // Store current appender state
                        ByteBuffer originalBuffer = appender.getBuffer();
                        int originalPosition = originalBuffer.position();

                        try {
                            // Try to append all nodes from this level
                            for (HierarchicalClusterTree.TreeNode node : nodes) {
                                double[] arr = node.getCentroid();

                                // Create tuple data (same as outputHierarchicalCentroids)
                                ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                                DataOutput embBytesOutput = new DataOutputStream(embBytes);
                                AMutableDouble aDouble = new AMutableDouble(0);
                                AMutableInt32 aInt32 = new AMutableInt32(0);
                                OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                                ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                                orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));

                                for (double value : arr) {
                                    aDouble.setValue(value);
                                    listStorage.reset();
                                    listStorage.getDataOutput().writeByte(ATypeTag.FLOAT.serialize());
                                    ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble,
                                            listStorage.getDataOutput());
                                    orderedListBuilder.addItem(listStorage);
                                }

                                embBytes.reset();
                                orderedListBuilder.write(embBytesOutput, true);

                                // Create tuple builder with level, clusterId, centroidId, and embedding
                                ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
                                tupleBuilder.reset();

                                // Add level (int)
                                listStorage.reset();
                                listStorage.getDataOutput().writeByte(ATypeTag.INTEGER.serialize());
                                aInt32.setValue(node.getLevel());
                                AInt32SerializerDeserializer.INSTANCE.serialize(aInt32, listStorage.getDataOutput());
                                tupleBuilder.addField(listStorage.getByteArray(), 0, listStorage.getLength());

                                // Add clusterId (int) 
                                listStorage.reset();
                                listStorage.getDataOutput().writeByte(ATypeTag.INTEGER.serialize());
                                aInt32.setValue(node.getClusterId());
                                AInt32SerializerDeserializer.INSTANCE.serialize(aInt32, listStorage.getDataOutput());
                                tupleBuilder.addField(listStorage.getByteArray(), 0, listStorage.getLength());

                                // Add centroidId (int) - using globalId as centroidId
                                listStorage.reset();
                                listStorage.getDataOutput().writeByte(ATypeTag.INTEGER.serialize());
                                aInt32.setValue((int) node.getGlobalId());
                                AInt32SerializerDeserializer.INSTANCE.serialize(aInt32, listStorage.getDataOutput());
                                tupleBuilder.addField(listStorage.getByteArray(), 0, listStorage.getLength());

                                // Add embedding (float array)
                                tupleBuilder.addField(embBytes.getByteArray(), 0, embBytes.getLength());

                                // Try to append - if this fails, nodes don't fit in one frame
                                if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                        tupleBuilder.getSize())) {
                                    System.err.println("Tree level nodes do NOT fit in one frame (failed at node "
                                            + nodes.indexOf(node) + " of " + nodes.size() + ")");
                                    return false;
                                }
                            }

                            System.err.println("Tree level nodes fit in one frame (" + nodes.size() + " nodes)");
                            return true;

                        } finally {
                            // Reset appender to original state
                            originalBuffer.position(originalPosition);
                            appender.reset(new VSizeFrame(ctx), true);
                        }

                    } catch (Exception e) {
                        System.err.println("Error testing tree level frame capacity: " + e.getMessage());
                        return false;
                    }
                }

                /**
                 * Outputs the complete tree using BFS traversal.
                 * 
                 * ALGORITHM OVERVIEW:
                 * ===================
                 * This method outputs the complete hierarchical tree structure using breadth-first
                 * search (BFS) traversal. BFS ensures that nodes are output in a consistent order
                 * that reflects the hierarchical structure of the tree.
                 * 
                 * WHY BFS TRAVERSAL:
                 * =================
                 * 1. CONSISTENT ORDER: BFS provides a predictable traversal order
                 * 2. LEVEL-BY-LEVEL: Nodes at the same level are output together
                 * 3. NATURAL HIERARCHY: Root first, then children, then grandchildren, etc.
                 * 4. EFFICIENT: Simple queue-based traversal
                 * 
                 * BFS TRAVERSAL PROCESS:
                 * =====================
                 * 1. START: Add root node to queue
                 * 2. WHILE queue not empty:
                 *    a. Dequeue next node
                 *    b. Output node data to next operator
                 *    c. Add all children to queue
                 * 3. CONTINUE: Until all nodes are processed
                 * 
                 * TREE STRUCTURE EXAMPLE:
                 * ======================
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
                 * BFS OUTPUT ORDER:
                 * ================
                 * 1. Root
                 * 2. Parent1, Parent2
                 * 3. Child1, Child2, Child3, Child4, Child5, Child6
                 * 
                 * INPUT:
                 * ======
                 * - tree: Complete hierarchical tree structure
                 * - appender: Frame appender for output
                 * - indexWriter: Writer for JSON side file
                 * - partition: Partition ID
                 * 
                 * OUTPUT:
                 * =======
                 * - All tree nodes streamed to next operator in BFS order
                 * - JSON side file with complete tree structure
                 * 
                 * MEMORY EFFICIENCY:
                 * ==================
                 * - Uses queue for BFS traversal (O(max_width) space)
                 * - Streams nodes as they are processed
                 * - No need to store entire tree in memory
                 * 
                 * COMPLEXITY:
                 * ===========
                 * - Time: O(n) where n = total number of nodes
                 * - Space: O(w) where w = maximum width of tree
                 */
                @SuppressWarnings("deprecation")
                private void outputCompleteTree(HierarchicalClusterTree tree, FrameTupleAppender appender,
                        HierarchicalClusterIndexWriter indexWriter, int partition)
                        throws HyracksDataException, IOException {

                    System.err.println("Outputting complete tree in BFS order...");

                    // Get all nodes in BFS order
                    System.err.println(
                            "Calling toFlatList() on tree with root: " + (tree.getRoot() != null ? "exists" : "null"));
                    if (tree.getRoot() != null) {
                        System.err.println("Root has " + tree.getRoot().getChildren().size() + " children");
                    }
                    List<HierarchicalClusterTree.TreeNode> allNodes = tree.toFlatList();
                    System.err.println("Found " + allNodes.size() + " total nodes in tree");

                    // Debug: print all nodes
                    for (int i = 0; i < allNodes.size(); i++) {
                        HierarchicalClusterTree.TreeNode node = allNodes.get(i);
                        System.err.println("Node " + i + ": level=" + node.getLevel() + ", cluster_id="
                                + node.getClusterId() + ", global_id=" + node.getGlobalId() + ", parent_global_id="
                                + node.getParentGlobalId());
                    }

                    // Group nodes by level and add to index
                    Map<Integer, List<HierarchicalCentroidsState.HierarchicalCentroid>> centroidsByLevel =
                            new HashMap<>();

                    for (HierarchicalClusterTree.TreeNode node : allNodes) {
                        // Convert tree node to hierarchical centroid for index
                        HierarchicalClusterId clusterId = new HierarchicalClusterId(node.getLevel(),
                                node.getClusterId(), (int) node.getParentGlobalId());
                        HierarchicalCentroidsState.HierarchicalCentroid centroid =
                                new HierarchicalCentroidsState.HierarchicalCentroid(clusterId, node.getCentroid());

                        // Group by level
                        centroidsByLevel.computeIfAbsent(node.getLevel(), k -> new ArrayList<>()).add(centroid);
                    }

                    // Add each level to index
                    for (Map.Entry<Integer, List<HierarchicalCentroidsState.HierarchicalCentroid>> entry : centroidsByLevel
                            .entrySet()) {
                        int level = entry.getKey();
                        List<HierarchicalCentroidsState.HierarchicalCentroid> centroids = entry.getValue();
                        indexWriter.addClusterLevel(level, centroids);
                    }

                    // Output all nodes
                    ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                    DataOutput embBytesOutput = new DataOutputStream(embBytes);
                    AMutableDouble aDouble = new AMutableDouble(0);
                    AMutableInt32 aInt32 = new AMutableInt32(0);
                    OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                    ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                    orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);

                    int tupleOutputCount = 0;
                    int frameCount = 0;
                    
                    for (HierarchicalClusterTree.TreeNode node : allNodes) {
                        tupleOutputCount++;
                        System.err.println("=== OUTPUTTING TUPLE #" + tupleOutputCount + " ===");
                        System.err.println("=== NODE: level=" + node.getLevel() + ", clusterId=" + node.getClusterId() + ", globalId=" + node.getGlobalId() + " ===");
                        
                        double[] arr = node.getCentroid();
                        orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));
                        for (double value : arr) {
                            aDouble.setValue(value);
                            listStorage.reset();
                            listStorage.getDataOutput().writeByte(ATypeTag.FLOAT.serialize());
                            ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, listStorage.getDataOutput());
                            orderedListBuilder.addItem(listStorage);
                        }
                        embBytes.reset();
                        orderedListBuilder.write(embBytesOutput, true);
                        tupleBuilder.reset();

                        // Add level (int)
                        listStorage.reset();
                        listStorage.getDataOutput().writeByte(ATypeTag.INTEGER.serialize());
                        aInt32.setValue(node.getLevel());
                        AInt32SerializerDeserializer.INSTANCE.serialize(aInt32, listStorage.getDataOutput());
                        tupleBuilder.addField(listStorage.getByteArray(), 0, listStorage.getLength());

                        // Add clusterId (int) 
                        listStorage.reset();
                        listStorage.getDataOutput().writeByte(ATypeTag.INTEGER.serialize());
                        aInt32.setValue(node.getClusterId());
                        AInt32SerializerDeserializer.INSTANCE.serialize(aInt32, listStorage.getDataOutput());
                        tupleBuilder.addField(listStorage.getByteArray(), 0, listStorage.getLength());

                        // Add centroidId (int) - using globalId as centroidId
                        listStorage.reset();
                        listStorage.getDataOutput().writeByte(ATypeTag.INTEGER.serialize());
                        aInt32.setValue((int) node.getGlobalId());
                        AInt32SerializerDeserializer.INSTANCE.serialize(aInt32, listStorage.getDataOutput());
                        tupleBuilder.addField(listStorage.getByteArray(), 0, listStorage.getLength());

                        // Add embedding (float array)
                        tupleBuilder.addField(embBytes.getByteArray(), 0, embBytes.getLength());

                        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            // Frame is full, flush and reset
                            frameCount++;
                            System.err.println("=== FRAME #" + frameCount + " FULL, FLUSHING ===");
                            System.err.println("=== FRAME #" + frameCount + " CONTAINS " + (tupleOutputCount - 1) + " TUPLES ===");
                            
                            FrameUtils.flushFrame(appender.getBuffer(), writer);
                            appender.reset(new VSizeFrame(ctx), true);
                            appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                    tupleBuilder.getSize());
                        }
                    }
                    
                    // CRITICAL: Flush the final frame after outputting all nodes
                    // This ensures that the last batch of nodes is sent to the next operator
                    frameCount++;
                    System.err.println("=== FLUSHING FINAL FRAME #" + frameCount + " ===");
                    System.err.println("=== TOTAL TUPLES OUTPUT: " + tupleOutputCount + " ===");
                    System.err.println("=== TOTAL FRAMES OUTPUT: " + frameCount + " ===");
                    FrameUtils.flushFrame(appender.getBuffer(), writer);
                    System.err.println("=== FINAL FRAME FLUSHED ===");
                }

            };
        }
    }
}
