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

import static org.apache.asterix.om.types.BuiltinType.ADOUBLE;
import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;
import static org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
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
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
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
public final class HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor extends AbstractOperatorDescriptor {

    /**
     * Data structure to hold hierarchical clustering results for VCTreeStaticStructureCreator.
     */
    private static class HierarchicalClusterStructure {
        private final List<List<List<double[]>>> levelClusters; // level -> cluster -> centroids
        private final List<List<Integer>> clustersPerLevel;
        private final List<List<List<Integer>>> centroidsPerCluster;

        public HierarchicalClusterStructure() {
            this.levelClusters = new ArrayList<>();
            this.clustersPerLevel = new ArrayList<>();
            this.centroidsPerCluster = new ArrayList<>();
        }

        public void addLevel(List<List<double[]>> levelClusters) {
            this.levelClusters.add(levelClusters);

            List<Integer> levelClusterCounts = new ArrayList<>();
            List<List<Integer>> levelCentroidCounts = new ArrayList<>();

            for (List<double[]> cluster : levelClusters) {
                levelClusterCounts.add(1); // Each cluster has 1 centroid
                List<Integer> centroidCounts = new ArrayList<>();
                centroidCounts.add(cluster.size()); // Number of centroids in this cluster
                levelCentroidCounts.add(centroidCounts);
            }

            this.clustersPerLevel.add(levelClusterCounts);
            this.centroidsPerCluster.add(levelCentroidCounts);
        }

        public List<List<List<double[]>>> getLevelClusters() {
            return levelClusters;
        }

        public List<List<Integer>> getClustersPerLevel() {
            return clustersPerLevel;
        }

        public List<List<List<Integer>>> getCentroidsPerCluster() {
            return centroidsPerCluster;
        }

        public int getNumLevels() {
            return levelClusters.size();
        }
    }

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
    private final UUID materializedDataUUID;

    // Configuration parameters for hierarchical clustering
    private IScalarEvaluatorFactory args; // Evaluator for extracting vector data from tuples
    private int K; // Number of clusters for initial level (leaf nodes)
    private int maxScalableKmeansIter; // Maximum iterations for scalable K-means++ candidate selection
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
            // Use distance function if available, otherwise fall back to euclidean squared
            if (distanceFunction != null) {
                return distanceFunction.apply(a, b);
            } else {
                return euclidean_squared(a, b);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error calculating distance", e);
        }
    }

    public HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor outputRecDesc, RecordDescriptor secondaryRecDesc, UUID sampleUUID, UUID centroidsUUID,
            UUID materializedDataUUID, IScalarEvaluatorFactory args, int K, int maxScalableKmeansIter) {
        super(spec, 1, 1);
        // Output record descriptor defines the format of output tuples (level, clusterId, centroidId, embedding)
        // Input record descriptor is the 2-field format with vector embeddings
        outRecDescs[0] = outputRecDesc; // Output format (hierarchical structure)
        this.secondaryRecDesc = secondaryRecDesc; // Input format (2-field with vector embeddings)
        this.sampleUUID = sampleUUID;
        this.centroidsUUID = centroidsUUID;
        this.materializedDataUUID = materializedDataUUID;
        this.args = args;
        this.K = K;
        this.maxScalableKmeansIter = maxScalableKmeansIter;

        // Initialize distance function to euclidean squared to avoid null pointer issues
        this.distanceFunction = new EuclideanSquaredDistanceFunction();
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        // Activity 1: Store centroids and materialize data
        StoreCentroidsActivity storeCentroidsActivity = new StoreCentroidsActivity(new ActivityId(odId, 0));
        // Activity 2: Find candidates and perform hierarchical clustering
        FindCandidatesActivity findCandidatesActivity = new FindCandidatesActivity(new ActivityId(odId, 1));

        builder.addActivity(this, storeCentroidsActivity);
        builder.addSourceEdge(0, storeCentroidsActivity, 0);

        builder.addActivity(this, findCandidatesActivity);
        builder.addTargetEdge(0, findCandidatesActivity, 0);

        // Add blocking edge to ensure data accumulation completes before clustering
        builder.addBlockingEdge(storeCentroidsActivity, findCandidatesActivity);
    }

    /**
     * Activity 1: Store Centroids and Materialize Data
     * This activity performs initial K-means++ on raw data and materializes all data for later processing.
     */
    protected class StoreCentroidsActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

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
                    first = true;
                    kMeansUtils = new KMeansUtils(new VoidPointable(), new ArrayBackedValueStorage());
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (first) {
                        // CRITICAL: Perform initial K-means++ on raw data to generate K centroids
                        // This is essential for hierarchical clustering - we need multiple centroids
                        // to start the hierarchical tree building process
                        System.err.println("Starting initial K-means++ on raw data with K=" + K);

                        // Perform K-means++ on the raw data
                        Random rand = new Random();
                        int maxKMeansIterations = 20;
                        List<double[]> initialCentroids = performInitialKMeansPlusPlus(buffer, eval, inputVal,
                                kMeansUtils, K, rand, maxKMeansIterations);

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

                /**
                 * Performs initial K-means++ on raw data to generate K centroids
                 */
                private List<double[]> performInitialKMeansPlusPlus(ByteBuffer buffer, IScalarEvaluator eval,
                        IPointable inputVal, KMeansUtils kMeansUtils, int k, Random rand, int maxIterations)
                        throws HyracksDataException {

                    if (k <= 0) {
                        return new ArrayList<>();
                    }

                    List<double[]> centroids = new ArrayList<>();
                    List<double[]> allPoints = new ArrayList<>();

                    // First pass: collect all data points
                    FrameTupleAccessor fta = new FrameTupleAccessor(secondaryRecDesc);
                    fta.reset(buffer);
                    int tupleCount = fta.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        FrameTupleReference tuple = new FrameTupleReference();
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

                    System.err.println("performInitialKMeansPlusPlus: collected " + allPoints.size()
                            + " data points, target k = " + k);

                    // K-means++ initialization
                    // 1. Choose first centroid randomly
                    int firstIdx = rand.nextInt(allPoints.size());
                    centroids.add(Arrays.copyOf(allPoints.get(firstIdx), allPoints.get(firstIdx).length));
                    System.err.println("Added first centroid at index " + firstIdx);

                    // 2. Choose remaining centroids using weighted selection
                    for (int i = 1; i < k; i++) {
                        double[] distances = new double[allPoints.size()];
                        double totalDistance = 0.0;

                        // Calculate minimum distance to existing centroids for each point
                        for (int j = 0; j < allPoints.size(); j++) {
                            double minDist = Double.POSITIVE_INFINITY;
                            for (double[] centroid : centroids) {
                                double dist = calculateDistance(allPoints.get(j), centroid);
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
                        System.err.println("Added centroid " + i + " at index " + selectedIdx);
                    }

                    // 3. Lloyd's algorithm for refinement
                    for (int iter = 0; iter < maxIterations; iter++) {
                        // Assign points to closest centroids
                        int[] assignments = new int[allPoints.size()];
                        for (int i = 0; i < allPoints.size(); i++) {
                            double minDist = Double.POSITIVE_INFINITY;
                            int closestCentroid = 0;
                            for (int j = 0; j < centroids.size(); j++) {
                                double dist = calculateDistance(allPoints.get(i), centroids.get(j));
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
                                double dist = calculateDistance(centroids.get(i), newCentroids[i]);
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

                    System.err.println("performInitialKMeansPlusPlus: generated " + centroids.size()
                            + " centroids (target was " + k + ")");

                    return centroids;
                }
            };
        }
    }

    /**
     * Activity 2: Find Candidates and Perform Hierarchical Clustering
     * This activity performs memory-efficient hierarchical clustering using the materialized data.
     */
    protected class FindCandidatesActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected FindCandidatesActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
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
                        fta = new FrameTupleAccessor(secondaryRecDesc);
                        tuple = new FrameTupleReference();
                        VSizeFrame vSizeFrame = new VSizeFrame(ctx);
                        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                        ListAccessor listAccessorConstant = new ListAccessor();

                        writer.open();

                        // Perform memory-efficient hierarchical K-means clustering
                        HierarchicalClusterStructure clusterStructure =
                                performMemoryEfficientHierarchicalKMeans(ctx, in, fta, tuple, eval, inputVal,
                                        listAccessorConstant, KMeansUtils, vSizeFrame, partition);

                        if (clusterStructure.getNumLevels() == 0) {
                            System.err.println("No clustering structure generated");
                            return;
                        }

                        // Convert to VCTreeStaticStructureCreator format
                        convertToVCTreeFormat(clusterStructure, appender);

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
                 * Perform memory-efficient hierarchical K-means clustering using run files.
                 */
                private HierarchicalClusterStructure performMemoryEfficientHierarchicalKMeans(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, int partition)
                        throws HyracksDataException, IOException {

                    System.err.println("=== PERFORMING MEMORY-EFFICIENT HIERARCHICAL K-MEANS ===");

                    HierarchicalClusterStructure structure = new HierarchicalClusterStructure();

                    // Get the first centroid that was stored in StoreCentroidsActivity
                    CentroidsState centroidsState =
                            (CentroidsState) ctx.getStateObject(new PartitionedUUID(centroidsUUID, partition));
                    List<double[]> existingCentroids = centroidsState.getCentroids();

                    if (existingCentroids.isEmpty()) {
                        System.err.println("No existing centroids found, cannot perform hierarchical clustering");
                        return structure;
                    }

                    System.err.println("Found " + existingCentroids.size()
                            + " existing centroids, starting hierarchical clustering");

                    // Add Level 0 (initial centroids)
                    List<List<double[]>> level0Clusters = new ArrayList<>();
                    for (double[] centroid : existingCentroids) {
                        List<double[]> cluster = new ArrayList<>();
                        cluster.add(centroid);
                        level0Clusters.add(cluster);
                    }
                    structure.addLevel(level0Clusters);

                    // Build subsequent levels using scalable K-means++ on centroids
                    List<double[]> currentCentroids = existingCentroids;
                    int currentK = Math.min(K, existingCentroids.size());
                    Random rand = new Random();
                    int maxIterations = 20;
                    int maxLevels = 5;
                    int currentLevel = 1;

                    // Build subsequent levels
                    while (currentCentroids.size() > 1 && currentK > 1 && currentLevel < maxLevels) {
                        System.err.println("Level " + currentLevel + ": Clustering " + currentCentroids.size()
                                + " centroids into " + currentK + " clusters");

                        // Perform K-means++ clustering on centroids from previous level
                        List<double[]> levelCentroids = performScalableKMeansPlusPlusOnCentroids(currentCentroids,
                                currentK, rand, maxIterations);

                        if (levelCentroids.isEmpty()) {
                            System.err.println("K-means++ produced no centroids, stopping clustering");
                            break;
                        }

                        // Group centroids into clusters for this level
                        List<List<double[]>> levelClusters = new ArrayList<>();
                        for (double[] centroid : levelCentroids) {
                            List<double[]> cluster = new ArrayList<>();
                            cluster.add(centroid);
                            levelClusters.add(cluster);
                        }

                        structure.addLevel(levelClusters);

                        // Prepare for next level
                        currentCentroids = levelCentroids;
                        currentK = Math.max(1, currentK / 2);
                        currentLevel++;
                    }

                    System.err
                            .println("Hierarchical clustering completed with " + structure.getNumLevels() + " levels");
                    return structure;
                }

                /**
                 * Perform scalable K-means++ on centroids (not raw data).
                 */
                private List<double[]> performScalableKMeansPlusPlusOnCentroids(List<double[]> centroids, int k,
                        Random rand, int maxIterations) {
                    if (centroids.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    List<double[]> resultCentroids = new ArrayList<>();

                    // K-means++ initialization
                    // 1. Choose first centroid randomly
                    int firstIdx = rand.nextInt(centroids.size());
                    resultCentroids.add(Arrays.copyOf(centroids.get(firstIdx), centroids.get(firstIdx).length));

                    // 2. Choose remaining centroids using weighted selection
                    for (int i = 1; i < k && i < centroids.size(); i++) {
                        double[] distances = new double[centroids.size()];
                        double totalDistance = 0.0;

                        // Calculate minimum distance to existing centroids for each point
                        for (int j = 0; j < centroids.size(); j++) {
                            double minDist = Double.POSITIVE_INFINITY;
                            for (double[] centroid : resultCentroids) {
                                double dist = calculateDistance(centroids.get(j), centroid);
                                minDist = Math.min(minDist, dist);
                            }
                            distances[j] = minDist;
                            totalDistance += minDist;
                        }

                        // Weighted random selection
                        double r = rand.nextDouble() * totalDistance;
                        double cumulativeDistance = 0.0;
                        int selectedIdx = 0;
                        for (int j = 0; j < centroids.size(); j++) {
                            cumulativeDistance += distances[j];
                            if (cumulativeDistance >= r) {
                                selectedIdx = j;
                                break;
                            }
                        }

                        resultCentroids
                                .add(Arrays.copyOf(centroids.get(selectedIdx), centroids.get(selectedIdx).length));
                    }

                    // 3. Lloyd's algorithm for refinement
                    for (int iter = 0; iter < maxIterations; iter++) {
                        // Assign points to closest centroids
                        int[] assignments = new int[centroids.size()];
                        for (int i = 0; i < centroids.size(); i++) {
                            double minDist = Double.POSITIVE_INFINITY;
                            int closestCentroid = 0;
                            for (int j = 0; j < resultCentroids.size(); j++) {
                                double dist = calculateDistance(centroids.get(i), resultCentroids.get(j));
                                if (dist < minDist) {
                                    minDist = dist;
                                    closestCentroid = j;
                                }
                            }
                            assignments[i] = closestCentroid;
                        }

                        // Update centroids
                        double[][] newCentroids = new double[k][centroids.get(0).length];
                        int[] counts = new int[k];

                        for (int i = 0; i < centroids.size(); i++) {
                            int centroidIdx = assignments[i];
                            for (int d = 0; d < centroids.get(i).length; d++) {
                                newCentroids[centroidIdx][d] += centroids.get(i)[d];
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
                                double dist = calculateDistance(resultCentroids.get(i), newCentroids[i]);
                                if (dist > 1e-4) {
                                    converged = false;
                                }
                                resultCentroids.set(i, newCentroids[i]);
                            }
                        }

                        if (converged) {
                            break;
                        }
                    }

                    return resultCentroids;
                }

                /**
                 * Convert hierarchical clustering results to VCTreeStaticStructureCreator format.
                 * Creates 4-field tuples: [level, clusterId, centroidId, embedding] for VCTreeStaticStructureCreatorOperatorDescriptor.
                 */
                private void convertToVCTreeFormat(HierarchicalClusterStructure structure, FrameTupleAppender appender)
                        throws HyracksDataException {
                    try {
                        System.err.println("=== CONVERTING TO VCTREE FORMAT ===");
                        System.err.println("Total levels: " + structure.getNumLevels());

                        int globalCentroidId = 0;
                        int totalTuplesCreated = 0;

                        System.err.println("Creating VCTree tuples...");

                        // Process each level
                        for (int level = 0; level < structure.getNumLevels(); level++) {
                            List<List<double[]>> levelClusters = structure.getLevelClusters().get(level);
                            System.err.println(
                                    "Processing level " + level + " with " + levelClusters.size() + " clusters...");

                            // Process each cluster in the level
                            for (int clusterId = 0; clusterId < levelClusters.size(); clusterId++) {
                                List<double[]> clusterCentroids = levelClusters.get(clusterId);

                                // Process each centroid in the cluster
                                for (int centroidId = 0; centroidId < clusterCentroids.size(); centroidId++) {
                                    double[] embedding = clusterCentroids.get(centroidId);

                                    // Create tuple using ArrayTupleBuilder for proper field end offsets
                                    // Format: [level, clusterId, centroidId, embedding]
                                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
                                    tupleBuilder.reset();

                                    // Add level (field 0)
                                    tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, level);

                                    // Add clusterId (field 1)
                                    tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, clusterId);

                                    // Add centroidId (field 2)
                                    tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, globalCentroidId);

                                    // Add embedding (field 3) - create AsterixDB AOrderedList format
                                    OrderedListBuilder listBuilder = new OrderedListBuilder();
                                    listBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));

                                    ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                                    AMutableDouble aDouble = new AMutableDouble(0.0);

                                    for (int i = 0; i < embedding.length; i++) {
                                        aDouble.setValue(embedding[i]);
                                        storage.reset();
                                        storage.getDataOutput().writeByte(ATypeTag.DOUBLE.serialize());
                                        ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble,
                                                storage.getDataOutput());
                                        listBuilder.addItem(storage);
                                    }

                                    storage.reset();
                                    listBuilder.write(storage.getDataOutput(), true);
                                    tupleBuilder.addField(storage.getByteArray(), 0, storage.getLength());

                                    // Append tuple to output frame
                                    appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                            tupleBuilder.getSize());

                                    totalTuplesCreated++;
                                    globalCentroidId++;
                                }
                            }
                        }

                        System.err.println("VCTree tuple creation complete: " + totalTuplesCreated + " tuples created");

                    } catch (Exception e) {
                        System.err.println("ERROR: VCTree format conversion failed: " + e.getMessage());
                        e.printStackTrace();
                        throw HyracksDataException.create(e);
                    }
                }
            };
        }
    }
}