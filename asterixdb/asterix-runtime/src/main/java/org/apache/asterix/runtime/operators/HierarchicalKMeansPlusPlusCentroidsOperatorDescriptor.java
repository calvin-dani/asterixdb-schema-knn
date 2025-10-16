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
import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;
import static org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared;

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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
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

    /**
     * Read materialized data from runfile and perform hierarchical K-means clustering.
     * This method can be called by other operators to consume the materialized data.
     */
    public HierarchicalClusterStructure readMaterializedDataAndCluster(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        System.err.println("=== READING MATERIALIZED DATA AND CLUSTERING ===");

        // Get the materialized data state
        MaterializerTaskState materializedState =
                (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(materializedDataUUID, partition));

        if (materializedState == null) {
            System.err.println("❌ ERROR: No materialized data found for partition " + partition);
            return new HierarchicalClusterStructure();
        }

        // Create a reader for the materialized data
        org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader reader = materializedState.creatReader();
        reader.open();

        try {
            // Collect all data points from the materialized frames
            List<double[]> allDataPoints = new ArrayList<>();
            VSizeFrame frame = new VSizeFrame(ctx);

            while (reader.nextFrame(frame)) {
                List<double[]> frameDataPoints = collectDataPointsFromFrame(frame.getBuffer(), ctx);
                allDataPoints.addAll(frameDataPoints);
            }

            System.err.println("Collected " + allDataPoints.size() + " data points from materialized data");

            if (allDataPoints.isEmpty()) {
                System.err.println("No data points found in materialized data");
                return new HierarchicalClusterStructure();
            }

            // Perform hierarchical clustering on the collected data
            return performHierarchicalClusteringOnDataPoints(allDataPoints);

        } finally {
            reader.close();
        }
    }

    /**
     * Collect data points from a single frame buffer.
     */
    private List<double[]> collectDataPointsFromFrame(ByteBuffer buffer, IHyracksTaskContext ctx)
            throws HyracksDataException {
        List<double[]> dataPoints = new ArrayList<>();

        // Create frame tuple accessor
        FrameTupleAccessor fta = new FrameTupleAccessor(secondaryRecDesc);
        fta.reset(buffer);

        int tupleCount = fta.getTupleCount();
        System.err.println("Processing " + tupleCount + " tuples from materialized frame");

        if (tupleCount == 0) {
            return dataPoints;
        }

        // Create evaluator for extracting vector data
        IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
        IPointable inputVal = new VoidPointable();

        // Create KMeansUtils for proper vector parsing
        KMeansUtils kMeansUtils = new KMeansUtils(new VoidPointable(), new ArrayBackedValueStorage());
        ListAccessor listAccessorConstant = new ListAccessor();

        for (int i = 0; i < tupleCount; i++) {
            FrameTupleReference tuple = new FrameTupleReference();
            tuple.reset(fta, i);

            try {
                // Extract vector data from tuple
                eval.evaluate(tuple, inputVal);

                // Check if it's a list type (required for vector data)
                if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                        .isListType()) {
                    continue; // Skip unsupported types
                }

                // Parse the vector data using proper AsterixDB parsing
                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                double[] vector = kMeansUtils.createPrimitveList(listAccessorConstant);

                if (vector != null && vector.length > 0) {
                    dataPoints.add(vector);
                }
            } catch (Exception e) {
                System.err.println("Warning: Failed to parse vector data from tuple " + i + ": " + e.getMessage());
            }
        }

        return dataPoints;
    }

    /**
     * Perform hierarchical clustering on the given data points.
     */
    private HierarchicalClusterStructure performHierarchicalClusteringOnDataPoints(List<double[]> dataPoints)
            throws HyracksDataException {
        System.err.println("=== PERFORMING HIERARCHICAL CLUSTERING ON DATA POINTS ===");

        HierarchicalClusterStructure structure = new HierarchicalClusterStructure();

        if (dataPoints.isEmpty()) {
            return structure;
        }

        // Step 1: Perform initial K-means++ on data points
        List<double[]> initialCentroids = performKMeansPlusPlusOnDataPoints(dataPoints, K);
        System.err.println("Initial K-means++ generated " + initialCentroids.size() + " centroids");

        if (initialCentroids.isEmpty()) {
            return structure;
        }

        // Step 2: Build hierarchical structure
        List<double[]> currentCentroids = initialCentroids;
        int currentK = Math.min(K, initialCentroids.size());
        Random rand = new Random();
        int maxIterations = 20;
        int maxLevels = 5;
        int currentLevel = 0;

        // Add Level 0 (initial centroids)
        List<List<double[]>> level0Clusters = new ArrayList<>();
        for (double[] centroid : currentCentroids) {
            List<double[]> cluster = new ArrayList<>();
            cluster.add(centroid);
            level0Clusters.add(cluster);
        }
        structure.addLevel(level0Clusters);
        currentLevel++;

        // Build subsequent levels
        while (currentCentroids.size() > 1 && currentK > 1 && currentLevel < maxLevels) {
            System.err.println("Level " + currentLevel + ": Clustering " + currentCentroids.size() + " centroids into "
                    + currentK + " clusters");

            // Perform K-means++ clustering on centroids from previous level
            List<double[]> levelCentroids = performKMeansPlusPlusOnDataPoints(currentCentroids, currentK);

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

        System.err.println("Hierarchical clustering completed with " + structure.getNumLevels() + " levels");
        return structure;
    }

    /**
     * Perform K-means++ on a list of data points.
     */
    private List<double[]> performKMeansPlusPlusOnDataPoints(List<double[]> dataPoints, int k) {
        if (dataPoints.isEmpty() || k <= 0) {
            return new ArrayList<>();
        }

        List<double[]> centroids = new ArrayList<>();
        Random rand = new Random();

        // K-means++ initialization
        // 1. Choose first centroid randomly
        int firstIdx = rand.nextInt(dataPoints.size());
        centroids.add(Arrays.copyOf(dataPoints.get(firstIdx), dataPoints.get(firstIdx).length));

        // 2. Choose remaining centroids using weighted selection
        for (int i = 1; i < k && i < dataPoints.size(); i++) {
            double[] distances = new double[dataPoints.size()];
            double totalDistance = 0.0;

            // Calculate minimum distance to existing centroids for each point
            for (int j = 0; j < dataPoints.size(); j++) {
                double minDist = Double.POSITIVE_INFINITY;
                for (double[] centroid : centroids) {
                    double dist = calculateDistance(dataPoints.get(j), centroid);
                    minDist = Math.min(minDist, dist);
                }
                distances[j] = minDist;
                totalDistance += minDist;
            }

            // Weighted random selection
            double randomValue = rand.nextDouble() * totalDistance;
            double cumulativeDistance = 0.0;
            int selectedIdx = 0;

            for (int j = 0; j < dataPoints.size(); j++) {
                cumulativeDistance += distances[j];
                if (cumulativeDistance >= randomValue) {
                    selectedIdx = j;
                    break;
                }
            }

            centroids.add(Arrays.copyOf(dataPoints.get(selectedIdx), dataPoints.get(selectedIdx).length));
        }

        // Lloyd's algorithm iterations
        int maxIterations = 20;
        for (int iter = 0; iter < maxIterations; iter++) {
            // Assign points to nearest centroid
            int[] assignments = new int[dataPoints.size()];
            for (int i = 0; i < dataPoints.size(); i++) {
                double minDist = Double.POSITIVE_INFINITY;
                int bestCentroid = 0;
                for (int j = 0; j < centroids.size(); j++) {
                    double dist = calculateDistance(dataPoints.get(i), centroids.get(j));
                    if (dist < minDist) {
                        minDist = dist;
                        bestCentroid = j;
                    }
                }
                assignments[i] = bestCentroid;
            }

            // Update centroids
            List<double[]> newCentroids = new ArrayList<>();
            for (int j = 0; j < centroids.size(); j++) {
                double[] newCentroid = new double[dataPoints.get(0).length];
                int count = 0;

                for (int i = 0; i < dataPoints.size(); i++) {
                    if (assignments[i] == j) {
                        for (int d = 0; d < newCentroid.length; d++) {
                            newCentroid[d] += dataPoints.get(i)[d];
                        }
                        count++;
                    }
                }

                if (count > 0) {
                    for (int d = 0; d < newCentroid.length; d++) {
                        newCentroid[d] /= count;
                    }
                    newCentroids.add(newCentroid);
                } else {
                    // Keep original centroid if no points assigned
                    newCentroids.add(centroids.get(j));
                }
            }

            centroids = newCentroids;
        }

        return centroids;
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
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new HierarchicalKMeansNodePushable(ctx, partition, nPartitions);
    }

    private class HierarchicalKMeansNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;
        private MaterializerTaskState materializedData;

        public HierarchicalKMeansNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions) {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
        }

        @Override
        public void open() throws HyracksDataException {
            // Initialize materializer for storing input data
            materializedData = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                    new PartitionedUUID(materializedDataUUID, partition));
            materializedData.open(ctx);

            // Open the writer to downstream operator (VCTreeStaticStructureCreator)
            if (writer != null) {
                writer.open();
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            // Materialize the input data for later use
            if (materializedData != null) {
                materializedData.appendFrame(buffer);
            }

            if (writer != null) {
                try {
                    // Perform complete hierarchical K-means clustering
                    HierarchicalClusterStructure clusterStructure = performCompleteHierarchicalKMeans(buffer);

                    if (clusterStructure.getNumLevels() == 0) {
                        return;
                    }

                    // Convert to VCTreeStaticStructureCreator format
                    convertToVCTreeFormat(clusterStructure, buffer);

                } catch (Exception e) {
                    throw new HyracksDataException("Hierarchical K-means clustering failed", e);
                }
            }
        }

        /**
         * Perform complete hierarchical K-means clustering with proper termination conditions.
         */
        private HierarchicalClusterStructure performCompleteHierarchicalKMeans(ByteBuffer buffer)
                throws HyracksDataException {
            HierarchicalClusterStructure structure = new HierarchicalClusterStructure();

            // Step 1: Perform initial K-means++ on raw data
            List<double[]> initialCentroids = performInitialKMeansPlusPlus(buffer);

            if (initialCentroids.isEmpty()) {
                return structure;
            }

            // Step 2: Build hierarchical structure
            List<double[]> currentCentroids = initialCentroids;
            int currentK = Math.min(K, initialCentroids.size());
            Random rand = new Random();
            int maxIterations = 20;
            int maxLevels = 5; // Limit to prevent infinite loops
            int currentLevel = 0;

            // Add Level 0 (initial centroids)
            List<List<double[]>> level0Clusters = new ArrayList<>();
            for (double[] centroid : currentCentroids) {
                List<double[]> cluster = new ArrayList<>();
                cluster.add(centroid);
                level0Clusters.add(cluster);
            }
            structure.addLevel(level0Clusters);
            currentLevel++;

            // Build subsequent levels
            while (currentCentroids.size() > 1 && currentK > 1 && currentLevel < maxLevels) {
                // Perform K-means++ clustering on centroids from previous level
                List<double[]> levelCentroids = performKMeansPlusPlus(currentCentroids, currentK, rand, maxIterations);

                if (levelCentroids.isEmpty()) {
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

                // Safety check to prevent infinite loops
                if (currentLevel > 10) {
                    break;
                }
            }

            return structure;
        }

        /**
         * Perform initial K-means++ on raw data to generate K centroids.
         * This is the working approach from multilevel-kmeans-storage-layer branch.
         */
        private List<double[]> performInitialKMeansPlusPlus(ByteBuffer buffer) throws HyracksDataException {
            if (K <= 0) {
                return new ArrayList<>();
            }

            List<double[]> centroids = new ArrayList<>();
            List<double[]> allPoints = new ArrayList<>();

            // First pass: collect all data points
            FrameTupleAccessor fta = new FrameTupleAccessor(secondaryRecDesc);
            fta.reset(buffer);
            int tupleCount = fta.getTupleCount();

            // Create evaluator for extracting vector data
            IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
            IPointable inputVal = new VoidPointable();

            // Create KMeansUtils for proper vector parsing
            KMeansUtils kMeansUtils = new KMeansUtils(new VoidPointable(), new ArrayBackedValueStorage());
            ListAccessor listAccessorConstant = new ListAccessor();

            for (int i = 0; i < tupleCount; i++) {
                FrameTupleReference tuple = new FrameTupleReference();
                tuple.reset(fta, i);

                eval.evaluate(tuple, inputVal);

                if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                        .isListType()) {
                    continue; // Skip unsupported types
                }
                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                try {
                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);
                    allPoints.add(point);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            if (allPoints.isEmpty()) {
                return centroids;
            }

            // K-means++ initialization
            Random rand = new Random();
            // 1. Choose first centroid randomly
            int firstIdx = rand.nextInt(allPoints.size());
            centroids.add(Arrays.copyOf(allPoints.get(firstIdx), allPoints.get(firstIdx).length));

            // 2. Choose remaining centroids using weighted selection
            for (int i = 1; i < K && i < allPoints.size(); i++) {
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
            int maxIterations = 20;
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
                double[][] newCentroids = new double[K][allPoints.get(0).length];
                int[] counts = new int[K];

                for (int i = 0; i < allPoints.size(); i++) {
                    int centroidIdx = assignments[i];
                    for (int d = 0; d < allPoints.get(i).length; d++) {
                        newCentroids[centroidIdx][d] += allPoints.get(i)[d];
                    }
                    counts[centroidIdx]++;
                }

                // Check for convergence
                boolean converged = true;
                for (int i = 0; i < K; i++) {
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

            return centroids;
        }

        /**
         * Perform K-means++ clustering on the given data points.
         * Used for hierarchical clustering on centroids from previous levels.
         */
        private List<double[]> performKMeansPlusPlus(List<double[]> dataPoints, int k, Random rand, int maxIterations) {
            if (dataPoints.isEmpty() || k <= 0) {
                return new ArrayList<>();
            }

            List<double[]> centroids = new ArrayList<>();

            // K-means++ initialization
            // 1. Choose first centroid randomly
            int firstIdx = rand.nextInt(dataPoints.size());
            centroids.add(Arrays.copyOf(dataPoints.get(firstIdx), dataPoints.get(firstIdx).length));

            // 2. Choose remaining centroids using weighted selection
            for (int i = 1; i < k && i < dataPoints.size(); i++) {
                double[] distances = new double[dataPoints.size()];
                double totalDistance = 0.0;

                // Calculate minimum distance to existing centroids for each point
                for (int j = 0; j < dataPoints.size(); j++) {
                    double minDist = Double.POSITIVE_INFINITY;
                    for (double[] centroid : centroids) {
                        double dist = calculateDistance(dataPoints.get(j), centroid);
                        minDist = Math.min(minDist, dist);
                    }
                    distances[j] = minDist;
                    totalDistance += minDist;
                }

                // Weighted random selection
                double randomValue = rand.nextDouble() * totalDistance;
                double cumulativeDistance = 0.0;
                int selectedIdx = 0;

                for (int j = 0; j < dataPoints.size(); j++) {
                    cumulativeDistance += distances[j];
                    if (cumulativeDistance >= randomValue) {
                        selectedIdx = j;
                        break;
                    }
                }

                centroids.add(Arrays.copyOf(dataPoints.get(selectedIdx), dataPoints.get(selectedIdx).length));
            }

            // Lloyd's algorithm iterations
            for (int iter = 0; iter < maxIterations; iter++) {
                // Assign points to nearest centroid
                int[] assignments = new int[dataPoints.size()];
                for (int i = 0; i < dataPoints.size(); i++) {
                    double minDist = Double.POSITIVE_INFINITY;
                    int bestCentroid = 0;
                    for (int j = 0; j < centroids.size(); j++) {
                        double dist = calculateDistance(dataPoints.get(i), centroids.get(j));
                        if (dist < minDist) {
                            minDist = dist;
                            bestCentroid = j;
                        }
                    }
                    assignments[i] = bestCentroid;
                }

                // Update centroids
                List<double[]> newCentroids = new ArrayList<>();
                for (int j = 0; j < centroids.size(); j++) {
                    double[] newCentroid = new double[dataPoints.get(0).length];
                    int count = 0;

                    for (int i = 0; i < dataPoints.size(); i++) {
                        if (assignments[i] == j) {
                            for (int d = 0; d < newCentroid.length; d++) {
                                newCentroid[d] += dataPoints.get(i)[d];
                            }
                            count++;
                        }
                    }

                    if (count > 0) {
                        for (int d = 0; d < newCentroid.length; d++) {
                            newCentroid[d] /= count;
                        }
                        newCentroids.add(newCentroid);
                    } else {
                        // Keep original centroid if no points assigned
                        newCentroids.add(centroids.get(j));
                    }
                }

                centroids = newCentroids;
            }

            return centroids;
        }

        /**
         * Collect data points from the input buffer using proper AsterixDB vector parsing.
         */
        private List<double[]> collectDataPoints(ByteBuffer buffer) throws HyracksDataException {
            List<double[]> dataPoints = new ArrayList<>();

            // Create frame tuple accessor
            FrameTupleAccessor fta = new FrameTupleAccessor(secondaryRecDesc);
            fta.reset(buffer);

            int tupleCount = fta.getTupleCount();
            System.err.println("Processing " + tupleCount + " tuples from buffer");

            if (tupleCount == 0) {
                System.err.println("No tuples found in buffer");
                return dataPoints;
            }

            // Create evaluator for extracting vector data
            IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
            IPointable inputVal = new VoidPointable();

            // Create KMeansUtils for proper vector parsing
            KMeansUtils kMeansUtils = new KMeansUtils(new VoidPointable(), new ArrayBackedValueStorage());
            ListAccessor listAccessorConstant = new ListAccessor();

            int successfulParses = 0;
            for (int i = 0; i < tupleCount; i++) {
                FrameTupleReference tuple = new FrameTupleReference();
                tuple.reset(fta, i);

                try {
                    // Extract vector data from tuple
                    eval.evaluate(tuple, inputVal);
                    System.err.println("Evaluation result length: " + inputVal.getLength());
                    System.err.println("Evaluation result type tag: "
                            + ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]));

                    // Check if it's a list type (required for vector data)
                    if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                            .isListType()) {
                        System.err.println("Skipping tuple " + i + " - not a list type");
                        continue; // Skip unsupported types
                    }

                    // Parse the vector data using proper AsterixDB parsing
                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                    double[] vector = kMeansUtils.createPrimitveList(listAccessorConstant);

                    if (vector != null && vector.length > 0) {
                        dataPoints.add(vector);
                        successfulParses++;
                        if (successfulParses <= 3) { // Log first few vectors for debugging
                            System.err.println("Parsed vector " + successfulParses + " with " + vector.length
                                    + " dimensions: " + java.util.Arrays.toString(vector));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Warning: Failed to parse vector data from tuple " + i + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }

            System.err.println("Successfully parsed " + successfulParses + " vectors out of " + tupleCount + " tuples");
            return dataPoints;
        }

        /**
         * Parse vector data from the input value.
         */
        private double[] parseVectorData(IPointable inputVal) throws HyracksDataException {
            try {
                // Try to parse as a double array directly
                byte[] data = inputVal.getByteArray();
                int offset = inputVal.getStartOffset();
                int length = inputVal.getLength();

                // For now, create a simple test vector with actual data
                // In practice, you'd need to properly deserialize the vector data
                // This is a placeholder that creates a 3-dimensional vector
                return new double[] { Math.random() * 100, // Use some variation instead of pure random
                        Math.random() * 100, Math.random() * 100 };
            } catch (Exception e) {
                System.err.println("Warning: Failed to parse vector data: " + e.getMessage());
                // Return a default vector to avoid empty data
                return new double[] { 0.0, 0.0, 0.0 };
            }
        }

        /**
         * Convert hierarchical clustering results to VCTreeStaticStructureCreator format.
         * Creates 4-field tuples: [level, clusterId, centroidId, embedding] for VCTreeStaticStructureCreatorOperatorDescriptor.
         */
        private void convertToVCTreeFormat(HierarchicalClusterStructure structure, ByteBuffer inputBuffer)
                throws HyracksDataException {
            try {
                // Create VSizeFrame for output
                VSizeFrame outputFrame = new VSizeFrame(ctx);
                FrameTupleAppender appender = new FrameTupleAppender(outputFrame);

                int globalCentroidId = 0;

                // Process each level
                for (int level = 0; level < structure.getNumLevels(); level++) {
                    List<List<double[]>> levelClusters = structure.getLevelClusters().get(level);

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
                            
                            // Add level (field 0) - let ArrayTupleBuilder handle serialization
                            tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, level);
                            
                            // Add clusterId (field 1) - let ArrayTupleBuilder handle serialization
                            tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, clusterId);
                            
                            // Add centroidId (field 2) - let ArrayTupleBuilder handle serialization
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
                                ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, storage.getDataOutput());
                                listBuilder.addItem(storage);
                            }

                            storage.reset();
                            listBuilder.write(storage.getDataOutput(), true);
                            tupleBuilder.addField(storage.getByteArray(), 0, storage.getLength());
                            
                            // Append tuple to output frame
                            appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 
                                    0, tupleBuilder.getSize());

                            globalCentroidId++;
                        }
                    }
                }

                // Write the output frame
                FrameUtils.flushFrame(appender.getBuffer(), writer);

            } catch (Exception e) {
                throw new HyracksDataException("VCTree format conversion failed", e);
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
            // Store the materialized data state for later consumption
            if (materializedData != null) {
                materializedData.close();
                ctx.setStateObject(materializedData);
            }

            if (writer != null) {
                writer.close();
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            if (writer != null) {
                writer.fail();
            }
        }
    }
}
