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

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunction;
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
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
     * Simple state class to pass tuple count between activities.
     */
    private static class TupleCountState extends AbstractStateObject {
        private static final long serialVersionUID = 1L;
        private int totalTupleCount;

        public TupleCountState(JobId jobId, PartitionedUUID objectId) {
            super(jobId, objectId);
            this.totalTupleCount = 0;
        }

        public int getTotalTupleCount() {
            return totalTupleCount;
        }

        public void setTotalTupleCount(int totalTupleCount) {
            this.totalTupleCount = totalTupleCount;
        }

        public void addTupleCount(int count) {
            this.totalTupleCount += count;
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
            out.writeInt(totalTupleCount);
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
            totalTupleCount = in.readInt();
        }
    }

    /**
     * Result class for K-means++ clustering operations.
     */
    private static class ClusteringResult {
        public final List<double[]> centroids;
        public final int[] assignments;

        public ClusteringResult(List<double[]> centroids, int[] assignments) {
            this.centroids = centroids;
            this.assignments = assignments;
        }
    }

    /**
     * Data structure to hold hierarchical clustering results with parent-child relationships.
     */
    private static class HierarchicalClusterStructure {
        // Store centroids for each level (separate parent and child levels)
        private final Map<Integer, List<CentroidInfo>> levelCentroids;

        // Track parent-child relationships
        private final Map<Integer, Map<Integer, List<Integer>>> parentChildRelations;

        public HierarchicalClusterStructure() {
            this.levelCentroids = new HashMap<>();
            this.parentChildRelations = new HashMap<>();
        }

        public static class CentroidInfo {
            public final int centroidId;
            public final int parentClusterId;
            public final double[] embedding;
            public final int level;
            public final List<Integer> childrenIds;

            public CentroidInfo(int centroidId, int parentClusterId, double[] embedding, int level) {
                this.centroidId = centroidId;
                this.parentClusterId = parentClusterId;
                this.embedding = embedding;
                this.level = level;
                this.childrenIds = new ArrayList<>();
            }
        }

        /**
         * Initialize a level with empty centroids (for parents)
         */
        public void initializeParentLevel(int level, int parentCount) {
            List<CentroidInfo> parentLevel = new ArrayList<>();
            Map<Integer, List<Integer>> parentChildMap = new HashMap<>();

            // Initialize empty parent centroids
            for (int i = 0; i < parentCount; i++) {
                parentLevel.add(new CentroidInfo(i, -1, null, level)); // -1 means no parent (root level)
                parentChildMap.put(i, new ArrayList<>());
            }

            this.levelCentroids.put(level, parentLevel);
            this.parentChildRelations.put(level, parentChildMap);

            System.err.println("Initialized parent level " + level + " with " + parentCount + " empty centroids");
        }

        /**
         * Build parent-child relationships using assignments
         */
        public void buildLevelFromAssignments(List<double[]> childCentroids, List<double[]> parentCentroids,
                int[] assignments, int parentLevel, int childLevel) {

            // 1. Populate parent centroids
            List<CentroidInfo> parentLevelInfo = this.levelCentroids.get(parentLevel);
            for (int i = 0; i < parentCentroids.size() && i < parentLevelInfo.size(); i++) {
                CentroidInfo parentInfo = parentLevelInfo.get(i);
                // Update parent centroid with actual embedding
                parentLevelInfo.set(i,
                        new CentroidInfo(parentInfo.centroidId, -1, parentCentroids.get(i), parentLevel));
            }

            // 2. Create child level with proper parent assignments
            List<CentroidInfo> childLevelInfo = new ArrayList<>();
            Map<Integer, List<Integer>> parentChildMap = this.parentChildRelations.get(parentLevel);

            for (int i = 0; i < assignments.length; i++) {
                int parentClusterId = assignments[i]; // Which parent cluster this child belongs to
                int childId = i; // Child centroid index

                // Create child centroid info
                CentroidInfo childInfo = new CentroidInfo(childId, parentClusterId, childCentroids.get(i), childLevel);
                childLevelInfo.add(childInfo);

                // Add child to parent's children list
                if (parentChildMap.containsKey(parentClusterId)) {
                    parentChildMap.get(parentClusterId).add(childId);
                }
            }

            // Store child level information
            this.levelCentroids.put(childLevel, childLevelInfo);

            System.err.println("Built level " + childLevel + " with " + childLevelInfo.size() + " child centroids");
            System.err.println(
                    "Parent level " + parentLevel + " now has " + parentCentroids.size() + " centroids with children");
        }

        /**
         * Output format: <treeLevel, centroidId, parentClusterId, embedding>
         * Uses BFS traversal starting from root level
         */
        public void outputHierarchicalStructure(FrameTupleAppender appender, IFrameWriter writer,
                IHyracksTaskContext ctx) throws HyracksDataException {
            System.err.println("=== OUTPUTTING HIERARCHICAL STRUCTURE (BFS) ===");

            // Find the root level (highest level number)
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }

            if (maxLevel == -1) {
                System.err.println("No levels found to output");
                return;
            }

            // BFS traversal starting from root level
            Queue<Integer> levelQueue = new LinkedList<>();
            levelQueue.offer(maxLevel); // Start from root
            int treeLevel = 0;
            int globalCentroidId = 0;
            while (!levelQueue.isEmpty()) {
                int currentLevel = levelQueue.poll();
                List<CentroidInfo> levelInfo = levelCentroids.get(currentLevel);

                if (levelInfo == null)
                    continue;

                System.err.println("Processing level " + currentLevel + " with " + levelInfo.size() + " centroids");

                // Output all centroids in current level
                for (CentroidInfo centroid : levelInfo) {
                    createHierarchicalTuple(treeLevel, globalCentroidId, centroid.parentClusterId, centroid.embedding,
                            appender, writer, ctx);
                    globalCentroidId++;
                }

                // Add child level to queue if it exists
                int childLevel = currentLevel - 1;
                treeLevel++;
                if (levelCentroids.containsKey(childLevel)) {
                    levelQueue.offer(childLevel);
                }
            }
        }

        private void createHierarchicalTuple(int treeLevel, int centroidId, int parentClusterId, double[] embedding,
                FrameTupleAppender appender, IFrameWriter writer, IHyracksTaskContext ctx) throws HyracksDataException {
            try {
                // Apply clipping to embedding before creating tuple to prevent exorbitant values
                double[] clippedEmbedding = clipCentroid(embedding);

                // Create tuple: <treeLevel, centroidId, parentClusterId, embedding>
                ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
                tupleBuilder.reset();

                // Field 0: Tree Level
                tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, treeLevel);

                // Field 1: Centroid ID
                tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, centroidId);

                // Field 2: Parent Cluster ID
                tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, parentClusterId);

                // Field 3: Embedding - create AsterixDB AOrderedList format using clipped embedding
                OrderedListBuilder listBuilder = new OrderedListBuilder();
                listBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));

                ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                AMutableDouble aDouble = new AMutableDouble(0.0);

                for (int i = 0; i < clippedEmbedding.length; i++) {
                    aDouble.setValue(clippedEmbedding[i]);
                    storage.reset();
                    storage.getDataOutput().writeByte(ATypeTag.DOUBLE.serialize());
                    ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, storage.getDataOutput());
                    listBuilder.addItem(storage);
                }

                storage.reset();
                listBuilder.write(storage.getDataOutput(), true);
                tupleBuilder.addField(storage.getByteArray(), 0, storage.getLength());

                // Append tuple to frame, handle buffer overflow manually
                if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                        tupleBuilder.getSize())) {
                    // Frame is full, flush and reset
                    FrameUtils.flushFrame(appender.getBuffer(), writer);
                    appender.reset(new VSizeFrame(ctx), true);
                    appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize());
                }

                System.err.println("Output: <" + treeLevel + ", " + centroidId + ", " + parentClusterId + ", embedding["
                        + clippedEmbedding.length + "]>");

            } catch (Exception e) {
                System.err.println("ERROR: Hierarchical tuple creation failed: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        public int getNumLevels() {
            return levelCentroids.size();
        }

        /**
         * Get the maximum level number (root level)
         */
        public int getMaxLevel() {
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }
            return maxLevel;
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

    // Clipping constants for centroid values
    private static final double DEFAULT_CLIP_MIN = -1e3;
    private static final double DEFAULT_CLIP_MAX = 1e3;

    private final UUID sampleUUID;
    private final UUID tupleCountUUID;
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

    /**
     * Clips centroid values to reasonable bounds to prevent exorbitant values.
     * 
     * @param centroid The centroid array to clip
     * @return Clipped centroid array with values bounded between DEFAULT_CLIP_MIN and DEFAULT_CLIP_MAX
     */
    private static double[] clipCentroid(double[] centroid) {
        if (centroid == null) {
            return centroid;
        }

        double[] clipped = new double[centroid.length];
        boolean wasClipped = false;

        for (int i = 0; i < centroid.length; i++) {
            double value = centroid[i];

            // Check for NaN or Infinity
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                clipped[i] = 0.0; // Replace with 0
                wasClipped = true;
            } else if (value < DEFAULT_CLIP_MIN) {
                clipped[i] = DEFAULT_CLIP_MIN;
                wasClipped = true;
            } else if (value > DEFAULT_CLIP_MAX) {
                clipped[i] = DEFAULT_CLIP_MAX;
                wasClipped = true;
            } else {
                clipped[i] = value;
            }
        }

        if (wasClipped) {
            System.err.println("WARNING: Centroid values were clipped to bounds [" + DEFAULT_CLIP_MIN + ", "
                    + DEFAULT_CLIP_MAX + "]");
        }

        return clipped;
    }

    /**
     * Creates a RecordDescriptor for the hierarchical clustering output format.
     * Format: <treeLevel, centroidId, parentClusterId, embedding>
     * 
     * @return RecordDescriptor with 4 fields: 3 integers + 1 AOrderedList of doubles
     */
    public static RecordDescriptor createHierarchicalOutputRecordDescriptor() {
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[4];

        // Field 0: Tree Level (int)
        fieldSerdes[0] = IntegerSerializerDeserializer.INSTANCE;

        // Field 1: Centroid ID (int)
        fieldSerdes[1] = IntegerSerializerDeserializer.INSTANCE;

        // Field 2: Parent Cluster ID (int)
        fieldSerdes[2] = IntegerSerializerDeserializer.INSTANCE;

        // Field 3: Embedding (AOrderedList of doubles)
        fieldSerdes[3] = new AOrderedListSerializerDeserializer(new AOrderedListType(ADOUBLE, "embedding"));

        return new RecordDescriptor(fieldSerdes);
    }

    /**
     * Parses a hierarchical tuple from the output format.
     * Format: <treeLevel, centroidId, parentClusterId, embedding>
     * 
     * @param tuple The input tuple to parse
     * @return HierarchicalTupleInfo containing the parsed data
     */
    public static HierarchicalTupleInfo parseHierarchicalTuple(ITupleReference tuple) throws HyracksDataException {
        try {
            // Field 0: Tree Level
            int treeLevel = IntegerPointable.getInteger(tuple.getFieldData(0), tuple.getFieldStart(0));

            // Field 1: Centroid ID
            int centroidId = IntegerPointable.getInteger(tuple.getFieldData(1), tuple.getFieldStart(1));

            // Field 2: Parent Cluster ID
            int parentClusterId = IntegerPointable.getInteger(tuple.getFieldData(2), tuple.getFieldStart(2));

            // Field 3: Embedding (AOrderedList of doubles)
            byte[] embeddingData = tuple.getFieldData(3);
            int embeddingStart = tuple.getFieldStart(3);

            // Parse the embedding using ListAccessor
            ListAccessor listAccessor = new ListAccessor();
            listAccessor.reset(embeddingData, embeddingStart);

            // Extract double values from the AOrderedList
            double[] embedding = new double[listAccessor.size()];
            ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
            VoidPointable tempVal = new VoidPointable();

            for (int i = 0; i < listAccessor.size(); i++) {
                listAccessor.getOrWriteItem(i, tempVal, storage);
                embedding[i] = extractNumericValue(tempVal);
            }

            return new HierarchicalTupleInfo(treeLevel, centroidId, parentClusterId, embedding);

        } catch (Exception e) {
            throw new HyracksDataException("Failed to parse hierarchical tuple: " + e.getMessage(), e);
        }
    }

    /**
     * Helper class to hold parsed hierarchical tuple information.
     */
    public static class HierarchicalTupleInfo {
        public final int treeLevel;
        public final int centroidId;
        public final int parentClusterId;
        public final double[] embedding;

        public HierarchicalTupleInfo(int treeLevel, int centroidId, int parentClusterId, double[] embedding) {
            this.treeLevel = treeLevel;
            this.centroidId = centroidId;
            this.parentClusterId = parentClusterId;
            this.embedding = embedding;
        }

        @Override
        public String toString() {
            return String.format("<%d, %d, %d, [%d values]>", treeLevel, centroidId, parentClusterId, embedding.length);
        }
    }

    /**
     * Extracts numeric value from a pointable (helper method for parsing).
     */
    private static double extractNumericValue(IPointable pointable) throws HyracksDataException {
        byte[] data = pointable.getByteArray();
        int start = pointable.getStartOffset();

        ATypeTag typeTag = ATYPETAGDESERIALIZER.deserialize(data[start]);

        switch (typeTag) {
            case DOUBLE:
                return DoublePointable.getDouble(data, start + 1);
            case FLOAT:
                return FloatPointable.getFloat(data, start + 1);
            case INTEGER:
                return IntegerPointable.getInteger(data, start + 1);
            case BIGINT:
                return LongPointable.getLong(data, start + 1);
            default:
                throw new HyracksDataException("Unsupported numeric type: " + typeTag);
        }
    }

    public HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor outputRecDesc, RecordDescriptor secondaryRecDesc, UUID sampleUUID, UUID tupleCountUUID,
            UUID materializedDataUUID, IScalarEvaluatorFactory args, int K, int maxScalableKmeansIter) {
        super(spec, 1, 1);
        // Output record descriptor defines the format of output tuples (treeLevel, centroidId, parentClusterId, embedding)
        // Input record descriptor is the 2-field format with vector embeddings
        outRecDescs[0] = outputRecDesc; // Output format (hierarchical structure with parent-child relationships)
        this.secondaryRecDesc = secondaryRecDesc; // Input format (2-field with vector embeddings)
        this.sampleUUID = sampleUUID;
        this.tupleCountUUID = tupleCountUUID;
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
                private MaterializerTaskState materializedSample;
                private TupleCountState tupleCountState;

                @Override
                public void open() throws HyracksDataException {
                    // Initialize data persistence for multiple passes over the data
                    materializedSample = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(sampleUUID, partition));
                    materializedSample.open(ctx);

                    // Initialize tuple count state
                    tupleCountState = new TupleCountState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(tupleCountUUID, partition));
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    // Count tuples in this frame
                    FrameTupleAccessor fta = new FrameTupleAccessor(secondaryRecDesc);
                    fta.reset(buffer);
                    int tupleCount = fta.getTupleCount();
                    tupleCountState.addTupleCount(tupleCount);

                    // Materialize all data to disk for subsequent processing passes
                    // This allows us to make multiple passes over the data without loading it all into memory
                    materializedSample.appendFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (materializedSample != null) {
                        materializedSample.close();
                        ctx.setStateObject(materializedSample);
                    }
                    if (tupleCountState != null) {
                        ctx.setStateObject(tupleCountState);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
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
                    in.open(); // Open the reader before using it
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

                        // Get tuple count from first activity
                        TupleCountState tupleCountState =
                                (TupleCountState) ctx.getStateObject(new PartitionedUUID(tupleCountUUID, partition));
                        int totalTupleCount = tupleCountState != null ? tupleCountState.getTotalTupleCount() : 0;

                        System.err.println("Retrieved total tuple count: " + totalTupleCount);

                        // Perform memory-efficient hierarchical K-means clustering
                        HierarchicalClusterStructure clusterStructure =
                                performMemoryEfficientHierarchicalKMeans(ctx, in, fta, tuple, eval, inputVal,
                                        listAccessorConstant, KMeansUtils, vSizeFrame, partition, totalTupleCount);

                        if (clusterStructure.getNumLevels() == 0) {
                            System.err.println("No clustering structure generated");
                            return;
                        }

                        // Output hierarchical structure with parent-child relationships
                        // Manual buffer management handles flushing when needed
                        clusterStructure.outputHierarchicalStructure(appender, writer, ctx);

                        // Final flush
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
                 * Performs initial K-means++ on all data from run file to generate K centroids
                 */
                private ClusteringResult performInitialKMeansPlusPlus(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, int k, Random rand, int maxIterations, int totalTupleCount)
                        throws HyracksDataException, IOException {

                    if (k <= 0 || totalTupleCount <= 0) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }

                    List<double[]> centroids = new ArrayList<>();
                    int[] assignments = new int[totalTupleCount];

                    System.err.println("performInitialKMeansPlusPlus: starting streaming K-means++ with "
                            + totalTupleCount + " total tuples, target k = " + k);

                    // K-means++ initialization using streaming approach
                    // 1. Choose first centroid randomly from all data
                    int firstIdx = rand.nextInt(totalTupleCount);
                    double[] firstCentroid = getPointAtIndex(in, fta, tuple, eval, inputVal, listAccessorConstant,
                            kMeansUtils, firstIdx, ctx);
                    if (firstCentroid != null) {
                        centroids.add(firstCentroid);
                        System.err.println("Added first centroid at index " + firstIdx);
                    } else {
                        return new ClusteringResult(centroids, assignments);
                    }

                    // 2. Choose remaining centroids using weighted selection
                    for (int i = 1; i < k; i++) {
                        double totalDistance = 0.0;
                        int selectedIdx = 0;

                        // Calculate total distance and select next centroid
                        VSizeFrame frame = new VSizeFrame(ctx);
                        int currentIdx = 0;
                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    currentIdx++;
                                    continue;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                    // Calculate minimum distance to existing centroids
                                    double minDist = Double.POSITIVE_INFINITY;
                                    for (double[] centroid : centroids) {
                                        double dist = calculateDistance(point, centroid);
                                        minDist = Math.min(minDist, dist);
                                    }

                                    totalDistance += minDist;

                                    // Weighted random selection
                                    if (totalDistance > 0) {
                                        double r = rand.nextDouble() * totalDistance;
                                        if (r <= minDist) {
                                            selectedIdx = currentIdx;
                                        }
                                    }

                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                currentIdx++;
                            }
                        }

                        // Reset reader for next iteration
                        in = resetRunFileReader(ctx, sampleUUID, partition);

                        // Get the selected centroid
                        double[] selectedCentroid = getPointAtIndex(in, fta, tuple, eval, inputVal,
                                listAccessorConstant, kMeansUtils, selectedIdx, ctx);
                        if (selectedCentroid != null) {
                            centroids.add(selectedCentroid);
                            System.err.println("Added centroid " + i + " at index " + selectedIdx);
                        }
                    }

                    // 3. Lloyd's algorithm for refinement using streaming approach
                    for (int iter = 0; iter < maxIterations; iter++) {
                        // Assignment phase: assign each point to closest centroid
                        VSizeFrame frame = new VSizeFrame(ctx);
                        int currentIdx = 0;
                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    currentIdx++;
                                    continue;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                    // Find closest centroid
                                    double minDist = Double.POSITIVE_INFINITY;
                                    int closestCentroid = 0;
                                    for (int c = 0; c < centroids.size(); c++) {
                                        double dist = calculateDistance(point, centroids.get(c));
                                        if (dist < minDist) {
                                            minDist = dist;
                                            closestCentroid = c;
                                        }
                                    }
                                    assignments[currentIdx] = closestCentroid;

                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                currentIdx++;
                            }
                        }

                        // Reset reader for update phase
                        in = resetRunFileReader(ctx, sampleUUID, partition);

                        // Update phase: calculate new centroids
                        double[][] newCentroids = new double[k][centroids.get(0).length];
                        int[] counts = new int[k];

                        frame = new VSizeFrame(ctx);
                        currentIdx = 0;
                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    currentIdx++;
                                    continue;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                    int centroidIdx = assignments[currentIdx];
                                    for (int d = 0; d < point.length; d++) {
                                        newCentroids[centroidIdx][d] += point[d];
                                    }
                                    counts[centroidIdx]++;

                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                currentIdx++;
                            }
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

                        // Reset reader for next iteration
                        in = resetRunFileReader(ctx, sampleUUID, partition);
                    }

                    System.err.println("performInitialKMeansPlusPlus: generated " + centroids.size()
                            + " centroids (target was " + k + ")");

                    return new ClusteringResult(centroids, assignments);
                }

                /**
                 * Get a specific point by index from the run file.
                 */
                private double[] getPointAtIndex(GeneratedRunFileReader in, FrameTupleAccessor fta,
                        FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, int targetIndex,
                        IHyracksTaskContext ctx) throws HyracksDataException, IOException {

                    VSizeFrame frame = new VSizeFrame(ctx);
                    int currentIndex = 0;

                    while (in.nextFrame(frame)) {
                        ByteBuffer buffer = frame.getBuffer();
                        fta.reset(buffer);
                        int tupleCount = fta.getTupleCount();

                        for (int j = 0; j < tupleCount; j++) {
                            if (currentIndex == targetIndex) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    return null;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                return kMeansUtils.createPrimitveList(listAccessorConstant);
                            }
                            currentIndex++;
                        }
                    }
                    return null;
                }

                /**
                 * Reset the run file reader to the beginning.
                 */
                private GeneratedRunFileReader resetRunFileReader(IHyracksTaskContext ctx, UUID sampleUUID,
                        int partition) throws HyracksDataException {
                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader reader = sampleState.creatReader();
                    reader.open(); // Open the reader before returning it
                    return reader;
                }

                /**
                 * Perform memory-efficient hierarchical K-means clustering using run files.
                 */
                private HierarchicalClusterStructure performMemoryEfficientHierarchicalKMeans(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, int partition, int totalTupleCount)
                        throws HyracksDataException, IOException {

                    System.err.println("=== PERFORMING MEMORY-EFFICIENT HIERARCHICAL K-MEANS ===");

                    HierarchicalClusterStructure structure = new HierarchicalClusterStructure();

                    // Perform initial K-means++ on all data to generate initial centroids
                    Random rand = new Random();
                    int maxKMeansIterations = 20;
                    ClusteringResult initialResult = performInitialKMeansPlusPlus(ctx, in, fta, tuple, eval, inputVal,
                            listAccessorConstant, kMeansUtils, K, rand, maxKMeansIterations, totalTupleCount);

                    if (initialResult.centroids.isEmpty()) {
                        System.err.println("No initial centroids generated, cannot perform hierarchical clustering");
                        return structure;
                    }

                    System.err.println("Generated " + initialResult.centroids.size()
                            + " initial centroids from all data, starting hierarchical clustering");

                    // Add Level 0 (initial centroids) - these are the leaf nodes
                    List<HierarchicalClusterStructure.CentroidInfo> level0Info = new ArrayList<>();
                    for (int i = 0; i < initialResult.centroids.size(); i++) {
                        level0Info.add(new HierarchicalClusterStructure.CentroidInfo(i, -1,
                                initialResult.centroids.get(i), 0));
                    }
                    structure.levelCentroids.put(0, level0Info);

                    // Build subsequent levels using scalable K-means++ on centroids
                    List<double[]> currentCentroids = initialResult.centroids;
                    int currentK = Math.min(K, Math.max(1, initialResult.centroids.size() / 2));
                    int maxIterations = 20;
                    int maxLevels = 5;
                    int currentLevel = 1;

                    // Build subsequent levels
                    while (currentCentroids.size() > 1 && currentK > 1 && currentLevel < maxLevels) {
                        System.err.println("Level " + currentLevel + ": Clustering " + currentCentroids.size()
                                + " centroids into " + currentK + " clusters");

                        // Initialize parent level with empty centroids
                        structure.initializeParentLevel(currentLevel, currentK);

                        // Perform K-means++ clustering on centroids from previous level
                        ClusteringResult levelResult = performScalableKMeansPlusPlusOnCentroids(currentCentroids,
                                currentK, rand, maxIterations);

                        if (levelResult.centroids.isEmpty()) {
                            System.err.println("K-means++ produced no centroids, stopping clustering");
                            break;
                        }

                        // Build level using assignments - currentCentroids are children, levelResult.centroids are parents
                        structure.buildLevelFromAssignments(currentCentroids, levelResult.centroids,
                                levelResult.assignments, currentLevel, currentLevel - 1);

                        // Prepare for next level
                        currentCentroids = levelResult.centroids;
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
                private ClusteringResult performScalableKMeansPlusPlusOnCentroids(List<double[]> centroids, int k,
                        Random rand, int maxIterations) {
                    if (centroids.isEmpty() || k <= 0) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }

                    List<double[]> resultCentroids = new ArrayList<>();
                    int[] assignments = new int[centroids.size()]; // Declare assignments outside the loop

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

                    return new ClusteringResult(resultCentroids, assignments);
                }

            };
        }
    }
}
