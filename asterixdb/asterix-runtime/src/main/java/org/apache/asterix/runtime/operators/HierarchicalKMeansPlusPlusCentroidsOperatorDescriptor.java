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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.storage.OptimizedScalarQuantizationSampleFile;
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
import org.apache.hyracks.api.io.FileReference;
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
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        return VectorDistanceArrCalculation.euclideanSquared(a, b);
    }
}

class CosineDistanceFunction implements DistanceFunction, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.cosineDistance(a, b);
    }
}

class DotProductDistanceFunction implements DistanceFunction, Serializable {
    private static final long serialVersionUID = 1L;

    /** Returns -dot(a,b) so that minimizing "distance" equals maximizing dot product (MIPS). */
    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return -VectorDistanceArrCalculation.dot(a, b);
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

    private static final Logger LOGGER =
            LogManager.getLogger(HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.class);

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
     * Format a double array as a JSON array string.
     * 
     * @param vector Double array to format
     * @return JSON array string like "[1.23,4.56,7.89]"
     */
    private static String formatVectorAsJsonArray(double[] vector) {
        if (vector == null) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(vector[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Data structure to hold hierarchical clustering results with parent-child relationships.
     */
    private static class HierarchicalClusterStructure {
        // Store centroids for each level (separate parent and child levels)
        private static final Logger LOGGER = LogManager.getLogger();
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

            //            System.err.println("Initialized parent level " + level + " with " + parentCount + " empty centroids");
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
            //            System.err.println("=== OUTPUTTING HIERARCHICAL STRUCTURE (BFS) ===");

            // Find the root level (highest level number)
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }

            if (maxLevel == -1) {
                //                System.err.println("No levels found to output");
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

                //                System.err.println("Processing level " + currentLevel + " with " + levelInfo.size() + " centroids");

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

        /**
         * Log all centroids from all levels as JSON objects to System.err.
         * Each centroid is logged as a single-line JSON object with complete information.
         * Uses BFS traversal to assign global IDs matching outputHierarchicalStructure().
         */
        public void logAllCentroids() {
            //            System.err.println("=== LOGGING ALL HIERARCHICAL CENTROIDS ===");

            // Find the root level (highest level number)
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }

            if (maxLevel == -1) {
                //                System.err.println("No levels found to log");
                return;
            }

            // BFS traversal starting from root level (same as outputHierarchicalStructure)
            Queue<Integer> levelQueue = new LinkedList<>();
            levelQueue.offer(maxLevel); // Start from root
            int treeLevel = 0;
            int globalCentroidId = 0;

            while (!levelQueue.isEmpty()) {
                int currentLevel = levelQueue.poll();
                List<CentroidInfo> levelInfo = levelCentroids.get(currentLevel);

                if (levelInfo == null) {
                    continue;
                }

                // Log all centroids in current level with global IDs
                for (CentroidInfo centroid : levelInfo) {
                    StringBuilder json = new StringBuilder();
                    json.append("{\"event\":\"hierarchical_centroid\"");
                    json.append(",\"level\":").append(currentLevel);
                    json.append(",\"centroidId\":").append(globalCentroidId);
                    json.append(",\"levelLocalId\":").append(centroid.centroidId);
                    json.append(",\"parentClusterId\":").append(centroid.parentClusterId);
                    json.append(",\"childrenCount\":").append(centroid.childrenIds.size());

                    if (centroid.embedding != null) {
                        json.append(",\"vectorDim\":").append(centroid.embedding.length);
                        json.append(",\"vector\":").append(formatVectorAsJsonArray(centroid.embedding));
                    } else {
                        json.append(",\"vectorDim\":0");
                        json.append(",\"vector\":[]");
                    }

                    json.append("}");
                    LOGGER.info(json.toString());
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

                //                LOGGER.info("Output: <{}, {}, {}, embedding[{}]>", treeLevel, centroidId, parentClusterId, clippedEmbedding.length);

            } catch (Exception e) {
                //                System.err.println("ERROR: Hierarchical tuple creation failed: " + e.getMessage());
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

        /**
         * Calculate estimated tuple size for hierarchical output format (DOUBLE type).
         * Formula: 38 + 13 × dimension bytes
         * Breakdown:
         * - Tuple overhead: 20 bytes (4 bytes tuple offset + 4×4 bytes field offsets)
         * - Fixed fields: 12 bytes (3 integers: treeLevel, centroidId, parentClusterId)
         * - AOrderedList overhead: 6 bytes (tag + itemTag + list size)
         * - Item offsets: 4 bytes × dimension
         * - Item data: 9 bytes × dimension (1 byte type tag + 8 bytes double)
         * 
         * @param embeddingDimension The dimension of the embedding vector
         * @return Estimated tuple size in bytes
         */
        public static long calculateEstimatedTupleSize(int embeddingDimension) {
            return 38L + 13L * embeddingDimension;
        }

        /**
         * Check if a level with given number of centroids fits in one frame.
         * 
         * @param centroidCount Number of centroids at the level
         * @param embeddingDimension Dimension of embedding vectors
         * @param frameSize Frame size in bytes
         * @return true if the level fits in one frame, false otherwise
         */
        public static boolean doesLevelFitInFrame(int centroidCount, int embeddingDimension, int frameSize) {
            if (centroidCount <= 0 || embeddingDimension <= 0 || frameSize <= 0) {
                return false;
            }
            long tupleSize = calculateEstimatedTupleSize(embeddingDimension);
            long totalDataSize = (long) centroidCount * tupleSize;
            long frameOverhead = 9L + (4L * centroidCount); // META_DATA_LEN + tuple offsets
            long totalSize = totalDataSize + frameOverhead;
            return totalSize <= frameSize;
        }

        /**
         * Computes the root fan-out P (Approach 2): the largest number of routing centroids that physically
         * fit on a single disk page, given the exact per-entry byte cost of an interior routing tuple and the
         * interior page header overhead. The single-page root bottleneck has no overflow pages, so P bounds
         * the root branching factor directly.
         *
         * @param pageSize Disk page size in bytes (from the index buffer cache)
         * @param interiorHeaderSize Bytes reserved by the interior page header (unavailable for entries)
         * @param perEntryBytes Exact bytes to write one routing entry, including its slot
         * @return root fan-out P, at least 2
         */
        public static int computeP(int pageSize, int interiorHeaderSize, int perEntryBytes) {
            if (pageSize <= 0 || perEntryBytes <= 0) {
                return 2;
            }
            long usable = (long) pageSize - Math.max(0, interiorHeaderSize);
            int p = (int) (usable / perEntryBytes);
            return Math.max(2, p);
        }

        /**
         * Computes the intermediate fan-out K (Approach 2): an interior routing node spans one primary page
         * plus up to V chained overflow pages, so it can route to K = P * (1 + V) children.
         *
         * @param p Root/per-page fan-out
         * @param v Maximum number of overflow pages per intermediate routing node
         * @return interior fan-out K, at least 2
         */
        public static int computeK(int p, int v) {
            long k = (long) Math.max(2, p) * (1L + Math.max(0, v));
            if (k > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            return (int) Math.max(2, k);
        }

        /**
         * Emit the tree top-down (root level 0 first, then increasing levels toward the leaves), assigning
         * global centroid ids in emission order. Each centroid's {@code parentClusterId} is the position
         * index of its parent within the previous level's emission order, which is the grouping key the
         * downstream {@code VCTreeStaticStructureCreator} relies on (it sorts children by this key and
         * matches them to parents by order). Reuses {@link #createHierarchicalTuple}.
         */
        public void outputTopDownStructure(FrameTupleAppender appender, IFrameWriter writer, IHyracksTaskContext ctx)
                throws HyracksDataException {
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }
            if (maxLevel == -1) {
                return;
            }
            int globalCentroidId = 0;
            for (int level = 0; level <= maxLevel; level++) {
                List<CentroidInfo> levelInfo = levelCentroids.get(level);
                if (levelInfo == null) {
                    continue;
                }
                for (CentroidInfo centroid : levelInfo) {
                    createHierarchicalTuple(level, globalCentroidId, centroid.parentClusterId, centroid.embedding,
                            appender, writer, ctx);
                    globalCentroidId++;
                }
            }
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
    private static final UTF8StringPointable COSINE_FORMAT = UTF8StringPointable.generateUTF8Pointable("cosine");
    private static final UTF8StringPointable COSINE_ALIAS = UTF8StringPointable.generateUTF8Pointable("cosine");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot");

    // Distance function hash map (includes "cosine" alias for DDL-normalized metric)
    private static final Map<Integer, DistanceFunction> DISTANCE_MAP = buildDistanceMap();

    private static Map<Integer, DistanceFunction> buildDistanceMap() {
        DistanceFunction cosineFunc = new CosineDistanceFunction();
        Map<Integer, DistanceFunction> m = new HashMap<>();
        m.put(EUCLIDEAN_DISTANCE.hash(), new EuclideanDistanceFunction());
        m.put(EUCLIDEAN_DISTANCE_L2.hash(), new EuclideanDistanceFunction());
        m.put(EUCLIDEAN_DISTANCE_SQUARED.hash(), new EuclideanSquaredDistanceFunction());
        m.put(EUCLIDEAN_DISTANCE_L2_SQUARED.hash(), new EuclideanSquaredDistanceFunction());
        m.put(COSINE_FORMAT.hash(), cosineFunc);
        m.put(COSINE_ALIAS.hash(), cosineFunc);
        m.put(DOT_PRODUCT_FORMAT.hash(), new DotProductDistanceFunction());
        return Collections.unmodifiableMap(m);
    }

    // Clipping constants for centroid values
    private static final double DEFAULT_CLIP_MIN = -1e3;
    private static final double DEFAULT_CLIP_MAX = 1e3;

    // ===== Top-down FSCL (Approach 2) tuning constants =====
    // Maximum overflow pages per intermediate routing node; interior fan-out K = P * (1 + V).
    public static final int DEFAULT_TOPDOWN_V_OVERFLOW_PAGES = 5;
    // FSCL structural stiffness (gamma): higher forces more uniform cluster sizes.
    public static final double DEFAULT_TOPDOWN_FSCL_GAMMA = 3.0;
    // FSCL batch optimization passes (epochs) per node level.
    private static final int TOPDOWN_FSCL_EPOCHS = 20;
    // Strict height cap: levels 0..MAX_LEVEL (height 5 => deepest level index 4).
    public static final int DEFAULT_TOPDOWN_MAX_LEVEL = 4;

    private final UUID sampleUUID;
    private final UUID tupleCountUUID;
    private final UUID materializedDataUUID;
    private final UUID scalarValuesUUID;

    // Configuration parameters for hierarchical clustering
    private IScalarEvaluatorFactory args; // Evaluator for extracting vector data from tuples
    private int K; // Number of clusters for initial level (leaf nodes)
    private int maxScalableKmeansIter; // Maximum iterations for scalable K-means++ candidate selection
    private DistanceFunction distanceFunction;
    private RecordDescriptor secondaryRecDesc; // Input record descriptor (2-field format)
    private IIndexDataflowHelperFactory indexHelperFactory; // For accessing index directory per partition
    private int[][] partitionsMap; // Maps compute partition to storage partition(s)
    private int vectorDimension;
    // When true, build the tree top-down (root branching computed from frame fit, split per cluster into run
    // files level-by-level). When false, use the original bottom-up memory-efficient algorithm.
    private boolean topDown;
    private int vOverflowPages;
    private double fsclGamma;
    private int maxLevel;

    /**
     * Supplies freshly-opened readers over a run file so a clustering pass can stream the same data
     * multiple times. Each call returns a NEW, already-opened reader that the caller must close.
     */
    @FunctionalInterface
    private interface RunFileSource {
        GeneratedRunFileReader openReader() throws HyracksDataException;
    }

    /**
     * Result of partitioning a run file into one child run file per cluster.
     */
    private static class SplitResult {
        final List<RunFileWriter> files;
        final List<Integer> counts;

        SplitResult(List<RunFileWriter> files, List<Integer> counts) {
            this.files = files;
            this.counts = counts;
        }
    }

    private static DistanceFunction getDistanceFunction(String distanceType) {
        if (distanceType == null || distanceType.trim().isEmpty()) {
            return new EuclideanSquaredDistanceFunction();
        }
        String normalized = distanceType.toLowerCase().trim();
        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(normalized);
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
                throw new Exception("Distance function not implemented yet");
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
     * Whether the current distance function requires centroids to be L2-normalized after each
     * Lloyd update. Normalization is required only for cosine (spherical k-means); aligns with
     * FAISS spherical k-means and Spark's CosineDistanceMeasure. Dot product (MIPS) uses raw
     * centroids and does not require normalization.
     */
    private boolean requiresNormalizedCentroids() {
        return distanceFunction instanceof CosineDistanceFunction;
    }

    /**
     * Normalizes centroid in place to unit L2 norm when using cosine similarity (spherical
     * k-means), so that centroid semantics match FAISS/Spark. Dot product is not normalized.
     * No-op for other metrics.
     */
    private void maybeNormalizeCentroid(double[] centroid) {
        if (centroid != null && requiresNormalizedCentroids()) {
            VectorDistanceArrCalculation.normalizeL2(centroid);
        }
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
            UUID materializedDataUUID, UUID scalarValuesUUID, IScalarEvaluatorFactory args, int K,
            int maxScalableKmeansIter, IIndexDataflowHelperFactory indexHelperFactory, int[][] partitionsMap,
            String distanceMetric, int vectorDimension, boolean topDown, int vOverflowPages, double fsclGamma,
            int maxLevel) {
        super(spec, 1, 1);
        // Output record descriptor defines the format of output tuples (treeLevel, centroidId, parentClusterId, embedding)
        // Input record descriptor is the 2-field format with vector embeddings
        outRecDescs[0] = outputRecDesc; // Output format (hierarchical structure with parent-child relationships)
        this.secondaryRecDesc = secondaryRecDesc; // Input format (2-field with vector embeddings)
        this.sampleUUID = sampleUUID;
        this.tupleCountUUID = tupleCountUUID;
        this.materializedDataUUID = materializedDataUUID;
        this.scalarValuesUUID = scalarValuesUUID;
        this.args = args;
        this.K = K;
        this.maxScalableKmeansIter = maxScalableKmeansIter;
        this.indexHelperFactory = indexHelperFactory;
        this.partitionsMap = partitionsMap;
        this.vectorDimension = vectorDimension;
        this.topDown = topDown;
        this.vOverflowPages = vOverflowPages;
        this.fsclGamma = fsclGamma;
        this.maxLevel = maxLevel;

        // Distance function from index DDL (WITH similarity "euclidean"|"cosine"|"cosine similarity"|etc.); default euclidean squared
        this.distanceFunction = getDistanceFunction(distanceMetric);
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
                    GeneratedRunFileReader in = sampleState.createReader();
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

                        //                        System.err.println("Retrieved total tuple count: " + totalTupleCount);

                        // Compute and write optimized scalar quantization parameters per partition
                        if (indexHelperFactory != null) {
                            //                            computeAndWriteQuantizationParams(ctx, partition, sampleState, eval, inputVal,
                            //                                    listAccessorConstant, KMeansUtils, fta, tuple);
                        }

                        // Build the hierarchical clustering structure (top-down or bottom-up per the strategy flag)
                        HierarchicalClusterStructure clusterStructure;
                        if (topDown) {
                            clusterStructure = buildTopDownHierarchicalKMeans(ctx, sampleState, fta, tuple, eval,
                                    inputVal, listAccessorConstant, KMeansUtils, partition, totalTupleCount);
                        } else {
                            clusterStructure =
                                    performMemoryEfficientHierarchicalKMeans(ctx, in, fta, tuple, eval, inputVal,
                                            listAccessorConstant, KMeansUtils, vSizeFrame, partition, totalTupleCount);
                        }

                        if (clusterStructure.getNumLevels() == 0) {
                            //                            System.err.println("No clustering structure generated");
                            return;
                        }

                        // Log all centroids from all levels as JSON
                        //                        clusterStructure.logAllCentroids();

                        // Output hierarchical structure with parent-child relationships. Top-down stores the root at
                        // level 0, so emit ascending; bottom-up numbers the root as the highest level (BFS emit).
                        // Manual buffer management handles flushing when needed
                        if (topDown) {
                            clusterStructure.outputTopDownStructure(appender, writer, ctx);
                        } else {
                            clusterStructure.outputHierarchicalStructure(appender, writer, ctx);
                        }

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
                 * Computes optimized scalar quantization parameters from sampled vectors and writes them
                 * to a per-partition file in the index directory. Also writes individual embedding values
                 * to a run file for downstream processing.
                 * 
                 * @param ctx The Hyracks task context
                 * @param partition The partition ID
                 * @param sampleState The materialized sample state containing the run file
                 * @param eval Scalar evaluator for extracting vector data
                 * @param inputVal Pointable for input values
                 * @param listAccessorConstant List accessor for vector data
                 * @param kMeansUtils K-means utilities
                 * @param fta Frame tuple accessor
                 * @param tuple Frame tuple reference
                 */
                private void computeAndWriteQuantizationParams(IHyracksTaskContext ctx, int partition,
                        MaterializerTaskState sampleState, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, FrameTupleAccessor fta,
                        FrameTupleReference tuple) throws HyracksDataException {
                    IIndexDataflowHelper indexHelper = null;
                    GeneratedRunFileReader quantReader = null;
                    ScalarValueRunFileWriter scalarWriter = null;
                    try {
                        // Initialize scalar value writer for writing individual embedding values
                        if (scalarValuesUUID != null) {
                            scalarWriter = new ScalarValueRunFileWriter();
                            scalarWriter.initialize(ctx, partition, scalarValuesUUID);
                        }

                        // Get index directory for this partition (resolve storage partition from compute-storage map)
                        int storagePartition = partition;
                        if (partitionsMap != null && partition < partitionsMap.length
                                && partitionsMap[partition] != null && partitionsMap[partition].length > 0) {
                            storagePartition = partitionsMap[partition][0];
                        }
                        indexHelper =
                                indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), storagePartition);
                        indexHelper.open();
                        IIndex indexInstance = indexHelper.getIndexInstance();
                        if (!(indexInstance instanceof ILSMIndex)) {
                            // If not an LSM index, skip quantization computation
                            return;
                        }
                        ILSMIndex lsmIndex = (ILSMIndex) indexInstance;
                        //                        ILSMIndexFileManager fileManager = lsmIndex.getFileManager();
                        //                        FileReference indexDir = fileManager.getBaseDir();

                        // Collect sampled vectors (limit to 20,000)
                        final int MAX_SAMPLES = 2000;
                        List<float[]> sampledVectors = new ArrayList<>();
                        quantReader = sampleState.createReader();
                        quantReader.open();

                        VSizeFrame frame = new VSizeFrame(ctx);
                        int collected = 0;

                        while (quantReader.nextFrame(frame) && collected < MAX_SAMPLES) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount && collected < MAX_SAMPLES; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);

                                // Check if the value is a list type (vector)
                                ATypeTag tag = ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
                                if (!tag.isListType()) {
                                    continue;
                                }

                                // Extract vector as double[]
                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Convert double[] to float[]
                                float[] floatPoint = new float[point.length];
                                for (int i = 0; i < point.length; i++) {
                                    floatPoint[i] = (float) point[i];
                                }
                                //                                sampledVectors.add(floatPoint);

                                // Write each embedding value as individual entries to run file
                                if (scalarWriter != null && point.length > 0 && point.length == vectorDimension) {
                                    scalarWriter.writeEmbeddingAsScalars(point);
                                }

                                collected++;
                            }
                        }

                        // Compute quantization parameters if we collected any vectors
                        // Note: Quantization parameters are now written via sidecar file mechanism
                        if (!sampledVectors.isEmpty()) {
                            OptimizedScalarQuantizationSampleFile.Params params =
                                    OptimizedScalarQuantizationSampleFile.computeFromSamples(sampledVectors, 0.99f, 7);
                            System.err.println("Computed quantization for partition " + partition + " from "
                                    + sampledVectors.size() + " vectors: bits=" + params.bits + ", alpha="
                                    + params.alpha + ", minQ=" + params.minQuantile + ", maxQ=" + params.maxQuantile
                                    + ", conf=" + params.confidenceInterval + ", sampleCount=" + params.sampleCount);
                        } else {
                            System.err.println("No sampled vectors collected for partition " + partition
                                    + ", skipping quantization computation");
                        }

                        // Close scalar writer and save state for downstream operators
                        if (scalarWriter != null) {
                            scalarWriter.close(ctx);
                            System.err.println("UPDATE CALL: Wrote " + collected
                                    + " embeddings as individual scalar values to run file for partition " + partition);
                        }

                    } catch (Exception e) {
                        // Log error but don't fail the job - quantization is optional
                        System.err.println("WARNING: Failed to compute quantization parameters for partition "
                                + partition + ": " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        if (quantReader != null) {
                            try {
                                quantReader.close();
                            } catch (Exception e) {
                                // Ignore close errors
                            }
                        }
                        if (indexHelper != null) {
                            try {
                                indexHelper.close();
                            } catch (Exception e) {
                                // Ignore close errors
                            }
                        }
                    }
                }

                /**
                 * Performs   k-means|| (k-means parallel) on all data from run file to generate K centroids.
                 * Uses multiple rounds of probabilistic sampling to build candidate set, then reduces to k centroids.
                 */
                private ClusteringResult performInitialKMeansPlusPlus(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, int k, Random rand, int maxIterations, int totalTupleCount,
                        int partition) throws HyracksDataException, IOException {
                    //   k-means|| configuration
                    int numRounds = 5; // Number of sampling rounds (default 5-10)
                    double oversamplingFactor = 2.0 * k; // Oversampling factor l ≈ 2k

                    return performKMeansParallel(ctx, in, fta, tuple, eval, inputVal, listAccessorConstant, kMeansUtils,
                            k, rand, maxIterations, totalTupleCount, partition, numRounds, oversamplingFactor);
                }

                /**
                 * Implements   k-means|| algorithm with configurable parameters.
                 */
                private ClusteringResult performKMeansParallel(IHyracksTaskContext ctx, GeneratedRunFileReader in,
                        FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, int k, Random rand,
                        int maxIterations, int totalTupleCount, int partition, int numRounds, double oversamplingFactor)
                        throws HyracksDataException, IOException {

                    if (k <= 0 || totalTupleCount <= 0) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }

                    //                    System.err.println("performKMeansParallel: starting   k-means|| with " + totalTupleCount
                    //                            + " total tuples, target k = " + k + ", rounds = " + numRounds + ", oversampling factor = "
                    //                            + oversamplingFactor);

                    int[] assignments = new int[totalTupleCount];

                    // Step 1: Choose first centroid uniformly at random
                    int firstIdx = rand.nextInt(totalTupleCount);
                    double[] firstCentroid = getPointAtIndex(in, fta, tuple, eval, inputVal, listAccessorConstant,
                            kMeansUtils, firstIdx, ctx);
                    if (firstCentroid == null) {
                        return new ClusteringResult(new ArrayList<>(), assignments);
                    }

                    // Current centers for distance computation (starts with first centroid)
                    List<double[]> currentCenters = new ArrayList<>();
                    currentCenters.add(firstCentroid);

                    // Candidate set (will be oversampled)
                    List<double[]> candidates = new ArrayList<>();

                    // Step 2: Multiple rounds of probabilistic sampling (k-means||)
                    for (int round = 0; round < numRounds; round++) {
                        //                        System.err.println("Round " + (round + 1) + "/" + numRounds + ": Sampling candidates...");

                        // PASS 1: Compute S = Σ_x D(x) by streaming (NO DISTANCE STORAGE)
                        double totalDistance = 0.0;

                        in = resetRunFileReader(ctx, sampleUUID, partition);
                        VSizeFrame frame = new VSizeFrame(ctx);
                        int tempIdx = 0;

                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    tempIdx++;
                                    continue;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);
                                    // Compute D(x) = min distance to current centers
                                    if (point.length != vectorDimension) {
                                        continue;
                                    }
                                    double minDist = Double.POSITIVE_INFINITY;
                                    for (double[] center : currentCenters) {
                                        double dist = calculateDistance(point, center);
                                        minDist = Math.min(minDist, dist);
                                    }

                                    // Accumulate sum (NO STORAGE)
                                    totalDistance += minDist;
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                tempIdx++;
                            }
                        }

                        if (totalDistance <= 0) {
                            //                            System.err.println("Total distance is 0, skipping round " + (round + 1));
                            break;
                        }

                        // PASS 2: Stream again, recompute D(x), and sample probabilistically
                        in = resetRunFileReader(ctx, sampleUUID, partition);
                        frame = new VSizeFrame(ctx);
                        int currentIdx = 0;
                        int sampledCount = 0;

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

                                    // RECOMPUTE D(x) (no storage from pass 1)
                                    double minDist = Double.POSITIVE_INFINITY;
                                    for (double[] center : currentCenters) {
                                        double dist = calculateDistance(point, center);
                                        minDist = Math.min(minDist, dist);
                                    }

                                    //   probabilistic sampling: p(x) = l * D(x) / S
                                    double probability = oversamplingFactor * minDist / totalDistance;

                                    // Independent Bernoulli trial for each point
                                    if (rand.nextDouble() < probability) {
                                        // Add to candidates (copy to avoid mutation)
                                        candidates.add(Arrays.copyOf(point, point.length));
                                        sampledCount++;
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                currentIdx++;
                            }
                        }

                        //                        System.err.println("Round " + (round + 1) + ": Sampled " + sampledCount + " candidates (total: "
                        //                                + candidates.size() + ")");

                        // Update current centers: add all candidates from this round for next round's distance computation
                        if (sampledCount > 0) {
                            int startIdx = candidates.size() - sampledCount;
                            for (int idx = startIdx; idx < candidates.size(); idx++) {
                                currentCenters.add(Arrays.copyOf(candidates.get(idx), candidates.get(idx).length));
                            }
                        }
                    }

                    //                    System.err.println(
                    //                            "Sampling complete: " + candidates.size() + " total candidates (target k = " + k + ")");

                    // Step 3: Weight candidates - count how many original points are nearest to each candidate
                    int[] candidateWeights = new int[candidates.size()];
                    Arrays.fill(candidateWeights, 0);

                    in = resetRunFileReader(ctx, sampleUUID, partition);
                    VSizeFrame weightFrame = new VSizeFrame(ctx);
                    int weightIdx = 0;

                    while (in.nextFrame(weightFrame)) {
                        ByteBuffer buffer = weightFrame.getBuffer();
                        fta.reset(buffer);
                        int tupleCount = fta.getTupleCount();

                        for (int j = 0; j < tupleCount; j++) {
                            tuple.reset(fta, j);
                            eval.evaluate(tuple, inputVal);
                            if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                    .isListType()) {
                                weightIdx++;
                                continue;
                            }

                            listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                            try {
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Find nearest candidate (recompute distance)
                                double minDist = Double.POSITIVE_INFINITY;
                                int nearestCandidate = -1;
                                for (int c = 0; c < candidates.size(); c++) {
                                    double dist = calculateDistance(point, candidates.get(c));
                                    if (dist < minDist) {
                                        minDist = dist;
                                        nearestCandidate = c;
                                    }
                                }
                                if (nearestCandidate >= 0) {
                                    candidateWeights[nearestCandidate]++;
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            weightIdx++;
                        }
                    }

                    System.err.println("Weighting complete");

                    List<double[]> weightedCandidates = new ArrayList<>();
                    List<Integer> weightedCandidateWeights = new ArrayList<>();

                    // Reduce duplicates: combine identical/very close candidates and sum their weights
                    for (int i = 0; i < candidates.size(); i++) {
                        if (candidateWeights[i] == 0) {
                            continue; // Skip zero-weight candidates (  filter)
                        }

                        // Check if this candidate is a duplicate of an existing weighted candidate
                        boolean foundDuplicate = false;
                        for (int j = 0; j < weightedCandidates.size(); j++) {
                            double dist = calculateDistance(candidates.get(i), weightedCandidates.get(j));
                            if (dist < 1e-10) { // Consider identical if very close
                                // Accumulate weight (  reduceByKey(_ + _))
                                weightedCandidateWeights.set(j, weightedCandidateWeights.get(j) + candidateWeights[i]);
                                foundDuplicate = true;
                                break;
                            }
                        }

                        if (!foundDuplicate) {
                            weightedCandidates.add(Arrays.copyOf(candidates.get(i), candidates.get(i).length));
                            weightedCandidateWeights.add(candidateWeights[i]);
                        }
                    }

                    System.err.println("Reduced " + candidates.size() + " candidates to " + weightedCandidates.size()
                            + " distinct weighted candidates (after filtering zeros)");

                    // Convert to arrays for easier use
                    int[] finalWeights = new int[weightedCandidateWeights.size()];
                    for (int i = 0; i < weightedCandidateWeights.size(); i++) {
                        finalWeights[i] = weightedCandidateWeights.get(i);
                    }

                    // Step 4: Reduce candidates to k using weighted k-means++
                    //   ensures k initial centers by filling gaps if fewer than k candidates are produced
                    List<double[]> centroids;
                    if (weightedCandidates.isEmpty()) {
                        // Fallback: use first centroid only
                        centroids = new ArrayList<>();
                        centroids.add(firstCentroid);
                        System.err.println("No weighted candidates (all filtered), using first centroid only");
                    } else if (weightedCandidates.size() <= k) {
                        //   behavior: use weighted candidates and pad to k by sampling from data
                        System.err.println("Have " + weightedCandidates.size()
                                + " distinct weighted candidates (<= k), padding to k=" + k);

                        // Start with weighted candidates ( : centersBuf = weightedCandidates.map(_._1).toBuffer)
                        centroids = new ArrayList<>();
                        for (double[] candidate : weightedCandidates) {
                            centroids.add(Arrays.copyOf(candidate, candidate.length));
                        }

                        // pad with random points from data (data.takeSample)
                        int needed = k - centroids.size();
                        if (needed > 0) {
                            System.err.println("Padding with " + needed + " additional points from dataset");
                            // Sample additional points from dataset deterministically (similar to takeSample)
                            List<Integer> additionalIndices = new ArrayList<>();
                            for (int i = 0; i < needed; i++) {
                                // Deterministic sampling: evenly spaced across dataset
                                int index = (i * totalTupleCount) / needed;
                                if (index >= totalTupleCount) {
                                    index = totalTupleCount - 1;
                                }
                                additionalIndices.add(index);
                            }

                            // Get the additional points
                            in = resetRunFileReader(ctx, sampleUUID, partition);
                            for (int idx : additionalIndices) {
                                double[] additionalPoint = getPointAtIndex(in, fta, tuple, eval, inputVal,
                                        listAccessorConstant, kMeansUtils, idx, ctx);
                                if (additionalPoint != null) {
                                    // Avoid duplicates - only add if not too close to existing centroids
                                    boolean isDuplicate = false;
                                    for (double[] existingCentroid : centroids) {
                                        double dist = calculateDistance(additionalPoint, existingCentroid);
                                        if (dist < 1e-10) { // Consider it a duplicate if very close
                                            isDuplicate = true;
                                            break;
                                        }
                                    }
                                    if (!isDuplicate) {
                                        centroids.add(additionalPoint);
                                    }
                                }
                            }

                            // If we still don't have k (edge case: duplicates), pad with perturbed copies
                            while (centroids.size() < k && centroids.size() > 0) {
                                double[] base = centroids.get(centroids.size() - 1);
                                double[] perturbed = Arrays.copyOf(base, base.length);
                                // Add tiny random perturbation to make it distinct
                                for (int d = 0; d < perturbed.length; d++) {
                                    perturbed[d] += rand.nextGaussian() * 1e-6;
                                }
                                centroids.add(perturbed);
                            }
                        }

                        System.err.println("Padded to " + centroids.size() + " initial centers (target was " + k + ")");
                    } else {
                        // Normal path - we have more than k weighted candidates
                        // Run weighted k-means++ on weightedCandidates to select exactly k
                        centroids = performWeightedKMeansPlusPlusOnCandidates(weightedCandidates, finalWeights, k, rand,
                                maxIterations);
                        System.err.println("Reduced " + weightedCandidates.size() + " weighted candidates to "
                                + centroids.size() + " final centroids using weighted k-means++");
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
                        double[][] newCentroids = new double[centroids.size()][centroids.get(0).length];
                        int[] counts = new int[centroids.size()];

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
                        for (int i = 0; i < centroids.size(); i++) {
                            if (counts[i] > 0) {
                                for (int d = 0; d < newCentroids[i].length; d++) {
                                    newCentroids[i][d] /= counts[i];
                                }
                                // Check if centroid moved significantly
                                double dist = calculateDistance(centroids.get(i), newCentroids[i]);
                                if (dist > 1e-4) {
                                    converged = false;
                                }
                                maybeNormalizeCentroid(newCentroids[i]);
                                centroids.set(i, newCentroids[i]);
                            }
                        }

                        if (converged) {
                            break;
                        }

                        // Reset reader for next iteration
                        in = resetRunFileReader(ctx, sampleUUID, partition);
                    }

                    System.err.println("performKMeansParallel: generated " + centroids.size()
                            + " centroids (target was " + k + ")");

                    return new ClusteringResult(centroids, assignments);
                }

                /**
                 * Perform weighted K-means++ on candidates to select exactly k centroids.
                 * Uses weights when computing probabilities and weighted averages.
                 */
                private List<double[]> performWeightedKMeansPlusPlusOnCandidates(List<double[]> candidates,
                        int[] weights, int k, Random rand, int maxIterations) {
                    if (candidates.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    // If we have <= k candidates, use them but ensure we have exactly k by duplicating if needed
                    // (always return k initial centers)
                    if (candidates.size() <= k) {
                        List<double[]> result = new ArrayList<>(candidates);
                        // If we have fewer than k, duplicate the most weighted candidates to fill the gap
                        if (result.size() < k) {
                            // Find candidates with highest weights for duplication
                            List<Integer> candidateIndices = new ArrayList<>();
                            for (int i = 0; i < candidates.size(); i++) {
                                candidateIndices.add(i);
                            }
                            // Sort by weight (descending)
                            candidateIndices.sort((a, b) -> Integer.compare(weights[b], weights[a]));

                            int remaining = k - result.size();
                            for (int i = 0; i < remaining && i < candidateIndices.size(); i++) {
                                int idx = candidateIndices.get(i);
                                // Add a slightly perturbed copy to ensure distinctness
                                double[] base = candidates.get(idx);
                                double[] copy = Arrays.copyOf(base, base.length);
                                for (int d = 0; d < copy.length; d++) {
                                    copy[d] += rand.nextGaussian() * 1e-6;
                                }
                                result.add(copy);
                            }
                        }
                        return result;
                    }

                    List<double[]> resultCentroids = new ArrayList<>();
                    int[] assignments = new int[candidates.size()];

                    // Weighted K-means++ initialization
                    // 1. Choose first centroid randomly (weighted by weights)
                    int firstIdx = selectWeightedRandomIndex(candidates, weights, rand);
                    resultCentroids.add(Arrays.copyOf(candidates.get(firstIdx), candidates.get(firstIdx).length));

                    // 2. Choose remaining centroids using weighted selection
                    for (int i = 1; i < k && i < candidates.size(); i++) {
                        double[] weightedDistances = new double[candidates.size()];
                        double totalWeightedDistance = 0.0;

                        // Calculate minimum weighted distance to existing centroids for each candidate
                        for (int j = 0; j < candidates.size(); j++) {
                            double minDist = Double.POSITIVE_INFINITY;
                            for (double[] centroid : resultCentroids) {
                                double dist = calculateDistance(candidates.get(j), centroid);
                                minDist = Math.min(minDist, dist);
                            }
                            // Weighted distance: weight[j] * D(c_j)
                            weightedDistances[j] = weights[j] * minDist;
                            totalWeightedDistance += weightedDistances[j];
                        }

                        if (totalWeightedDistance <= 0) {
                            break;
                        }

                        // Weighted random selection
                        double r = rand.nextDouble() * totalWeightedDistance;
                        double cumulativeDistance = 0.0;
                        int selectedIdx = 0;
                        for (int j = 0; j < candidates.size(); j++) {
                            cumulativeDistance += weightedDistances[j];
                            if (cumulativeDistance >= r) {
                                selectedIdx = j;
                                break;
                            }
                        }

                        resultCentroids
                                .add(Arrays.copyOf(candidates.get(selectedIdx), candidates.get(selectedIdx).length));
                    }

                    // 3. Weighted Lloyd's algorithm for refinement
                    for (int iter = 0; iter < maxIterations; iter++) {
                        // Assign candidates to closest centroids
                        for (int i = 0; i < candidates.size(); i++) {
                            double minDist = Double.POSITIVE_INFINITY;
                            int closestCentroid = 0;
                            for (int j = 0; j < resultCentroids.size(); j++) {
                                double dist = calculateDistance(candidates.get(i), resultCentroids.get(j));
                                if (dist < minDist) {
                                    minDist = dist;
                                    closestCentroid = j;
                                }
                            }
                            assignments[i] = closestCentroid;
                        }

                        // Update centroids using weighted averages
                        int numCentroids = resultCentroids.size();
                        double[][] newCentroids = new double[numCentroids][candidates.get(0).length];
                        double[] totalWeights = new double[numCentroids]; // Use double for weighted sums

                        for (int i = 0; i < candidates.size(); i++) {
                            int centroidIdx = assignments[i];
                            // Ensure centroidIdx is within bounds (safety check)
                            if (centroidIdx >= 0 && centroidIdx < numCentroids) {
                                double weight = weights[i];
                                for (int d = 0; d < candidates.get(i).length; d++) {
                                    // Weighted sum: Σ(weight[i] * candidate[i])
                                    newCentroids[centroidIdx][d] += weight * candidates.get(i)[d];
                                }
                                totalWeights[centroidIdx] += weight;
                            }
                        }

                        // Check for convergence - iterate up to actual number of centroids, not k
                        boolean converged = true;
                        for (int i = 0; i < numCentroids; i++) {
                            if (totalWeights[i] > 0) {
                                for (int d = 0; d < newCentroids[i].length; d++) {
                                    // Weighted average: Σ(weight[i] * candidate[i]) / Σ weight[i]
                                    newCentroids[i][d] /= totalWeights[i];
                                }
                                // Check if centroid moved significantly
                                double dist = calculateDistance(resultCentroids.get(i), newCentroids[i]);
                                if (dist > 1e-4) {
                                    converged = false;
                                }
                                maybeNormalizeCentroid(newCentroids[i]);
                                resultCentroids.set(i, newCentroids[i]);
                            }
                        }

                        if (converged) {
                            break;
                        }
                    }

                    // Ensure exactly k centroids by filling gaps if initialization terminated early
                    if (resultCentroids.size() < k) {
                        System.err.println("Weighted k-means++ initialization produced only " + resultCentroids.size()
                                + " centroids, filling gap to reach k=" + k);

                        // Fill gap by selecting additional candidates that are farthest from existing centroids
                        int remaining = k - resultCentroids.size();
                        for (int gap = 0; gap < remaining; gap++) {
                            double maxMinDist = -1.0;
                            int bestCandidateIdx = -1;

                            // Find candidate with maximum minimum distance to existing centroids
                            for (int i = 0; i < candidates.size(); i++) {
                                // Check if this candidate is already a centroid
                                boolean isAlreadyCentroid = false;
                                for (double[] centroid : resultCentroids) {
                                    double dist = calculateDistance(candidates.get(i), centroid);
                                    if (dist < 1e-10) {
                                        isAlreadyCentroid = true;
                                        break;
                                    }
                                }
                                if (isAlreadyCentroid) {
                                    continue;
                                }

                                // Find minimum distance to existing centroids
                                double minDist = Double.POSITIVE_INFINITY;
                                for (double[] centroid : resultCentroids) {
                                    double dist = calculateDistance(candidates.get(i), centroid);
                                    minDist = Math.min(minDist, dist);
                                }

                                // Weight by candidate weight and distance
                                double weightedScore = weights[i] * minDist;
                                if (weightedScore > maxMinDist) {
                                    maxMinDist = weightedScore;
                                    bestCandidateIdx = i;
                                }
                            }

                            if (bestCandidateIdx >= 0) {
                                resultCentroids.add(Arrays.copyOf(candidates.get(bestCandidateIdx),
                                        candidates.get(bestCandidateIdx).length));
                            } else {
                                // Fallback: if all candidates are duplicates, add a slightly perturbed copy of last centroid
                                if (resultCentroids.size() > 0) {
                                    double[] base = resultCentroids.get(resultCentroids.size() - 1);
                                    double[] perturbed = Arrays.copyOf(base, base.length);
                                    for (int d = 0; d < perturbed.length; d++) {
                                        perturbed[d] += rand.nextGaussian() * 1e-6;
                                    }
                                    resultCentroids.add(perturbed);
                                }
                            }
                        }

                        System.err.println("Filled gap in weighted selection: now have " + resultCentroids.size()
                                + " centroids (target was " + k + ")");
                    }

                    return resultCentroids;
                }

                /**
                 * Select a random index weighted by the weights array.
                 */
                private int selectWeightedRandomIndex(List<double[]> candidates, int[] weights, Random rand) {
                    int totalWeight = 0;
                    for (int w : weights) {
                        totalWeight += w;
                    }

                    if (totalWeight <= 0) {
                        // Fallback to uniform random if all weights are zero
                        return rand.nextInt(candidates.size());
                    }

                    int r = rand.nextInt(totalWeight);
                    int cumulativeWeight = 0;
                    for (int i = 0; i < candidates.size(); i++) {
                        cumulativeWeight += weights[i];
                        if (cumulativeWeight > r) {
                            return i;
                        }
                    }
                    return candidates.size() - 1; // Fallback
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
                    GeneratedRunFileReader reader = sampleState.createReader();
                    reader.open(); // Open the reader before returning it
                    return reader;
                }

                // ============================================================================================
                // TOP-DOWN HIERARCHICAL K-MEANS (Approach 2)
                // Builds the tree root-first using a page-fit fan-out: P routing centroids fit on one interior
                // disk page (root level), and an interior node spans K = P*(1+V) children across its primary page
                // plus V overflow pages. The sample is clustered into P groups (k-means++ + FSCL) and partitioned
                // into one run file per group; each file is then clustered into K sub-groups level-by-level until
                // the cumulative cluster count reaches the target (num_clusters) or the strict height cap. FSCL
                // (Frequency-Sensitive Competitive Learning) keeps cluster sizes balanced. Intermediate run files
                // are deleted as soon as a level has consumed them.
                // ============================================================================================

                /**
                 * Streams a run file and updates {@code minDist[i]} with the minimum of its current value and the
                 * distance from point i to {@code centroid}. Non-vector tuples are treated as distance 0 (never
                 * selected as a k-means++ seed). Index space spans every tuple in the file so it stays aligned
                 * with {@link #getPointAtIndex}.
                 */
                private void streamUpdateMinDist(IHyracksTaskContext ctx, RunFileSource source, double[] minDist,
                        double[] centroid, FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval,
                        IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu)
                        throws HyracksDataException, IOException {
                    GeneratedRunFileReader in = source.openReader();
                    try {
                        VSizeFrame frame = new VSizeFrame(ctx);
                        int idx = 0;
                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();
                            for (int j = 0; j < tupleCount; j++) {
                                if (idx >= minDist.length) {
                                    idx++;
                                    continue;
                                }
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    minDist[idx] = 0.0;
                                    idx++;
                                    continue;
                                }
                                listAcc.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kmu.createPrimitveList(listAcc);
                                    double d = calculateDistance(point, centroid);
                                    if (d < minDist[idx]) {
                                        minDist[idx] = d;
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                idx++;
                            }
                        }
                    } finally {
                        in.close();
                    }
                }

                /**
                 * Clusters all records in a single run file into (up to) k centroids using k-means++ initialization
                 * followed by streaming FSCL (Frequency-Sensitive Competitive Learning) refinement. Operates over
                 * an arbitrary {@link RunFileSource}, so it works for both the root sample file and per-cluster
                 * child files. Throws if FSCL leaves any cluster empty.
                 *
                 * @param recordCount total tuple count in the file (defines the index space and assignment array)
                 * @param k requested number of clusters (capped to recordCount)
                 * @param maxIterations retained for signature compatibility; FSCL uses {@code TOPDOWN_FSCL_EPOCHS}
                 */
                private ClusteringResult clusterRunFile(IHyracksTaskContext ctx, RunFileSource source, int recordCount,
                        int k, FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval,
                        IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu, Random rand, int maxIterations)
                        throws HyracksDataException, IOException {
                    if (k <= 0 || recordCount <= 0) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }
                    int effectiveK = Math.min(k, recordCount);
                    List<double[]> centroids = new ArrayList<>();

                    // k-means++ seeding using a stored nearest-distance array (O(recordCount) memory).
                    GeneratedRunFileReader idxReader = source.openReader();
                    double[] firstCentroid;
                    try {
                        firstCentroid = getPointAtIndex(idxReader, fta, tuple, eval, inputVal, listAcc, kmu,
                                rand.nextInt(recordCount), ctx);
                    } finally {
                        idxReader.close();
                    }
                    if (firstCentroid == null) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }
                    centroids.add(firstCentroid);

                    double[] minDist = new double[recordCount];
                    Arrays.fill(minDist, Double.POSITIVE_INFINITY);
                    streamUpdateMinDist(ctx, source, minDist, firstCentroid, fta, tuple, eval, inputVal, listAcc, kmu);

                    while (centroids.size() < effectiveK) {
                        double total = 0.0;
                        for (double d : minDist) {
                            if (d > 0 && !Double.isInfinite(d)) {
                                total += d;
                            }
                        }
                        if (total <= 0) {
                            break;
                        }
                        double targetMass = rand.nextDouble() * total;
                        double cumulative = 0.0;
                        int chosenIdx = -1;
                        for (int i = 0; i < recordCount; i++) {
                            double d = minDist[i];
                            if (d <= 0 || Double.isInfinite(d)) {
                                continue;
                            }
                            cumulative += d;
                            if (cumulative >= targetMass) {
                                chosenIdx = i;
                                break;
                            }
                        }
                        if (chosenIdx < 0) {
                            break;
                        }
                        GeneratedRunFileReader r2 = source.openReader();
                        double[] newCentroid;
                        try {
                            newCentroid = getPointAtIndex(r2, fta, tuple, eval, inputVal, listAcc, kmu, chosenIdx, ctx);
                        } finally {
                            r2.close();
                        }
                        if (newCentroid == null) {
                            break;
                        }
                        centroids.add(newCentroid);
                        streamUpdateMinDist(ctx, source, minDist, newCentroid, fta, tuple, eval, inputVal, listAcc,
                                kmu);
                    }

                    // Streaming FSCL (Frequency-Sensitive Competitive Learning) refinement. Each point is assigned
                    // by a balancing penalty score_i = distance(x, c_i) * exp(gamma * (f_i / mu)), where f_i is the
                    // running headcount of cluster i in this pass and mu = recordCount / k is the fair share. The
                    // exponential repulsion keeps cluster sizes near mu, avoiding starved or overflowing nodes.
                    // The distance is the operator's configured metric (euclidean for L2, angular for cosine).
                    int actualK = centroids.size();
                    int[] assignments = new int[recordCount];
                    double mu = actualK > 0 ? (double) recordCount / actualK : recordCount;
                    if (mu <= 0) {
                        mu = 1.0;
                    }
                    int[] finalFsclCounts = null;
                    for (int iter = 0; iter < TOPDOWN_FSCL_EPOCHS; iter++) {
                        double[][] sums = new double[actualK][];
                        int[] counts = new int[actualK];
                        long[] headcount = new long[actualK]; // running f_i accumulated within this epoch
                        GeneratedRunFileReader in = source.openReader();
                        try {
                            VSizeFrame frame = new VSizeFrame(ctx);
                            int idx = 0;
                            while (in.nextFrame(frame)) {
                                ByteBuffer buffer = frame.getBuffer();
                                fta.reset(buffer);
                                int tupleCount = fta.getTupleCount();
                                for (int j = 0; j < tupleCount; j++) {
                                    tuple.reset(fta, j);
                                    eval.evaluate(tuple, inputVal);
                                    if (!ATYPETAGDESERIALIZER
                                            .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                            .isListType()) {
                                        idx++;
                                        continue;
                                    }
                                    listAcc.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    try {
                                        double[] point = kmu.createPrimitveList(listAcc);
                                        int nearest = 0;
                                        double best = Double.POSITIVE_INFINITY;
                                        for (int c = 0; c < actualK; c++) {
                                            double dist = calculateDistance(point, centroids.get(c));
                                            double penalty = Math.exp(fsclGamma * (headcount[c] / mu));
                                            double score = dist * penalty;
                                            if (Double.isNaN(score)) {
                                                // 0 * Infinity (zero-distance into a saturated cluster); deprioritize.
                                                score = Double.POSITIVE_INFINITY;
                                            }
                                            if (score < best) {
                                                best = score;
                                                nearest = c;
                                            }
                                        }
                                        if (idx < recordCount) {
                                            assignments[idx] = nearest;
                                        }
                                        if (sums[nearest] == null) {
                                            sums[nearest] = new double[point.length];
                                        }
                                        for (int d = 0; d < point.length; d++) {
                                            sums[nearest][d] += point[d];
                                        }
                                        counts[nearest]++;
                                        headcount[nearest]++;
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    idx++;
                                }
                            }
                        } finally {
                            in.close();
                        }

                        // FSCL must keep every cluster populated; an empty cluster breaks the height-balanced
                        // guarantee, so fail fast rather than silently producing a skewed tree.
                        for (int c = 0; c < actualK; c++) {
                            if (counts[c] == 0 || sums[c] == null) {
                                throw new HyracksDataException("FSCL produced an empty cluster (cluster " + c + " of "
                                        + actualK + ", recordCount=" + recordCount
                                        + "); cannot build a height-balanced tree");
                            }
                        }

                        boolean converged = true;
                        for (int c = 0; c < actualK; c++) {
                            double[] updated = new double[sums[c].length];
                            for (int d = 0; d < sums[c].length; d++) {
                                updated[d] = sums[c][d] / counts[c];
                            }
                            if (calculateDistance(centroids.get(c), updated) > 1e-4) {
                                converged = false;
                            }
                            maybeNormalizeCentroid(updated);
                            centroids.set(c, updated);
                        }
                        if (converged) {
                            finalFsclCounts = counts;
                            break;
                        }
                        finalFsclCounts = counts;
                    }
                    if (finalFsclCounts != null) {
                        logTopDownCountDistribution("FSCL assignment: recordCount=" + recordCount + " k=" + actualK,
                                finalFsclCounts);
                    }
                    return new ClusteringResult(centroids, assignments);
                }

                /**
                 * Partitions a run file into one child run file per centroid by assigning each record to its
                 * nearest centroid. Child files use the same {@code secondaryRecDesc} layout as the input, so they
                 * can be re-read by the same clustering code. Returns the (closed) child writers and per-cluster
                 * record counts; the caller owns and must delete them.
                 */
                private SplitResult splitRunFileByAssignment(IHyracksTaskContext ctx, RunFileSource source,
                        List<double[]> centroids, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu)
                        throws HyracksDataException, IOException {
                    int kk = centroids.size();
                    List<RunFileWriter> writers = new ArrayList<>(kk);
                    List<FrameTupleAppender> appenders = new ArrayList<>(kk);
                    List<Integer> counts = new ArrayList<>(kk);
                    for (int c = 0; c < kk; c++) {
                        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile("topdown-cluster");
                        RunFileWriter w = new RunFileWriter(file, ctx.getIoManager());
                        w.open();
                        writers.add(w);
                        appenders.add(new FrameTupleAppender(new VSizeFrame(ctx)));
                        counts.add(0);
                    }
                    GeneratedRunFileReader in = source.openReader();
                    try {
                        VSizeFrame frame = new VSizeFrame(ctx);
                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();
                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    continue;
                                }
                                listAcc.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point;
                                try {
                                    point = kmu.createPrimitveList(listAcc);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                int nearest = 0;
                                double best = Double.POSITIVE_INFINITY;
                                for (int c = 0; c < kk; c++) {
                                    double dd = calculateDistance(point, centroids.get(c));
                                    if (dd < best) {
                                        best = dd;
                                        nearest = c;
                                    }
                                }
                                FrameTupleAppender appender = appenders.get(nearest);
                                if (!appender.append(fta, j)) {
                                    appender.write(writers.get(nearest), true);
                                    if (!appender.append(fta, j)) {
                                        throw new HyracksDataException(
                                                "Tuple too large to fit in a frame during cluster split");
                                    }
                                }
                                counts.set(nearest, counts.get(nearest) + 1);
                            }
                        }
                    } finally {
                        in.close();
                    }
                    for (int c = 0; c < kk; c++) {
                        FrameTupleAppender appender = appenders.get(c);
                        if (appender.getTupleCount() > 0) {
                            appender.write(writers.get(c), true);
                        }
                        writers.get(c).close();
                    }
                    logTopDownCountDistribution("nearest-neighbor split", counts);
                    return new SplitResult(writers, counts);
                }

                /**
                 * Best-effort deletion of intermediate run files.
                 */
                private void deleteRunFiles(List<RunFileWriter> files) {
                    if (files == null) {
                        return;
                    }
                    for (RunFileWriter w : files) {
                        if (w != null) {
                            try {
                                w.getFileReference().delete();
                            } catch (Exception e) {
                                // best-effort cleanup; ignore
                            }
                        }
                    }
                }

                /**
                 * Computes the Approach-2 fan-out from the index's real on-disk page geometry: P routing
                 * centroids fit on one interior page, and an interior node spans P*(1+V) children across its
                 * primary page plus V overflow pages. Opens the partition's VTree to size a routing entry exactly
                 * (via the interior frame's tuple writer + page header), so build-time fan-out matches what the
                 * static-structure layout actually writes.
                 *
                 * @return a two-element array {P, K}
                 */
                private int[] computeFanOut(IHyracksTaskContext ctx, int partition, int dim)
                        throws HyracksDataException {
                    int storagePartition = partition;
                    if (partitionsMap != null && partition < partitionsMap.length && partitionsMap[partition] != null
                            && partitionsMap[partition].length > 0) {
                        storagePartition = partitionsMap[partition][0];
                    }
                    IIndexDataflowHelper indexHelper =
                            indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), storagePartition);
                    indexHelper.open();
                    try {
                        IIndex indexInstance = indexHelper.getIndexInstance();
                        if (!(indexInstance instanceof LSMVTree)) {
                            throw new HyracksDataException(
                                    "Top-down VTree build requires an LSMVTree index instance, got: "
                                            + (indexInstance == null ? "null" : indexInstance.getClass().getName()));
                        }
                        LSMVTree lsmVTree = (LSMVTree) indexInstance;
                        int pageSize = lsmVTree.getBufferCache().getPageSize();
                        ITreeIndexFrame interiorFrame = lsmVTree.getInteriorFrameFactory().createFrame();
                        // Representative interior routing entry: [centroidId:int, embedding:double[dim],
                        // childPageId:int], identical to VTreeStaticStructureBuilder.createEntryTuple (2-field input).
                        double[] sampleEmbedding = new double[dim];
                        ITupleReference entry = TupleUtils.createTuple(
                                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                                        DoubleArraySerializerDeserializer.INSTANCE,
                                        IntegerSerializerDeserializer.INSTANCE },
                                Integer.valueOf(0), sampleEmbedding, Integer.valueOf(0));
                        int perEntryBytes = interiorFrame.getBytesRequiredToWriteTuple(entry); // includes the slot
                        int interiorHeaderSize = interiorFrame.getPageHeaderSize();
                        int p = HierarchicalClusterStructure.computeP(pageSize, interiorHeaderSize, perEntryBytes);
                        int k = HierarchicalClusterStructure.computeK(p, vOverflowPages);
                        return new int[] { p, k };
                    } finally {
                        try {
                            indexHelper.close();
                        } catch (Exception e) {
                            // best-effort cleanup; ignore
                        }
                    }
                }

                /**
                 * Orchestrates the top-down Approach-2 build. Computes the page-fit fan-out (P at the root,
                 * K = P*(1+V) at interior levels), clusters the sample into P root centroids with k-means++ then
                 * FSCL, and splits/re-clusters each parent run file level-by-level. Enforces a strict height cap
                 * (levels 0..{@code TOPDOWN_MAX_LEVEL}) and stops at the first level whose cumulative centroid count
                 * reaches the target (num_clusters), keeping the overshoot. If any parent run file holds fewer
                 * records than the interior fan-out K, the build stops and returns the height-balanced tree built so
                 * far. Throws on a degenerate (early-leaf) clustering that would break the height-balanced guarantee.
                 */
                private HierarchicalClusterStructure buildTopDownHierarchicalKMeans(IHyracksTaskContext ctx,
                        MaterializerTaskState sampleState, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu,
                        int partition, int totalTupleCount) throws HyracksDataException, IOException {
                    HierarchicalClusterStructure structure = new HierarchicalClusterStructure();
                    if (totalTupleCount <= 0) {
                        LOGGER.info("[TopDown] partition={}: empty sample, skipping build", partition);
                        return structure;
                    }
                    Random rand = new Random();
                    int target = K; // num_clusters

                    // Determine embedding dimension (from config or first vector).
                    int dim = vectorDimension;
                    if (dim <= 0) {
                        GeneratedRunFileReader probe = sampleState.createReader();
                        probe.open();
                        double[] firstPoint;
                        try {
                            firstPoint = getPointAtIndex(probe, fta, tuple, eval, inputVal, listAcc, kmu, 0, ctx);
                        } finally {
                            probe.close();
                        }
                        if (firstPoint == null) {
                            return structure;
                        }
                        dim = firstPoint.length;
                    }

                    // Page-fit fan-out from the index's real on-disk geometry (P at root, K at interior levels).
                    int[] fanOut = computeFanOut(ctx, partition, dim);
                    int pFanOut = fanOut[0];
                    int kFanOut = fanOut[1];
                    LOGGER.info("[TopDown] partition={} start: tuples={} target={} dim={} P={} K={} maxLevel={}",
                            partition, totalTupleCount, target, dim, pFanOut, kFanOut, maxLevel);

                    // ---- Root level (level 0): cluster the full sample into P centroids (k-means++ + FSCL) ----
                    final MaterializerTaskState sampleStateRef = sampleState;
                    RunFileSource sampleSource = () -> {
                        GeneratedRunFileReader rd = sampleStateRef.createReader();
                        rd.open();
                        return rd;
                    };
                    int kRoot = Math.min(pFanOut, totalTupleCount);
                    ClusteringResult rootResult = clusterRunFile(ctx, sampleSource, totalTupleCount, kRoot, fta, tuple,
                            eval, inputVal, listAcc, kmu, rand, TOPDOWN_FSCL_EPOCHS);
                    if (rootResult.centroids.isEmpty()) {
                        return structure;
                    }
                    if (rootResult.centroids.size() < kRoot) {
                        throw new HyracksDataException(
                                "Top-down root clustering produced " + rootResult.centroids.size()
                                        + " centroids but expected " + kRoot + " (degenerate/early leaf)");
                    }
                    List<HierarchicalClusterStructure.CentroidInfo> level0 = new ArrayList<>();
                    for (int i = 0; i < rootResult.centroids.size(); i++) {
                        level0.add(
                                new HierarchicalClusterStructure.CentroidInfo(i, -1, rootResult.centroids.get(i), 0));
                    }
                    structure.levelCentroids.put(0, level0);
                    int cumulative = rootResult.centroids.size();
                    LOGGER.info("[TopDown] partition={} level 0 (root): centroids={} (requested {})", partition,
                            cumulative, kRoot);

                    // Root already meets the target, or the height cap is the root itself: root is the leaf level.
                    if (cumulative >= target || maxLevel <= 0) {
                        String stopReason = cumulative >= target ? "target reached at root" : "maxLevel=0";
                        LOGGER.info("[TopDown] partition={} stop: {}, levels={} leafCentroids={}", partition,
                                stopReason, structure.getNumLevels(), cumulative);
                        return structure;
                    }

                    // Partition the sample into one run file per root cluster.
                    SplitResult rootSplit = splitRunFileByAssignment(ctx, sampleSource, rootResult.centroids, fta,
                            tuple, eval, inputVal, listAcc, kmu);
                    List<RunFileWriter> parentFiles = rootSplit.files;
                    List<Integer> parentCounts = rootSplit.counts;
                    logTopDownCountDistribution("partition=" + partition + " root split", parentCounts);
                    List<RunFileWriter> pendingNextFiles = null; // tracked so failure paths can clean up
                    String stopReason = null;

                    try {
                        int level = 1;
                        while (level <= maxLevel) {
                            // Feasibility: every parent file must hold at least K records to fan out uniformly. If
                            // any cannot, stop the entire build and keep the height-balanced tree built so far.
                            boolean feasible = true;
                            int infeasibleParent = -1;
                            int infeasibleCount = -1;
                            for (int p = 0; p < parentFiles.size(); p++) {
                                RunFileWriter pf = parentFiles.get(p);
                                int recCount = parentCounts.get(p);
                                if (pf == null || recCount < kFanOut) {
                                    feasible = false;
                                    infeasibleParent = p;
                                    infeasibleCount = recCount;
                                    break;
                                }
                            }
                            if (!feasible) {
                                stopReason = "infeasible parent at level " + level + " (parent=" + infeasibleParent
                                        + " recCount=" + infeasibleCount + ", need>=" + kFanOut + ")";
                                logTopDownCountDistribution("partition=" + partition + " parent counts before stop",
                                        parentCounts);
                                LOGGER.info("[TopDown] partition={} stop: {}", partition, stopReason);
                                deleteRunFiles(parentFiles);
                                break;
                            }

                            logTopDownCountDistribution("partition=" + partition + " level " + level + " parent input",
                                    parentCounts);

                            // Cluster each parent file into exactly K centroids (k-means++ + FSCL).
                            List<List<double[]>> perParentCentroids = new ArrayList<>(parentFiles.size());
                            int childCount = 0;
                            for (int p = 0; p < parentFiles.size(); p++) {
                                final RunFileWriter pf = parentFiles.get(p);
                                int recCount = parentCounts.get(p);
                                LOGGER.info("[TopDown] partition={} level {} clustering parent {}: recCount={} -> K={}",
                                        partition, level, p, recCount, kFanOut);
                                RunFileSource src = () -> {
                                    GeneratedRunFileReader rd = pf.createReader();
                                    rd.open();
                                    return rd;
                                };
                                ClusteringResult res = clusterRunFile(ctx, src, recCount, kFanOut, fta, tuple, eval,
                                        inputVal, listAcc, kmu, rand, TOPDOWN_FSCL_EPOCHS);
                                if (res.centroids.size() < kFanOut) {
                                    throw new HyracksDataException("Top-down clustering produced "
                                            + res.centroids.size() + " centroids but expected " + kFanOut + " at level "
                                            + level + " (degenerate/early leaf)");
                                }
                                perParentCentroids.add(res.centroids);
                                childCount += res.centroids.size();
                            }

                            // Store this level's centroids grouped by parent (parents ascending); the parent position
                            // index is the parentClusterId, preserving the downstream ordering invariant.
                            List<HierarchicalClusterStructure.CentroidInfo> levelInfo = new ArrayList<>(childCount);
                            int localId = 0;
                            for (int p = 0; p < perParentCentroids.size(); p++) {
                                for (double[] c : perParentCentroids.get(p)) {
                                    levelInfo.add(new HierarchicalClusterStructure.CentroidInfo(localId, p, c, level));
                                    localId++;
                                }
                            }
                            structure.levelCentroids.put(level, levelInfo);
                            cumulative = childCount;
                            LOGGER.info(
                                    "[TopDown] partition={} built level {}: parents={} -> centroids={} cumulative={} target={}",
                                    partition, level, parentFiles.size(), childCount, cumulative, target);

                            // Stop at the first level reaching the target (keep overshoot) or the strict height cap.
                            if (cumulative >= target || level >= maxLevel) {
                                stopReason = cumulative >= target ? "target reached at level " + level
                                        : "height cap at level " + level;
                                LOGGER.info("[TopDown] partition={} stop: {}", partition, stopReason);
                                if (!parentFiles.isEmpty() && !perParentCentroids.isEmpty()) {
                                    List<Integer> finalCounts = new ArrayList<>(childCount);
                                    for (int p = 0; p < parentFiles.size(); p++) {
                                        final RunFileWriter pf = parentFiles.get(p);
                                        List<double[]> centroids = perParentCentroids.get(p);
                                        RunFileSource src = () -> {
                                            GeneratedRunFileReader rd = pf.createReader();
                                            rd.open();
                                            return rd;
                                        };
                                        SplitResult sp = splitRunFileByAssignment(ctx, src, centroids, fta, tuple, eval,
                                                inputVal, listAcc, kmu);
                                        finalCounts.addAll(sp.counts);
                                        deleteRunFiles(sp.files);
                                    }
                                    logTopDownCountDistribution("partition=" + partition + " final leaf split (sample)",
                                            finalCounts);
                                }
                                deleteRunFiles(parentFiles);
                                parentFiles = null;
                                break;
                            }

                            // Split each parent file into K child files for the next level, preserving child order so
                            // the i-th child file aligns with the i-th centroid stored at this level.
                            List<RunFileWriter> nextFiles = new ArrayList<>(childCount);
                            List<Integer> nextCounts = new ArrayList<>(childCount);
                            pendingNextFiles = nextFiles; // visible to finally if a split throws mid-level
                            for (int p = 0; p < parentFiles.size(); p++) {
                                final RunFileWriter pf = parentFiles.get(p);
                                List<double[]> centroids = perParentCentroids.get(p);
                                RunFileSource src = () -> {
                                    GeneratedRunFileReader rd = pf.createReader();
                                    rd.open();
                                    return rd;
                                };
                                SplitResult sp = splitRunFileByAssignment(ctx, src, centroids, fta, tuple, eval,
                                        inputVal, listAcc, kmu);
                                nextFiles.addAll(sp.files);
                                nextCounts.addAll(sp.counts);
                            }
                            logTopDownCountDistribution("partition=" + partition + " level " + level + " child split",
                                    nextCounts);

                            // Done with this level's parent files.
                            deleteRunFiles(parentFiles);
                            parentFiles = nextFiles;
                            parentCounts = nextCounts;
                            pendingNextFiles = null;
                            level++;
                        }
                    } finally {
                        // Best-effort cleanup of any intermediate run files still around (exception or final leaf).
                        deleteRunFiles(parentFiles);
                        deleteRunFiles(pendingNextFiles);
                    }

                    int maxStoredLevel = -1;
                    for (Integer lvl : structure.levelCentroids.keySet()) {
                        maxStoredLevel = Math.max(maxStoredLevel, lvl);
                    }
                    LOGGER.info("[TopDown] partition={} complete: levels={} leafLevel={} leafCentroids={} target={}{}",
                            partition, structure.getNumLevels(), maxStoredLevel, cumulative, target,
                            stopReason != null ? " stopReason=" + stopReason : "");

                    return structure;
                }

                /** Log min/max/total/mean and full per-bucket counts. */
                private void logTopDownCountDistribution(String label, List<Integer> counts) {
                    if (counts == null || counts.isEmpty()) {
                        LOGGER.info("[TopDown] {}: counts=(empty)", label);
                        return;
                    }
                    int[] arr = new int[counts.size()];
                    for (int i = 0; i < counts.size(); i++) {
                        arr[i] = counts.get(i);
                    }
                    logTopDownCountDistribution(label, arr);
                }

                private void logTopDownCountDistribution(String label, int[] counts) {
                    if (counts == null || counts.length == 0) {
                        LOGGER.info("[TopDown] {}: counts=(empty)", label);
                        return;
                    }
                    int min = Integer.MAX_VALUE;
                    int max = Integer.MIN_VALUE;
                    long sum = 0;
                    for (int c : counts) {
                        min = Math.min(min, c);
                        max = Math.max(max, c);
                        sum += c;
                    }
                    double mean = (double) sum / counts.length;
                    LOGGER.info("[TopDown] {}: buckets={} min={} max={} sum={} mean={} counts={}", label, counts.length,
                            min, max, sum, mean, Arrays.toString(counts));
                }

                /**
                 * Perform memory-efficient hierarchical K-means clustering using run files.
                 */
                private HierarchicalClusterStructure performMemoryEfficientHierarchicalKMeans(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, int partition, int totalTupleCount)
                        throws HyracksDataException, IOException {

                    //                    System.err.println("=== PERFORMING MEMORY-EFFICIENT HIERARCHICAL K-MEANS ===");

                    HierarchicalClusterStructure structure = new HierarchicalClusterStructure();

                    // Perform initial K-means++ on all data to generate initial centroids
                    Random rand = new Random();
                    int maxKMeansIterations = 20;
                    ClusteringResult initialResult =
                            performInitialKMeansPlusPlus(ctx, in, fta, tuple, eval, inputVal, listAccessorConstant,
                                    kMeansUtils, K, rand, maxKMeansIterations, totalTupleCount, partition);

                    if (initialResult.centroids.isEmpty()) {
                        //                        System.err.println("No initial centroids generated, cannot perform hierarchical clustering");
                        return structure;
                    }

                    // Extract embedding dimension and frame size for frame fit calculations
                    int embeddingDimension = initialResult.centroids.get(0).length;
                    if (embeddingDimension <= 0) {
                        //                        System.err.println("Invalid embedding dimension: " + embeddingDimension);
                        return structure;
                    }
                    int frameSize = ctx.getInitialFrameSize();
                    //                    System.err.println(
                    //                            "Embedding dimension: " + embeddingDimension + ", Frame size: " + frameSize + " bytes");

                    //                    System.err.println("Generated " + initialResult.centroids.size()
                    //                            + " initial centroids from all data, starting hierarchical clustering");

                    // Add Level 0 (initial centroids) - these are the leaf nodes
                    List<HierarchicalClusterStructure.CentroidInfo> level0Info = new ArrayList<>();
                    for (int i = 0; i < initialResult.centroids.size(); i++) {
                        level0Info.add(new HierarchicalClusterStructure.CentroidInfo(i, -1,
                                initialResult.centroids.get(i), 0));
                    }
                    structure.levelCentroids.put(0, level0Info);

                    // Build subsequent levels using scalable K-means++ on centroids
                    List<double[]> currentCentroids = initialResult.centroids;
                    // Initialize currentK using square root reduction for balanced hierarchical structure
                    // int reductionFactor = 100; // Integer reduction factor for hierarchical structure
                    // int currentK = Math.min(K, Math.max(1, initialResult.centroids.size() / reductionFactor));
                    int currentK =
                            Math.min(K, Math.max(1, (int) Math.floor(Math.sqrt(initialResult.centroids.size()))));
                    int maxIterations = 20;
                    int maxLevels = 100;
                    int currentLevel = 0;

                    // Build subsequent levels
                    while (currentCentroids.size() > 1 && currentK > 1 && currentLevel < maxLevels) {
                        //                        System.err.println("Level " + currentLevel + ": Clustering " + currentCentroids.size()
                        //                                + " centroids into " + currentK + " clusters");
                        //                        System.err.println("Checking frame fit: target " + currentK + " centroids, frame size: "
                        //                                + frameSize + " bytes");

                        // Initialize parent level with empty centroids
                        structure.initializeParentLevel(currentLevel, currentK);

                        // Perform K-means++ clustering on centroids from previous level
                        ClusteringResult levelResult = performScalableKMeansPlusPlusOnCentroids(currentCentroids,
                                currentK, rand, maxIterations);

                        if (levelResult.centroids.isEmpty()) {
                            //                            System.err.println("K-means++ produced no centroids, stopping clustering");
                            break;
                        }

                        // Check if current level fits in one frame
                        if (HierarchicalClusterStructure.doesLevelFitInFrame(levelResult.centroids.size(),
                                embeddingDimension, frameSize)) {
                            long tupleSize =
                                    HierarchicalClusterStructure.calculateEstimatedTupleSize(embeddingDimension);
                            long totalDataSize = (long) levelResult.centroids.size() * tupleSize;
                            long frameOverhead = 9L + (4L * levelResult.centroids.size());
                            long totalSize = totalDataSize + frameOverhead;
                            //                            System.err.println("Level " + currentLevel + " with " + levelResult.centroids.size()
                            //                                    + " centroids fits in frame (estimated size: " + totalSize + " bytes, frame size: "
                            //                                    + frameSize + " bytes), stopping here as root level");
                            // Build this level before breaking (so it's stored in structure)
                            structure.buildLevelFromAssignments(currentCentroids, levelResult.centroids,
                                    levelResult.assignments, currentLevel, currentLevel - 1);
                            break;
                        }

                        // Build level using assignments - currentCentroids are children, levelResult.centroids are parents
                        structure.buildLevelFromAssignments(currentCentroids, levelResult.centroids,
                                levelResult.assignments, currentLevel, currentLevel - 1);

                        // Prepare for next level
                        currentCentroids = levelResult.centroids;
                        // Update currentK using square root reduction (more gradual than division by 2)
                        // currentK = Math.max(1, (int) Math.floor((double) currentK / reductionFactor));
                        currentK = Math.max(1, (int) Math.floor(Math.sqrt(currentK)));
                        currentLevel++;
                    }

                    //                    System.err
                    //                            .println("Hierarchical clustering completed with " + structure.getNumLevels() + " levels");
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

                    // Gap-filling: If we have fewer than k centroids, fill gaps
                    if (resultCentroids.size() < k && !centroids.isEmpty()) {
                        int remaining = k - resultCentroids.size();
                        //                        System.err.println("Filling gap: have " + resultCentroids.size() + " centroids, need " + k
                        //                                + " (input had " + centroids.size() + ")");

                        for (int gap = 0; gap < remaining; gap++) {
                            double maxMinDist = -1.0;
                            int bestIdx = -1;

                            // Find centroid farthest from all existing centroids
                            for (int j = 0; j < centroids.size(); j++) {
                                // Check if this centroid is already selected
                                boolean alreadySelected = false;
                                for (double[] existing : resultCentroids) {
                                    double dist = calculateDistance(centroids.get(j), existing);
                                    if (dist < 1e-10) {
                                        alreadySelected = true;
                                        break;
                                    }
                                }
                                if (alreadySelected) {
                                    continue;
                                }

                                // Find minimum distance to existing centroids
                                double minDist = Double.POSITIVE_INFINITY;
                                for (double[] existing : resultCentroids) {
                                    double dist = calculateDistance(centroids.get(j), existing);
                                    minDist = Math.min(minDist, dist);
                                }

                                if (minDist > maxMinDist) {
                                    maxMinDist = minDist;
                                    bestIdx = j;
                                }
                            }

                            // Add best candidate or fallback to random
                            if (bestIdx >= 0) {
                                resultCentroids
                                        .add(Arrays.copyOf(centroids.get(bestIdx), centroids.get(bestIdx).length));
                            } else {
                                // Fallback: all candidates are duplicates, select random
                                int randomIdx = rand.nextInt(centroids.size());
                                resultCentroids
                                        .add(Arrays.copyOf(centroids.get(randomIdx), centroids.get(randomIdx).length));
                                //                                System.err.println(
                                //                                        "Warning: All candidates were duplicates, selected random centroid for gap-filling");
                            }
                        }

                        //                        System.err.println("Gap-filling complete: now have " + resultCentroids.size() + " centroids");
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
                                maybeNormalizeCentroid(newCentroids[i]);
                                resultCentroids.set(i, newCentroids[i]);
                            } else {
                                // Reinitialize empty cluster
                                // Select random centroid from input centroids list
                                if (!centroids.isEmpty()) {
                                    int randomIdx = rand.nextInt(centroids.size());
                                    double[] reinit =
                                            Arrays.copyOf(centroids.get(randomIdx), centroids.get(randomIdx).length);
                                    maybeNormalizeCentroid(reinit);
                                    resultCentroids.set(i, reinit);
                                    //                                    System.err.println(
                                    //                                            "Reinitialized empty cluster " + i + " with random centroid from input");
                                    converged = false; // Force continuation since we changed a centroid
                                } else {
                                    // Edge case: no input centroids (shouldn't happen, but defensive)
                                    //                                    System.err.println("Warning: Cannot reinitialize empty cluster " + i
                                    //                                            + " - no input centroids available");
                                }
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
