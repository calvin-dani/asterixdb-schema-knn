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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunctionDouble;
import org.apache.asterix.runtime.utils.RuntimeComponentsProvider;
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.application.INCServiceContext;
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
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
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
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.vector.frames.VTreeLeafFrameFactory;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class EuclideanDistanceFunctionDouble implements DistanceFunctionDouble, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.euclidean(a, b);
    }
}

class EuclideanSquaredDistanceFunctionDouble implements DistanceFunctionDouble, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.euclideanSquared(a, b);
    }
}

class CosineDistanceFunctionDouble implements DistanceFunctionDouble, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public double apply(double[] a, double[] b) throws HyracksDataException {
        return VectorDistanceArrCalculation.cosineDistance(a, b);
    }
}

class DotProductDistanceFunctionDouble implements DistanceFunctionDouble, Serializable {
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
 * ALGORITHM OVERVIEW:
 * ===================
 * This operator implements a hierarchical K-means++ clustering algorithm that builds
 * a complete tree structure from bottom-up. The algorithm works as follows:
 * 1. MEMORY-EFFICIENT K-MEANS++ ON RAW DATA:
 *    - Uses probabilistic selection to avoid loading all data points into memory
 *    - Performs iterative candidate selection with weighted K-means++
 *    - Applies Lloyd's algorithm for centroid refinement
 *    - Output: Initial set of leaf centroids (Level 0)
 * 2. HIERARCHICAL TREE BUILDING:
 *    - Takes centroids from current level and clusters them into fewer centroids
 *    - Uses scalable K-means++ on centroids (not raw data) for efficiency
 *    - Establishes parent-child relationships using Lloyd's assignments
 *    - Continues until centroids fit in one frame or only one centroid remains
 * 3. TREE STRUCTURE ORGANIZATION:
 *    - Builds complete tree with nodes containing centroids and relationships
 *    - Assigns BFS-based cluster IDs for efficient traversal
 *    - Organizes parent-child relationships naturally in tree structure
 * 4. OUTPUT:
 *    - Emits all tree nodes in BFS order as tuples (treeLevel, centroidId, parentClusterId, embedding)
 *      on output frames to the downstream static-structure builder
 * MEMORY EFFICIENCY:
 * ==================
 * - Never loads all data points into memory simultaneously
 * - Uses streaming approach with probabilistic selection
 * - Only stores centroids and tree structure in memory
 * - Frame-based stopping criterion prevents memory overflow
 * TREE STRUCTURE:
 * ===============
 * The algorithm builds a tree where:
 * - Leaf nodes (Level 0): Clusters of raw data points
 * - Interior nodes (Level 1+): Clusters of centroids from previous level
 * - Root node: Single centroid representing entire dataset
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
     * Format a double array as a JSON array string.
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
    static class HierarchicalClusterStructure {
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
        }

        /**
         * Output format: <treeLevel, centroidId, parentClusterId, embedding>
         * Uses BFS traversal starting from root level
         */
        public void outputHierarchicalStructure(FrameTupleAppender appender, IFrameWriter writer,
                IHyracksTaskContext ctx) throws HyracksDataException {
            // levelCentroids keys: 0 = leaf level in k-means terms, maxLevel = root.
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }

            if (maxLevel == -1) {
                return;
            }

            // Emission order: bottom-up (leaves first, root last) so that
            // VTreeStaticStructureBuilder writes leaves at the lowest page ids and
            // the root last (at the highest page id).
            //
            // Centroid IDs preserve the BFS-from-root convention (root = 0..N_root-1,
            // leaves at the highest IDs), independent of emission order. To achieve
            // that with bottom-up emission, we pre-compute per-level ID offsets so the
            // root level starts at 0, the next level down starts at root_size, etc.
            int[] idOffset = new int[maxLevel + 1];
            idOffset[maxLevel] = 0;
            for (int L = maxLevel - 1; L >= 0; L--) {
                List<CentroidInfo> levelAbove = levelCentroids.get(L + 1);
                int sizeAbove = (levelAbove != null) ? levelAbove.size() : 0;
                idOffset[L] = idOffset[L + 1] + sizeAbove;
            }

            // Walk levels bottom-up: levelCentroids key 0 (leaves) → key maxLevel (root).
            // The tuple's treeLevel field keeps the existing convention: root = 0, leaf = maxLevel.
            for (int L = 0; L <= maxLevel; L++) {
                List<CentroidInfo> levelInfo = levelCentroids.get(L);
                if (levelInfo == null) {
                    continue;
                }
                int treeLevel = maxLevel - L;
                int globalCentroidId = idOffset[L];
                for (CentroidInfo centroid : levelInfo) {
                    createHierarchicalTuple(treeLevel, globalCentroidId, centroid.parentClusterId, centroid.embedding,
                            appender, writer, ctx);
                    globalCentroidId++;
                }
            }
        }

        /**
         * Log all centroids from all levels as JSON objects via LOGGER at INFO level.
         * Each centroid is logged as a single-line JSON object with complete information.
         * Uses BFS traversal to assign global IDs matching outputHierarchicalStructure().
         */
        public void logAllCentroids() {
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }

            if (maxLevel == -1) {
                return;
            }

            // Same offset table as outputHierarchicalStructure so that the IDs we log
            // here line up with the IDs written into the static structure tuples.
            int[] idOffset = new int[maxLevel + 1];
            idOffset[maxLevel] = 0;
            for (int L = maxLevel - 1; L >= 0; L--) {
                List<CentroidInfo> levelAbove = levelCentroids.get(L + 1);
                int sizeAbove = (levelAbove != null) ? levelAbove.size() : 0;
                idOffset[L] = idOffset[L + 1] + sizeAbove;
            }

            for (int L = 0; L <= maxLevel; L++) {
                List<CentroidInfo> levelInfo = levelCentroids.get(L);
                if (levelInfo == null) {
                    continue;
                }
                int globalCentroidId = idOffset[L];
                for (CentroidInfo centroid : levelInfo) {
                    StringBuilder json = new StringBuilder();
                    json.append("{\"event\":\"hierarchical_centroid\"");
                    json.append(",\"level\":").append(L);
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

            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        public int getNumLevels() {
            return levelCentroids.size();
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
         * @param embeddingDimension The dimension of the embedding vector
         * @return Estimated tuple size in bytes
         */
        public static long calculateEstimatedTupleSize(int embeddingDimension) {
            return 38L + 13L * embeddingDimension;
        }

        /**
         * Check if a level with given number of centroids fits in one frame.
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
         * Computes how many quantized leaf routing entries fit on one leaf disk page.
         */
        public static int computeLeafPageCapacity(int pageSize, int leafHeaderSize, int perLeafEntryBytes) {
            if (pageSize <= 0 || perLeafEntryBytes <= 0) {
                return 1;
            }
            long usable = (long) pageSize - Math.max(0, leafHeaderSize);
            int cap = (int) (usable / perLeafEntryBytes);
            return Math.max(1, cap);
        }

        /**
         * Emit the tree top-down (root level 0 first, then increasing levels toward the leaves), assigning
         * global centroid ids in emission order. Each centroid's {@code parentClusterId} is the position
         * index of its parent within the previous level's emission order, which is the grouping key the
         * downstream {@code VCTreeStaticStructureCreator} relies on (it sorts children by this key and
         * matches them to parents by order). Reuses {@link #createHierarchicalTuple}.
         * <p>
         * Not for the static-structure pipeline — use {@link #outputBottomUpForStaticStructure} instead.
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

        /**
         * Emit for {@code VTreeStaticStructureBuilder}: leaf-first order with top-down {@code levelCentroids}
         * layout (key 0 = root, key maxLevel = leaves). Centroid ids are pre-assigned by scanning levels
         * 0..maxLevel in the same order as {@link #outputTopDownStructure}; only emission order differs
         * (levels maxLevel..0). Use this instead of {@link #outputTopDownStructure} for the static-structure
         * pipeline.
         */
        public void outputBottomUpForStaticStructure(FrameTupleAppender appender, IFrameWriter writer,
                IHyracksTaskContext ctx) throws HyracksDataException {
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }
            if (maxLevel == -1) {
                return;
            }

            // Pre-assign global ids in top-down scan order (same as outputTopDownStructure).
            List<List<Integer>> levelGlobalIds = new ArrayList<>(maxLevel + 1);
            for (int level = 0; level <= maxLevel; level++) {
                levelGlobalIds.add(new ArrayList<>());
            }
            int globalCentroidId = 0;
            for (int level = 0; level <= maxLevel; level++) {
                List<CentroidInfo> levelInfo = levelCentroids.get(level);
                if (levelInfo == null) {
                    continue;
                }
                List<Integer> idsAtLevel = levelGlobalIds.get(level);
                for (int i = 0; i < levelInfo.size(); i++) {
                    idsAtLevel.add(globalCentroidId++);
                }
            }

            // Emit bottom-up: leaves first, root last.
            for (int level = maxLevel; level >= 0; level--) {
                List<CentroidInfo> levelInfo = levelCentroids.get(level);
                if (levelInfo == null) {
                    continue;
                }
                List<Integer> idsAtLevel = levelGlobalIds.get(level);
                for (int i = 0; i < levelInfo.size(); i++) {
                    CentroidInfo centroid = levelInfo.get(i);
                    createHierarchicalTuple(level, idsAtLevel.get(i), centroid.parentClusterId, centroid.embedding,
                            appender, writer, ctx);
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
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot");

    // Distance function hash map (includes "cosine" alias for DDL-normalized metric)
    private static final Map<Integer, DistanceFunctionDouble> DISTANCE_MAP = buildDistanceMap();

    private static Map<Integer, DistanceFunctionDouble> buildDistanceMap() {
        DistanceFunctionDouble cosineFunc = new CosineDistanceFunctionDouble();
        Map<Integer, DistanceFunctionDouble> m = new HashMap<>();
        m.put(EUCLIDEAN_DISTANCE.hash(), new EuclideanDistanceFunctionDouble());
        m.put(EUCLIDEAN_DISTANCE_L2.hash(), new EuclideanDistanceFunctionDouble());
        m.put(EUCLIDEAN_DISTANCE_SQUARED.hash(), new EuclideanSquaredDistanceFunctionDouble());
        m.put(EUCLIDEAN_DISTANCE_L2_SQUARED.hash(), new EuclideanSquaredDistanceFunctionDouble());
        m.put(COSINE_FORMAT.hash(), cosineFunc);
        m.put(DOT_PRODUCT_FORMAT.hash(), new DotProductDistanceFunctionDouble());
        return Collections.unmodifiableMap(m);
    }

    // Clipping constants for centroid values
    private static final double DEFAULT_CLIP_MIN = -1e3;
    private static final double DEFAULT_CLIP_MAX = 1e3;

    // ===== BKT-style top-down clustering (SPANN-inspired) =====
    public static final int BKT_KMEANS_K = 32;
    public static final int BKT_SAMPLES = 1000;
    private static final int BKT_TRY_ITERS = 3;
    private static final int BKT_MAX_ITERS = 100;
    private static final double BKT_CONV_EPS = 1e-3;
    private static final int BKT_NO_IMPROVE = 5;
    /** Negative value selects lambda automatically via DynamicFactorSelect. */
    public static final double DEFAULT_TOPDOWN_LAMBDA_FACTOR = -1.0;
    // Strict height cap: levels 0..MAX_LEVEL (height 5 => deepest level index 4).
    private static final int TOPDOWN_MAX_LEVEL = 4;

    // ===== SPANN SelectHead (scratch BKT + head walk) =====
    public static final double DEFAULT_HEAD_RATIO = 0.15;
    public static final String DEFAULT_SELECT_HEAD_TYPE = "bkt";

    private final UUID sampleUUID;
    private final UUID tupleCountUUID;
    private final UUID headSelectionUUID;
    private final UUID materializedDataUUID;
    private final UUID scalarValuesUUID;

    // Configuration parameters for hierarchical clustering
    private IScalarEvaluatorFactory args; // Evaluator for extracting vector data from tuples
    private int K; // Number of clusters for initial level (leaf nodes)
    private int maxScalableKmeansIter; // Maximum iterations for scalable K-means++ candidate selection
    private DistanceFunctionDouble distanceFunction;
    private RecordDescriptor secondaryRecDesc; // Input record descriptor (2-field format)
    private int vectorDimension;
    // When true, build the tree top-down (root branching computed from frame fit, split per cluster into run
    // files level-by-level). When false, use the original bottom-up memory-efficient algorithm.
    private boolean topDown;
    private int quantizationBits;
    private double lambdaFactor;
    private int maxLevel;
    private boolean selectHeadEnabled;
    private double headRatio;
    private int headCount;
    private String selectHeadType;

    /**
     * Partition-scoped result of SPANN SelectHead (not consumed by static-structure build yet).
     */
    public static final class HeadSelectionTaskState extends AbstractStateObject {
        private static final long serialVersionUID = 1L;
        public final int[] headSampleIndices;
        public final int targetHeadCount;
        public final float achievedRatio;
        public final int selectThreshold;
        public final int splitThreshold;

        public HeadSelectionTaskState(JobId jobId, PartitionedUUID objectId, int[] headSampleIndices,
                int targetHeadCount, float achievedRatio, int selectThreshold, int splitThreshold) {
            super(jobId, objectId);
            this.headSampleIndices = headSampleIndices;
            this.targetHeadCount = targetHeadCount;
            this.achievedRatio = achievedRatio;
            this.selectThreshold = selectThreshold;
            this.splitThreshold = splitThreshold;
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
            out.writeInt(targetHeadCount);
            out.writeFloat(achievedRatio);
            out.writeInt(selectThreshold);
            out.writeInt(splitThreshold);
            out.writeInt(headSampleIndices.length);
            for (int idx : headSampleIndices) {
                out.writeInt(idx);
            }
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
            throw new IOException("HeadSelectionTaskState deserialization not supported");
        }
    }

    /**
     * Supplies freshly-opened readers over a run file so a clustering pass can stream the same data
     * multiple times. Each call returns a NEW, already-opened reader that the caller must close.
     */
    @FunctionalInterface
    private interface RunFileSource {
        GeneratedRunFileReader openReader() throws HyracksDataException;
    }

    /**
     * Temp run file with compact local indices {@code 0..recordCount-1}.
     */
    private static final class MaterializedRunFileSource {
        final RunFileWriter writer;
        final int recordCount;

        MaterializedRunFileSource(RunFileWriter writer, int recordCount) {
            this.writer = writer;
            this.recordCount = recordCount;
        }

        RunFileSource asRunFileSource() {
            return () -> {
                GeneratedRunFileReader rd = writer.createReader();
                rd.open();
                return rd;
            };
        }

        void cleanup() {
            try {
                writer.getFileReference().delete();
            } catch (Exception e) {
                // best-effort managed workspace cleanup
            }
        }
    }

    private static final Logger LOGGER = LogManager.getLogger();
    public static final int DEFAULT_TOPDOWN_MAX_LEVEL = 5;

    /**
     * Copy sample tuples at ascending {@code globalIndices} into a compact run file (local indices
     * {@code 0..|globalIndices|-1} in the same order).
     */
    private static MaterializedRunFileSource materializeIndexedRunFile(IHyracksTaskContext ctx,
            MaterializerTaskState sampleState, int[] globalIndices, FrameTupleAccessor fta, String workspaceName)
            throws HyracksDataException, IOException {
        if (globalIndices == null || globalIndices.length == 0) {
            return null;
        }
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(workspaceName);
        RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
        writer.open();
        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
        int ptr = 0;
        int nextIndex = globalIndices[ptr];
        int currentIndex = 0;
        int written = 0;
        GeneratedRunFileReader in = sampleState.createReader();
        in.open();
        try {
            VSizeFrame frame = new VSizeFrame(ctx);
            while (in.nextFrame(frame) && ptr < globalIndices.length) {
                ByteBuffer buffer = frame.getBuffer();
                fta.reset(buffer);
                int tupleCount = fta.getTupleCount();
                for (int j = 0; j < tupleCount; j++) {
                    if (ptr >= globalIndices.length) {
                        break;
                    }
                    if (currentIndex == nextIndex) {
                        if (!appender.append(fta, j)) {
                            appender.write(writer, true);
                            if (!appender.append(fta, j)) {
                                throw new HyracksDataException(
                                        "Tuple too large to fit in a frame during indexed run-file materialization");
                            }
                        }
                        written++;
                        ptr++;
                        if (ptr < globalIndices.length) {
                            nextIndex = globalIndices[ptr];
                        }
                    }
                    currentIndex++;
                }
            }
            if (appender.getTupleCount() > 0) {
                appender.write(writer, true);
            }
        } finally {
            in.close();
            writer.close();
        }
        if (written != globalIndices.length) {
            LOGGER.warn("[SelectHead] materializeIndexedRunFile: wrote {} of {} indices (streamIndex={})", written,
                    globalIndices.length, currentIndex);
        }
        return new MaterializedRunFileSource(writer, written);
    }

    /**
     * Materialize a single training-format tuple (field 0 = embedding) for childless-centroid promotion.
     * Non-embedding fields are copied from {@code prototypeTuple} so the run file matches {@code recDesc}.
     */
    private static RunFileWriter materializeSingleVectorRunFile(IHyracksTaskContext ctx,
            FrameTupleReference prototypeTuple, double[] embedding, RecordDescriptor recDesc)
            throws HyracksDataException, IOException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile("topdown-promote");
        RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
        writer.open();
        try {
            int nFields = recDesc.getFieldCount();
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(nFields);
            tupleBuilder.reset();

            OrderedListBuilder listBuilder = new OrderedListBuilder();
            listBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));
            ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
            AMutableDouble aDouble = new AMutableDouble(0.0);
            for (double v : embedding) {
                aDouble.setValue(v);
                storage.reset();
                storage.getDataOutput().writeByte(ATypeTag.DOUBLE.serialize());
                ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, storage.getDataOutput());
                listBuilder.addItem(storage);
            }
            storage.reset();
            listBuilder.write(storage.getDataOutput(), true);
            tupleBuilder.addField(storage.getByteArray(), 0, storage.getLength());

            for (int f = 1; f < nFields; f++) {
                tupleBuilder.addField(prototypeTuple.getFieldData(f), prototypeTuple.getFieldStart(f),
                        prototypeTuple.getFieldLength(f));
            }

            FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new HyracksDataException("Tuple too large to fit in a frame during single-vector promotion");
            }
            appender.write(writer, true);
        } finally {
            writer.close();
        }
        return writer;
    }

    private static boolean isSortedAscending(int[] values) {
        for (int i = 1; i < values.length; i++) {
            if (values[i] < values[i - 1]) {
                return false;
            }
        }
        return true;
    }

    /** {@code perm[sortedPos] = originalPos} after sorting {@code values} ascending. */
    private static int[] sortPermutation(int[] values) {
        int n = values.length;
        int[] perm = new int[n];
        for (int i = 0; i < n; i++) {
            perm[i] = i;
        }
        for (int i = 1; i < n; i++) {
            int j = i;
            while (j > 0 && values[perm[j - 1]] > values[perm[j]]) {
                int tmp = perm[j];
                perm[j] = perm[j - 1];
                perm[j - 1] = tmp;
                j--;
            }
        }
        return perm;
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

    private static DistanceFunctionDouble getDistanceFunctionDouble(String distanceType) {
        if (distanceType == null || distanceType.trim().isEmpty()) {
            return new EuclideanSquaredDistanceFunctionDouble();
        }
        String normalized = distanceType.toLowerCase().trim();
        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(normalized);
        DistanceFunctionDouble func = DISTANCE_MAP
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
     * @param centroid The centroid array to clip
     * @return Clipped centroid array with values bounded between DEFAULT_CLIP_MIN and DEFAULT_CLIP_MAX
     */
    private static double[] clipCentroid(double[] centroid) {
        if (centroid == null) {
            return centroid;
        }

        double[] clipped = new double[centroid.length];

        for (int i = 0; i < centroid.length; i++) {
            double value = centroid[i];

            // Check for NaN or Infinity
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                clipped[i] = 0.0; // Replace with 0
            } else if (value < DEFAULT_CLIP_MIN) {
                clipped[i] = DEFAULT_CLIP_MIN;
            } else if (value > DEFAULT_CLIP_MAX) {
                clipped[i] = DEFAULT_CLIP_MAX;
            } else {
                clipped[i] = value;
            }
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
        return distanceFunction instanceof CosineDistanceFunctionDouble;
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
     * Sizes a representative quantized leaf routing entry via {@link VTreeLeafFrameFactory}, then computes how
     * many such entries fit on one leaf page using the NC buffer-cache page size.
     */
    private int computeLeafPageCapacity(IHyracksTaskContext ctx, int dim) throws HyracksDataException {
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        int pageSize = RuntimeComponentsProvider.RUNTIME_PROVIDER.getBufferCache(serviceCtx).getPageSize();
        boolean quantized = quantizationBits > 0;
        ITreeIndexFrame leafFrame = new VTreeLeafFrameFactory(dim, quantized, null, null).createFrame();
        double[] sampleEmbedding = new double[dim];
        int quantizedByteLen = (dim * quantizationBits + 7) / 8;
        byte[] quantizedBytes = new byte[quantizedByteLen];
        ITupleReference entry = TupleUtils.createTuple(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        DoubleArraySerializerDeserializer.INSTANCE, ByteArraySerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE },
                Integer.valueOf(0), sampleEmbedding, quantizedBytes, Integer.valueOf(0));
        int perEntryBytes = leafFrame.getBytesRequiredToWriteTuple(entry);
        int leafHeaderSize = leafFrame.getPageHeaderSize();
        return HierarchicalClusterStructure.computeLeafPageCapacity(pageSize, leafHeaderSize, perEntryBytes);
    }

    public HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor outputRecDesc, RecordDescriptor secondaryRecDesc, UUID sampleUUID, UUID tupleCountUUID,
            UUID materializedDataUUID, UUID scalarValuesUUID, IScalarEvaluatorFactory args, int K,
            int maxScalableKmeansIter, String distanceMetric, int vectorDimension, boolean topDown,
            int quantizationBits, double lambdaFactor, int maxLevel, UUID headSelectionUUID, boolean selectHeadEnabled,
            double headRatio, int headCount, String selectHeadType) {
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
        this.vectorDimension = vectorDimension;
        this.topDown = topDown;
        this.quantizationBits = quantizationBits;
        this.lambdaFactor = lambdaFactor;
        this.maxLevel = maxLevel;
        this.headSelectionUUID = headSelectionUUID;
        this.selectHeadEnabled = selectHeadEnabled;
        this.headRatio = headRatio;
        this.headCount = headCount;
        this.selectHeadType = selectHeadType != null ? selectHeadType : DEFAULT_SELECT_HEAD_TYPE;

        // Distance function from index DDL (WITH similarity "euclidean"|"cosine"|"cosine similarity"|etc.); default euclidean squared
        this.distanceFunction = getDistanceFunctionDouble(distanceMetric);
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
                        LOGGER.info(
                                "[FindCandidates] partition={} sampleTuples={} selectHeadEnabled={} headRatio={} headCount={} selectType={} topDown={}",
                                partition, totalTupleCount, selectHeadEnabled, headRatio, headCount, selectHeadType,
                                topDown);

                        //                        System.err.println("Retrieved total tuple count: " + totalTupleCount);

                        // Build the hierarchical clustering structure (top-down or bottom-up per the strategy flag)
                        HierarchicalClusterStructure clusterStructure;
                        boolean emitTopDown = topDown;

                        if (selectHeadEnabled) {
                            HeadSelectionTaskState headState = runSelectHeadPhase(ctx, sampleState, fta, tuple, eval,
                                    inputVal, listAccessorConstant, KMeansUtils, partition, totalTupleCount);
                            if (headState == null) {
                                LOGGER.warn(
                                        "[BuildHead] partition={}: SelectHead returned null (empty sample or unreadable vectors); "
                                                + "emitting 0 tuples — VCTree will fail",
                                        partition);
                                return;
                            }
                            if (headState.headSampleIndices.length == 0) {
                                LOGGER.warn(
                                        "[BuildHead] partition={}: SelectHead selected 0 heads (target={}); "
                                                + "emitting 0 tuples — VCTree will fail",
                                        partition, headState.targetHeadCount);
                                return;
                            }
                            ctx.setStateObject(headState);
                            MaterializedRunFileSource headSource =
                                    materializeHeadRunFile(ctx, sampleState, headState.headSampleIndices, fta, tuple);
                            if (headSource == null || headSource.recordCount == 0) {
                                LOGGER.warn(
                                        "[BuildHead] partition={}: failed to materialize head run file (headsRequested={}); "
                                                + "emitting 0 tuples — VCTree will fail",
                                        partition, headState.headSampleIndices.length);
                                return;
                            }
                            LOGGER.info(
                                    "[BuildHead] partition={} heads={} sample={} replacing full-sample structure build",
                                    partition, headSource.recordCount, totalTupleCount);
                            try {
                                clusterStructure = buildTopDownHierarchicalKMeans(ctx, headSource.asRunFileSource(),
                                        headSource.recordCount, true, fta, tuple, eval, inputVal, listAccessorConstant,
                                        KMeansUtils, partition);
                            } finally {
                                headSource.cleanup();
                            }
                            emitTopDown = true;
                        } else if (topDown) {
                            final MaterializerTaskState sampleStateRef = sampleState;
                            RunFileSource sampleSource = () -> {
                                GeneratedRunFileReader rd = sampleStateRef.createReader();
                                rd.open();
                                return rd;
                            };
                            clusterStructure = buildTopDownHierarchicalKMeans(ctx, sampleSource, totalTupleCount, false,
                                    fta, tuple, eval, inputVal, listAccessorConstant, KMeansUtils, partition);
                        } else {
                            clusterStructure =
                                    performMemoryEfficientHierarchicalKMeans(ctx, in, fta, tuple, eval, inputVal,
                                            listAccessorConstant, KMeansUtils, vSizeFrame, partition, totalTupleCount);
                        }

                        if (clusterStructure.getNumLevels() == 0) {
                            LOGGER.warn(
                                    "[FindCandidates] partition={}: clustering produced 0 levels (selectHeadEnabled={} "
                                            + "sampleTuples={}); emitting 0 tuples — VCTree will fail",
                                    partition, selectHeadEnabled, totalTupleCount);
                            return;
                        }

                        int totalCentroids = 0;
                        for (Integer lvl : clusterStructure.levelCentroids.keySet()) {
                            List<HierarchicalClusterStructure.CentroidInfo> infos =
                                    clusterStructure.levelCentroids.get(lvl);
                            if (infos != null) {
                                totalCentroids += infos.size();
                            }
                        }
                        LOGGER.info(
                                "[FindCandidates] partition={}: emitting structure levels={} totalCentroids={} selectHeadEnabled={}",
                                partition, clusterStructure.getNumLevels(), totalCentroids, selectHeadEnabled);

                        // Log all centroids from all levels as JSON
                        //                        clusterStructure.logAllCentroids();

                        // Output hierarchical structure with parent-child relationships. Top-down/SPANN stores
                        // the root at level 0 internally; emit leaf-first for VTreeStaticStructureBuilder.
                        // Legacy bottom-up k-means uses outputHierarchicalStructure (BFS id offsets).
                        if (emitTopDown) {
                            clusterStructure.outputBottomUpForStaticStructure(appender, writer, ctx);
                        } else {
                            clusterStructure.outputHierarchicalStructure(appender, writer, ctx);
                        }

                        // Final flush
                        FrameUtils.flushFrame(appender.getBuffer(), writer);

                    } catch (Throwable e) {
                        LOGGER.error("[FindCandidates] partition={} selectHeadEnabled={} failed during structure build",
                                partition, selectHeadEnabled, e);
                        writer.fail();
                        throw HyracksDataException.create(
                                new RuntimeException("SelectHead/BuildHead failed on partition=" + partition, e));
                    } finally {
                        in.close();
                        writer.close();
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

                        // Update current centers: add all candidates from this round for next round's distance computation
                        if (sampledCount > 0) {
                            int startIdx = candidates.size() - sampledCount;
                            for (int idx = startIdx; idx < candidates.size(); idx++) {
                                currentCenters.add(Arrays.copyOf(candidates.get(idx), candidates.get(idx).length));
                            }
                        }
                    }

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

                    List<double[]> weightedCandidates = new ArrayList<>();
                    List<Integer> weightedCandidateWeights = new ArrayList<>();

                    // Reduce duplicates: combine identical/very close candidates and sum their weights
                    for (int i = 0; i < candidates.size(); i++) {
                        if (candidateWeights[i] == 0) {
                            continue; // Skip candidates with no assigned points (zero weight)
                        }

                        // Check if this candidate is a duplicate of an existing weighted candidate
                        boolean foundDuplicate = false;
                        for (int j = 0; j < weightedCandidates.size(); j++) {
                            double dist = calculateDistance(candidates.get(i), weightedCandidates.get(j));
                            if (dist < 1e-10) { // Consider identical if very close
                                // Merge near-duplicate candidates by adding their weights
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

                    // Convert to arrays for easier use
                    int[] finalWeights = new int[weightedCandidateWeights.size()];
                    for (int i = 0; i < weightedCandidateWeights.size(); i++) {
                        finalWeights[i] = weightedCandidateWeights.get(i);
                    }

                    // Step 4: Select k initial centroids via weighted k-means++; pad from materialized sample if fewer than k
                    List<double[]> centroids;
                    if (weightedCandidates.isEmpty()) {
                        // Fallback: use first centroid only
                        centroids = new ArrayList<>();
                        centroids.add(firstCentroid);
                    } else if (weightedCandidates.size() <= k) {
                        // Fewer than k distinct candidates after dedup: keep them and pad from materialized sample
                        // Seed initial centroids from all distinct weighted candidates
                        centroids = new ArrayList<>();
                        for (double[] candidate : weightedCandidates) {
                            centroids.add(Arrays.copyOf(candidate, candidate.length));
                        }
                        // Pad to k centroids by reading additional points from the materialized sample
                        int needed = k - centroids.size();
                        if (needed > 0) {
                            List<Integer> additionalIndices = new ArrayList<>();
                            for (int i = 0; i < needed; i++) {
                                // Evenly spaced tuple indices over [0, totalTupleCount) for reproducible padding
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

                    } else {
                        // Normal path - we have more than k weighted candidates
                        // Run weighted k-means++ on weightedCandidates to select exactly k
                        centroids = performWeightedKMeansPlusPlusOnCandidates(weightedCandidates, finalWeights, k, rand,
                                maxIterations);
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
                // BKT-STYLE TOP-DOWN HIERARCHICAL CLUSTERING
                // SPANN-inspired: dynamic fan-out capped at 32, lambda-balanced k-means on a sample, full-data
                // assign, real-record pivots, leaf stop when a bag fits on one quantized leaf page.
                // ============================================================================================

                /** Work item for level-by-level expansion: one parent run file and its parent centroid index. */
                final class ParentBatch {
                    final RunFileWriter file;
                    final int recCount;
                    final int parentClusterId;

                    ParentBatch(RunFileWriter file, int recCount, int parentClusterId) {
                        this.file = file;
                        this.recCount = recCount;
                        this.parentClusterId = parentClusterId;
                    }
                }

                private int dynamicK(int recordCount, int leafPageCapacity) {
                    if (recordCount <= leafPageCapacity) {
                        return recordCount;
                    }
                    int k = Math.min(recordCount / leafPageCapacity + 1, BKT_KMEANS_K);
                    return Math.max(2, Math.min(k, recordCount));
                }

                /**
                 * Fan-out for a k-means split: BuildHead uses fixed {@link #BKT_KMEANS_K} (SPANN {@code dynamicK=false});
                 * full-sample TopDown and SelectHead scratch BKT use {@link #dynamicK}.
                 */
                private int splitK(int recordCount, int leafPageCapacity, boolean headOnlyBuild) {
                    if (recordCount <= leafPageCapacity) {
                        return recordCount;
                    }
                    if (headOnlyBuild) {
                        return Math.max(2, Math.min(BKT_KMEANS_K, recordCount));
                    }
                    return dynamicK(recordCount, leafPageCapacity);
                }

                /** True when any pending batch still needs a k-means split (exceeds one leaf page). */
                private boolean anyBucketNeedsSplit(List<ParentBatch> batches, int leafPageCapacity) {
                    if (batches == null) {
                        return false;
                    }
                    for (ParentBatch batch : batches) {
                        if (batch != null && batch.recCount > leafPageCapacity) {
                            return true;
                        }
                    }
                    return false;
                }

                private double[] copyVector(double[] v) {
                    return v == null ? null : Arrays.copyOf(v, v.length);
                }

                private void deleteRunFile(RunFileWriter file) {
                    if (file != null) {
                        try {
                            file.getFileReference().delete();
                        } catch (Exception e) {
                            // best-effort cleanup; ignore
                        }
                    }
                }

                /**
                 * Register routing centroids and child run files only for k-means buckets with assigned records.
                 * Empty buckets are dropped so level-L centroid count matches level-(L+1) cluster count for
                 * {@code VTreeStaticStructureBuilder} (Nth centroid at L points to Nth cluster at L+1).
                 */
                private int registerNonEmptySplitBuckets(String logTag, int partition, int level, int parentClusterId,
                        List<HierarchicalClusterStructure.CentroidInfo> centroidsOut, List<ParentBatch> batchesOut,
                        List<double[]> centroids, SplitResult split, int localId) {
                    int k = centroids.size();
                    for (int i = 0; i < k; i++) {
                        if (split.counts.get(i) <= 0) {
                            LOGGER.info("[{}] partition={} level={} parent={}: dropped empty bucket {}/{} (recCount=0)",
                                    logTag, partition, level, parentClusterId, i, k);
                            deleteRunFile(split.files.get(i));
                            continue;
                        }
                        int idAtLevel = localId++;
                        centroidsOut.add(new HierarchicalClusterStructure.CentroidInfo(idAtLevel, parentClusterId,
                                centroids.get(i), level));
                        batchesOut.add(new ParentBatch(split.files.get(i), split.counts.get(i), idAtLevel));
                    }
                    return localId;
                }

                // ============================================================================================
                // SPANN SelectHead: scratch BKT + post-order head walk (not wired to structure build)
                // ============================================================================================

                /** Mirrors SPTAG {@code BKTNode}. */
                final class ScratchBktNode {
                    int centerid;
                    int childStart;
                    int childEnd;

                    ScratchBktNode(int centerid, int childStart, int childEnd) {
                        this.centerid = centerid;
                        this.childStart = childStart;
                        this.childEnd = childEnd;
                    }
                }

                final class ScratchBktTree {
                    final List<ScratchBktNode> nodes;
                    final int rootIndex;
                    final int recordCount;

                    ScratchBktTree(List<ScratchBktNode> nodes, int rootIndex, int recordCount) {
                        this.nodes = nodes;
                        this.rootIndex = rootIndex;
                        this.recordCount = recordCount;
                    }
                }

                final class ScratchBktStackItem {
                    final int nodeIndex;
                    final int first;
                    final int last;

                    ScratchBktStackItem(int nodeIndex, int first, int last) {
                        this.nodeIndex = nodeIndex;
                        this.first = first;
                        this.last = last;
                    }
                }

                final class SelectHeadOptions {
                    final int selectThreshold;
                    final int splitThreshold;
                    final int splitFactor;
                    final int targetHeadCount;
                    final double targetRatio;

                    SelectHeadOptions(int selectThreshold, int splitThreshold, int splitFactor, int targetHeadCount,
                            double targetRatio) {
                        this.selectThreshold = selectThreshold;
                        this.splitThreshold = splitThreshold;
                        this.splitFactor = splitFactor;
                        this.targetHeadCount = targetHeadCount;
                        this.targetRatio = targetRatio;
                    }
                }

                private void reorderByAssignment(int[] localIndices, int first, int size, int[] assignments,
                        int[] counts) {
                    int[] temp = Arrays.copyOfRange(localIndices, first, first + size);
                    int k = counts.length;
                    int[] writePos = new int[k];
                    writePos[0] = first;
                    for (int c = 1; c < k; c++) {
                        writePos[c] = writePos[c - 1] + counts[c - 1];
                    }
                    int[] filled = new int[k];
                    for (int i = 0; i < size; i++) {
                        int c = assignments[i];
                        localIndices[writePos[c] + filled[c]++] = temp[i];
                    }
                }

                private ScratchBktTree buildScratchBkt(IHyracksTaskContext ctx, MaterializerTaskState sampleState,
                        RunFileSource sampleSource, int recordCount, int leafPageCapacity, float tunedLambdaFactor,
                        FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAcc, KMeansUtils kmu, Random rand) throws HyracksDataException, IOException {
                    int[] localIndices = new int[recordCount];
                    for (int i = 0; i < recordCount; i++) {
                        localIndices[i] = i;
                    }
                    List<ScratchBktNode> nodes = new ArrayList<>();
                    nodes.add(new ScratchBktNode(recordCount, 0, 0));
                    Deque<ScratchBktStackItem> stack = new ArrayDeque<>();
                    stack.push(new ScratchBktStackItem(0, 0, recordCount));
                    int splitCount = 0;
                    int leafListCount = 0;

                    LOGGER.info("[SelectHead] buildScratchBkt: begin records={} leafPageCap={} lambdaFactor={}",
                            recordCount, leafPageCapacity, tunedLambdaFactor);

                    while (!stack.isEmpty()) {
                        ScratchBktStackItem item = stack.pop();
                        int nodeIndex = item.nodeIndex;
                        int first = item.first;
                        int last = item.last;
                        int size = last - first;
                        nodes.get(nodeIndex).childStart = nodes.size();

                        if (size <= leafPageCapacity) {
                            int leafNodes = last - first;
                            leafListCount++;
                            LOGGER.debug(
                                    "[SelectHead] scratch leaf-list node={} range=[{},{}] size={} stackRemaining={}",
                                    nodeIndex, first, last, leafNodes, stack.size());
                            for (int j = first; j < last; j++) {
                                nodes.add(new ScratchBktNode(localIndices[j], -1, -1));
                            }
                        } else {
                            int k = dynamicK(size, leafPageCapacity);
                            splitCount++;
                            LOGGER.info(
                                    "[SelectHead] scratch split #{} node={} range=[{},{}] size={} k={} stackRemaining={}",
                                    splitCount, nodeIndex, first, last, size, k, stack.size());

                            RunFileSource splitSource = sampleSource;
                            MaterializedRunFileSource subrangeMat = null;
                            int[] perm = null;
                            if (size < recordCount) {
                                int[] subMap = Arrays.copyOfRange(localIndices, first, last);
                                int[] materializeIndices = subMap;
                                if (!isSortedAscending(subMap)) {
                                    perm = sortPermutation(subMap);
                                    materializeIndices = new int[size];
                                    for (int i = 0; i < size; i++) {
                                        materializeIndices[i] = subMap[perm[i]];
                                    }
                                }
                                subrangeMat = materializeIndexedRunFile(ctx, sampleState, materializeIndices, fta,
                                        "scratch-bkt-subrange");
                                if (subrangeMat == null || subrangeMat.recordCount != size) {
                                    throw new HyracksDataException(
                                            "Failed to materialize scratch-BKT subrange of size " + size);
                                }
                                splitSource = subrangeMat.asRunFileSource();
                            }

                            try {
                                ClusteringResult res = clusterRunFile(ctx, splitSource, size, k, tunedLambdaFactor, fta,
                                        tuple, eval, inputVal, listAcc, kmu, rand);
                                if (res.assignments == null || res.assignments.length != size) {
                                    LOGGER.warn(
                                            "[SelectHead] scratch split node={}: assignment length mismatch expected={} got={}",
                                            nodeIndex, size, res.assignments == null ? -1 : res.assignments.length);
                                }
                                if (res.centroids.isEmpty()) {
                                    LOGGER.warn(
                                            "[SelectHead] scratch split node={}: clusterRunFile returned 0 centroids",
                                            nodeIndex);
                                }
                                int[] assignments = res.assignments;
                                if (perm != null && assignments != null && assignments.length == size) {
                                    int[] restored = new int[size];
                                    for (int i = 0; i < size; i++) {
                                        restored[perm[i]] = assignments[i];
                                    }
                                    assignments = restored;
                                }
                                int[] counts = new int[k];
                                if (assignments != null) {
                                    for (int i = 0; i < size; i++) {
                                        if (i >= assignments.length) {
                                            break;
                                        }
                                        int cluster = assignments[i];
                                        if (cluster < 0 || cluster >= k) {
                                            LOGGER.warn(
                                                    "[SelectHead] scratch split node={}: invalid assignment[{}]={} (k={})",
                                                    nodeIndex, i, cluster, k);
                                            continue;
                                        }
                                        counts[cluster]++;
                                    }
                                }
                                reorderByAssignment(localIndices, first, size, assignments, counts);
                                int nonEmpty = 0;
                                for (int c = 0; c < k; c++) {
                                    if (counts[c] > 0) {
                                        nonEmpty++;
                                    }
                                }
                                if (nonEmpty <= 1) {
                                    // SPTAG numClusters <= 1: treat as leaf-list, do not re-push
                                    LOGGER.warn(
                                            "[SelectHead] scratch split node={} range=[{},{}] size={} k={}: "
                                                    + "degenerate assignment (nonEmpty={}), emitting leaf-list",
                                            nodeIndex, first, last, size, k, nonEmpty);
                                    leafListCount++;
                                    for (int j = first; j < last; j++) {
                                        nodes.add(new ScratchBktNode(localIndices[j], -1, -1));
                                    }
                                } else {
                                    int pos = first;
                                    for (int c = 0; c < k; c++) {
                                        if (counts[c] == 0) {
                                            continue;
                                        }
                                        int childFirst = pos;
                                        int childLast = pos + counts[c];
                                        int centerid = localIndices[childLast - 1];
                                        int childNodeIndex = nodes.size();
                                        nodes.add(new ScratchBktNode(centerid, 0, 0));
                                        if (counts[c] > 1) {
                                            stack.push(new ScratchBktStackItem(childNodeIndex, childFirst, childLast));
                                        }
                                        pos = childLast;
                                    }
                                }
                            } finally {
                                if (subrangeMat != null) {
                                    subrangeMat.cleanup();
                                }
                            }
                        }
                        nodes.get(nodeIndex).childEnd = nodes.size();
                    }
                    LOGGER.info("[SelectHead] scratch BKT built: nodes={} records={} splits={} leafLists={}",
                            nodes.size(), recordCount, splitCount, leafListCount);
                    return new ScratchBktTree(nodes, 0, recordCount);
                }

                private SelectHeadOptions adjustSelectHeadOptions(int recordCount, double ratio,
                        int explicitHeadCount) {
                    double effectiveRatio = ratio;
                    if (explicitHeadCount > 0) {
                        effectiveRatio = explicitHeadCount / (double) recordCount;
                    }
                    int targetHeadCount = Math.max(1, (int) Math.round(effectiveRatio * recordCount));
                    if (targetHeadCount >= recordCount) {
                        targetHeadCount = recordCount;
                    }
                    int selectThreshold = Math.min(recordCount - 1, Math.max(2, (int) (1.0 / effectiveRatio)));
                    int splitThreshold = Math.min(recordCount - 1, selectThreshold * 2);
                    int splitFactor =
                            Math.min(recordCount - 1, Math.max(2, (int) Math.round(1.0 / effectiveRatio + 0.5)));
                    LOGGER.info(
                            "[SelectHead] adjustOptions: records={} ratio={} explicitHeadCount={} -> targetHeads={} "
                                    + "selectThreshold={} splitThreshold={} splitFactor={}",
                            recordCount, ratio, explicitHeadCount, targetHeadCount, selectThreshold, splitThreshold,
                            splitFactor);
                    return new SelectHeadOptions(selectThreshold, splitThreshold, splitFactor, targetHeadCount,
                            effectiveRatio);
                }

                private int selectHeadDynamicallyInternal(ScratchBktTree tree, int nodeId, SelectHeadOptions opts,
                        List<Integer> selected) {
                    int childrenSize = 1;
                    List<int[]> childMeta = new ArrayList<>();
                    ScratchBktNode node = tree.nodes.get(nodeId);
                    if (node.childStart >= 0) {
                        for (int i = node.childStart; i < node.childEnd; i++) {
                            int cs = selectHeadDynamicallyInternal(tree, i, opts, selected);
                            if (cs > 0) {
                                childMeta.add(new int[] { i, cs });
                                childrenSize += cs;
                            }
                        }
                    }

                    if (childrenSize >= opts.selectThreshold) {
                        int rootSentinel = tree.nodes.get(tree.rootIndex).centerid;
                        if (node.centerid < rootSentinel && node.centerid >= 0 && node.centerid < tree.recordCount) {
                            selected.add(node.centerid);
                        }
                        if (childrenSize > opts.splitThreshold && !childMeta.isEmpty()) {
                            childMeta.sort((a, b) -> Integer.compare(b[1], a[1]));
                            int selectCnt = (int) Math.ceil(childrenSize * 1.0 / opts.splitFactor + 0.5);
                            for (int i = 0; i < selectCnt && i < childMeta.size(); i++) {
                                int childId = childMeta.get(i)[0];
                                int cid = tree.nodes.get(childId).centerid;
                                if (cid >= 0 && cid < tree.recordCount) {
                                    selected.add(cid);
                                }
                            }
                        }
                        return 0;
                    }
                    return childrenSize;
                }

                final class SelectHeadWalkResult {
                    final int[] headIndices;
                    final int selectThreshold;
                    final int splitThreshold;

                    SelectHeadWalkResult(int[] headIndices, int selectThreshold, int splitThreshold) {
                        this.headIndices = headIndices;
                        this.selectThreshold = selectThreshold;
                        this.splitThreshold = splitThreshold;
                    }
                }

                private SelectHeadWalkResult selectHeadDynamically(ScratchBktTree tree, SelectHeadOptions baseOpts) {
                    LOGGER.info(
                            "[SelectHead] walk begin: treeNodes={} records={} targetHeads={} targetRatio={} "
                                    + "initialSelectThreshold={} initialSplitThreshold={}",
                            tree.nodes.size(), tree.recordCount, baseOpts.targetHeadCount, baseOpts.targetRatio,
                            baseOpts.selectThreshold, baseOpts.splitThreshold);
                    if (baseOpts.targetHeadCount >= tree.recordCount) {
                        LOGGER.info("[SelectHead] walk: targetHeads>={} records, selecting all indices",
                                tree.recordCount);
                        int[] all = new int[tree.recordCount];
                        for (int i = 0; i < tree.recordCount; i++) {
                            all[i] = i;
                        }
                        return new SelectHeadWalkResult(all, baseOpts.selectThreshold, baseOpts.splitThreshold);
                    }

                    int selectThreshold = baseOpts.selectThreshold;
                    int splitThreshold = baseOpts.splitThreshold;
                    double minDiff = 1.0;
                    int tuningTrials = 0;
                    for (int select = 2; select <= baseOpts.selectThreshold; select++) {
                        int l = baseOpts.splitFactor;
                        int r = baseOpts.splitThreshold;
                        while (l < r - 1) {
                            int mid = (l + r) / 2;
                            SelectHeadOptions trial = new SelectHeadOptions(select, mid, baseOpts.splitFactor,
                                    baseOpts.targetHeadCount, baseOpts.targetRatio);
                            List<Integer> trialSelected = new ArrayList<>();
                            selectHeadDynamicallyInternal(tree, tree.rootIndex, trial, trialSelected);
                            int[] unique = dedupeAndSortHeads(trialSelected);
                            double diff = unique.length / (double) tree.recordCount - baseOpts.targetRatio;
                            tuningTrials++;
                            LOGGER.debug("[SelectHead] walk trial #{}: select={} split={} heads={} diffFromTarget={}",
                                    tuningTrials, select, mid, unique.length, diff);
                            if (Math.abs(diff) < minDiff) {
                                minDiff = Math.abs(diff);
                                selectThreshold = select;
                                splitThreshold = mid;
                            }
                            if (diff > 0) {
                                l = mid;
                            } else {
                                r = mid;
                            }
                        }
                    }

                    SelectHeadOptions finalOpts = new SelectHeadOptions(selectThreshold, splitThreshold,
                            baseOpts.splitFactor, baseOpts.targetHeadCount, baseOpts.targetRatio);
                    List<Integer> selected = new ArrayList<>();
                    selectHeadDynamicallyInternal(tree, tree.rootIndex, finalOpts, selected);
                    int[] finalHeads = dedupeAndSortHeads(selected);
                    LOGGER.info(
                            "[SelectHead] walk complete: tuningTrials={} tunedSelect={} tunedSplit={} targetRatio={} "
                                    + "selectedHeads={} achievedRatio={}",
                            tuningTrials, selectThreshold, splitThreshold, baseOpts.targetRatio, finalHeads.length,
                            finalHeads.length / (double) tree.recordCount);
                    return new SelectHeadWalkResult(finalHeads, selectThreshold, splitThreshold);
                }

                private int[] dedupeAndSortHeads(List<Integer> selected) {
                    if (selected.isEmpty()) {
                        return new int[0];
                    }
                    Collections.sort(selected);
                    List<Integer> unique = new ArrayList<>();
                    int prev = -1;
                    for (int id : selected) {
                        if (id != prev) {
                            unique.add(id);
                            prev = id;
                        }
                    }
                    int[] out = new int[unique.size()];
                    for (int i = 0; i < unique.size(); i++) {
                        out[i] = unique.get(i);
                    }
                    return out;
                }

                private int[] selectHeadsRandom(int recordCount, int targetCount, Random rand) {
                    int[] indices = new int[recordCount];
                    for (int i = 0; i < recordCount; i++) {
                        indices[i] = i;
                    }
                    for (int i = recordCount - 1; i > 0; i--) {
                        int j = rand.nextInt(i + 1);
                        int tmp = indices[i];
                        indices[i] = indices[j];
                        indices[j] = tmp;
                    }
                    int take = Math.min(targetCount, recordCount);
                    int[] heads = new int[take];
                    System.arraycopy(indices, 0, heads, 0, take);
                    Arrays.sort(heads);
                    return heads;
                }

                /**
                 * Copy sample tuples at {@code headIndices} (global sample positions) into a compact head-only run
                 * file with local indices {@code 0..|H|-1}.
                 */
                private MaterializedRunFileSource materializeHeadRunFile(IHyracksTaskContext ctx,
                        MaterializerTaskState sampleState, int[] headIndices, FrameTupleAccessor fta,
                        FrameTupleReference tuple) throws HyracksDataException, IOException {
                    if (headIndices == null || headIndices.length == 0) {
                        LOGGER.warn("[SelectHead] materializeHeadRunFile: no head indices to copy");
                        return null;
                    }
                    LOGGER.info("[SelectHead] materializeHeadRunFile: copying {} head indices from sample",
                            headIndices.length);
                    MaterializedRunFileSource result =
                            materializeIndexedRunFile(ctx, sampleState, headIndices, fta, "head-vectors");
                    if (result != null && result.recordCount == headIndices.length) {
                        LOGGER.info("[SelectHead] materializeHeadRunFile: wrote all {} head tuples",
                                result.recordCount);
                    }
                    return result;
                }

                private HeadSelectionTaskState runSelectHeadPhase(IHyracksTaskContext ctx,
                        MaterializerTaskState sampleState, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu,
                        int partition, int totalTupleCount) throws HyracksDataException, IOException {
                    LOGGER.info(
                            "[SelectHead] partition={}: phase begin sample={} headRatio={} headCount={} selectType={}",
                            partition, totalTupleCount, headRatio, headCount, selectHeadType);
                    if (totalTupleCount <= 0) {
                        LOGGER.warn("[SelectHead] partition={}: empty sample, aborting SelectHead", partition);
                        return null;
                    }
                    Random rand = new Random();
                    int dim = vectorDimension;
                    if (dim <= 0) {
                        GeneratedRunFileReader probe = sampleState.createReader();
                        probe.open();
                        try {
                            double[] firstPoint =
                                    getPointAtIndex(probe, fta, tuple, eval, inputVal, listAcc, kmu, 0, ctx);
                            if (firstPoint == null) {
                                LOGGER.warn(
                                        "[SelectHead] partition={}: first sample tuple at index 0 is not a readable vector",
                                        partition);
                                return null;
                            }
                            dim = firstPoint.length;
                        } finally {
                            probe.close();
                        }
                    }

                    final MaterializerTaskState sampleStateRef = sampleState;
                    RunFileSource sampleSource = () -> {
                        GeneratedRunFileReader rd = sampleStateRef.createReader();
                        rd.open();
                        return rd;
                    };

                    SelectHeadOptions opts = adjustSelectHeadOptions(totalTupleCount, headRatio, headCount);
                    int[] headIndices;
                    int selectThreshold = opts.selectThreshold;
                    int splitThreshold = opts.splitThreshold;

                    if ("random".equalsIgnoreCase(selectHeadType)) {
                        headIndices = selectHeadsRandom(totalTupleCount, opts.targetHeadCount, rand);
                    } else {
                        int leafPageCapacity = computeLeafPageCapacity(ctx, dim);
                        float tunedLambdaFactor;
                        if (lambdaFactor > 0) {
                            tunedLambdaFactor = (float) lambdaFactor;
                        } else {
                            int probeK = dynamicK(totalTupleCount, leafPageCapacity);
                            tunedLambdaFactor = dynamicFactorSelect(ctx, sampleSource, totalTupleCount, probeK, dim,
                                    fta, tuple, eval, inputVal, listAcc, kmu, rand);
                        }
                        LOGGER.info("[SelectHead] partition={}: building scratch BKT (leafPageCap={} lambdaFactor={})",
                                partition, leafPageCapacity, tunedLambdaFactor);
                        ScratchBktTree scratchTree;
                        try {
                            scratchTree =
                                    buildScratchBkt(ctx, sampleState, sampleSource, totalTupleCount, leafPageCapacity,
                                            tunedLambdaFactor, fta, tuple, eval, inputVal, listAcc, kmu, rand);
                        } catch (Throwable t) {
                            LOGGER.error("[SelectHead] partition={}: buildScratchBkt failed", partition, t);
                            throw t;
                        }
                        LOGGER.info("[SelectHead] partition={}: running SelectHead walk on scratch tree", partition);
                        SelectHeadWalkResult walkResult;
                        try {
                            walkResult = selectHeadDynamically(scratchTree, opts);
                        } catch (Throwable t) {
                            LOGGER.error("[SelectHead] partition={}: selectHeadDynamically failed", partition, t);
                            throw t;
                        }
                        headIndices = walkResult.headIndices;
                        selectThreshold = walkResult.selectThreshold;
                        splitThreshold = walkResult.splitThreshold;
                    }

                    float achievedRatio = headIndices.length / (float) totalTupleCount;
                    LOGGER.info(
                            "[SelectHead] partition={} sample={} targetHeads={} selected={} achievedRatio={} selectThreshold={} splitThreshold={}",
                            partition, totalTupleCount, opts.targetHeadCount, headIndices.length, achievedRatio,
                            selectThreshold, splitThreshold);

                    return new HeadSelectionTaskState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(headSelectionUUID, partition), headIndices, opts.targetHeadCount,
                            achievedRatio, selectThreshold, splitThreshold);
                }

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

                /** Read the first tuple from a run file as a template for non-embedding fields in promotion batches. */
                private FrameTupleReference readPrototypeTuple(IHyracksTaskContext ctx, RunFileSource source,
                        FrameTupleAccessor fta, FrameTupleReference tuple) throws HyracksDataException, IOException {
                    GeneratedRunFileReader in = source.openReader();
                    try {
                        VSizeFrame frame = new VSizeFrame(ctx);
                        if (!in.nextFrame(frame)) {
                            throw new HyracksDataException("Cannot read prototype tuple from empty run file");
                        }
                        fta.reset(frame.getBuffer());
                        if (fta.getTupleCount() <= 0) {
                            throw new HyracksDataException("Cannot read prototype tuple from empty frame");
                        }
                        tuple.reset(fta, 0);
                        return tuple;
                    } finally {
                        in.close();
                    }
                }

                /**
                 * Emits one real-record centroid per vector in the run file (leaf bag, no further split).
                 */
                private ClusteringResult emitAllRecordsAsCentroids(IHyracksTaskContext ctx, RunFileSource source,
                        int recordCount, FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval,
                        IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu)
                        throws HyracksDataException, IOException {
                    List<double[]> centroids = new ArrayList<>();
                    int[] assignments = new int[recordCount];
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
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    if (idx < recordCount) {
                                        assignments[idx] = centroids.size();
                                    }
                                    idx++;
                                    continue;
                                }
                                listAcc.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kmu.createPrimitveList(listAcc);
                                int cid = centroids.size();
                                centroids.add(copyVector(point));
                                if (idx < recordCount) {
                                    assignments[idx] = cid;
                                }
                                idx++;
                            }
                        }
                    } finally {
                        in.close();
                    }
                    return new ClusteringResult(centroids, assignments);
                }

                /**
                 * Lambda-balanced assignment: score = distance + lambda * priorCounts[c].
                 */
                private double kmeansAssign(IHyracksTaskContext ctx, RunFileSource source, int recordCount,
                        int[] indexOrder, int batchSize, double[][] centers, int k, int[] priorCounts, double lambda,
                        boolean updateCenters, int[] outCounts, double[][] centerSums, int[] clusterIdx,
                        double[] clusterDist, int[] assignments, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu)
                        throws HyracksDataException, IOException {
                    Arrays.fill(outCounts, 0);
                    if (centerSums != null) {
                        for (int c = 0; c < k; c++) {
                            if (centerSums[c] != null) {
                                Arrays.fill(centerSums[c], 0.0);
                            }
                        }
                    }
                    Arrays.fill(clusterIdx, -1);
                    Arrays.fill(clusterDist, Double.POSITIVE_INFINITY);
                    double totalDist = 0.0;
                    int limit = Math.min(batchSize, recordCount);
                    GeneratedRunFileReader in = source.openReader();
                    try {
                        VSizeFrame frame = new VSizeFrame(ctx);
                        int idx = 0;
                        boolean[] inBatch = new boolean[recordCount];
                        for (int i = 0; i < limit; i++) {
                            inBatch[indexOrder[i]] = true;
                        }
                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();
                            for (int j = 0; j < tupleCount; j++) {
                                if (idx >= recordCount) {
                                    break;
                                }
                                if (!inBatch[idx]) {
                                    idx++;
                                    continue;
                                }
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    idx++;
                                    continue;
                                }
                                listAcc.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kmu.createPrimitveList(listAcc);
                                int nearest = 0;
                                double best = Double.POSITIVE_INFINITY;
                                for (int c = 0; c < k; c++) {
                                    double dist = calculateDistance(point, centers[c]);
                                    double score = dist + lambda * priorCounts[c];
                                    if (score < best) {
                                        best = score;
                                        nearest = c;
                                    }
                                }
                                if (assignments != null && idx < assignments.length) {
                                    assignments[idx] = nearest;
                                }
                                outCounts[nearest]++;
                                totalDist += best;
                                if (updateCenters && centerSums != null) {
                                    if (centerSums[nearest] == null) {
                                        centerSums[nearest] = new double[point.length];
                                    }
                                    for (int d = 0; d < point.length; d++) {
                                        centerSums[nearest][d] += point[d];
                                    }
                                    if (best >= clusterDist[nearest]) {
                                        clusterDist[nearest] = best;
                                        clusterIdx[nearest] = idx;
                                    }
                                } else if (best <= clusterDist[nearest]) {
                                    clusterDist[nearest] = best;
                                    clusterIdx[nearest] = idx;
                                }
                                idx++;
                            }
                        }
                    } finally {
                        in.close();
                    }
                    return totalDist;
                }

                private double refineCentersFromSums(double[][] centers, double[][] newCenters, int k, int[] counts,
                        double[][] centerSums, int[] clusterIdx, int dim) throws HyracksDataException {
                    int maxCluster = -1;
                    int maxCount = 0;
                    for (int c = 0; c < k; c++) {
                        if (counts[c] > maxCount && counts[c] > 0) {
                            maxCluster = c;
                            maxCount = counts[c];
                        }
                    }
                    double diff = 0.0;
                    for (int c = 0; c < k; c++) {
                        if (counts[c] == 0 || centerSums[c] == null) {
                            if (maxCluster >= 0 && clusterIdx[maxCluster] >= 0) {
                                System.arraycopy(centers[maxCluster], 0, newCenters[c], 0, dim);
                            } else {
                                System.arraycopy(centers[c], 0, newCenters[c], 0, dim);
                            }
                        } else {
                            for (int d = 0; d < dim; d++) {
                                newCenters[c][d] = centerSums[c][d] / counts[c];
                            }
                            maybeNormalizeCentroid(newCenters[c]);
                        }
                        diff += calculateDistance(centers[c], newCenters[c]);
                    }
                    return diff;
                }

                private float refineLambda(int k, int[] counts, double[] clusterDist, double totalDist, int batchSize) {
                    int maxCluster = -1;
                    int maxCount = 0;
                    for (int c = 0; c < k; c++) {
                        if (counts[c] > maxCount && counts[c] > 0) {
                            maxCluster = c;
                            maxCount = counts[c];
                        }
                    }
                    if (maxCluster < 0 || batchSize <= 0) {
                        return 0f;
                    }
                    float avgDist = (float) (totalDist / batchSize);
                    float lambda = (float) ((clusterDist[maxCluster] - avgDist) / batchSize);
                    return Math.max(0f, lambda);
                }

                private float initCenters(IHyracksTaskContext ctx, RunFileSource source, int recordCount, int k,
                        int batchSize, int[] indexOrder, double[][] centers, double[][] newCenters, int dim,
                        FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAcc, KMeansUtils kmu, Random rand) throws HyracksDataException, IOException {
                    float adjustedLambda = 0f;
                    double minClusterDist = Double.POSITIVE_INFINITY;
                    int[] counts = new int[k];
                    int[] clusterIdx = new int[k];
                    double[] clusterDist = new double[k];
                    double[][] centerSums = new double[k][];
                    for (int trial = 0; trial < BKT_TRY_ITERS; trial++) {
                        for (int c = 0; c < k; c++) {
                            int ridx = rand.nextInt(recordCount);
                            GeneratedRunFileReader seedReader = source.openReader();
                            double[] pt;
                            try {
                                pt = getPointAtIndex(seedReader, fta, tuple, eval, inputVal, listAcc, kmu, ridx, ctx);
                            } finally {
                                seedReader.close();
                            }
                            if (pt == null) {
                                continue;
                            }
                            System.arraycopy(pt, 0, centers[c], 0, dim);
                        }
                        Arrays.fill(counts, 0);
                        double totalDist = kmeansAssign(ctx, source, recordCount, indexOrder, batchSize, centers, k,
                                counts, 0.0, true, counts, centerSums, clusterIdx, clusterDist, null, fta, tuple, eval,
                                inputVal, listAcc, kmu);
                        if (totalDist < minClusterDist) {
                            minClusterDist = totalDist;
                            for (int c = 0; c < k; c++) {
                                System.arraycopy(centers[c], 0, newCenters[c], 0, dim);
                            }
                            adjustedLambda = refineLambda(k, counts, clusterDist, totalDist, batchSize);
                        }
                    }
                    for (int c = 0; c < k; c++) {
                        System.arraycopy(newCenters[c], 0, centers[c], 0, dim);
                    }
                    return adjustedLambda;
                }

                private float countStd(int k, int[] counts, int batchSize) {
                    float avg = batchSize * 1.0f / k;
                    float var = 0f;
                    int nonZero = 0;
                    for (int c = 0; c < k; c++) {
                        var += (counts[c] - avg) * (counts[c] - avg);
                        if (counts[c] > 0) {
                            nonZero++;
                        }
                    }
                    if (nonZero <= 1) {
                        return Float.MAX_VALUE;
                    }
                    return (float) (Math.sqrt(var / k) / avg);
                }

                private float tryClusteringCountStd(IHyracksTaskContext ctx, RunFileSource source, int recordCount,
                        int k, float lambdaFactor, int dim, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu, Random rand)
                        throws HyracksDataException, IOException {
                    int batchSize = Math.min(recordCount, BKT_SAMPLES);
                    int[] indexOrder = new int[recordCount];
                    for (int i = 0; i < recordCount; i++) {
                        indexOrder[i] = i;
                    }
                    double[][] centers = new double[k][dim];
                    double[][] newCenters = new double[k][dim];
                    float adjustedLambda = initCenters(ctx, source, recordCount, k, batchSize, indexOrder, centers,
                            newCenters, dim, fta, tuple, eval, inputVal, listAcc, kmu, rand);
                    float originalLambda = 1.0f / lambdaFactor / batchSize;
                    int[] priorCounts = new int[k];
                    int[] counts = new int[k];
                    int[] clusterIdx = new int[k];
                    double[] clusterDist = new double[k];
                    double[][] centerSums = new double[k][];
                    double minClusterDist = Double.POSITIVE_INFINITY;
                    int noImprovement = 0;
                    for (int iter = 0; iter < BKT_MAX_ITERS; iter++) {
                        for (int i = batchSize - 1; i > 0; i--) {
                            int j = rand.nextInt(i + 1);
                            int tmp = indexOrder[i];
                            indexOrder[i] = indexOrder[j];
                            indexOrder[j] = tmp;
                        }
                        float lambda = Math.min(adjustedLambda, originalLambda);
                        double totalDist = kmeansAssign(ctx, source, recordCount, indexOrder, batchSize, centers, k,
                                priorCounts, lambda, true, counts, centerSums, clusterIdx, clusterDist, null, fta,
                                tuple, eval, inputVal, listAcc, kmu);
                        refineCentersFromSums(centers, newCenters, k, counts, centerSums, clusterIdx, dim);
                        for (int c = 0; c < k; c++) {
                            System.arraycopy(newCenters[c], 0, centers[c], 0, dim);
                        }
                        System.arraycopy(counts, 0, priorCounts, 0, k);
                        if (totalDist < minClusterDist) {
                            minClusterDist = totalDist;
                            noImprovement = 0;
                        } else {
                            noImprovement++;
                        }
                        double diff = 0.0;
                        for (int c = 0; c < k; c++) {
                            diff += calculateDistance(centers[c], newCenters[c]);
                        }
                        if (diff < BKT_CONV_EPS || noImprovement >= BKT_NO_IMPROVE) {
                            break;
                        }
                    }
                    Arrays.fill(priorCounts, 0);
                    kmeansAssign(ctx, source, recordCount, indexOrder, batchSize, centers, k, priorCounts, 0.0, false,
                            counts, null, clusterIdx, clusterDist, null, fta, tuple, eval, inputVal, listAcc, kmu);
                    return countStd(k, counts, batchSize);
                }

                private float dynamicFactorSelect(IHyracksTaskContext ctx, RunFileSource source, int recordCount, int k,
                        int dim, FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval,
                        IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu, Random rand)
                        throws HyracksDataException, IOException {
                    float bestFactor = 100.0f;
                    float bestStd = Float.MAX_VALUE;
                    for (float factor = 0.001f; factor <= 1000.0f + 1e-3f; factor *= 10.0f) {
                        float std = tryClusteringCountStd(ctx, source, recordCount, k, factor, dim, fta, tuple, eval,
                                inputVal, listAcc, kmu, rand);
                        if (std < bestStd) {
                            bestStd = std;
                            bestFactor = factor;
                        }
                    }
                    LOGGER.info("[TopDown] DynamicFactorSelect: bestLambdaFactor={} countStd={}", bestFactor, bestStd);
                    return bestFactor;
                }

                /**
                 * BKT-style split: sample-optimized lambda-balanced k-means, full assign, real-record pivots.
                 */
                private ClusteringResult clusterRunFile(IHyracksTaskContext ctx, RunFileSource source, int recordCount,
                        int k, float tunedLambdaFactor, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAcc, KMeansUtils kmu, Random rand)
                        throws HyracksDataException, IOException {
                    if (k <= 0 || recordCount <= 0) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }
                    int effectiveK = Math.min(k, recordCount);
                    if (effectiveK == 1) {
                        return emitAllRecordsAsCentroids(ctx, source, recordCount, fta, tuple, eval, inputVal, listAcc,
                                kmu);
                    }

                    GeneratedRunFileReader dimProbe = source.openReader();
                    double[] probe;
                    try {
                        probe = getPointAtIndex(dimProbe, fta, tuple, eval, inputVal, listAcc, kmu, 0, ctx);
                    } finally {
                        dimProbe.close();
                    }
                    if (probe == null) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }
                    int dim = probe.length;

                    int batchSize = Math.min(recordCount, BKT_SAMPLES);
                    int[] indexOrder = new int[recordCount];
                    for (int i = 0; i < recordCount; i++) {
                        indexOrder[i] = i;
                    }
                    double[][] centers = new double[effectiveK][dim];
                    double[][] newCenters = new double[effectiveK][dim];
                    float adjustedLambda = initCenters(ctx, source, recordCount, effectiveK, batchSize, indexOrder,
                            centers, newCenters, dim, fta, tuple, eval, inputVal, listAcc, kmu, rand);
                    float originalLambda = 1.0f / tunedLambdaFactor / batchSize;

                    int[] priorCounts = new int[effectiveK];
                    int[] counts = new int[effectiveK];
                    int[] clusterIdx = new int[effectiveK];
                    double[] clusterDist = new double[effectiveK];
                    double[][] centerSums = new double[effectiveK][];
                    double minClusterDist = Double.POSITIVE_INFINITY;
                    int noImprovement = 0;
                    for (int iter = 0; iter < BKT_MAX_ITERS; iter++) {
                        for (int i = batchSize - 1; i > 0; i--) {
                            int j = rand.nextInt(i + 1);
                            int tmp = indexOrder[i];
                            indexOrder[i] = indexOrder[j];
                            indexOrder[j] = tmp;
                        }
                        float lambda = Math.min(adjustedLambda, originalLambda);
                        double totalDist = kmeansAssign(ctx, source, recordCount, indexOrder, batchSize, centers,
                                effectiveK, priorCounts, lambda, true, counts, centerSums, clusterIdx, clusterDist,
                                null, fta, tuple, eval, inputVal, listAcc, kmu);
                        refineCentersFromSums(centers, newCenters, effectiveK, counts, centerSums, clusterIdx, dim);
                        for (int c = 0; c < effectiveK; c++) {
                            System.arraycopy(newCenters[c], 0, centers[c], 0, dim);
                        }
                        System.arraycopy(counts, 0, priorCounts, 0, effectiveK);
                        if (totalDist < minClusterDist) {
                            minClusterDist = totalDist;
                            noImprovement = 0;
                        } else {
                            noImprovement++;
                        }
                        double diff = 0.0;
                        for (int c = 0; c < effectiveK; c++) {
                            diff += calculateDistance(centers[c], newCenters[c]);
                        }
                        if (diff < BKT_CONV_EPS || noImprovement >= BKT_NO_IMPROVE) {
                            break;
                        }
                    }

                    int[] assignments = new int[recordCount];
                    int[] fullOrder = new int[recordCount];
                    for (int i = 0; i < recordCount; i++) {
                        fullOrder[i] = i;
                    }
                    Arrays.fill(priorCounts, 0);
                    kmeansAssign(ctx, source, recordCount, fullOrder, recordCount, centers, effectiveK, priorCounts,
                            0.0, false, counts, null, clusterIdx, clusterDist, assignments, fta, tuple, eval, inputVal,
                            listAcc, kmu);
                    logTopDownCountDistribution("BKT full assign: recordCount=" + recordCount + " k=" + effectiveK,
                            counts);

                    List<double[]> pivots = new ArrayList<>(effectiveK);
                    for (int c = 0; c < effectiveK; c++) {
                        if (clusterIdx[c] >= 0) {
                            GeneratedRunFileReader pr = source.openReader();
                            double[] pivot;
                            try {
                                pivot = getPointAtIndex(pr, fta, tuple, eval, inputVal, listAcc, kmu, clusterIdx[c],
                                        ctx);
                            } finally {
                                pr.close();
                            }
                            if (pivot != null) {
                                pivots.add(copyVector(pivot));
                                continue;
                            }
                        }
                        pivots.add(copyVector(centers[c]));
                    }
                    return new ClusteringResult(pivots, assignments);
                }

                /**
                 * Partitions a run file into one child run file per centroid by assigning each record to its
                 * nearest centroid. Child files use the same {@code secondaryRecDesc} layout as the input, so they
                 * can be re-read by the same clustering code. Returns the (closed) child writers and per-cluster
                 * record counts; the caller owns and must delete them. Some buckets may have count 0 — callers
                 * feeding the static-structure pipeline must not emit routing centroids for those buckets.
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
                 * BKT-style top-down build: lambda-balanced splits, real pivots, leaf stop when a bag fits on one
                 * quantized leaf page. BuildHead ({@code headOnlyBuild}) uses fixed fan-out
                 * {@link #BKT_KMEANS_K}; full-sample TopDown uses {@link #dynamicK}. BuildHead stops when no pending
                 * bucket exceeds {@code leafPageCapacity} (plus {@code maxLevel} safety cap). After each split, only
                 * non-empty assignment buckets are registered as routing centroids so counts match
                 * {@code VTreeStaticStructureBuilder}.
                 *
                 * @param headOnlyBuild when true (SPANN BuildHead), lambda is tuned on |H| and {@code num_clusters}
                 *            early-stop is disabled
                 */
                private HierarchicalClusterStructure buildTopDownHierarchicalKMeans(IHyracksTaskContext ctx,
                        RunFileSource sampleSource, int totalTupleCount, boolean headOnlyBuild, FrameTupleAccessor fta,
                        FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal, ListAccessor listAcc,
                        KMeansUtils kmu, int partition) throws HyracksDataException, IOException {
                    HierarchicalClusterStructure structure = new HierarchicalClusterStructure();
                    if (totalTupleCount <= 0) {
                        LOGGER.info("[TopDown] partition={}: empty input, skipping build", partition);
                        return structure;
                    }
                    Random rand = new Random();
                    int target = headOnlyBuild ? Integer.MAX_VALUE : K;
                    String logTag = headOnlyBuild ? "BuildHead" : "TopDown";

                    int dim = vectorDimension;
                    if (dim <= 0) {
                        GeneratedRunFileReader probe = sampleSource.openReader();
                        double[] firstPoint;
                        try {
                            firstPoint = getPointAtIndex(probe, fta, tuple, eval, inputVal, listAcc, kmu, 0, ctx);
                        } finally {
                            probe.close();
                        }
                        if (firstPoint == null) {
                            LOGGER.warn("[{}] partition={}: cannot read vector at index 0 from input run file", logTag,
                                    partition);
                            return structure;
                        }
                        dim = firstPoint.length;
                    }

                    int leafPageCapacity = computeLeafPageCapacity(ctx, dim);

                    float tunedLambdaFactor;
                    if (lambdaFactor > 0) {
                        tunedLambdaFactor = (float) lambdaFactor;
                    } else {
                        int probeK = splitK(totalTupleCount, leafPageCapacity, headOnlyBuild);
                        tunedLambdaFactor = dynamicFactorSelect(ctx, sampleSource, totalTupleCount, probeK, dim, fta,
                                tuple, eval, inputVal, listAcc, kmu, rand);
                    }

                    if (headOnlyBuild) {
                        LOGGER.info(
                                "[BuildHead] partition={} start: heads={} dim={} leafPageCap={} lambdaFactor={} (tuned on heads) maxLevel={}",
                                partition, totalTupleCount, dim, leafPageCapacity, tunedLambdaFactor, maxLevel);
                    } else {
                        LOGGER.info(
                                "[TopDown] partition={} start: tuples={} target={} dim={} leafPageCap={} lambdaFactor={} maxLevel={}",
                                partition, totalTupleCount, target, dim, leafPageCapacity, tunedLambdaFactor, maxLevel);
                    }

                    String stopReason = null;
                    int cumulative = 0;

                    // ---- Root level (level 0) ----
                    ClusteringResult rootResult;
                    if (totalTupleCount <= leafPageCapacity) {
                        rootResult = emitAllRecordsAsCentroids(ctx, sampleSource, totalTupleCount, fta, tuple, eval,
                                inputVal, listAcc, kmu);
                        stopReason = "root leaf bag";
                    } else {
                        int kRoot = splitK(totalTupleCount, leafPageCapacity, headOnlyBuild);
                        LOGGER.info("[{}] partition={} root: recCount={} -> splitK={}{}", logTag, partition,
                                totalTupleCount, kRoot, headOnlyBuild ? " (fixed)" : " (dynamicK)");
                        rootResult = clusterRunFile(ctx, sampleSource, totalTupleCount, kRoot, tunedLambdaFactor, fta,
                                tuple, eval, inputVal, listAcc, kmu, rand);
                    }
                    if (rootResult.centroids.isEmpty()) {
                        LOGGER.warn(
                                "[{}] partition={}: root clustering produced 0 centroids (inputTuples={} headOnly={})",
                                logTag, partition, totalTupleCount, headOnlyBuild);
                        return structure;
                    }

                    List<HierarchicalClusterStructure.CentroidInfo> level0 = new ArrayList<>();
                    List<ParentBatch> queue;

                    if (totalTupleCount <= leafPageCapacity) {
                        for (int i = 0; i < rootResult.centroids.size(); i++) {
                            level0.add(new HierarchicalClusterStructure.CentroidInfo(i, -1, rootResult.centroids.get(i),
                                    0));
                        }
                        structure.levelCentroids.put(0, level0);
                        cumulative = level0.size();
                        LOGGER.info("[{}] partition={} level 0 (root): centroids={}", logTag, partition, cumulative);

                        boolean stopAtRoot = maxLevel <= 0 || headOnlyBuild;
                        if (!headOnlyBuild) {
                            stopAtRoot = stopAtRoot || cumulative >= target;
                        }
                        if (stopAtRoot) {
                            if (headOnlyBuild && stopReason == null) {
                                stopReason = "all heads fit one leaf page at root";
                            }
                            LOGGER.info("[{}] partition={} stop at root: centroids={} target={}{}", logTag, partition,
                                    cumulative, headOnlyBuild ? "n/a" : target,
                                    stopReason != null ? " reason=" + stopReason : "");
                            return structure;
                        }

                        FrameTupleReference prototypeTuple = readPrototypeTuple(ctx, sampleSource, fta, tuple);
                        queue = new ArrayList<>(rootResult.centroids.size());
                        int promoted = 0;
                        for (int i = 0; i < rootResult.centroids.size(); i++) {
                            RunFileWriter promo = materializeSingleVectorRunFile(ctx, prototypeTuple,
                                    rootResult.centroids.get(i), secondaryRecDesc);
                            queue.add(new ParentBatch(promo, 1, i));
                            promoted++;
                        }
                        LOGGER.info(
                                "[{}/Promote] partition={} level=0 root leaf-bag: centroids={} promoted={} (maxLevel={})",
                                logTag, partition, rootResult.centroids.size(), promoted, maxLevel);
                    } else {
                        SplitResult rootSplit = splitRunFileByAssignment(ctx, sampleSource, rootResult.centroids, fta,
                                tuple, eval, inputVal, listAcc, kmu);
                        queue = new ArrayList<>();
                        registerNonEmptySplitBuckets(logTag, partition, 0, -1, level0, queue, rootResult.centroids,
                                rootSplit, 0);
                        structure.levelCentroids.put(0, level0);
                        cumulative = level0.size();
                        LOGGER.info("[{}] partition={} level 0 (root): centroids={}", logTag, partition, cumulative);
                        logTopDownCountDistribution("partition=" + partition + " root split", rootSplit.counts);

                        boolean stopAtRoot = maxLevel <= 0;
                        if (!headOnlyBuild) {
                            stopAtRoot = stopAtRoot || cumulative >= target;
                        }
                        if (stopAtRoot) {
                            LOGGER.info("[{}] partition={} stop at root: centroids={} target={}{}", logTag, partition,
                                    cumulative, headOnlyBuild ? "n/a" : target,
                                    stopReason != null ? " reason=" + stopReason : "");
                            for (ParentBatch batch : queue) {
                                deleteRunFile(batch.file);
                            }
                            return structure;
                        }
                    }

                    try {
                        int level = 1;
                        while (level <= maxLevel && !queue.isEmpty()) {
                            List<HierarchicalClusterStructure.CentroidInfo> levelInfo = new ArrayList<>();
                            List<ParentBatch> nextQueue = new ArrayList<>();
                            int localId = 0;

                            for (ParentBatch batch : queue) {
                                if (batch.file == null || batch.recCount <= 0) {
                                    continue;
                                }
                                RunFileSource src = () -> {
                                    GeneratedRunFileReader rd = batch.file.createReader();
                                    rd.open();
                                    return rd;
                                };

                                if (batch.recCount <= leafPageCapacity) {
                                    ClusteringResult leafRes = emitAllRecordsAsCentroids(ctx, src, batch.recCount, fta,
                                            tuple, eval, inputVal, listAcc, kmu);
                                    FrameTupleReference prototypeTuple = readPrototypeTuple(ctx, src, fta, tuple);
                                    int promotedCount = 0;
                                    for (double[] c : leafRes.centroids) {
                                        int idAtLevel = localId++;
                                        levelInfo.add(new HierarchicalClusterStructure.CentroidInfo(idAtLevel,
                                                batch.parentClusterId, c, level));
                                        if (level < maxLevel) {
                                            RunFileWriter promo = materializeSingleVectorRunFile(ctx, prototypeTuple, c,
                                                    secondaryRecDesc);
                                            nextQueue.add(new ParentBatch(promo, 1, idAtLevel));
                                            promotedCount++;
                                        }
                                    }
                                    if (promotedCount > 0) {
                                        LOGGER.info(
                                                "[{}/Promote] partition={} level={} parent={} leaf-bag centroids={} promoted={}",
                                                logTag, partition, level, batch.parentClusterId,
                                                leafRes.centroids.size(), promotedCount);
                                    }
                                    batch.file.getFileReference().delete();
                                    continue;
                                }

                                int kSplit = splitK(batch.recCount, leafPageCapacity, headOnlyBuild);
                                LOGGER.info("[{}] partition={} level {} parent={}: recCount={} -> splitK={}{}", logTag,
                                        partition, level, batch.parentClusterId, batch.recCount, kSplit,
                                        headOnlyBuild ? " (fixed)" : " (dynamicK)");
                                ClusteringResult res = clusterRunFile(ctx, src, batch.recCount, kSplit,
                                        tunedLambdaFactor, fta, tuple, eval, inputVal, listAcc, kmu, rand);
                                SplitResult sp = splitRunFileByAssignment(ctx, src, res.centroids, fta, tuple, eval,
                                        inputVal, listAcc, kmu);
                                int nonEmpty = 0;
                                for (int c : sp.counts) {
                                    if (c > 0) {
                                        nonEmpty++;
                                    }
                                }
                                if (headOnlyBuild && nonEmpty <= 1) {
                                    LOGGER.info(
                                            "[{}] partition={} level {} parent={}: degenerate split (nonEmpty={}), "
                                                    + "emitting leaf-list",
                                            logTag, partition, level, batch.parentClusterId, nonEmpty);
                                    deleteRunFiles(sp.files);
                                    RunFileSource leafSrc = () -> {
                                        GeneratedRunFileReader rd = batch.file.createReader();
                                        rd.open();
                                        return rd;
                                    };
                                    ClusteringResult leafRes = emitAllRecordsAsCentroids(ctx, leafSrc, batch.recCount,
                                            fta, tuple, eval, inputVal, listAcc, kmu);
                                    FrameTupleReference prototypeTuple = readPrototypeTuple(ctx, leafSrc, fta, tuple);
                                    for (double[] c : leafRes.centroids) {
                                        int idAtLevel = localId++;
                                        levelInfo.add(new HierarchicalClusterStructure.CentroidInfo(idAtLevel,
                                                batch.parentClusterId, c, level));
                                        if (level < maxLevel) {
                                            RunFileWriter promo = materializeSingleVectorRunFile(ctx, prototypeTuple, c,
                                                    secondaryRecDesc);
                                            nextQueue.add(new ParentBatch(promo, 1, idAtLevel));
                                        }
                                    }
                                } else {
                                    localId = registerNonEmptySplitBuckets(logTag, partition, level,
                                            batch.parentClusterId, levelInfo, nextQueue, res.centroids, sp, localId);
                                }
                                batch.file.getFileReference().delete();
                            }

                            if (levelInfo.isEmpty()) {
                                break;
                            }
                            structure.levelCentroids.put(level, levelInfo);
                            cumulative = levelInfo.size();
                            LOGGER.info("[{}] partition={} built level {}: centroids={} cumulative={} target={}",
                                    logTag, partition, level, levelInfo.size(), cumulative,
                                    headOnlyBuild ? "n/a" : target);

                            boolean stopLevel = level >= maxLevel;
                            if (!headOnlyBuild && cumulative >= target) {
                                stopLevel = true;
                                stopReason = "target reached at level " + level;
                            } else if (stopLevel) {
                                stopReason = "height cap at level " + level;
                            }
                            if (headOnlyBuild && !anyBucketNeedsSplit(nextQueue, leafPageCapacity)) {
                                stopLevel = true;
                                if (stopReason == null) {
                                    stopReason = "all buckets fit one leaf page at level " + level;
                                }
                            }
                            if (stopLevel) {
                                LOGGER.info("[{}] partition={} stop: {}", logTag, partition, stopReason);
                                for (ParentBatch nb : nextQueue) {
                                    if (nb.file != null) {
                                        nb.file.getFileReference().delete();
                                    }
                                }
                                break;
                            }

                            queue = nextQueue;
                            level++;
                        }
                    } finally {
                        for (ParentBatch batch : queue) {
                            if (batch.file != null) {
                                try {
                                    batch.file.getFileReference().delete();
                                } catch (Exception e) {
                                    // best-effort
                                }
                            }
                        }
                    }

                    int maxStoredLevel = -1;
                    for (Integer lvl : structure.levelCentroids.keySet()) {
                        maxStoredLevel = Math.max(maxStoredLevel, lvl);
                    }
                    LOGGER.info("[{}] partition={} complete: levels={} leafLevel={} lastLevelCentroids={} target={}{}",
                            logTag, partition, structure.getNumLevels(), maxStoredLevel, cumulative,
                            headOnlyBuild ? "n/a" : target, stopReason != null ? " stopReason=" + stopReason : "");

                    return structure;
                }

                /**
                 * Perform memory-efficient hierarchical K-means clustering using run files.
                 */
                private HierarchicalClusterStructure performMemoryEfficientHierarchicalKMeans(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, int partition, int totalTupleCount)
                        throws HyracksDataException, IOException {

                    HierarchicalClusterStructure structure = new HierarchicalClusterStructure();

                    // Perform initial K-means++ on all data to generate initial centroids
                    Random rand = new Random();
                    int maxKMeansIterations = 20;
                    ClusteringResult initialResult =
                            performInitialKMeansPlusPlus(ctx, in, fta, tuple, eval, inputVal, listAccessorConstant,
                                    kMeansUtils, K, rand, maxKMeansIterations, totalTupleCount, partition);

                    if (initialResult.centroids.isEmpty()) {
                        return structure;
                    }

                    // Extract embedding dimension and frame size for frame fit calculations
                    int embeddingDimension = initialResult.centroids.get(0).length;
                    if (embeddingDimension <= 0) {
                        return structure;
                    }
                    int frameSize = ctx.getInitialFrameSize();

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
                    int currentK =
                            Math.min(K, Math.max(1, (int) Math.floor(Math.sqrt(initialResult.centroids.size()))));
                    int maxIterations = 20;
                    int maxLevels = 100;
                    int currentLevel = 0;

                    // Build subsequent levels
                    while (currentCentroids.size() > 1 && currentK > 1 && currentLevel < maxLevels) {
                        // Initialize parent level with empty centroids
                        structure.initializeParentLevel(currentLevel, currentK);

                        // Perform K-means++ clustering on centroids from previous level
                        ClusteringResult levelResult = performScalableKMeansPlusPlusOnCentroids(currentCentroids,
                                currentK, rand, maxIterations);

                        if (levelResult.centroids.isEmpty()) {
                            break;
                        }

                        // Check if current level fits in one frame
                        if (HierarchicalClusterStructure.doesLevelFitInFrame(levelResult.centroids.size(),
                                embeddingDimension, frameSize)) {
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
                        currentK = Math.max(1, (int) Math.floor(Math.sqrt(currentK)));
                        currentLevel++;
                    }

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
                            }
                        }
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
                                    converged = false; // Force continuation since we changed a centroid
                                }
                            }
                        }

                        if (converged) {
                            break;
                        }
                    }

                    return new ClusteringResult(resultCentroids, assignments);
                }

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

            };
        }
    }
}
