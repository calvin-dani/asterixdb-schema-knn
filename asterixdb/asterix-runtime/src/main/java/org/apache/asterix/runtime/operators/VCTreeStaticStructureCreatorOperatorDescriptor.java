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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.storage.OptimizedScalarQuantizationSampleFile;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
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
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.LSMVCTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTreeDiskComponent;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.utils.VectorUtils;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Operator that creates VCTree static structure files using VCTreeStaticStructureBuilder.
 * Two-activity structure: first creates static file, second reads materialized data and passes through.
 */
public class VCTreeStaticStructureCreatorOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final int maxEntriesPerPage;
    private final float fillFactor;
    private final UUID permitUUID;
    // private final UUID materializedDataUUID;
    private final UUID scalarValuesUUID;
    private final RecordDescriptor inputRecordDescriptor; // Store for CreateStructureActivity
    private final int[][] partitionsMap; // Maps task partition to storage partition(s)

    public VCTreeStaticStructureCreatorOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID, UUID materializedDataUUID, UUID scalarValuesUUID,
            RecordDescriptor scalarRecDesc, int[][] partitionsMap) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID;
        // this.materializedDataUUID = materializedDataUUID;
        this.scalarValuesUUID = scalarValuesUUID;
        this.inputRecordDescriptor = inputRecordDescriptor; // Store for CreateStructureActivity
        this.partitionsMap = partitionsMap; // Store partition mapping
        // PassThroughActivity outputs scalar values, so use scalarRecDesc
        // CreateStructureActivity uses inputRecordDescriptor (stored separately)
        this.outRecDescs[0] = scalarRecDesc != null ? scalarRecDesc : inputRecordDescriptor;
        System.err.println("VCTreeStaticStructureCreatorOperatorDescriptor created with permit UUID: " + permitUUID);
        System.err.println(
                "VCTreeStaticStructureCreatorOperatorDescriptor created with scalarValuesUUID: " + scalarValuesUUID);
        System.err.println("Using scalar values output record descriptor: " + outRecDescs[0]);
        System.err.println("Input record descriptor (for CreateStructureActivity): " + inputRecordDescriptor);
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

    /**
     * Create grouping strategy for VCTree centroids based on SHAPIRO formula results.
     * Uses range partitioning instead of round-robin.
     * 
     * @param K Total number of centroids
     * @param numPartitions Number of partitions from SHAPIRO
     * @param memoryBudget Available memory budget in frames
     * @return GroupingStrategy object with run file allocation plan
     */
    public GroupingStrategy createGroupingStrategy(int K, int numPartitions, int memoryBudget) {
        System.err.println("=== CREATING GROUPING STRATEGY (RANGE PARTITIONING) ===");
        System.err.println("K (centroids): " + K);
        System.err.println("Number of partitions: " + numPartitions);
        System.err.println("Memory budget: " + memoryBudget + " frames");

        int centroidsPerPartition = (int) Math.ceil((double) K / numPartitions);
        int numRunFiles = Math.min(numPartitions, memoryBudget);

        System.err.println("Centroids per partition: " + centroidsPerPartition);
        System.err.println("Number of run files: " + numRunFiles);

        // Create range partition assignment for centroids
        Map<Integer, List<Integer>> partitionToCentroids = new HashMap<>();
        for (int i = 0; i < K; i++) {
            int partition = i / centroidsPerPartition;
            partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(i);
        }

        // Determine strategy based on centroids per partition
        String strategy;
        if (centroidsPerPartition > 1) {
            strategy = "GROUP_MULTIPLE_CENTROIDS";
            System.err.println("Strategy: Group multiple centroids in one run file (range partitioning)");
        } else {
            strategy = "ONE_FRAME_PER_CENTROID";
            System.err.println("Strategy: Allocate 1 frame per centroid (range partitioning)");
        }

        return new GroupingStrategy(numRunFiles, centroidsPerPartition, partitionToCentroids, strategy);
    }

    /**
     * Create range partitioning for a list of centroids.
     * 
     * @param centroids List of centroid IDs to partition
     * @param numPartitions Number of partitions to create
     * @return Map of partition ID to list of centroid IDs
     */
    public Map<Integer, List<Integer>> createRangePartitioning(List<Integer> centroids, int numPartitions) {
        System.err.println("=== CREATING RANGE PARTITIONING ===");
        System.err.println("Centroids to partition: " + centroids.size());
        System.err.println("Number of partitions: " + numPartitions);

        int centroidsPerPartition = (int) Math.ceil((double) centroids.size() / numPartitions);
        Map<Integer, List<Integer>> partitionToCentroids = new HashMap<>();

        for (int i = 0; i < centroids.size(); i++) {
            int partition = i / centroidsPerPartition;
            int originalCentroidId = centroids.get(i); // Keep original ID
            partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(originalCentroidId);
        }

        System.err.println("Range partitioning complete:");
        for (Map.Entry<Integer, List<Integer>> entry : partitionToCentroids.entrySet()) {
            System.err.println("  Partition " + entry.getKey() + ": " + entry.getValue().size() + " centroids");
        }

        return partitionToCentroids;
    }

    /**
     * Apply recursive partitioning logic with hardcoded memory budget and create run files.
     * 
     * @param K Total number of centroids
     * @param inputDataBytesSize Size of input data in bytes
     * @param frameSize Frame size in bytes
     * @param ctx Hyracks task context for file operations
     * @return RunFileManager with created run files and dummy data
     */
    public RunFileManager applyRecursivePartitioning(int K, long inputDataBytesSize, int frameSize,
            IHyracksTaskContext ctx) {
        System.err.println("=== APPLYING RECURSIVE PARTITIONING LOGIC ===");

        // Hardcoded memory budget (following other operators pattern)
        int memoryBudget = 32; // frames - typical value from other operators
        System.err.println("Using hardcoded memory budget: " + memoryBudget + " frames");

        // Step 1: Calculate initial partitions using SHAPIRO
        int numPartitions = calculatePartitionsUsingShapiro(K, inputDataBytesSize, frameSize, memoryBudget);
        GroupingStrategy strategy = createGroupingStrategy(K, numPartitions, memoryBudget);

        System.err.println("Initial strategy: " + strategy);

        // Step 2: Create run files
        RunFileManager runFileManager = new RunFileManager(ctx);
        runFileManager.createRunFiles(strategy.numRunFiles);

        // Step 3: Generate dummy data and distribute to run files
        DummyData dummyData = getDummyValues();
        try {
            runFileManager.distributeDummyData(dummyData, strategy);
        } catch (HyracksDataException e) {
            System.err.println("ERROR: Failed to distribute dummy data: " + e.getMessage());
            e.printStackTrace();
        }

        // Step 4: Apply recursive logic if needed
        int level = 0;
        // Calculate max recursion depth based on memory budget approach
        int maxLevels = (int) Math.ceil(Math.log(memoryBudget) / Math.log(2)); // log base 2
        maxLevels = Math.max(2, Math.min(maxLevels, 10)); // reasonable bounds

        while (level < maxLevels && strategy.strategy.equals("GROUP_MULTIPLE_CENTROIDS")) {
            System.err.println("=== RECURSIVE LEVEL " + (level + 1) + " ===");

            // Check if further partitioning is needed
            boolean needsMorePartitioning =
                    runFileManager.checkSizeReduction(strategy.centroidsPerPartition, memoryBudget);

            if (!needsMorePartitioning) {
                System.err.println("Size reduction effective, stopping recursion at level " + level);
                break;
            }

            // Apply SHAPIRO again on each large partition using range partitioning
            for (int partitionId = 0; partitionId < strategy.numRunFiles; partitionId++) {
                List<Integer> partitionCentroids = strategy.partitionToCentroids.get(partitionId);

                // Check if this partition needs sub-partitioning using frame-based calculation
                int bytesPerCentroid = 4 + (256 * 8); // centroidId + embedding = 2052 bytes
                long partitionDataSize = partitionCentroids.size() * bytesPerCentroid;
                long partitionFramesNeeded = (partitionDataSize + frameSize - 1) / frameSize;
                double threshold = 0.6;

                if (partitionFramesNeeded > (memoryBudget * threshold)) {
                    System.err.println("Partition " + partitionId + " has " + partitionCentroids.size() + " centroids ("
                            + partitionFramesNeeded + " frames), sub-partitioning...");

                    // Calculate sub-partitions for this partition
                    int subPartitions = calculatePartitionsUsingShapiro(partitionCentroids.size(), partitionDataSize,
                            frameSize, memoryBudget);

                    // Apply range partitioning to sub-partition the centroids
                    Map<Integer, List<Integer>> subPartitionToCentroids =
                            createRangePartitioning(partitionCentroids, subPartitions);

                    // Create sub-run files for this partition
                    runFileManager.createSubRunFiles(partitionId, subPartitions);

                    // Update the strategy with sub-partitioned centroids
                    strategy.partitionToCentroids.put(partitionId, subPartitionToCentroids.get(0));
                    for (int subPart = 1; subPart < subPartitions; subPart++) {
                        strategy.partitionToCentroids.put(strategy.numRunFiles + subPart - 1,
                                subPartitionToCentroids.get(subPart));
                    }
                }
            }

            level++;
        }

        System.err.println("=== RECURSIVE PARTITIONING COMPLETE ===");
        System.err.println("Final level: " + level);
        System.err.println("Total run files created: " + runFileManager.getTotalRunFiles());

        return runFileManager;
    }

    /**
     * Inner class to manage run files and dummy data distribution using actual RunFileWriter.
     */
    public static class RunFileManager {
        private final Map<Integer, RunFileWriter> runFileWriters = new HashMap<>();
        private final Map<Integer, List<Integer>> subRunFiles = new HashMap<>();
        private final IHyracksTaskContext ctx;
        private int totalRunFiles = 0;

        public RunFileManager(IHyracksTaskContext ctx) {
            this.ctx = ctx;
        }

        public void createRunFiles(int numRunFiles) {
            try {
                System.err.println("Creating " + numRunFiles + " run files");
                for (int i = 0; i < numRunFiles; i++) {
                    // Create actual RunFileWriter like MaterializerTaskState
                    FileReference file = ctx.getJobletContext()
                            .createManagedWorkspaceFile("VCTreeRunFile_" + i + "_" + System.currentTimeMillis());
                    RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
                    writer.open();

                    runFileWriters.put(i, writer);
                }
                totalRunFiles = numRunFiles;
            } catch (HyracksDataException e) {
                System.err.println("ERROR: Failed to create run files: " + e.getMessage());
                e.printStackTrace();
            }
        }

        public void createSubRunFiles(int parentPartition, int numSubPartitions) {
            try {
                System.err.println("Creating " + numSubPartitions + " sub-run files for partition " + parentPartition);
                List<Integer> subFiles = new ArrayList<>();
                for (int i = 0; i < numSubPartitions; i++) {
                    int subFileId = totalRunFiles++;

                    // Create actual RunFileWriter for sub-file
                    FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                            "VCTreeSubRunFile_" + parentPartition + "_" + i + "_" + System.currentTimeMillis());
                    RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
                    writer.open();

                    runFileWriters.put(subFileId, writer);
                    subFiles.add(subFileId);
                }
                subRunFiles.put(parentPartition, subFiles);
            } catch (HyracksDataException e) {
                System.err.println("ERROR: Failed to create sub-run files: " + e.getMessage());
                e.printStackTrace();
            }
        }

        public void distributeDummyData(DummyData dummyData, GroupingStrategy strategy) throws HyracksDataException {
            System.err.println("Distributing " + dummyData.tuples.size() + " dummy tuples to run files");

            // Write tuples directly to run files
            for (DummyTuple tuple : dummyData.tuples) {
                int partition = tuple.centroidId % strategy.numRunFiles;
                RunFileWriter writer = runFileWriters.get(partition);

                // Convert dummy tuple to frame and write to run file
                MockTupleReference mockTuple = new MockTupleReference(tuple);
                ByteBuffer frameBuffer = createFrameFromTuple(mockTuple);
                writer.nextFrame(frameBuffer);
            }

            System.err.println("All dummy tuples written to run files");
        }

        private ByteBuffer createFrameFromTuple(MockTupleReference tuple) {
            // Create a simple frame containing the tuple data
            // This is a simplified version - in practice you'd use proper frame formatting
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            // Write tuple count (1)
            buffer.putInt(1);

            // Write tuple data
            byte[] tupleData = tuple.getFieldData(0);
            buffer.putInt(tupleData.length);
            buffer.put(tupleData);

            buffer.flip();
            return buffer;
        }

        public boolean checkSizeReduction(int centroidsPerPartition, int memoryBudget) {
            // Calculate actual memory usage for this partition
            int bytesPerCentroid = 4 + (256 * 8); // centroidId + embedding = 2052 bytes
            int frameSize = 32768; // 32KB configurable frame size
            long partitionDataSize = centroidsPerPartition * bytesPerCentroid;
            long partitionFramesNeeded = (partitionDataSize + frameSize - 1) / frameSize;

            // 60% threshold for aggressive memory management
            double threshold = 0.6;
            double currentRatio = (double) partitionFramesNeeded / memoryBudget;

            System.err.println("Size reduction check: framesNeeded=" + partitionFramesNeeded + ", memoryBudget="
                    + memoryBudget + ", ratio=" + currentRatio + ", threshold=" + threshold);

            // If ratio is still high, needs more partitioning
            return currentRatio > threshold;
        }

        public void closeAllRunFiles() throws HyracksDataException {
            System.err.println("Closing all " + runFileWriters.size() + " run files");
            for (RunFileWriter writer : runFileWriters.values()) {
                if (writer != null) {
                    writer.close();
                }
            }
        }

        public RunFileReader getRunFileReader(int runFileId) throws HyracksDataException {
            RunFileWriter writer = runFileWriters.get(runFileId);
            if (writer != null) {
                return writer.createReader();
            }
            return null;
        }

        public int getTotalRunFiles() {
            return totalRunFiles;
        }

        @Override
        public String toString() {
            return String.format("RunFileManager{totalFiles=%d, subFiles=%s}", totalRunFiles, subRunFiles);
        }
    }

    /**
     * Inner class representing grouping strategy for VCTree centroids.
     */
    public static class GroupingStrategy {
        public final int numRunFiles;
        public final int centroidsPerPartition;
        public final Map<Integer, List<Integer>> partitionToCentroids;
        public final String strategy;

        public GroupingStrategy(int numRunFiles, int centroidsPerPartition,
                Map<Integer, List<Integer>> partitionToCentroids, String strategy) {
            this.numRunFiles = numRunFiles;
            this.centroidsPerPartition = centroidsPerPartition;
            this.partitionToCentroids = partitionToCentroids;
            this.strategy = strategy;
        }

        @Override
        public String toString() {
            return String.format("GroupingStrategy{runFiles=%d, centroidsPerPartition=%d, strategy=%s}", numRunFiles,
                    centroidsPerPartition, strategy);
        }
    }

    /**
     * Dummy function that returns dummy values for testing purposes.
     * Returns dummy distances, centroid IDs, and tuples for VCTree structure creation.
     * Generates K=10 centroids with multiple tuples per centroid.
     * 
     * @return DummyData object containing dummy distances, centroid IDs, and tuples
     */
    public DummyData getDummyValues() {
        System.err.println("=== GENERATING DUMMY VALUES FOR VCTree TESTING (K=10) ===");

        // Create dummy distances (simulating vector distances)
        List<Double> dummyDistances = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dummyDistances.add(Math.random() * 100.0); // Random distances between 0-100
        }

        // Create dummy centroid IDs (0-9 for K=10)
        List<Integer> dummyCentroidIds = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dummyCentroidIds.add(i); // Centroid IDs: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        }

        // Create dummy tuples (simulating hierarchical clustering data)
        List<DummyTuple> dummyTuples = new ArrayList<>();

        // Generate multiple tuples per centroid (5-10 tuples per centroid)
        for (int centroidId = 0; centroidId < 10; centroidId++) {
            int tuplesPerCentroid = 5 + (int) (Math.random() * 6); // 5-10 tuples per centroid

            for (int tupleIdx = 0; tupleIdx < tuplesPerCentroid; tupleIdx++) {
                DummyTuple tuple = new DummyTuple();
                tuple.level = 0; // All at level 0 for simplicity
                tuple.clusterId = centroidId % 3; // Distribute across 3 clusters
                tuple.centroidId = centroidId; // Same centroid ID for all tuples in this group
                tuple.embedding = generateDummyEmbedding(256); // 256-dimensional vector for actual data
                tuple.distance = Math.random() * 100.0; // Random distance between 0-100
                dummyTuples.add(tuple);
            }
        }

        System.err.println("Generated " + dummyDistances.size() + " dummy distances");
        System.err.println("Generated " + dummyCentroidIds.size() + " dummy centroid IDs (0-9)");
        System.err.println("Generated " + dummyTuples.size() + " dummy tuples");

        // Log distribution per centroid
        Map<Integer, Integer> centroidCounts = new HashMap<>();
        for (DummyTuple tuple : dummyTuples) {
            centroidCounts.put(tuple.centroidId, centroidCounts.getOrDefault(tuple.centroidId, 0) + 1);
        }

        System.err.println("Tuples per centroid:");
        for (int i = 0; i < 10; i++) {
            System.err.println("  Centroid " + i + ": " + centroidCounts.getOrDefault(i, 0) + " tuples");
        }

        return new DummyData(dummyDistances, dummyCentroidIds, dummyTuples);
    }

    /**
     * Generates a dummy embedding vector of specified dimension.
     * 
     * @param dimension The dimension of the embedding vector
     * @return Array of random float values representing the embedding
     */
    private float[] generateDummyEmbedding(int dimension) {
        float[] embedding = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            embedding[i] = (float) (Math.random() * 2.0 - 1.0); // Random values between -1 and 1
        }
        return embedding;
    }

    /**
     * Inner class to hold dummy data for testing.
     */
    public static class DummyData {
        public final List<Double> distances;
        public final List<Integer> centroidIds;
        public final List<DummyTuple> tuples;

        public DummyData(List<Double> distances, List<Integer> centroidIds, List<DummyTuple> tuples) {
            this.distances = distances;
            this.centroidIds = centroidIds;
            this.tuples = tuples;
        }

        @Override
        public String toString() {
            return String.format("DummyData{distances=%d, centroidIds=%d, tuples=%d}", distances.size(),
                    centroidIds.size(), tuples.size());
        }
    }

    /**
     * Inner class representing a dummy tuple for hierarchical clustering.
     */
    public static class DummyTuple {
        public int level;
        public int clusterId;
        public int centroidId;
        public float[] embedding;
        public double distance;

        @Override
        public String toString() {
            return String.format("DummyTuple{level=%d, clusterId=%d, centroidId=%d, embeddingDim=%d, distance=%.2f}",
                    level, clusterId, centroidId, embedding != null ? embedding.length : 0, distance);
        }
    }

    /**
     * Mock tuple reference that adapts DummyTuple to ITupleReference interface
     * for use with VCTreeStaticStructureCreator.
     * VCTreeStaticStructureCreator expects exactly 2 fields:
     * - Field 0: Integer (centroidId)
     * - Field 1: double[] (embedding vector)
     */
    public static class MockTupleReference implements ITupleReference {
        private final DummyTuple dummyTuple;
        private final byte[] serializedData;
        private final int[] fieldStarts;
        private final int[] fieldLengths;

        public MockTupleReference(DummyTuple dummyTuple) {
            this.dummyTuple = dummyTuple;
            this.serializedData = serializeDummyTuple(dummyTuple);
            this.fieldStarts = new int[2];
            this.fieldLengths = new int[2];
            calculateFieldPositions();
        }

        private byte[] serializeDummyTuple(DummyTuple tuple) {
            // VCTreeStaticStructureCreator expects: [centroidId(int), embedding(double[])]
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            // Field 0: centroidId (4 bytes)
            buffer.putInt(tuple.centroidId);

            // Field 1: embedding as double array
            if (tuple.embedding != null) {
                // Convert float[] to double[] and serialize
                buffer.putInt(tuple.embedding.length); // length prefix
                for (float f : tuple.embedding) {
                    buffer.putDouble((double) f); // convert float to double
                }
            } else {
                buffer.putInt(0); // empty array
            }

            byte[] result = new byte[buffer.position()];
            buffer.flip();
            buffer.get(result);
            return result;
        }

        private void calculateFieldPositions() {
            // Field 0: centroidId starts at 0, length 4
            fieldStarts[0] = 0;
            fieldLengths[0] = 4;

            // Field 1: embedding starts after centroidId, includes length prefix + data
            fieldStarts[1] = 4;
            fieldLengths[1] = 4 + (dummyTuple.embedding != null ? dummyTuple.embedding.length * 8 : 0); // 8 bytes per double
        }

        @Override
        public int getFieldCount() {
            return 2; // centroidId, embedding
        }

        @Override
        public byte[] getFieldData(int fIdx) {
            return serializedData;
        }

        @Override
        public int getFieldStart(int fIdx) {
            if (fIdx >= 0 && fIdx < fieldStarts.length) {
                return fieldStarts[fIdx];
            }
            return 0;
        }

        @Override
        public int getFieldLength(int fIdx) {
            if (fIdx >= 0 && fIdx < fieldLengths.length) {
                return fieldLengths[fIdx];
            }
            return 0;
        }

        @Override
        public String toString() {
            return String.format("MockTupleReference{centroidId=%d, embeddingDim=%d, level=%d, clusterId=%d}",
                    dummyTuple.centroidId, dummyTuple.embedding != null ? dummyTuple.embedding.length : 0,
                    dummyTuple.level, dummyTuple.clusterId);
        }
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        CreateStructureActivity sa = new CreateStructureActivity(new ActivityId(odId, 0));
        PassThroughActivity ca = new PassThroughActivity(new ActivityId(odId, 1));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ca);
        builder.addTargetEdge(0, ca, 0);

        builder.addBlockingEdge(sa, ca);
        builder.addTargetEdge(0, ca, 0);
    }

    protected class CreateStructureActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private static final Logger LOGGER = LogManager.getLogger();

        protected CreateStructureActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private final List<ByteBuffer> frameAccumulator = new ArrayList<>();
                private int tupleCount = 0;
                private FrameTupleReference tuple = new FrameTupleReference();
                private FrameTupleAccessor fta;
                private IScalarEvaluator levelEval;
                private IScalarEvaluator clusterIdEval;
                private IScalarEvaluator centroidIdEval;
                private IPointable levelVal;
                private IPointable clusterIdVal;
                private IPointable centroidIdVal;
                private Map<Integer, Integer> levelDistribution = null;
                private Map<String, Map<Integer, Integer>> clusterDistribution = null;
                // private MaterializerTaskState materializedData;

                // Quantization-related instance variables
                private int maxLevel = -1; // Will be set after buildStructureInfo()
                private int quantizationBits = 7; // Default: 7 bits (OSQ)
                private float minQuantile = 0.0f;
                private float maxQuantile = 1.0f;
                private float alpha = 0.9f;
                private float confidenceInterval = 0.999f;
                private int sampleCount = 0;
                private boolean quantizationParamsLoaded = false;

                // Get all storage partitions for this compute partition
                private final int[] storagePartitions = partitionsMap[partition];

                @Override
                public void open() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity OPENING ===");
                    System.err.println("Task partition: " + partition + ", Storage partitions: "
                            + java.util.Arrays.toString(storagePartitions));
                    try {
                        EvaluatorContext evalCtx = new EvaluatorContext(ctx);
                        levelEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(evalCtx); // treeLevel
                        centroidIdEval = new ColumnAccessEvalFactory(1).createScalarEvaluator(evalCtx); // centroidId
                        clusterIdEval = new ColumnAccessEvalFactory(2).createScalarEvaluator(evalCtx); // parentClusterId
                        // Field 3 is embedding - handled separately in convertToVCTreeBuilderFormat

                        // Initialize pointables for evaluator results
                        levelVal = new VoidPointable();
                        clusterIdVal = new VoidPointable();
                        centroidIdVal = new VoidPointable();

                        // Use inputRecordDescriptor for reading input frames (hierarchical data)
                        fta = new FrameTupleAccessor(inputRecordDescriptor);

                        // Read quantization parameters from the LSMVCTreeLocalResource
                        readQuantizationParamsFromMetadata(ctx);

                        System.err.println("CreateStructureActivity opened successfully");
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to open CreateStructureActivity: " + e.getMessage());
                        throw HyracksDataException.create(e);
                    }
                }

                /**
                 * Reads quantization parameters from the LSMVCTreeLocalResource metadata.
                 */
                private void readQuantizationParamsFromMetadata(IHyracksTaskContext taskCtx)
                        throws HyracksDataException {
                    try {
                        // Create index helper to access the resource (use first storage partition to read metadata)
                        IIndexDataflowHelper tempHelper = indexHelperFactory
                                .create(taskCtx.getJobletContext().getServiceContext(), storagePartitions[0]);

                        // Get the local resource from the helper
                        LocalResource localResource = tempHelper.getResource();
                        if (localResource != null) {
                            IResource resource = localResource.getResource();
                            if (resource instanceof DatasetLocalResource) {
                                DatasetLocalResource datasetResource = (DatasetLocalResource) resource;
                                IResource delegate = datasetResource.getResource();
                                if (delegate instanceof LSMVCTreeLocalResource) {
                                    LSMVCTreeLocalResource vcTreeResource = (LSMVCTreeLocalResource) delegate;

                                    // Read quantization parameters with defaults
                                    Integer bits = vcTreeResource.getBits();
                                    if (bits != null) {
                                        quantizationBits = bits;
                                    }

                                    Float ci = vcTreeResource.getConfidenceInterval();
                                    if (ci != null) {
                                        confidenceInterval = ci;
                                    }

                                    Float minQ = vcTreeResource.getMinQuantile();
                                    if (minQ != null) {
                                        minQuantile = minQ;
                                    }

                                    Float maxQ = vcTreeResource.getMaxQuantile();
                                    if (maxQ != null) {
                                        maxQuantile = maxQ;
                                    }

                                    Float a = vcTreeResource.getAlpha();
                                    if (a != null) {
                                        alpha = a;
                                    }

                                    Integer sc = vcTreeResource.getSampleCount();
                                    if (sc != null) {
                                        sampleCount = sc;
                                    }

                                    quantizationParamsLoaded = vcTreeResource.hasQuantizationParams();
                                    System.err.println("Quantization params loaded=" + quantizationParamsLoaded
                                            + ": bits=" + quantizationBits + ", confidenceInterval="
                                            + confidenceInterval + ", minQuantile=" + minQuantile + ", maxQuantile="
                                            + maxQuantile + ", alpha=" + alpha + ", sampleCount=" + sampleCount);
                                }
                            }
                        }

                        if (!quantizationParamsLoaded) {
                            System.err.println("WARNING: Could not load quantization params, using defaults: bits="
                                    + quantizationBits);
                        }
                    } catch (Exception e) {
                        System.err.println(
                                "WARNING: Failed to read quantization params: " + e.getMessage() + ", using defaults");
                        // Not fatal - will use default parameters
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    //                    System.err.println("=== CreateStructureActivity nextFrame ===");
                    fta.reset(buffer);
                    int frameTupleCount = fta.getTupleCount();
                    //                    System.err.println("Processing " + frameTupleCount + " tuples in this frame");

                    // Accumulate frames for batch processing
                    frameAccumulator.add(buffer.duplicate());

                    // Process tuples in this frame
                    for (int i = 0; i < fta.getTupleCount(); i++) {
                        tuple.reset(fta, i);
                        processTuple(tuple);
                    }

                    // Log progress every 1000 tuples to reduce noise
                    //                    if (tupleCount % 1000 == 0 && tupleCount > 0) {
                    //                        System.err.println("PROGRESS: Processed " + tupleCount + " tuples for hierarchical clustering");
                    //                    }

                    // Store frame in materialized data
                    // materializedData.appendFrame(buffer);
                }

                private void processTuple(ITupleReference tuple) throws HyracksDataException {
                    try {
                        // Extract tuple data for structure analysis from 4-field hierarchical format
                        // Format: [treeLevel, centroidId, parentClusterId, embedding]
                        FrameTupleReference frameTuple = (FrameTupleReference) tuple;
                        levelEval.evaluate(frameTuple, levelVal); // treeLevel
                        centroidIdEval.evaluate(frameTuple, centroidIdVal); // centroidId
                        clusterIdEval.evaluate(frameTuple, clusterIdVal); // parentClusterId

                        int level = IntegerPointable.getInteger(levelVal.getByteArray(), levelVal.getStartOffset());
                        int centroidId = IntegerPointable.getInteger(centroidIdVal.getByteArray(),
                                centroidIdVal.getStartOffset());
                        int clusterId =
                                IntegerPointable.getInteger(clusterIdVal.getByteArray(), clusterIdVal.getStartOffset());

                        // Track structure for analysis
                        if (levelDistribution == null) {
                            levelDistribution = new HashMap<>();
                            clusterDistribution = new HashMap<>();
                        }

                        levelDistribution.put(level, levelDistribution.getOrDefault(level, 0) + 1);

                        String levelKey = "Level_" + level;
                        Map<Integer, Integer> levelClusters =
                                clusterDistribution.computeIfAbsent(levelKey, k -> new HashMap<>());
                        levelClusters.put(clusterId, levelClusters.getOrDefault(clusterId, 0) + 1);

                        tupleCount++;

                        // Log progress every 5000 tuples to reduce noise
                        if (tupleCount % 5000 == 0) {
                            System.err.println("Partition " + partition + " processed " + tupleCount + " tuples");
                        }

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to process tuple: " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                /**
                 * Convert 4-field tuple [treeLevel, centroidId, parentClusterId, embedding] to:
                 * - For leaf level (level == maxLevel): 3-field tuple [centroidId, embedding, quantizedBytes]
                 * - For interior levels: 2-field tuple [centroidId, embedding]
                 * Parsing embeddings using the same logic as HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.
                 */
                private ITupleReference convertToVCTreeBuilderFormat(ITupleReference inputTuple)
                        throws HyracksDataException {
                    try {
                        int level =
                                IntegerPointable.getInteger(inputTuple.getFieldData(0), inputTuple.getFieldStart(0));
                        // Extract centroidId from field 1 (second field)
                        int centroidId =
                                IntegerPointable.getInteger(inputTuple.getFieldData(1), inputTuple.getFieldStart(1));

                        // Extract embedding from field 3 (fourth field) using same logic as HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor
                        byte[] embeddingData = inputTuple.getFieldData(3);
                        int embeddingStart = inputTuple.getFieldStart(3);

                        // Parse the embedding using ListAccessor (same as in HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor)
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

                        if (embedding == null || embedding.length == 0) {
                            System.err
                                    .println("WARNING: Failed to extract embedding from hierarchical tuple, skipping");
                            throw new HyracksDataException("Failed to extract embedding from hierarchical tuple");
                        }

                        // Check if this is a leaf level (maxLevel) - apply quantization only to leaf nodes
                        boolean isLeafLevel = (maxLevel >= 0 && level == maxLevel);

                        if (isLeafLevel && quantizationParamsLoaded) {
                            // Leaf level: quantize and create 3-field tuple [centroidId, embedding, quantizedBytes]
                            return createLeafTupleWithQuantization(centroidId, embedding);
                        } else {
                            // Interior level: create 2-field tuple [centroidId, embedding]
                            return createInteriorTuple(centroidId, embedding);
                        }

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to convert tuple to VCTreeBuilder format: " + e.getMessage());
                        e.printStackTrace();
                        throw new HyracksDataException("Failed to convert tuple to VCTreeBuilder format", e);
                    }
                }

                /**
                 * Creates a 2-field tuple for interior nodes: [centroidId, embedding]
                 */
                private ITupleReference createInteriorTuple(int centroidId, double[] embedding)
                        throws HyracksDataException {
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
                    ArrayTupleReference tupleRef = new ArrayTupleReference();

                    @SuppressWarnings("rawtypes")
                    ISerializerDeserializer[] fieldSerdes =
                            new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, // centroid ID
                                    DoubleArraySerializerDeserializer.INSTANCE // embedding as double array
                    };
                    Object[] fieldValues = new Object[] { centroidId, embedding };

                    TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);
                    return tupleRef;
                }

                /**
                 * Creates a 3-field tuple for leaf nodes with quantization: [centroidId, embedding, quantizedBytes]
                 * The quantizedBytes field includes both the quantized vector and corrective multiplier.
                 */
                private ITupleReference createLeafTupleWithQuantization(int centroidId, double[] embedding)
                        throws HyracksDataException {
                    try {
                        // Create quantization params
                        OptimizedScalarQuantizationSampleFile.Params quantParams =
                                new OptimizedScalarQuantizationSampleFile.Params(quantizationBits, embedding.length,
                                        sampleCount, confidenceInterval, minQuantile, maxQuantile, alpha);

                        // Use DOT_PRODUCT as default similarity function (can be made configurable later)
                        OptimizedScalarQuantizationSampleFile.SimilarityFunction simFunc =
                                OptimizedScalarQuantizationSampleFile.SimilarityFunction.DOT_PRODUCT;

                        // Quantize the embedding
                        OptimizedScalarQuantizationSampleFile.QuantizedVector quantizedResult =
                                OptimizedScalarQuantizationSampleFile.quantizeVector(embedding, quantParams, simFunc);

                        // Extract quantized bytes as byte[] (assuming bits <= 8)
                        byte[] quantizedBytes;
                        if (quantizedResult.quantizedBytes instanceof byte[]) {
                            quantizedBytes = (byte[]) quantizedResult.quantizedBytes;
                        } else if (quantizedResult.quantizedBytes instanceof short[]) {
                            // Convert short[] to byte[] for storage (2 bytes per short)
                            short[] shorts = (short[]) quantizedResult.quantizedBytes;
                            quantizedBytes = new byte[shorts.length * 2];
                            for (int i = 0; i < shorts.length; i++) {
                                quantizedBytes[i * 2] = (byte) (shorts[i] >> 8);
                                quantizedBytes[i * 2 + 1] = (byte) shorts[i];
                            }
                        } else if (quantizedResult.quantizedBytes instanceof int[]) {
                            // Convert int[] to byte[] for storage (4 bytes per int)
                            int[] ints = (int[]) quantizedResult.quantizedBytes;
                            quantizedBytes = new byte[ints.length * 4];
                            for (int i = 0; i < ints.length; i++) {
                                quantizedBytes[i * 4] = (byte) (ints[i] >> 24);
                                quantizedBytes[i * 4 + 1] = (byte) (ints[i] >> 16);
                                quantizedBytes[i * 4 + 2] = (byte) (ints[i] >> 8);
                                quantizedBytes[i * 4 + 3] = (byte) ints[i];
                            }
                        } else {
                            throw new HyracksDataException(
                                    "Unexpected quantized bytes type: " + quantizedResult.quantizedBytes.getClass());
                        }

                        // Create 3-field tuple: [centroidId, embedding, quantizedBytes]
                        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);
                        ArrayTupleReference tupleRef = new ArrayTupleReference();

                        @SuppressWarnings("rawtypes")
                        ISerializerDeserializer[] fieldSerdes =
                                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, // centroid ID
                                        DoubleArraySerializerDeserializer.INSTANCE, // embedding as double array
                                        ByteArraySerializerDeserializer.INSTANCE // quantized embedding bytes
                        };
                        Object[] fieldValues = new Object[] { centroidId, embedding, quantizedBytes };

                        TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);

                        System.err.println("Created leaf tuple with quantization: centroidId=" + centroidId
                                + ", embeddingDim=" + embedding.length + ", quantizedLen=" + quantizedBytes.length);

                        return tupleRef;

                    } catch (Exception e) {
                        System.err.println("WARNING: Failed to quantize leaf embedding, falling back to 2-field tuple: "
                                + e.getMessage());
                        // Fall back to interior tuple format if quantization fails
                        return createInteriorTuple(centroidId, embedding);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    System.err.println("Partition " + partition + " finished processing. Total tuples: " + tupleCount);

                    try {
                        createStaticStructure();
                    } finally {
                        // indexHelper cleanup is now handled inside createStaticStructure per storage partition
                    }

                    System.err.println("=== CreateStructureActivity COMPLETE ===");
                }

                /**
                 * Build structure information from collected hierarchical data.
                 * Creates the arrays needed by VCTreeStaticStructureBuilder.
                 */
                private StructureInfo buildStructureInfo() throws HyracksDataException {
                    System.err.println("=== BUILDING STRUCTURE INFO FROM HIERARCHICAL DATA ===");

                    List<Integer> clustersPerLevel = new ArrayList<>();
                    List<List<Integer>> centroidsPerCluster = new ArrayList<>();

                    if (levelDistribution == null || levelDistribution.isEmpty()) {
                        System.err.println("ERROR: No level distribution found - cannot build structure without data");
                        throw HyracksDataException.create(
                                new RuntimeException("Cannot build vector index: no training vectors were found. "
                                        + "Verify that the indexed field exists in the dataset and contains vector data, "
                                        + "and that sampling produced enough vectors for train_list_fraction."));
                    }

                    // Find max level
                    int maxLevel = levelDistribution.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
                    System.err.println("Found " + (maxLevel + 1) + " levels in hierarchical structure");
                    System.err.println("Level distribution: " + levelDistribution);
                    System.err.println("Cluster distribution: " + clusterDistribution);

                    // Process each level
                    for (int level = 0; level <= maxLevel; level++) {
                        String levelKey = "Level_" + level;
                        Map<Integer, Integer> levelClusters = clusterDistribution.get(levelKey);

                        if (levelClusters != null && !levelClusters.isEmpty()) {
                            // For hierarchical structure, we need to determine the actual clustering structure
                            // Level 0: All centroids form 1 cluster (root level)
                            // Level 1+: Centroids are grouped by their parent cluster IDs

                            int clusterCount;
                            List<Integer> centroidsInClusters = new ArrayList<>();

                            if (level == 0) {
                                // Root level: all centroids form 1 cluster
                                clusterCount = 1;
                                int totalCentroids = levelClusters.values().stream().mapToInt(Integer::intValue).sum();
                                centroidsInClusters.add(totalCentroids);
                                System.err.println(
                                        "Level " + level + " (root): 1 cluster with " + totalCentroids + " centroids");
                            } else {
                                // Interior levels: group by parent cluster ID
                                clusterCount = levelClusters.size();

                                // Sort cluster IDs to ensure consistent ordering
                                List<Integer> sortedClusterIds = new ArrayList<>(levelClusters.keySet());
                                sortedClusterIds.sort(Integer::compareTo);

                                for (int clusterId : sortedClusterIds) {
                                    int centroidCount = levelClusters.get(clusterId);
                                    centroidsInClusters.add(centroidCount);
                                    System.err.println("Level " + level + ", Cluster " + clusterId + ": "
                                            + centroidCount + " centroids");
                                }
                                System.err.println("Level " + level + ": " + clusterCount
                                        + " clusters, centroids per cluster: " + centroidsInClusters);
                            }

                            clustersPerLevel.add(clusterCount);
                            centroidsPerCluster.add(centroidsInClusters);
                        } else {
                            // Empty level
                            clustersPerLevel.add(0);
                            centroidsPerCluster.add(new ArrayList<>());
                            System.err.println("Level " + level + ": 0 clusters (no data)");
                        }
                    }

                    System.err.println("=== STRUCTURE INFO COMPLETE ===");
                    System.err.println("clustersPerLevel: " + clustersPerLevel);
                    System.err.println("centroidsPerCluster: " + centroidsPerCluster);

                    return new StructureInfo(clustersPerLevel, centroidsPerCluster);
                }

                private void createStaticStructure() throws HyracksDataException {
                    try {
                        // Analyze collected data to determine structure (once for all partitions)
                        System.err.println("Analyzing collected data to determine hierarchical structure...");
                        StructureInfo structureInfo = buildStructureInfo();
                        List<Integer> clustersPerLevel = structureInfo.clustersPerLevel;
                        List<List<Integer>> centroidsPerCluster = structureInfo.centroidsPerCluster;

                        // Store maxLevel for use in convertToVCTreeBuilderFormat (leaf-only quantization)
                        maxLevel = levelDistribution.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
                        System.err.println("Stored maxLevel=" + maxLevel + " for leaf-only quantization");

                        // Pre-convert all tuples once (identical for all storage partitions)
                        List<ITupleReference> convertedTuples = new ArrayList<>();
                        for (ByteBuffer frameBuffer : frameAccumulator) {
                            FrameTupleAccessor frameFta = new FrameTupleAccessor(inputRecordDescriptor);
                            frameFta.reset(frameBuffer);
                            for (int i = 0; i < frameFta.getTupleCount(); i++) {
                                tuple.reset(frameFta, i);
                                convertedTuples.add(convertToVCTreeBuilderFormat(tuple));
                            }
                        }

                        // Build the static structure on EACH storage partition
                        System.err.println("Building static structure on " + storagePartitions.length
                                + " storage partitions: " + java.util.Arrays.toString(storagePartitions));
                        for (int sp = 0; sp < storagePartitions.length; sp++) {
                            int storagePartition = storagePartitions[sp];
                            System.err.println("Building static structure on storage partition " + storagePartition
                                    + " (" + (sp + 1) + "/" + storagePartitions.length + ")");
                            buildStaticStructureOnPartition(storagePartition, clustersPerLevel, centroidsPerCluster,
                                    convertedTuples);
                        }

                        LOGGER.info("=== STATIC STRUCTURE PARAMETERS ===");
                        LOGGER.info("clustersPerLevel: {}", clustersPerLevel);
                        LOGGER.info("centroidsPerCluster: {}", centroidsPerCluster);
                        LOGGER.info("=== END STATIC STRUCTURE PARAMETERS ===");
                        System.err.println("STATIC STRUCTURE CREATED SUCCESSFULLY on all " + storagePartitions.length
                                + " storage partitions");

                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    }
                }

                /**
                 * Build the static structure on a single storage partition.
                 */
                private void buildStaticStructureOnPartition(int storagePartition, List<Integer> clustersPerLevel,
                        List<List<Integer>> centroidsPerCluster, List<ITupleReference> convertedTuples)
                        throws HyracksDataException {
                    IIndexDataflowHelper partitionHelper = null;
                    try {
                        partitionHelper =
                                indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), storagePartition);

                        LocalResource resource = partitionHelper.getResource();
                        System.err.println("Storage partition " + storagePartition + " LocalResource type: "
                                + (resource != null ? resource.getResource().getClass().getName() : "null"));

                        partitionHelper.open();

                        IIndex indexInstance = partitionHelper.getIndexInstance();
                        if (!(indexInstance instanceof ILSMIndex)) {
                            throw new HyracksDataException("Index is not an ILSMIndex instance, got: "
                                    + (indexInstance != null ? indexInstance.getClass().getName() : "null"));
                        }
                        ILSMIndex partitionLsmIndex = (ILSMIndex) indexInstance;

                        if (!(partitionLsmIndex instanceof LSMVCTree)) {
                            throw new HyracksDataException("Index is not an LSMVCTree instance, got: "
                                    + partitionLsmIndex.getClass().getName());
                        }

                        int adjustedMaxEntriesPerPage = Math.min(maxEntriesPerPage, 10);

                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID,
                                LSMComponentId.DEFAULT_COMPONENT_ID);
                        parameters.put("numLevels", clustersPerLevel.size());
                        parameters.put("clustersPerLevel", clustersPerLevel);
                        parameters.put("centroidsPerCluster", centroidsPerCluster);
                        parameters.put("maxEntriesPerPage", adjustedMaxEntriesPerPage);

                        IIndexBulkLoader partitionBulkLoader =
                                partitionLsmIndex.createBulkLoader(fillFactor, false, 0L, false, parameters);

                        int totalTuplesProcessed = 0;
                        for (ITupleReference convertedTuple : convertedTuples) {
                            partitionBulkLoader.add(convertedTuple);
                            totalTuplesProcessed++;
                        }

                        partitionBulkLoader.end();
                        LOGGER.info("Static structure finalized on storage partition {} ({} tuples)", storagePartition,
                                totalTuplesProcessed);

                        // Print BFS traversal of static structure (only for first partition to reduce log noise)
                        if (storagePartition == storagePartitions[0]) {
                            try {
                                LSMVCTreeDiskComponent component =
                                        (LSMVCTreeDiskComponent) ((LSMIndexDiskComponentBulkLoader) partitionBulkLoader)
                                                .getComponent();
                                printStaticStructureBFS(component, null);
                            } catch (Exception e) {
                                // Don't fail structure creation if logging fails
                            }
                        }

                    } catch (Exception e) {
                        throw HyracksDataException.create(e);
                    } finally {
                        if (partitionHelper != null) {
                            try {
                                partitionHelper.close();
                            } catch (Exception e) {
                                // Don't throw - cleanup failures shouldn't mask original exceptions
                            }
                        }
                    }
                }

                /**
                 * Perform a breadth-first traversal of the static structure and print
                 * a human-readable dump of all interior/leaf pages and their tuples.
                 * Distances are computed w.r.t the provided query vector when dimensionality matches;
                 * otherwise marked as NA.
                 */
                private void printStaticStructureBFS(LSMVCTreeDiskComponent component, double[] queryVector)
                        throws HyracksDataException {
                    VectorClusteringTree vcTree = component.getIndex();
                    IBufferCache bufferCache = vcTree.getBufferCache();
                    int fileId = vcTree.getFileId();
                    int rootPageId = vcTree.getRootPageId();
                    ITreeIndexFrameFactory interiorFrameFactory = vcTree.getInteriorFrameFactory();
                    ITreeIndexFrameFactory leafFrameFactory = vcTree.getLeafFrameFactory();

                    if (bufferCache == null || interiorFrameFactory == null || leafFrameFactory == null) {
                        throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                                "Required components are not initialized");
                    }

                    final int printLimit = 8; // Limit for embedding dimension display

                    Queue<int[]> queue = new ArrayDeque<>();
                    Set<Integer> visited = new HashSet<>();
                    queue.add(new int[] { rootPageId, 0 });
                    visited.add(rootPageId);

                    int visitedPages = 0;
                    long processedTuples = 0L;

                    while (!queue.isEmpty()) {
                        int[] entry = queue.poll();
                        int currentPageId = entry[0];
                        int level = entry[1];

                        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                        try {
                            page.acquireReadLatch();

                            IVectorClusteringLeafFrame leafFrame =
                                    (IVectorClusteringLeafFrame) leafFrameFactory.createFrame();
                            leafFrame.setPage(page);
                            boolean isLeaf = leafFrame.isLeaf();

                            if (isLeaf) {
                                LOGGER.info("=== LEVEL {} | PAGE {} | TYPE: LEAF ===", level, currentPageId);
                                int tupleCount = leafFrame.getTupleCount();
                                for (int i = 0; i < tupleCount; i++) {
                                    try {
                                        ITreeIndexTupleReference frameTuple = leafFrame.createTupleReference();
                                        frameTuple.resetByTupleIndex(leafFrame, i);

                                        ISerializerDeserializer<?>[] serdes = new ISerializerDeserializer<?>[3];
                                        serdes[0] = IntegerSerializerDeserializer.INSTANCE;
                                        serdes[1] = DoubleArraySerializerDeserializer.INSTANCE;
                                        serdes[2] = IntegerSerializerDeserializer.INSTANCE;
                                        Object[] fields = TupleUtils.deserializeTuple(frameTuple, serdes);

                                        int cid = (Integer) fields[0];
                                        double[] centroid = (double[]) fields[1];
                                        int metadataPtr = (Integer) fields[2];
                                        int centroidId = leafFrame.getCentroidId(i);

                                        String centroidStr = formatCentroid(centroid, printLimit);
                                        String distStr = computeDistanceString(queryVector, centroid);

                                        LOGGER.info(
                                                "tuple={} | cid={} | centroidId={} | centroid={} | dist={} | metadata={}",
                                                i, cid, centroidId, centroidStr, distStr, metadataPtr);
                                        processedTuples++;
                                    } catch (Exception e) {
                                        //                                        System.err.println("ERROR processing leaf tuple " + i + " on page "
                                        //                                                + currentPageId + ": " + e.getMessage());
                                    }
                                }

                                // Only follow next leaf if overflow flag is set
                                boolean hasOverflow = leafFrame.getOverflowFlagBit();
                                if (hasOverflow) {
                                    int nextLeaf = leafFrame.getNextLeaf();
                                    if (visited.add(nextLeaf)) {
                                        queue.add(new int[] { nextLeaf, level });
                                    }
                                }

                            } else {
                                IVectorClusteringInteriorFrame interiorFrame =
                                        (IVectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                                interiorFrame.setPage(page);
                                LOGGER.info("=== LEVEL {} | PAGE {} | TYPE: INTERIOR ===", level, currentPageId);
                                int tupleCount = interiorFrame.getTupleCount();
                                for (int i = 0; i < tupleCount; i++) {
                                    try {
                                        ITreeIndexTupleReference frameTuple = interiorFrame.createTupleReference();
                                        frameTuple.resetByTupleIndex(interiorFrame, i);

                                        ISerializerDeserializer<?>[] serdes = new ISerializerDeserializer<?>[3];
                                        serdes[0] = IntegerSerializerDeserializer.INSTANCE;
                                        serdes[1] = DoubleArraySerializerDeserializer.INSTANCE;
                                        serdes[2] = IntegerSerializerDeserializer.INSTANCE;
                                        Object[] fields = TupleUtils.deserializeTuple(frameTuple, serdes);

                                        int cid = (Integer) fields[0];
                                        double[] centroid = (double[]) fields[1];
                                        int childPageId = interiorFrame.getChildPageId(i);

                                        String centroidStr = formatCentroid(centroid, printLimit);
                                        String distStr = computeDistanceString(queryVector, centroid);

                                        LOGGER.info("tuple={} | cid={} | centroid={} | dist={} | child={}", i, cid,
                                                centroidStr, distStr, childPageId);
                                        processedTuples++;

                                        if (childPageId != -1 && visited.add(childPageId)) {
                                            queue.add(new int[] { childPageId, level + 1 });
                                        }
                                    } catch (Exception e) {
                                        //                                        System.err.println("ERROR processing interior tuple " + i + " on page "
                                        //                                                + currentPageId + ": " + e.getMessage());
                                    }
                                }

                                // Only follow next page if overflow flag is set
                                boolean hasOverflow = interiorFrame.getOverflowFlagBit();
                                if (hasOverflow) {
                                    int nextPage = interiorFrame.getNextPage();
                                    if (visited.add(nextPage)) {
                                        queue.add(new int[] { nextPage, level });
                                    }
                                }
                            }

                            visitedPages++;
                        } finally {
                            page.releaseReadLatch();
                            bufferCache.unpin(page);
                        }
                    }

                    LOGGER.info("=== BFS PRINT COMPLETE | pages={} | tuples={} ===", visitedPages, processedTuples);
                }

                /**
                 * Format a centroid vector with truncation for display.
                 */
                private String formatCentroid(double[] centroid, int limit) {
                    if (centroid == null) {
                        return "null";
                    }
                    int n = centroid.length;
                    int toPrint = Math.min(limit, n);
                    StringBuilder sb = new StringBuilder();
                    sb.append('[');
                    for (int i = 0; i < toPrint; i++) {
                        if (i > 0) {
                            sb.append(", ");
                        }
                        sb.append(String.format("%.4f", centroid[i]));
                    }
                    sb.append(']');
                    if (n > toPrint) {
                        sb.append(" (+").append(n - toPrint).append(" more)");
                    }
                    return sb.toString();
                }

                /**
                 * Compute distance string between query vector and centroid.
                 * Returns "NA" if query vector is null or dimensions don't match.
                 */
                private String computeDistanceString(double[] queryVector, double[] centroid) {
                    if (queryVector == null || centroid == null) {
                        return "NA";
                    }
                    if (centroid.length != queryVector.length) {
                        return "NA (dim mismatch)";
                    }
                    double d = VectorUtils.calculateEuclideanDistance(queryVector, centroid);
                    return String.format("%.4f", d);
                }

                @Override
                public void fail() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity FAILING ===");
                    System.err.println("Total tuples processed before failure: " + tupleCount);
                    // Index helpers and bulk loaders are managed per-partition in
                    // buildStaticStructureOnPartition with try/finally cleanup

                    // if (materializedData != null) {
                    //     try {
                    //         materializedData.close();
                    //     } catch (Exception e) {
                    //         System.err.println("ERROR: Failed to close materialized data: " + e.getMessage());
                    //     }
                    // }
                    System.err.println("=== CreateStructureActivity FAILED ===");
                }
            };
        }
    }

    protected class PassThroughActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected PassThroughActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                private LSMIndexDiskComponentBulkLoader lsmBulkLoader;
                private IIndexDataflowHelper indexHelper;
                private ILSMIndex lsmIndex;

                @Override
                public void initialize() throws HyracksDataException {
                    try {
                        writer.open();

                        // Check if scalarValuesUUID is provided
                        if (scalarValuesUUID == null) {
                            System.err.println("WARNING: scalarValuesUUID is null, skipping scalar values read");
                            return;
                        }

                        // Get MaterializerTaskState from context
                        MaterializerTaskState scalarState = (MaterializerTaskState) ctx
                                .getStateObject(new PartitionedUUID(scalarValuesUUID, partition));

                        if (scalarState == null) {
                            System.err.println("WARNING: Scalar values state not found for partition " + partition
                                    + ", UUID: " + scalarValuesUUID);
                            return;
                        }

                        System.err.println("Reading scalar values run file for partition " + partition);
                        System.err.println("Output RecordDescriptor: " + recordDesc);
                        System.err.println("Output RecordDescriptor field count: "
                                + (recordDesc != null ? recordDesc.getFieldCount() : "null"));

                        // Manual frame reading - read frame by frame
                        GeneratedRunFileReader reader = scalarState.creatReader();
                        reader.open();

                        try {
                            VSizeFrame frame = new VSizeFrame(ctx);
                            int frameCount = 0;
                            int totalTuples = 0;

                            // Create FrameTupleAccessor to validate frame format
                            FrameTupleAccessor fta = new FrameTupleAccessor(recordDesc);

                            while (reader.nextFrame(frame)) {
                                fta.reset(frame.getBuffer());
                                int tupleCount = fta.getTupleCount();
                                totalTuples += tupleCount;
                                frameCount++;

                                if (frameCount == 1 && tupleCount > 0) {
                                    // Debug first tuple
                                    FrameTupleReference tuple = new FrameTupleReference();
                                    tuple.reset(fta, 0);
                                    System.err.println("First tuple - field count: " + tuple.getFieldCount());
                                    System.err.println("First tuple - field 0 start: " + tuple.getFieldStart(0)
                                            + ", length: " + tuple.getFieldLength(0));
                                }

                                writer.nextFrame(frame.getBuffer());
                            }

                            System.err.println("Read " + frameCount + " frames with " + totalTuples + " total tuples");
                        } finally {
                            reader.close();
                        }

                        System.err.println("Successfully read and passed scalar values to downstream operator");

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to read scalar values run file: " + e.getMessage());
                        e.printStackTrace();
                        if (writer != null) {
                            writer.fail();
                        }
                        throw HyracksDataException.create(e);
                    } finally {
                        if (writer != null) {
                            writer.close();
                        }
                    }
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    try {

                    } catch (Exception e) {
                    }
                }
            };
        }
    }

    /**
     * Helper class to hold structure information for VCTreeStaticStructureBuilder.
     */
    private static class StructureInfo {
        public final List<Integer> clustersPerLevel;
        public final List<List<Integer>> centroidsPerCluster;

        public StructureInfo(List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster) {
            this.clustersPerLevel = clustersPerLevel;
            this.centroidsPerCluster = centroidsPerCluster;
        }
    }

    /**
     * Extracts numeric value from a pointable (helper method for parsing embeddings).
     * Same logic as in HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.
     */
    private static double extractNumericValue(IPointable pointable) throws HyracksDataException {
        byte[] data = pointable.getByteArray();
        int start = pointable.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[start]);

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

}
