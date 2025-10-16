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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.common.IIndexBulkLoader;

/**
 * Operator that handles bulk loader initialization and recursive data grouping to run files.
 * This operator is designed for job 3 in the VCTree creation pipeline.
 * 
 * Responsibilities:
 * 1. Initialize LSM bulk loader for VectorClusteringTree
 * 2. Apply recursive partitioning logic using SHAPIRO formula
 * 3. Group data into run files based on memory budget and data size
 * 4. Manage run file creation and data distribution
 */
public class VCTreeBulkLoaderAndGroupingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final int VECTOR_DIMENSION = 256;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final float fillFactor;
    private final UUID permitUUID;
    private final UUID materializedDataUUID;

    public VCTreeBulkLoaderAndGroupingOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID, UUID materializedDataUUID) {
        super(spec, 1, 0);
        this.indexHelperFactory = indexHelperFactory;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID;
        this.materializedDataUUID = materializedDataUUID;
        System.err.println("VCTreeBulkLoaderAndGroupingOperatorDescriptor created with permit UUID: " + permitUUID);
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
                tuple.embedding = generateDummyEmbedding(VECTOR_DIMENSION); // 256-dimensional vector
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

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeBulkLoaderAndGroupingNodePushable(ctx, partition, nPartitions, inputRecDesc, permitUUID,
                materializedDataUUID);
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
            // Buffer needs: tuple count (4) + tuple data length (4) + tuple data (4 + 4 + VECTOR_DIMENSION * 8)
            ByteBuffer buffer = ByteBuffer.allocate(16 + VECTOR_DIMENSION * 8);

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
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + VECTOR_DIMENSION * 8);

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

    /**
     * Node pushable implementation for VCTreeBulkLoaderAndGroupingOperatorDescriptor.
     */
    private class VCTreeBulkLoaderAndGroupingNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final int partition;
        private final UUID materializedDataUUID;
        private LSMIndexDiskComponentBulkLoader lsmBulkLoader;
        private IIndexDataflowHelper indexHelper;
        private ILSMIndex lsmIndex;
        private MaterializerTaskState materializedData;

        public VCTreeBulkLoaderAndGroupingNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc, UUID permitUUID, UUID materializedDataUUID) {
            this.ctx = ctx;
            this.partition = partition;
            this.materializedDataUUID = materializedDataUUID;
        }

        @Override
        public void open() throws HyracksDataException {
            System.err.println("=== VCTreeBulkLoaderAndGroupingNodePushable OPENING ===");
            try {
                // Initialize materialized data state
                materializedData = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                        new PartitionedUUID(materializedDataUUID, partition));
                materializedData.open(ctx);

                // Initialize LSM Bulk Loader
                initializeLSMBulkLoader();

                // Apply recursive partitioning and create run files
                applyRecursivePartitioningAndGrouping();

                System.err.println("✅ VCTreeBulkLoaderAndGroupingNodePushable opened successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeBulkLoaderAndGroupingNodePushable: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        private void initializeLSMBulkLoader() throws HyracksDataException {
            try {
                System.err.println("=== INITIALIZING VectorClusteringTree Bulk Loader ===");

                // Get index helper and open it
                indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                indexHelper.open();
                lsmIndex = (ILSMIndex) indexHelper.getIndexInstance();

                // Create LSM bulk loader (which internally uses VectorClusteringTree)
                Map<String, Object> parameters = new HashMap<>();
                parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID, LSMComponentId.DEFAULT_COMPONENT_ID);
                IIndexBulkLoader vcBulkLoader = lsmIndex.createBulkLoader(fillFactor, false, 0L, false, parameters);

                // End the bulk loader immediately
                vcBulkLoader.end();

                System.err.println("✅ VectorClusteringTree Bulk Loader created and ended successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize VectorClusteringTree Bulk Loader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        private void applyRecursivePartitioningAndGrouping() throws HyracksDataException {
            try {
                System.err.println("=== APPLYING RECURSIVE PARTITIONING AND GROUPING ===");

                // Apply recursive partitioning with dummy data
                int K = 10; // Number of centroids
                long inputDataBytesSize = 1024 * 1024; // 1MB dummy data
                int frameSize = 32768; // 32KB frame size

                RunFileManager runFileManager = applyRecursivePartitioning(K, inputDataBytesSize, frameSize, ctx);

                System.err.println("✅ Recursive partitioning and grouping completed successfully");
                System.err.println("Created " + runFileManager.getTotalRunFiles() + " run files");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to apply recursive partitioning and grouping: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            // This operator doesn't process input frames, it just initializes bulk loader and grouping
            System.err.println("VCTreeBulkLoaderAndGroupingNodePushable: nextFrame called (no processing needed)");
        }

        @Override
        public void close() throws HyracksDataException {
            System.err.println("=== VCTreeBulkLoaderAndGroupingNodePushable CLOSING ===");
            try {
                if (lsmBulkLoader != null) {
                    lsmBulkLoader.end();
                }
                if (indexHelper != null) {
                    indexHelper.close();
                }
                if (materializedData != null) {
                    materializedData.close();
                    ctx.setStateObject(materializedData);
                }
                System.err.println("✅ VCTreeBulkLoaderAndGroupingNodePushable closed successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to close VCTreeBulkLoaderAndGroupingNodePushable: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            System.err.println("=== VCTreeBulkLoaderAndGroupingNodePushable FAILING ===");
            try {
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
                System.err
                        .println("ERROR: Failed to cleanup VCTreeBulkLoaderAndGroupingNodePushable: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
