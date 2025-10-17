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
import java.util.*;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.RawBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
// import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringMetadataFrameFactory;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VCTreeStaticStructureBuilder;
import org.apache.hyracks.storage.am.vector.impls.VCTreeStaticStructureNavigator;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringMetadataTupleWriterFactory;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;

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

    public VCTreeStaticStructureCreatorOperatorDescriptor(IOperatorDescriptorRegistry spec,
                                                          IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
                                                          RecordDescriptor inputRecordDescriptor, UUID permitUUID, UUID materializedDataUUID) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID;
        // this.materializedDataUUID = materializedDataUUID;
        this.outRecDescs[0] = inputRecordDescriptor;
        System.err.println("VCTreeStaticStructureCreatorOperatorDescriptor created with permit UUID: " + permitUUID);
        System.err.println("Using hierarchical clustering output record descriptor: " + outRecDescs[0]);
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
     * Main processing flow: dummy data -> partitioning.
     * 
     * @param ctx Hyracks task context for file operations
     * @return Map of centroid ID to file reference
     */
    public Map<Integer, FileReference> processVCTreePartitioning(IHyracksTaskContext ctx) throws HyracksDataException {
        System.err.println("=== VCTreePartitioning: Starting main processing flow ===");

        try {
                    // Step 1: Generate dummy data
                    System.err.println("Step 1: Generating dummy data (K=10)");
                    getDummyValues();

            // Step 2: Recursive partitioning
            System.err.println("Step 2: Starting recursive partitioning");
            VCTreePartitioner partitioner = new VCTreePartitioner(ctx, 32, 32768);
            // partitioner.partitionData(dummyData, 10); // K=10 - FIXED: partitionData expects (int K, long estimatedDataSize)
            partitioner.partitionData(10, 1000); // K=10, estimatedDataSize=1000
            Map<Integer, FileReference> centroidFiles = partitioner.getCentroidFiles();

            System.err.println("Partitioning complete. Created " + centroidFiles.size() + " centroid files");

            // Step 3: Cleanup
            partitioner.closeAllFiles();

            System.err.println("=== VCTreePartitioning: Processing complete ===");

            return centroidFiles;

        } catch (Exception e) {
            System.err.println("ERROR: VCTreePartitioning failed: " + e.getMessage());
            e.printStackTrace();
            throw HyracksDataException.create(e);
        }
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
                private VCTreeStaticStructureBuilder structureCreator;
                // private MaterializerTaskState materializedData;

                @Override
                public void open() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity OPENING ===");
                    try {
                        // Register permit state for coordination
                        //                        IterationPermitState permitState =
                        //                                (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));
                        //
                        //                        if (permitState == null) {
                        //                            java.util.concurrent.Semaphore permit = new java.util.concurrent.Semaphore(0);
                        //                            permitState = new IterationPermitState(ctx.getJobletContext().getJobId(),
                        //                                    new PartitionedUUID(permitUUID, partition), permit);
                        //                            ctx.setStateObject(permitState);
                        //                            System.err.println("✅ PERMIT STATE CREATED AND REGISTERED for UUID: " + permitUUID
                        //                                    + ", partition: " + partition);
                        //                        }

                        // Initialize materialized data state
                        //                        materializedData = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                        //                                new PartitionedUUID(materializedDataUUID, partition));
                        //                        materializedData.open(ctx);

                        // Initialize evaluators for extracting tuple fields from 4-field hierarchical format
                        // Format: [treeLevel, centroidId, parentClusterId, embedding]
                        EvaluatorContext evalCtx = new EvaluatorContext(ctx);
                        levelEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(evalCtx);        // treeLevel
                        centroidIdEval = new ColumnAccessEvalFactory(1).createScalarEvaluator(evalCtx);   // centroidId
                        clusterIdEval = new ColumnAccessEvalFactory(2).createScalarEvaluator(evalCtx);    // parentClusterId
                        // Field 3 is embedding - handled separately in convertToVCTreeBuilderFormat

                        // Initialize pointables for evaluator results
                        levelVal = new VoidPointable();
                        clusterIdVal = new VoidPointable();
                        centroidIdVal = new VoidPointable();

                        fta = new FrameTupleAccessor(outRecDescs[0]);
                        System.err.println("CreateStructureActivity opened successfully");
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to open CreateStructureActivity: " + e.getMessage());
                        throw HyracksDataException.create(e);
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity nextFrame ===");
                    fta.reset(buffer);
                    int frameTupleCount = fta.getTupleCount();
                    System.err.println("Processing " + frameTupleCount + " tuples in this frame");

                    // Accumulate frames for batch processing
                    frameAccumulator.add(buffer.duplicate());

                    // Process tuples in this frame
                    for (int i = 0; i < fta.getTupleCount(); i++) {
                        tuple.reset(fta, i);
                        processTuple(tuple);
                    }

                    // Log progress every 1000 tuples to reduce noise
                    if (tupleCount % 1000 == 0 && tupleCount > 0) {
                        System.err.println("PROGRESS: Processed " + tupleCount + " tuples for hierarchical clustering");
                    }

                    // Store frame in materialized data
                    // materializedData.appendFrame(buffer);
                }

                private void processTuple(ITupleReference tuple) throws HyracksDataException {
                    try {
                        // Extract tuple data for structure analysis from 4-field hierarchical format
                        // Format: [treeLevel, centroidId, parentClusterId, embedding]
                        FrameTupleReference frameTuple = (FrameTupleReference) tuple;
                        levelEval.evaluate(frameTuple, levelVal);        // treeLevel
                        centroidIdEval.evaluate(frameTuple, centroidIdVal);   // centroidId
                        clusterIdEval.evaluate(frameTuple, clusterIdVal);     // parentClusterId

                        int level = IntegerPointable.getInteger(levelVal.getByteArray(), levelVal.getStartOffset());
                        int centroidId = IntegerPointable.getInteger(centroidIdVal.getByteArray(),
                                centroidIdVal.getStartOffset());
                        int clusterId =
                                IntegerPointable.getInteger(clusterIdVal.getByteArray(), clusterIdVal.getStartOffset());

                        // Debug: Log suspicious level values and filter them out
                        if (level < 0 || level > 100) {
                            System.err.println("🔍 DEBUG: Suspicious level value: " + level + " (clusterId: "
                                    + clusterId + ", centroidId: " + centroidId + ") - IGNORING");
                            return; // Skip this tuple
                        }

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
                            System.err.println("Processed " + tupleCount + " tuples for structure analysis");
                        }

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to process tuple: " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                /**
                 * Convert 4-field tuple [treeLevel, centroidId, parentClusterId, embedding] to 2-field tuple [centroidId, embedding]
                 * for VCTreeStaticStructureBuilder, parsing embeddings using the same logic as HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor.
                 */
                private ITupleReference convertToVCTreeBuilderFormat(ITupleReference inputTuple)
                        throws HyracksDataException {
                    try {
                        // Extract centroidId from field 1 (second field)
                        int centroidId = IntegerPointable.getInteger(inputTuple.getFieldData(1), inputTuple.getFieldStart(1));

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
                            System.err.println("WARNING: Failed to extract embedding from hierarchical tuple, skipping");
                            throw new HyracksDataException("Failed to extract embedding from hierarchical tuple");
                        }

                        // Create 2-field tuple: [centroidId, embedding] - same format as test code
                        org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder tupleBuilder =
                                new org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder(2);
                        ArrayTupleReference tupleRef = new ArrayTupleReference();

                        // Create field serializers and values - use double array for full precision
                        @SuppressWarnings("rawtypes")
                        ISerializerDeserializer[] fieldSerdes =
                                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, // centroid ID
                                        DoubleArraySerializerDeserializer.INSTANCE // embedding as double array
                        };
                        Object[] fieldValues = new Object[] { centroidId, embedding };

                        // Build the tuple using TupleUtils - same as test code
                        TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);

                        return tupleRef;

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to convert tuple to VCTreeBuilder format: " + e.getMessage());
                        e.printStackTrace();
                        throw new HyracksDataException("Failed to convert tuple to VCTreeBuilder format", e);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity CLOSING ===");
                    System.err.println("Total tuples collected: " + tupleCount);
                    System.err.println("Frames accumulated: " + frameAccumulator.size());

                    // Test partitioning with dummy data
                    try {
                        System.err.println("=== TESTING PARTITIONING ===");
                        Map<Integer, FileReference> centroidFiles = processVCTreePartitioning(ctx);
                        System.err.println("=== PARTITIONING COMPLETE ===");
                        System.err.println("Created " + centroidFiles.size() + " centroid files");
                    } catch (Exception e) {
                        System.err.println("ERROR: Partitioning failed: " + e.getMessage());
                        e.printStackTrace();
                    }

                    // Process all accumulated tuples and create static structure
                    System.err.println("=== STARTING HIERARCHICAL CLUSTERING ANALYSIS ===");
                    System.err.println("Analyzing " + tupleCount + " tuples to determine structure...");
                    createStaticStructure();
                    System.err.println("=== HIERARCHICAL CLUSTERING ANALYSIS COMPLETE ===");

                    // Close materialized data
                    // if (materializedData != null) {
                    //     materializedData.close();
                    //     ctx.setStateObject(materializedData);
                    // }

                    // Signal Branch 2 that structure creation is complete
                    try {
                        System.err.println("=== SIGNALING BRANCH 2 COMPLETION ===");
                        IterationPermitState permitState =
                                (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));
                        if (permitState != null) {
                            permitState.getPermit().release();
                            System.err.println(
                                    "✅ PERMIT RELEASED - Branch 1 (structure creation) completed, signaling Branch 2");
                        } else {
                            System.err.println("WARNING: Permit state not found for UUID: " + permitUUID);
                        }
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to release permit: " + e.getMessage());
                        e.printStackTrace();
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
                        throw HyracksDataException.create(new RuntimeException("No hierarchical data available to build structure"));
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
                                System.err.println("Level " + level + " (root): 1 cluster with " + totalCentroids + " centroids");
                            } else {
                                // Interior levels: group by parent cluster ID
                                clusterCount = levelClusters.size();
                                
                                // Sort cluster IDs to ensure consistent ordering
                                List<Integer> sortedClusterIds = new ArrayList<>(levelClusters.keySet());
                                sortedClusterIds.sort(Integer::compareTo);
                                
                                for (int clusterId : sortedClusterIds) {
                                    int centroidCount = levelClusters.get(clusterId);
                                    centroidsInClusters.add(centroidCount);
                                    System.err.println("Level " + level + ", Cluster " + clusterId + ": " + centroidCount + " centroids");
                                }
                                System.err.println("Level " + level + ": " + clusterCount + " clusters, centroids per cluster: " + centroidsInClusters);
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
                    System.err.println("=== CREATING STATIC STRUCTURE WITH VCTreeStaticStructureCreator ===");
                    System.err.println("Processing " + frameAccumulator.size() + " accumulated frames");
                    System.err.println("Total tuples to process: " + tupleCount);

                    try {
                        // Analyze collected data to determine structure
                        System.err.println("Analyzing collected data to determine hierarchical structure...");
                        StructureInfo structureInfo = buildStructureInfo();
                        List<Integer> clustersPerLevel = structureInfo.clustersPerLevel;
                        List<List<Integer>> centroidsPerCluster = structureInfo.centroidsPerCluster;


                        // Get infrastructure
                        INcApplicationContext appCtx = (INcApplicationContext) ctx.getJobletContext()
                                .getServiceContext().getApplicationContext();
                        IBufferCache bufferCache = appCtx.getBufferCache();

                        // Get index path
                        System.err.println("Getting index file path...");
                        FileReference indexPathRef = getIndexFilePath();
                        if (indexPathRef == null) {
                            System.err.println("ERROR: Could not determine index path");
                            return;
                        }
                        System.err.println("Index path: " + indexPathRef);

                        // Create static structure file path
                        FileReference staticStructureFile = indexPathRef.getChild(".static_structure_vctree");
                        System.err.println("Static structure file path: " + staticStructureFile);

                        // Create or open the static structure file with proper coordination
                        System.err.println("Creating/opening static structure file...");
                        int fileId;
                        try {
                            // Check if file already exists in the file system
                            IIOManager ioManager = appCtx.getIoManager();
                            if (ioManager.exists(staticStructureFile)) {
                                System.err.println("Static structure file already exists, opening it...");
                                fileId = bufferCache.openFile(staticStructureFile);
                            } else {
                                System.err.println("Static structure file doesn't exist, creating it...");
                                fileId = bufferCache.createFile(staticStructureFile);
                            }
                            System.err.println("File ready with ID: " + fileId);
                        } catch (Exception e) {
                            System.err.println("ERROR: Failed to create/open static structure file: " + e.getMessage());
                            throw HyracksDataException.create(e);
                        }

                        // Create specific type traits for different frame types following test pattern
                        // Cluster tuples (Interior/Leaf): <cid, centroid, pointer>
                        ITypeTraits[] clusterTypeTraits = new ITypeTraits[3];
                        clusterTypeTraits[0] = IntegerPointable.TYPE_TRAITS; // cluster ID
                        clusterTypeTraits[1] = VarLengthTypeTrait.INSTANCE; // centroid (float array)
                        clusterTypeTraits[2] = IntegerPointable.TYPE_TRAITS; // pointer

                        // Metadata tuples: <max_distance, page_pointer>
                        ITypeTraits[] metadataTypeTraits = new ITypeTraits[2];
                        metadataTypeTraits[0] = FloatPointable.TYPE_TRAITS; // max distance
                        metadataTypeTraits[1] = IntegerPointable.TYPE_TRAITS; // page pointer

                        // Data tuples: <distance, cosine_similarity, vector, primary_key>
                        ITypeTraits[] dataTypeTraits = new ITypeTraits[4];
                        dataTypeTraits[0] = DoublePointable.TYPE_TRAITS; // distance
                        dataTypeTraits[1] = DoublePointable.TYPE_TRAITS; // cosine similarity
                        dataTypeTraits[2] = VarLengthTypeTrait.INSTANCE; // vector
                        dataTypeTraits[3] = VarLengthTypeTrait.INSTANCE; // primary key

                        // Create tuple writer factories for each frame type
                        VectorClusteringInteriorTupleWriterFactory interiorTupleWriterFactory =
                                new VectorClusteringInteriorTupleWriterFactory(clusterTypeTraits, null, null);
                        VectorClusteringLeafTupleWriterFactory leafTupleWriterFactory =
                                new VectorClusteringLeafTupleWriterFactory(clusterTypeTraits, null, null);
                        VectorClusteringMetadataTupleWriterFactory metadataTupleWriterFactory =
                                new VectorClusteringMetadataTupleWriterFactory(metadataTypeTraits, null, null);
                        VectorClusteringDataTupleWriterFactory dataTupleWriterFactory =
                                new VectorClusteringDataTupleWriterFactory(dataTypeTraits, null, null);

                        // Create frame factories with proper tuple writers
                        // Use 256 dimensions for actual data - need larger frame size
                        int vectorDimensions = 784;
                        ITreeIndexFrameFactory interiorFrameFactory = new VectorClusteringInteriorFrameFactory(
                                interiorTupleWriterFactory.createTupleWriter(), vectorDimensions);
                        ITreeIndexFrameFactory leafFrameFactory = new VectorClusteringLeafFrameFactory(
                                leafTupleWriterFactory.createTupleWriter(), vectorDimensions);
                        ITreeIndexFrameFactory metadataFrameFactory = new VectorClusteringMetadataFrameFactory(
                                metadataTupleWriterFactory.createTupleWriter(), vectorDimensions);
                        ITreeIndexFrameFactory dataFrameFactory =
                                new VectorClusteringDataFrameFactory(dataTupleWriterFactory, vectorDimensions);

                        // Create page manager
                        AppendOnlyLinkedMetadataPageManager pageManager =
                                new AppendOnlyLinkedMetadataPageManager(bufferCache, new LIFOMetaDataFrameFactory());

                        // Create comparator factories for DATA tuples (4 fields)
                        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[4];
                        cmpFactories[0] = DoubleBinaryComparatorFactory.INSTANCE; // distance
                        cmpFactories[1] = DoubleBinaryComparatorFactory.INSTANCE; // cosine similarity
                        cmpFactories[2] = RawBinaryComparatorFactory.INSTANCE; // vector
                        cmpFactories[3] = RawBinaryComparatorFactory.INSTANCE; // primary key

                        // Create VectorClusteringTree with correct parameters
                        // LSMVCTree


                        VectorClusteringTree vectorTree = new VectorClusteringTree(bufferCache, pageManager,
                                interiorFrameFactory, leafFrameFactory, metadataFrameFactory, dataFrameFactory,
                                cmpFactories, 4, vectorDimensions, staticStructureFile);

                        // Activate the tree (this makes the file available for operations)
                        // Note: We skip create() since we already created and opened the file
                        System.err.println("Activating VectorClusteringTree...");
                        vectorTree.activate();
                        System.err.println("VectorClusteringTree activated successfully");

                        // Create static structure builder using the tree's factory method
                        // Reduce maxEntriesPerPage for 256-dimensional vectors to fit in frame
                        int adjustedMaxEntriesPerPage = Math.min(maxEntriesPerPage, 10); // Limit to 10 entries for large vectors
                        System.err.println(
                                "Creating VCTreeStaticStructureBuilder with " + clustersPerLevel.size() + " levels...");
                        System.err.println("Adjusted maxEntriesPerPage from " + maxEntriesPerPage + " to "
                                + adjustedMaxEntriesPerPage + " for 256-dimensional vectors");
                        structureCreator =
                                vectorTree.createStaticStructureBuilder(clustersPerLevel.size(), clustersPerLevel,
                                        centroidsPerCluster, adjustedMaxEntriesPerPage, NoOpPageWriteCallback.INSTANCE);

                        System.err.println("Processing " + frameAccumulator.size() + " accumulated frames...");
                        // Process all accumulated tuples
                        int totalTuplesProcessed = 0;
                        for (ByteBuffer frameBuffer : frameAccumulator) {
                            FrameTupleAccessor frameFta = new FrameTupleAccessor(outRecDescs[0]);
                            frameFta.reset(frameBuffer);

                            System.err.println("Frame has " + frameFta.getTupleCount() + " tuples");

                            for (int i = 0; i < frameFta.getTupleCount(); i++) {
                                tuple.reset(frameFta, i);

                                // Debug the tuple being processed
                                System.err.println(
                                        "=== PROCESSING TUPLE " + totalTuplesProcessed + " FOR STATIC STRUCTURE ===");
                                System.err.println("Input tuple field count: " + tuple.getFieldCount());
                                for (int fieldIndex = 0; fieldIndex < tuple.getFieldCount(); fieldIndex++) {
                                    int fieldLength = tuple.getFieldLength(fieldIndex);
                                    int fieldStart = tuple.getFieldStart(fieldIndex);
                                    System.err.println("Field " + fieldIndex + ": length=" + fieldLength + ", start="
                                            + fieldStart);

                                    // Show first few bytes of each field
                                    if (fieldLength > 0) {
                                        byte[] fieldData = tuple.getFieldData(fieldIndex);
                                        int bytesToShow = Math.min(16, fieldLength);
                                        System.err.print(
                                                "Field " + fieldIndex + " data (first " + bytesToShow + " bytes): ");
                                        for (int j = 0; j < bytesToShow; j++) {
                                            System.err.printf("%02X ", fieldData[fieldStart + j] & 0xFF);
                                        }
                                        System.err.println();
                                    }
                                }

                                // Convert 4-field tuple to 3-field tuple for VCTreeStaticStructureBuilder
                                ITupleReference convertedTuple = convertToVCTreeBuilderFormat(tuple);
                                System.err.println("Converted tuple field count: " + convertedTuple.getFieldCount());

                                structureCreator.add(convertedTuple);
                                totalTuplesProcessed++;

                                // Only show first few tuples to avoid spam
                                if (totalTuplesProcessed >= 3) {
                                    System.err.println("... (showing first 3 tuples only)");
                                    break;
                                }
                            }

                            // Only process first frame to avoid spam
                            // Process all tuples for static structure creation
                        }
                        System.err
                                .println("Processed " + totalTuplesProcessed + " tuples for static structure creation");

                        System.err.println("Finalizing static structure...");
                        // Finalize the structure
                        structureCreator.end();
                        System.err.println("✅ STATIC STRUCTURE FINALIZED SUCCESSFULLY");

                        // Create navigator for static structure access
                        System.err.println("Creating VCTreeStaticStructureNavigator...");
                        VCTreeStaticStructureNavigator navigator = new VCTreeStaticStructureNavigator(bufferCache,
                                fileId, interiorFrameFactory, leafFrameFactory);

                        // Test the navigator with a sample vector
                        System.err.println("Testing navigator with sample vector...");
                        double[] testVector = new double[vectorDimensions];
                        for (int i = 0; i < vectorDimensions; i++) {
                            testVector[i] = 0d;
                        }

                        try {
                            ClusterSearchResult result = navigator.findClosestCentroid(testVector);
                            System.err.println("✅ NAVIGATOR TEST SUCCESSFUL");
                            System.err.println("   Closest cluster: pageId=" + result.leafPageId + ", clusterIndex="
                                    + result.clusterIndex + ", distance=" + result.distance);
                        } catch (Exception e) {
                            System.err.println("WARNING: Navigator test failed: " + e.getMessage());
                        }

                        // Force all data to disk before closing
                        System.err.println("Forcing static structure data to disk...");
                        try {
                            bufferCache.force(fileId, true);
                            System.err.println("✅ STATIC STRUCTURE DATA FORCED TO DISK SUCCESSFULLY");
                        } catch (Exception e) {
                            System.err.println("WARNING: Failed to force data to disk: " + e.getMessage());
                        }

                        // Close the file
                        System.err.println("Closing static structure file...");
                        try {
                            bufferCache.closeFile(fileId);
                            System.err.println("✅ STATIC STRUCTURE FILE CLOSED SUCCESSFULLY");
                        } catch (Exception e) {
                            System.err.println("WARNING: Failed to close static structure file: " + e.getMessage());
                        }

                        System.err.println(
                                "✅ STATIC STRUCTURE CREATED SUCCESSFULLY at: " + staticStructureFile.getAbsolutePath());

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to create static structure: " + e.getMessage());
                        e.printStackTrace();
                        throw HyracksDataException.create(e);
                    }
                }

                private FileReference getIndexFilePath() {
                    try {
                        IIndexDataflowHelper indexHelper =
                                indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                        LocalResource resource = indexHelper.getResource();
                        String resourcePath = resource.getPath();

                        IIOManager ioManager = ctx.getIoManager();
                        return ioManager.resolve(resourcePath);

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to get index file path: " + e.getMessage());
                        e.printStackTrace();
                        return null;
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity FAILING ===");
                    System.err.println("Total tuples processed before failure: " + tupleCount);

                    if (structureCreator != null) {
                        try {
                            structureCreator.abort();
                        } catch (Exception e) {
                            System.err.println("ERROR: Failed to abort structure creator: " + e.getMessage());
                        }
                    }

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
                    System.err.println("=== PassThroughActivity INITIALIZING ===");
                    try {
                        // Wait for permit from CreateStructureActivity
                        IterationPermitState permitState =
                                (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));
                        if (permitState != null) {
                            System.err.println("Waiting for permit from CreateStructureActivity...");
                            permitState.getPermit().acquire();
                            System.err.println("✅ PERMIT ACQUIRED - CreateStructureActivity completed");
                        }

                        // Initialize LSM Bulk Loader
                        initializeLSMBulkLoader();

                        System.err.println("✅ PassThroughActivity initialized successfully");

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to initialize PassThroughActivity: " + e.getMessage());
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
                        parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID,
                                LSMComponentId.DEFAULT_COMPONENT_ID);
                        IIndexBulkLoader vcBulkLoader =
                                lsmIndex.createBulkLoader(fillFactor, false, 0L, false, parameters);

                        // End the bulk loader immediately
                        vcBulkLoader.end();

                        System.err.println("✅ VectorClusteringTree Bulk Loader created and ended successfully");

                    } catch (Exception e) {
                        System.err.println(
                                "ERROR: Failed to initialize VectorClusteringTree Bulk Loader: " + e.getMessage());
                        e.printStackTrace();
                        throw HyracksDataException.create(e);
                    }
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    System.err.println("=== PassThroughActivity DEINITIALIZING ===");
                    try {
                        if (lsmBulkLoader != null) {
                            lsmBulkLoader.end();
                        }
                        if (indexHelper != null) {
                            indexHelper.close();
                        }
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to deinitialize PassThroughActivity: " + e.getMessage());
                    }
                    System.err.println("=== PassThroughActivity COMPLETE ===");
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
