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

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManager;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.impls.VCTreeStaticStructureCreator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriterFactory;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Operator that creates VCTree static structure files using VCTreeStaticStructureCreator.
 * Two-activity structure: first creates static file, second reads materialized data and passes through.
 */
public class VCTreeStaticStructureCreatorOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final int maxEntriesPerPage;
    private final float fillFactor;
    private final UUID permitUUID;
    private final UUID materializedDataUUID;

    public VCTreeStaticStructureCreatorOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID, UUID materializedDataUUID) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID;
        this.materializedDataUUID = materializedDataUUID;
        this.outRecDescs[0] = inputRecordDescriptor;
        System.err.println("VCTreeStaticStructureCreatorOperatorDescriptor created with permit UUID: " + permitUUID);
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
    public int  calculatePartitionsUsingShapiro(int K, long inputDataBytesSize, int frameSize, int memoryBudget) {
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
        long numberOfPartitions = (long) (Math.ceil((numberOfInputFrames * FUDGE_FACTOR - memoryBudget) / (memoryBudget - 1)));
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
     * 
     * @param K Total number of centroids
     * @param numPartitions Number of partitions from SHAPIRO
     * @param memoryBudget Available memory budget in frames
     * @return GroupingStrategy object with run file allocation plan
     */
    public GroupingStrategy createGroupingStrategy(int K, int numPartitions, int memoryBudget) {
        System.err.println("=== CREATING GROUPING STRATEGY ===");
        System.err.println("K (centroids): " + K);
        System.err.println("Number of partitions: " + numPartitions);
        System.err.println("Memory budget: " + memoryBudget + " frames");
        
        int centroidsPerPartition = (int) Math.ceil(1.0 * K / numPartitions);
        int numRunFiles = Math.min(numPartitions, memoryBudget);
        
        System.err.println("Centroids per partition: " + centroidsPerPartition);
        System.err.println("Number of run files: " + numRunFiles);
        
        // Create partition assignment for centroids
        Map<Integer, List<Integer>> partitionToCentroids = new HashMap<>();
        for (int i = 0; i < K; i++) {
            int partition = i % numPartitions;
            partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(i);
        }
        
        // Determine strategy based on centroids per partition
        String strategy;
        if (centroidsPerPartition > 1) {
            strategy = "GROUP_MULTIPLE_CENTROIDS";
            System.err.println("Strategy: Group multiple centroids in one run file");
        } else {
            strategy = "ONE_FRAME_PER_CENTROID";
            System.err.println("Strategy: Allocate 1 frame per centroid");
        }
        
        return new GroupingStrategy(numRunFiles, centroidsPerPartition, partitionToCentroids, strategy);
    }
    
    /**
     * Main processing flow: dummy data -> grouping -> sorting.
     * 
     * @param ctx Hyracks task context for file operations
     */
    public void processVCTreePartitioningAndSorting(IHyracksTaskContext ctx) throws HyracksDataException {
        System.err.println("=== VCTreePartitioningAndSorting: Starting main processing flow ===");
        
        try {
            // Step 1: Generate dummy data
            System.err.println("Step 1: Generating dummy data (K=10)");
            DummyData dummyData = getDummyValues();
            
            // Step 2: Recursive grouping
            System.err.println("Step 2: Starting recursive grouping");
            VCTreePartitioner partitioner = new VCTreePartitioner(ctx, 32, 32768);
            partitioner.partitionData(dummyData, 10); // K=10
            Map<Integer, FileReference> centroidFiles = partitioner.getCentroidFiles();
            
            System.err.println("Grouping complete. Created " + centroidFiles.size() + " centroid files");
            
            // Step 3: Sort each centroid file
            System.err.println("Step 3: Starting sorting phase");
            VCTreeSorter sorter = new VCTreeSorter(ctx);
            Map<Integer, FileReference> sortedFiles = new HashMap<>();
            
            for (Map.Entry<Integer, FileReference> entry : centroidFiles.entrySet()) {
                int centroidId = entry.getKey();
                FileReference inputFile = entry.getValue();
                FileReference sortedFile = sorter.sortCentroidFile(centroidId, inputFile);
                sortedFiles.put(centroidId, sortedFile);
                System.err.println("Sorted centroid " + centroidId + " to: " + sortedFile);
            }
            
            System.err.println("Sorting complete. Created " + sortedFiles.size() + " sorted files");
            
            // Step 4: Cleanup
            partitioner.closeAllFiles();
            sorter.cleanupIntermediateFiles();
            
            System.err.println("=== VCTreePartitioningAndSorting: Processing complete ===");
            
        } catch (Exception e) {
            System.err.println("ERROR: VCTreePartitioningAndSorting failed: " + e.getMessage());
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
    public RunFileManager applyRecursivePartitioning(int K, long inputDataBytesSize, int frameSize, IHyracksTaskContext ctx) {
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
        int maxLevels = 3; // Prevent infinite recursion
        
        while (level < maxLevels && strategy.strategy.equals("GROUP_MULTIPLE_CENTROIDS")) {
            System.err.println("=== RECURSIVE LEVEL " + (level + 1) + " ===");
            
            // Check if further partitioning is needed
            boolean needsMorePartitioning = runFileManager.checkSizeReduction(strategy.centroidsPerPartition, memoryBudget);
            
            if (!needsMorePartitioning) {
                System.err.println("Size reduction effective, stopping recursion at level " + level);
                break;
            }
            
            // Apply SHAPIRO again on each large partition
            for (int partitionId = 0; partitionId < strategy.numRunFiles; partitionId++) {
                List<Integer> partitionCentroids = strategy.partitionToCentroids.get(partitionId);
                if (partitionCentroids.size() > memoryBudget) {
                    System.err.println("Partition " + partitionId + " has " + partitionCentroids.size() + " centroids, sub-partitioning...");
                    
                    // Calculate sub-partitions for this partition
                    int subPartitions = calculatePartitionsUsingShapiro(
                        partitionCentroids.size(), 
                        inputDataBytesSize / strategy.numRunFiles, 
                        frameSize, 
                        memoryBudget
                    );
                    
                    // Create sub-run files for this partition
                    runFileManager.createSubRunFiles(partitionId, subPartitions);
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
                    FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                        "VCTreeRunFile_" + i + "_" + System.currentTimeMillis());
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
            // 80% size reduction rule (from Hybrid Hash Join)
            double threshold = 0.8;
            double currentRatio = (double) centroidsPerPartition / memoryBudget;
            
            System.err.println("Size reduction check: ratio=" + currentRatio + ", threshold=" + threshold);
            
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
            return String.format("RunFileManager{totalFiles=%d, subFiles=%s}", 
                totalRunFiles, subRunFiles);
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
            return String.format("GroupingStrategy{runFiles=%d, centroidsPerPartition=%d, strategy=%s}", 
                numRunFiles, centroidsPerPartition, strategy);
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
            int tuplesPerCentroid = 5 + (int)(Math.random() * 6); // 5-10 tuples per centroid
            
            for (int tupleIdx = 0; tupleIdx < tuplesPerCentroid; tupleIdx++) {
                DummyTuple tuple = new DummyTuple();
                tuple.level = 0; // All at level 0 for simplicity
                tuple.clusterId = centroidId % 3; // Distribute across 3 clusters
                tuple.centroidId = centroidId; // Same centroid ID for all tuples in this group
                tuple.embedding = generateDummyEmbedding(128); // 128-dimensional vector
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
            return String.format("DummyData{distances=%d, centroidIds=%d, tuples=%d}", 
                distances.size(), centroidIds.size(), tuples.size());
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
                level, clusterId, centroidId, 
                embedding != null ? embedding.length : 0, distance);
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
                dummyTuple.centroidId, 
                dummyTuple.embedding != null ? dummyTuple.embedding.length : 0,
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
        private VCTreeStaticStructureCreator structureCreator;
                private MaterializerTaskState materializedData;

        @Override
        public void open() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity OPENING ===");
            try {
                        // Register permit state for coordination
                IterationPermitState permitState =
                        (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));

                if (permitState == null) {
                    java.util.concurrent.Semaphore permit = new java.util.concurrent.Semaphore(0);
                    permitState = new IterationPermitState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(permitUUID, partition), permit);
                    ctx.setStateObject(permitState);
                    System.err.println("✅ PERMIT STATE CREATED AND REGISTERED for UUID: " + permitUUID + ", partition: "
                            + partition);
                }

                        // Initialize materialized data state
                        materializedData = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                                new PartitionedUUID(materializedDataUUID, partition));
                        materializedData.open(ctx);

                // Initialize evaluators for extracting tuple fields
                EvaluatorContext evalCtx = new EvaluatorContext(ctx);
                levelEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(evalCtx);
                clusterIdEval = new ColumnAccessEvalFactory(1).createScalarEvaluator(evalCtx);
                centroidIdEval = new ColumnAccessEvalFactory(2).createScalarEvaluator(evalCtx);

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
                    materializedData.appendFrame(buffer);
        }

        private void processTuple(ITupleReference tuple) throws HyracksDataException {
            try {
                // Extract tuple data for structure analysis
                FrameTupleReference frameTuple = (FrameTupleReference) tuple;
                levelEval.evaluate(frameTuple, levelVal);
                clusterIdEval.evaluate(frameTuple, clusterIdVal);
                centroidIdEval.evaluate(frameTuple, centroidIdVal);

                int level = AInt32SerializerDeserializer.getInt(levelVal.getByteArray(), levelVal.getStartOffset() + 1);
                int clusterId = AInt32SerializerDeserializer.getInt(clusterIdVal.getByteArray(),
                        clusterIdVal.getStartOffset() + 1);
                int centroidId = AInt32SerializerDeserializer.getInt(centroidIdVal.getByteArray(),
                        centroidIdVal.getStartOffset() + 1);
                
                // Debug: Log suspicious level values and filter them out
                if (level < 0 || level > 100) {
                    System.err.println("🔍 DEBUG: Suspicious level value: " + level + " (clusterId: " + clusterId + ", centroidId: " + centroidId + ") - IGNORING");
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

        @Override
        public void close() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity CLOSING ===");
            System.err.println("Total tuples collected: " + tupleCount);
            System.err.println("Frames accumulated: " + frameAccumulator.size());

            // Test partitioning and sorting with dummy data
            try {
                System.err.println("=== TESTING PARTITIONING AND SORTING ===");
                processVCTreePartitioningAndSorting(ctx);
                System.err.println("=== PARTITIONING AND SORTING COMPLETE ===");
            } catch (Exception e) {
                System.err.println("ERROR: Partitioning/sorting failed: " + e.getMessage());
                e.printStackTrace();
            }

            // Process all accumulated tuples and create static structure
                System.err.println("=== STARTING HIERARCHICAL CLUSTERING ANALYSIS ===");
                System.err.println("Analyzing " + tupleCount + " tuples to determine structure...");
                createStaticStructure();
                System.err.println("=== HIERARCHICAL CLUSTERING ANALYSIS COMPLETE ===");

                    // Close materialized data
                    if (materializedData != null) {
                        materializedData.close();
                        ctx.setStateObject(materializedData);
            }

            // Signal Branch 2 that structure creation is complete
            try {
                System.err.println("=== SIGNALING BRANCH 2 COMPLETION ===");
                IterationPermitState permitState =
                        (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));
                if (permitState != null) {
                    permitState.getPermit().release();
                    System.err
                            .println("✅ PERMIT RELEASED - Branch 1 (structure creation) completed, signaling Branch 2");
                } else {
                    System.err.println("WARNING: Permit state not found for UUID: " + permitUUID);
                }
            } catch (Exception e) {
                System.err.println("ERROR: Failed to release permit: " + e.getMessage());
                e.printStackTrace();
            }

                    System.err.println("=== CreateStructureActivity COMPLETE ===");
        }

        private void createStaticStructure() throws HyracksDataException {
            System.err.println("=== CREATING STATIC STRUCTURE WITH VCTreeStaticStructureCreator ===");
            System.err.println("Processing " + frameAccumulator.size() + " accumulated frames");
            System.err.println("Total tuples to process: " + tupleCount);

            try {
                // Analyze collected data to determine structure
                System.err.println("Analyzing collected data to determine hierarchical structure...");
                List<Integer> clustersPerLevel = new ArrayList<>();
                List<List<Integer>> centroidsPerCluster = new ArrayList<>();

                if (levelDistribution != null && !levelDistribution.isEmpty()) {
                    int maxLevel = levelDistribution.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
                    System.err.println("🔍 DEBUG: Raw maxLevel from levelDistribution: " + maxLevel);
                    System.err.println("🔍 DEBUG: levelDistribution keys: " + levelDistribution.keySet());
                    
                    // Safety check: limit maxLevel to prevent infinite loops
                    if (maxLevel > 1000) {
                        System.err.println("❌ WARNING: maxLevel is too large (" + maxLevel + "), limiting to 10 to prevent infinite loop");
                        maxLevel = 10;
                    }
                    
                    System.err.println("Found " + maxLevel + " levels in the hierarchical structure");

                    int loopCounter = 0;
                    for (int level = 0; level <= maxLevel; level++) {
                        loopCounter++;
                        System.err.println("🔍 DEBUG: Processing level " + level + " (loop iteration " + loopCounter + ")");
                        
                        // Safety check to prevent infinite loops
                        if (loopCounter > 100) {
                            System.err.println("❌ CRITICAL: Loop counter exceeded 100, breaking to prevent infinite loop");
                            break;
                        }
                        
                        String levelKey = "Level_" + level;
                        Map<Integer, Integer> levelClusters = clusterDistribution.get(levelKey);

                        if (levelClusters != null) {
                            int clusterCount = levelClusters.size();
                            clustersPerLevel.add(clusterCount);
                            System.err.println("Level " + level + ": " + clusterCount + " clusters");

                            List<Integer> centroidsInClusters = new ArrayList<>();
                            for (int clusterId : levelClusters.keySet()) {
                                int centroidCount = levelClusters.get(clusterId);
                                centroidsInClusters.add(centroidCount);
                                System.err.println("  Cluster " + clusterId + ": " + centroidCount + " centroids");
                            }
                            centroidsPerCluster.add(centroidsInClusters);
                        } else {
                            clustersPerLevel.add(0);
                            centroidsPerCluster.add(new ArrayList<>());
                            System.err.println("Level " + level + ": 0 clusters (no data)");
                        }
                    }
                } else {
                    // Default structure if no data collected
                    System.err.println("No level distribution found, using default structure");
                    clustersPerLevel.add(1);
                    centroidsPerCluster.add(List.of(1));
                }

                // Get infrastructure
                INcApplicationContext appCtx =
                        (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
                IBufferCache bufferCache = appCtx.getBufferCache();
                IIOManager ioManager = ctx.getIoManager();

                // Get index path
                System.err.println("Getting index file path...");
                FileReference indexPathRef = getIndexFilePath();
                if (indexPathRef == null) {
                    System.err.println("ERROR: Could not determine index path");
                    return;
                }
                System.err.println("Index path: " + indexPathRef);

                // Create static structure file path (same directory as .metadata)
                FileReference staticStructureFile = indexPathRef.getParent().getChild("static_structure.vctree");
                System.err.println("Static structure file path: " + staticStructureFile);

                // Create tuple writers with proper type traits
                // Tuple format: [centroidId (int), embedding (float[]), childPageId (int)]
                ITypeTraits[] typeTraits = new ITypeTraits[3];
                typeTraits[0] = IntegerPointable.TYPE_TRAITS; // centroidId
                typeTraits[1] = VarLengthTypeTrait.INSTANCE; // embedding (float array) - variable length
                typeTraits[2] = IntegerPointable.TYPE_TRAITS; // childPageId

                VectorClusteringLeafTupleWriterFactory leafTupleWriterFactory =
                        new VectorClusteringLeafTupleWriterFactory(typeTraits, null, null);
                VectorClusteringInteriorTupleWriterFactory interiorTupleWriterFactory =
                        new VectorClusteringInteriorTupleWriterFactory(typeTraits, null, null);

                // Create frames with proper tuple writers
                VectorClusteringLeafFrameFactory leafFrameFactory =
                        new VectorClusteringLeafFrameFactory(leafTupleWriterFactory.createTupleWriter(), 128);
                VectorClusteringInteriorFrameFactory interiorFrameFactory =
                        new VectorClusteringInteriorFrameFactory(interiorTupleWriterFactory.createTupleWriter(), 128);
                ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
                ITreeIndexFrame interiorFrame = interiorFrameFactory.createFrame();
                ITreeIndexMetadataFrame metaFrame =
                        new AppendOnlyLinkedMetadataPageManager(bufferCache, new LIFOMetaDataFrameFactory())
                                .createMetadataFrame();

                // Create static structure creator
                // Use the built-in no-op page write callback
                IPageWriteCallback pageWriteCallback = NoOpPageWriteCallback.INSTANCE;

                System.err.println(
                        "Creating VCTreeStaticStructureCreator with " + clustersPerLevel.size() + " levels...");
                structureCreator = new VCTreeStaticStructureCreator(bufferCache, ioManager, staticStructureFile, 4096,
                        fillFactor, leafFrame, interiorFrame, metaFrame, pageWriteCallback, clustersPerLevel.size(),
                        clustersPerLevel, centroidsPerCluster, maxEntriesPerPage);

                System.err.println("Processing " + frameAccumulator.size() + " accumulated frames...");
                // Process all accumulated tuples
                int totalTuplesProcessed = 0;
                for (ByteBuffer frameBuffer : frameAccumulator) {
                            FrameTupleAccessor frameFta = new FrameTupleAccessor(outRecDescs[0]);
                    frameFta.reset(frameBuffer);

                    for (int i = 0; i < frameFta.getTupleCount(); i++) {
                        tuple.reset(frameFta, i);
                        structureCreator.add(tuple);
                        totalTuplesProcessed++;
                    }
                }
                System.err.println("Processed " + totalTuplesProcessed + " tuples for static structure creation");

                System.err.println("Finalizing static structure...");
                // Finalize the structure
                structureCreator.finalize();
                System.err.println("✅ STATIC STRUCTURE FINALIZED SUCCESSFULLY");

                System.err.println("Writing metadata file...");
                // Write metadata file
                writeMetadataFile(indexPathRef, clustersPerLevel, centroidsPerCluster);
                System.err.println("✅ METADATA FILE WRITTEN SUCCESSFULLY");

                System.err.println(
                        "✅ STATIC STRUCTURE CREATED SUCCESSFULLY at: " + staticStructureFile.getAbsolutePath());

            } catch (Exception e) {
                System.err.println("ERROR: Failed to create static structure: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        private void writeMetadataFile(FileReference indexPathRef, List<Integer> clustersPerLevel,
                List<List<Integer>> centroidsPerCluster) {
            try {
                // Write to index subdirectory to match where VCTreeStaticStructureReader expects it
                FileReference metadataFile = indexPathRef.getChild(".staticstructure");
                System.err.println("Writing metadata file to: " + metadataFile.getAbsolutePath());

                Map<String, Object> metadata = new HashMap<>();
                metadata.put("numLevels", clustersPerLevel.size());
                metadata.put("clustersPerLevel", clustersPerLevel);
                metadata.put("centroidsPerCluster", centroidsPerCluster);
                metadata.put("maxEntriesPerPage", maxEntriesPerPage);
                metadata.put("totalTuplesProcessed", tupleCount);
                metadata.put("timestamp", System.currentTimeMillis());
                metadata.put("partition", partition);
                metadata.put("staticStructureFile", "static_structure.vctree");

                System.err.println("Metadata content: " + metadata);
                ObjectMapper mapper = new ObjectMapper();
                byte[] data = mapper.writeValueAsBytes(metadata);

                IIOManager ioManager = ctx.getIoManager();
                ioManager.overwrite(metadataFile, data);

                System.err.println("✅ METADATA FILE WRITTEN SUCCESSFULLY to: " + metadataFile.getAbsolutePath());

            } catch (Exception e) {
                System.err.println("ERROR: Failed to write metadata file: " + e.getMessage());
                e.printStackTrace();
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
                    structureCreator.close();
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to close structure creator: " + e.getMessage());
                }
            }

                    if (materializedData != null) {
                        try {
                            materializedData.close();
                        } catch (Exception e) {
                            System.err.println("ERROR: Failed to close materialized data: " + e.getMessage());
                        }
                    }
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
                        System.err.println("=== INITIALIZING LSM Bulk Loader ===");
                        
                        // Get index helper and open it
                        indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                        indexHelper.open();
                        lsmIndex = (ILSMIndex) indexHelper.getIndexInstance();
                        
                        // Create LSM bulk loader with VCTree-specific parameters
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("numLevels", 2);
                        parameters.put("clustersPerLevel", List.of(5, 10));
                        parameters.put("centroidsPerCluster", List.of(List.of(10), List.of(10)));
                        parameters.put("maxEntriesPerPage", 100);
                        parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID, 
                            LSMComponentId.DEFAULT_COMPONENT_ID);
                        
                        lsmBulkLoader = (LSMIndexDiskComponentBulkLoader) lsmIndex.createBulkLoader(
                            fillFactor, false, 0, false, parameters);
                        
                        System.err.println("✅ LSM Bulk Loader initialized successfully");
                        
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to initialize LSM Bulk Loader: " + e.getMessage());
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
}
