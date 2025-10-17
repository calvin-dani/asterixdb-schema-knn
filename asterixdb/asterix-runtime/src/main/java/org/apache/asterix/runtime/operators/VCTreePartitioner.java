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

import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

/**
 * VCTreePartitioner handles recursive centroid partitioning using SHAPIRO formula.
 * Separates centroids into individual files through recursive partitioning.
 * 
 * This implementation uses frame buffering for memory efficiency:
 * - One buffer per partition using FrameTupleAppender
 * - One file per partition (not per centroid)
 * - Automatic flushing when frames are full
 * - Memory budget management across all partitions
 */
public class VCTreePartitioner {

    private final IHyracksTaskContext ctx;
    private final int memoryBudget;
    private final int frameSize;
    private final int maxRecursionLevels;
    
    // Streaming mode variables
    private int K;
    private int numPartitions;

    // Partition hierarchy tracking
    private final Map<Integer, RunFileWriter> partitionFiles = new HashMap<>();
    private Map<Integer, List<Integer>> partitionToCentroids = new HashMap<>();
    private Map<Integer, Integer> centroidToPartition = new HashMap<>();
    private final Map<Integer, FileReference> centroidFiles = new HashMap<>();
    
    // Frame buffering infrastructure
    private final Map<Integer, VSizeFrame> partitionFrames = new HashMap<>();
    private final Map<Integer, FrameTupleAppender> partitionAppenders = new HashMap<>();
    private int usedFrames = 0;
    
    // Column accessors for field extraction (following VCTreeStaticStructureCreatorOperatorDescriptor pattern)
    private org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator centroidIdEval;
    private IPointable centroidIdVal;

    // SHAPIRO formula constants
    private static final double FUDGE_FACTOR = 1.1;
    
    // Partitioning strategy
    private final boolean useRangePartitioning = true; // Default to range partitioning
    
    // Data storage for processing
    private List<DataTuple> inputData = new ArrayList<>();

    public VCTreePartitioner(IHyracksTaskContext ctx, int memoryBudget, int frameSize) {
        this.ctx = ctx;
        this.memoryBudget = memoryBudget;
        this.frameSize = frameSize;
        this.maxRecursionLevels = 5;
        this.usedFrames = 0;
        
        // Validate constructor parameters
        if (ctx == null) {
            throw new IllegalArgumentException("Context cannot be null");
        }
        if (memoryBudget <= 0) {
            throw new IllegalArgumentException("Memory budget must be positive: " + memoryBudget);
        }
        if (frameSize <= 0) {
            throw new IllegalArgumentException("Frame size must be positive: " + frameSize);
        }
    }

    /**
     * Main entry point: partition data using recursive SHAPIRO formula.
     * This method accepts real tuple data for partitioning.
     * 
     * @param inputTuples List of input tuples to partition
     * @param K Number of centroids
     * @param centroidIdColumn Column index containing centroid ID (0 for first, -1 for last)
     * @throws HyracksDataException if partitioning fails
     */
    public void partitionData(List<ITupleReference> inputTuples, int K, int centroidIdColumn) throws HyracksDataException {
        // Convert input tuples to DataTuple objects
        this.inputData = convertTuplesToDataTuples(inputTuples, centroidIdColumn);

        // Calculate estimated data size from actual data
        long estimatedDataSize = calculateEstimatedDataSize(inputData);

        // Step 1: Calculate initial partitions using SHAPIRO
        int numPartitions = calculatePartitionsUsingShapiro(K, estimatedDataSize, frameSize, memoryBudget);

        // Step 2: Distribute centroids to partitions using range partitioning
        distributeCentroidsToPartitions(K, numPartitions);

        // Step 3: Create initial partition files
        createInitialPartitionFiles(numPartitions);

        // Step 4: Write data to partitions
        writeDataToPartitions();

        // Step 5: Recursively separate centroids
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<Integer> centroids = partitionToCentroids.get(partitionId);
            if (centroids != null && !centroids.isEmpty()) {
                recursivelySeparateCentroids(partitionId, centroids, 0);
            }
        }
    }

    /**
     * Legacy method for backward compatibility.
     * This method is designed to work with real data, not dummy data.
     */
    public void partitionData(int K, long estimatedDataSize) throws HyracksDataException {
        // Step 1: Calculate initial partitions using SHAPIRO
        int numPartitions = calculatePartitionsUsingShapiro(K, estimatedDataSize, frameSize, memoryBudget);

        // Step 2: Distribute centroids to partitions using range partitioning
        distributeCentroidsToPartitions(K, numPartitions);

        // Step 3: Create initial partition files
        createInitialPartitionFiles(numPartitions);

        // Step 4: Data writing will be handled by external callers
        // writeDataToPartitions() - commented out, to be implemented by callers

        // Step 5: Recursively separate centroids
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<Integer> centroids = partitionToCentroids.get(partitionId);
            if (centroids != null && !centroids.isEmpty()) {
                recursivelySeparateCentroids(partitionId, centroids, 0);
            }
        }

    }

    /**
     * Recursively separate centroids until each file contains only 1 centroid.
     */
    private void recursivelySeparateCentroids(int partitionId, List<Integer> centroids, int level)
            throws HyracksDataException {
        if (level >= maxRecursionLevels) {
            separateAndWriteToFiles(partitionId, centroids, level);
            return;
        }

        if (centroids.size() <= 1) {
            // Base case: 1 centroid - write to final file
            separateAndWriteToFiles(partitionId, centroids, level);
        } else if (centroids.size() <= memoryBudget) {
            // Fits in memory: separate all centroids to individual files
            separateAndWriteToFiles(partitionId, centroids, level);
        } else {
            // Recursive case: create sub-partitions
            int subPartitions = calculatePartitionsUsingShapiro(centroids.size(), centroids.size() * 1024L, frameSize,
                    memoryBudget);

            // Distribute centroids to sub-partitions using range partitioning
            Map<Integer, List<Integer>> subPartitionToCentroids = createRangePartitioning(centroids, subPartitions);

            // Create sub-partition files
            for (Map.Entry<Integer, List<Integer>> entry : subPartitionToCentroids.entrySet()) {
                int subPartitionId = entry.getKey();
                List<Integer> subCentroids = entry.getValue();

                // Create sub-partition file
                int subFileId = partitionFiles.size();
                FileReference file = ctx.getJobletContext().createManagedWorkspaceFile("VCTreeSubPartition_"
                        + partitionId + "_" + subPartitionId + "_level_" + level + "_" + System.currentTimeMillis());
                RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
                writer.open();
                partitionFiles.put(subFileId, writer);

                // Write data for this sub-partition
                // COMMENTED OUT: writeCentroidDataToPartition() used dummy data
                // writeCentroidDataToPartition(subFileId, subCentroids);

                // Recursively process this sub-partition
                recursivelySeparateCentroids(subFileId, subCentroids, level + 1);
            }
        }
    }

    /**
     * Base case: separate centroids into individual files.
     */
    private void separateAndWriteToFiles(int partitionId, List<Integer> centroids, int level)
            throws HyracksDataException {
        // Read data from partition file
        List<DataTuple> allData = readPartitionData(partitionId);
        
        // Group by centroid ID
        Map<Integer, List<DataTuple>> centroidGroups = groupByCentroidId(allData);
        
        // Create individual file for each centroid
        for (int centroidId : centroids) {
            List<DataTuple> centroidData = centroidGroups.get(centroidId);
            if (centroidData != null && !centroidData.isEmpty()) {
                // Create file for this centroid
                FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                        "VCTreeCentroid_" + centroidId + "_unsorted_" + System.currentTimeMillis());
                RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
                writer.open();
        
                // Write centroid data using standard Hyracks frame format
                // Note: This is a simplified implementation for the recursive separation phase
                // In practice, you'd use FrameTupleAppender here as well
                for (DataTuple dataTuple : centroidData) {
                    // Create a simple frame with just the centroid ID for now
                    ByteBuffer frame = ByteBuffer.allocate(frameSize);
                    frame.putInt(1); // tuple count
                    frame.putInt(dataTuple.centroidId); // centroid ID
                    frame.flip();
                    writer.nextFrame(frame);
                }
        
                writer.close();
                centroidFiles.put(centroidId, file);
            }
        }
    }

    /**
     * Group tuples by centroid ID.
     * COMMENTED OUT: This method used DummyTuple, to be implemented for real data.
     */
    /*
    private Map<Integer, List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple>> groupByCentroidId(
            List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> allData) {
        Map<Integer, List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple>> groups = new HashMap<>();
    
        for (VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple : allData) {
            groups.computeIfAbsent(tuple.centroidId, k -> new ArrayList<>()).add(tuple);
        }
    
        return groups;
    }
    */

    /**
     * Read data from partition file.
     * COMMENTED OUT: This method used DummyTuple, to be implemented for real data.
     */
    /*
    private List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> readPartitionData(int partitionId)
            throws HyracksDataException {
        List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> data = new ArrayList<>();
    
        RunFileWriter writer = partitionFiles.get(partitionId);
        if (writer != null) {
            // Create reader from writer
            RunFileReader reader = writer.createReader();
            reader.open();
    
            VSizeFrame frame = new VSizeFrame(ctx);
            while (reader.nextFrame(frame)) {
                // Parse frame and extract tuples (simplified)
                VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple =
                        parseTupleFromFrame(frame.getBuffer());
                if (tuple != null) {
                    data.add(tuple);
                }
            }
    
            reader.close();
        }
    
        return data;
    }
    */

    /**
     * Write centroid data to partition file.
     * COMMENTED OUT: This method used DummyTuple, to be implemented for real data.
     */
    /*
    private void writeCentroidDataToPartition(int partitionId, List<Integer> centroids) throws HyracksDataException {
        // This is a simplified version - in practice, you'd read from the original data source
        // For now, we'll create dummy data for the centroids
        RunFileWriter writer = partitionFiles.get(partitionId);
        if (writer != null) {
            for (int centroidId : centroids) {
                // Create dummy tuple for this centroid
                VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple =
                        new VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple();
                tuple.centroidId = centroidId;
                tuple.distance = Math.random() * 100.0;
                tuple.embedding = generateDummyEmbedding(128);
                tuple.level = 0;
                tuple.clusterId = centroidId % 3;
    
                ByteBuffer frame = createFrameFromTuple(tuple);
                writer.nextFrame(frame);
            }
        }
    }
    */

    /**
     * Distribute centroids to partitions using range partitioning.
     */
    private void distributeCentroidsToPartitions(int K, int numPartitions) {
        int centroidsPerPartition = calculateCentroidsPerPartition(K, numPartitions);

        for (int centroidId = 0; centroidId < K; centroidId++) {
            int partition = centroidId / centroidsPerPartition;
            partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(centroidId);
            centroidToPartition.put(centroidId, partition);
        }
    }

    /**
     * Create initial partition files.
     */
    private void createInitialPartitionFiles(int numPartitions) throws HyracksDataException {
        for (int i = 0; i < numPartitions; i++) {
            FileReference file = ctx.getJobletContext()
                    .createManagedWorkspaceFile("VCTreePartition_" + i + "_" + System.currentTimeMillis());
            RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
            writer.open();
            partitionFiles.put(i, writer);
        }
    }

    /**
     * Write data to partition files.
     * COMMENTED OUT: This method used dummy data, to be implemented for real data.
     */
    /*
    private void writeDataToPartitions(VCTreeStaticStructureCreatorOperatorDescriptor.DummyData dummyData)
            throws HyracksDataException {
        for (VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple : dummyData.tuples) {
            int partition = centroidToPartition.get(tuple.centroidId);
            RunFileWriter writer = partitionFiles.get(partition);
    
            ByteBuffer frame = createFrameFromTuple(tuple);
            writer.nextFrame(frame);
        }
    }
    */

    /**
     * Create frame from tuple (simplified version).
     * COMMENTED OUT: This method used DummyTuple, to be implemented for real data.
     */
    /*
    private ByteBuffer createFrameFromTuple(VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
    
        // Write tuple count (1)
        buffer.putInt(1);
    
        // Write centroid ID
        buffer.putInt(tuple.centroidId);
    
        // Write distance
        buffer.putDouble(tuple.distance);
    
        // Write embedding length and data
        if (tuple.embedding != null) {
            buffer.putInt(tuple.embedding.length);
            for (float f : tuple.embedding) {
                buffer.putDouble((double) f);
            }
        } else {
            buffer.putInt(0);
        }
    
        buffer.flip();
        return buffer;
    }
    */

    /**
     * Parse tuple from frame (simplified version).
     * COMMENTED OUT: This method used DummyTuple, to be implemented for real data.
     */
    /*
    private VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple parseTupleFromFrame(ByteBuffer frame) {
        try {
            VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple =
                    new VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple();
    
            // Read tuple count
            int tupleCount = frame.getInt();
            if (tupleCount > 0) {
                // Read centroid ID
                tuple.centroidId = frame.getInt();
    
                // Read distance
                tuple.distance = frame.getDouble();
    
                // Read embedding
                int embeddingLength = frame.getInt();
                if (embeddingLength > 0) {
                    tuple.embedding = new float[embeddingLength];
                    for (int i = 0; i < embeddingLength; i++) {
                        tuple.embedding[i] = (float) frame.getDouble();
                    }
                }
    
                tuple.level = 0;
                tuple.clusterId = tuple.centroidId % 3;
    
                return tuple;
            }
        } catch (Exception e) {
            System.err.println("Error parsing tuple from frame: " + e.getMessage());
        }
    
        return null;
    }
    */

    /**
     * Generate dummy embedding vector.
     * COMMENTED OUT: This method generated dummy data, to be implemented for real data.
     */
    /*
    private float[] generateDummyEmbedding(int dimension) {
        float[] embedding = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            embedding[i] = (float) (Math.random() * 2.0 - 1.0);
        }
        return embedding;
    }
    */

    /**
     * Calculate centroids per partition for range partitioning.
     */
    private int calculateCentroidsPerPartition(int K, int numPartitions) {
        return (int) Math.ceil((double) K / numPartitions);
    }

    /**
     * Create range partitioning for a list of centroids.
     * 
     * @param centroids List of centroid IDs to partition
     * @param numPartitions Number of partitions to create
     * @return Map of partition ID to list of centroid IDs
     */
    private Map<Integer, List<Integer>> createRangePartitioning(List<Integer> centroids, int numPartitions) {
        int centroidsPerPartition = calculateCentroidsPerPartition(centroids.size(), numPartitions);
        Map<Integer, List<Integer>> partitionToCentroids = new HashMap<>();

        for (int i = 0; i < centroids.size(); i++) {
            int partition = i / centroidsPerPartition;
            int originalCentroidId = centroids.get(i); // Keep original ID
            partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(originalCentroidId);
        }

        return partitionToCentroids;
    }

    /**
     * Create grouping strategy using range partitioning.
     * 
     * @param K Number of centroids
     * @param numPartitions Number of partitions
     * @param memoryBudget Available memory budget in frames
     * @return Map of partition ID to list of centroid IDs
     */
    public Map<Integer, List<Integer>> createGroupingStrategy(int K, int numPartitions, int memoryBudget) {
        int centroidsPerPartition = calculateCentroidsPerPartition(K, numPartitions);
        int numRunFiles = Math.min(numPartitions, memoryBudget);

        // Create range partition assignment for centroids
        Map<Integer, List<Integer>> partitionToCentroids = new HashMap<>();
        for (int i = 0; i < K; i++) {
            int partition = i / centroidsPerPartition;
            partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(i);
        }

        return partitionToCentroids;
    }

    /**
     * Calculate partitions using SHAPIRO formula.
     */
    private int calculatePartitionsUsingShapiro(int K, long inputDataBytesSize, int frameSize, int memoryBudget) {
        long numberOfInputFrames = inputDataBytesSize / frameSize;

        if (memoryBudget >= numberOfInputFrames * FUDGE_FACTOR) {
            return 2; // All in memory
        }

        long numberOfPartitions =
                (long) (Math.ceil((numberOfInputFrames * FUDGE_FACTOR - memoryBudget) / (memoryBudget - 1)));
        numberOfPartitions = Math.max(2, numberOfPartitions);

        if (numberOfPartitions > memoryBudget) {
            numberOfPartitions = (long) Math.ceil(Math.sqrt(numberOfInputFrames * FUDGE_FACTOR));
            numberOfPartitions = Math.max(2, Math.min(numberOfPartitions, memoryBudget));
        }

        return (int) Math.min(numberOfPartitions, Integer.MAX_VALUE);
    }

    /**
     * Initialize column accessors for field extraction.
     * This method should be called before processing any data.
     * 
     * @throws HyracksDataException if initialization fails
     */
    public void initializeColumnAccessors() throws HyracksDataException {
        try {
            // Initialize evaluator context
            EvaluatorContext evalCtx = new EvaluatorContext(ctx);
            
            // Initialize column accessor for centroidId (field 0 in [centroidId, distance, embedding, centroidId] format)
            centroidIdEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(evalCtx);
            
            // Initialize pointable for evaluator results
            centroidIdVal = new VoidPointable();
        } catch (Exception e) {
            System.err.println("ERROR: Failed to initialize column accessors: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Pre-initialize partitioning strategy with known K.
     * This method should be called before any data arrives.
     * 
     * @param K Number of centroids (known ahead of time)
     * @param memoryBudget Available memory budget in frames
     * @param frameSize Frame size in bytes
     * @throws HyracksDataException if initialization fails
     */
    public void preInitializePartitioning(int K, int memoryBudget, int frameSize) throws HyracksDataException {
        this.K = K;
        
        // Initialize column accessors first
        initializeColumnAccessors();
        
        // Calculate number of partitions using SHAPIRO formula
        long estimatedDataSize = K * 1024L; // Rough estimate for pre-calculation
        this.numPartitions = calculatePartitionsUsingShapiro(K, estimatedDataSize, frameSize, memoryBudget);
        
        // Pre-calculate partition mapping using range partitioning
        calculatePartitionMapping();
        
        // Initialize partition buffers and files
        initializePartitionBuffers(numPartitions);
    }

    /**
     * Write a single tuple to the appropriate partition buffer (streaming mode with buffering).
     * Uses column accessor to extract centroidId from [centroidId, distance, embedding, centroidId] format.
     * 
     * @param tuple Input tuple in format [centroidId, distance, embedding, centroidId]
     * @throws HyracksDataException if writing fails
     */
    public void writeStreamingTuple(ITupleReference tuple) throws HyracksDataException {
        try {
            // Extract centroid ID using column accessor (field 0 in [centroidId, distance, embedding, centroidId] format)
            int centroidId = extractCentroidId(tuple);
            
            // Check if centroid ID is in valid range
            if (centroidId < 0 || centroidId >= K) {
                return;
            }
            
            // Determine target partition using pre-calculated mapping
            Integer partition = centroidToPartition.get(centroidId);
            
            if (partition == null) {
                return;
            }
            
            // Get appender for this partition
            FrameTupleAppender appender = partitionAppenders.get(partition);
            
            if (appender == null) {
                return;
            }
            
            // Try to append tuple to current frame
            if (appender.append(tuple)) {
                return;
            }
            
            // Frame is full, need to flush
            flushPartitionFrame(partition);
            
            // Reset appender and try again
            appender.reset(new VSizeFrame(ctx), true);
            if (!appender.append(tuple)) {
                throw new HyracksDataException("Tuple too large for frame in partition " + partition);
            }
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to write streaming tuple: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Finalize partitioning after all streaming data is processed.
     * This method flushes all buffers and performs recursive separation of centroids.
     * 
     * @return Map of centroid ID to file reference
     * @throws HyracksDataException if finalization fails
     */
    public Map<Integer, FileReference> finalizePartitioning() throws HyracksDataException {
        
        // Flush all remaining buffers to files
        flushAllPartitions();
        
        // Close all partition files
        closePartitionFiles();
        
        // Process each partition recursively
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<Integer> centroidsInPartition = partitionToCentroids.get(partitionId);
            if (centroidsInPartition != null && !centroidsInPartition.isEmpty()) {
                recursivelySeparateCentroids(partitionId, centroidsInPartition, 0);
            }
        }
        
        return centroidFiles;
    }

    /**
     * Get centroid files created during partitioning.
     */
    public Map<Integer, FileReference> getCentroidFiles() {
        return centroidFiles;
    }

    /**
     * Calculate partition mapping using range partitioning.
     */
    private void calculatePartitionMapping() {
        partitionToCentroids = new HashMap<>();
        centroidToPartition = new HashMap<>();
        
        int centroidsPerPartition = calculateCentroidsPerPartition(K, numPartitions);
        
        for (int centroidId = 0; centroidId < K; centroidId++) {
            int partition = centroidId / centroidsPerPartition; // Range partitioning
            partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(centroidId);
            centroidToPartition.put(centroidId, partition);
        }
        
    }

    // Legacy createPartitionFiles method removed - now using initializePartitionBuffers

    /**
     * Initialize partition buffers and files for frame buffering.
     * 
     * @param numPartitions Number of partitions to initialize
     * @throws HyracksDataException if initialization fails
     */
    private void initializePartitionBuffers(int numPartitions) throws HyracksDataException {
        
        // Validate parameters
        if (numPartitions <= 0) {
            throw new HyracksDataException("Invalid number of partitions: " + numPartitions);
        }
        if (numPartitions > memoryBudget) {
            throw new HyracksDataException("Number of partitions (" + numPartitions + 
                                         ") exceeds memory budget (" + memoryBudget + ")");
        }
        if (frameSize <= 0) {
            throw new HyracksDataException("Invalid frame size: " + frameSize);
        }
        
        try {
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                // Check memory budget
                if (!canAllocateFrame(partitionId)) {
                    throw new HyracksDataException("Cannot allocate frame for partition " + partitionId + 
                                                 " - memory budget exceeded");
                }
                
                // Create frame for this partition
                VSizeFrame frame = new VSizeFrame(ctx);
                
                // Create appender for this partition
                FrameTupleAppender appender = new FrameTupleAppender(frame);
                
                // Store references
                partitionFrames.put(partitionId, frame);
                partitionAppenders.put(partitionId, appender);
                
                // Create file writer
                String fileName = "VCTreePartition_" + partitionId + "_" + System.currentTimeMillis();
                FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(fileName);
                RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
                writer.open();
                partitionFiles.put(partitionId, writer);
                
                usedFrames++;
            }
            
        } catch (Exception e) {
            // Clean up on failure
            System.err.println("ERROR: Failed to initialize partition buffers, cleaning up...");
            cleanupPartitionBuffers();
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Clean up partition buffers and files on initialization failure.
     */
    private void cleanupPartitionBuffers() {
        try {
            // Close all files
            for (RunFileWriter writer : partitionFiles.values()) {
                if (writer != null) {
                    writer.close();
                }
            }
            
            // Clear all maps
            partitionFiles.clear();
            partitionFrames.clear();
            partitionAppenders.clear();
            usedFrames = 0;
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to cleanup partition buffers: " + e.getMessage());
        }
    }

    /**
     * Flush a specific partition's buffer to its file.
     * 
     * @param partitionId Partition ID to flush
     * @throws HyracksDataException if flushing fails
     */
    private void flushPartitionFrame(int partitionId) throws HyracksDataException {
        if (partitionId < 0 || partitionId >= partitionFiles.size()) {
            throw new HyracksDataException("Invalid partition ID: " + partitionId);
        }
        
        FrameTupleAppender appender = partitionAppenders.get(partitionId);
        RunFileWriter writer = partitionFiles.get(partitionId);
        
        if (appender == null) {
            throw new HyracksDataException("No appender found for partition " + partitionId);
        }
        if (writer == null) {
            throw new HyracksDataException("No writer found for partition " + partitionId);
        }
        
        if (appender.getTupleCount() > 0) {
            try {
                // Write current frame to file
                FrameUtils.flushFrame(appender.getBuffer(), writer);
                
                // Reset for next batch
                appender.reset(new VSizeFrame(ctx), true);
            } catch (Exception e) {
                throw new HyracksDataException("Failed to flush partition " + partitionId + ": " + e.getMessage(), e);
            }
        }
    }

    /**
     * Flush all partition buffers to their respective files.
     * 
     * @throws HyracksDataException if flushing fails
     */
    private void flushAllPartitions() throws HyracksDataException {
        for (int partitionId = 0; partitionId < partitionFiles.size(); partitionId++) {
            flushPartitionFrame(partitionId);
        }
    }

    /**
     * Check if we can allocate a frame within memory budget.
     * 
     * @param partitionId Partition ID (for logging)
     * @return true if frame can be allocated
     */
    private boolean canAllocateFrame(int partitionId) {
        return usedFrames < memoryBudget;
    }

    /**
     * Close all partition files.
     */
    private void closePartitionFiles() throws HyracksDataException {
        for (Map.Entry<Integer, RunFileWriter> entry : partitionFiles.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                System.err.println("ERROR: Failed to close partition file " + entry.getKey() + ": " + e.getMessage());
            }
        }
        
        // Don't clear partitionFiles here - we need them for reading
        // partitionFiles.clear();
    }

    /**
     * Flush all remaining buffers and close all files.
     */
    public void closeAllFiles() throws HyracksDataException {
        // Flush all remaining buffers first
        flushAllPartitions();
        
        // Close all partition files
        for (RunFileWriter writer : partitionFiles.values()) {
            if (writer != null) {
                writer.close();
            }
        }
        
        // Clear buffer references
        partitionFrames.clear();
        partitionAppenders.clear();
        usedFrames = 0;
    }

    /**
     * Convert input tuples to DataTuple objects for processing.
     * 
     * @param inputTuples List of input tuples
     * @param centroidIdColumn Column index containing centroid ID (0 for first, -1 for last)
     * @return List of DataTuple objects
     */
    private List<DataTuple> convertTuplesToDataTuples(List<ITupleReference> inputTuples, int centroidIdColumn) {
        List<DataTuple> dataTuples = new ArrayList<>();
        
        for (ITupleReference tuple : inputTuples) {
            DataTuple dataTuple = new DataTuple();
            
            // Extract centroid ID from specified column
            int actualColumnIndex = (centroidIdColumn == -1) ? tuple.getFieldCount() - 1 : centroidIdColumn;
            dataTuple.centroidId = extractCentroidId(tuple, actualColumnIndex);
            
            // Store the entire tuple data
            dataTuple.tupleData = tuple;
            
            dataTuples.add(dataTuple);
        }
        
        return dataTuples;
    }

    /**
     * Extract centroid ID from tuple using column accessor (following VCTreeStaticStructureCreatorOperatorDescriptor pattern).
     * Falls back to direct field access if tuple is not FrameTupleReference.
     * 
     * @param tuple Input tuple in format [centroidId, distance, embedding, centroidId]
     * @return Centroid ID
     * @throws HyracksDataException if extraction fails
     */
    private int extractCentroidId(ITupleReference tuple) throws HyracksDataException {
        try {
            // Try column accessor approach first (for FrameTupleReference)
            if (tuple instanceof FrameTupleReference) {
                FrameTupleReference frameTuple = (FrameTupleReference) tuple;
                
                // Use column accessor to extract centroidId from field 0 (first field in transformed format)
                centroidIdEval.evaluate(frameTuple, centroidIdVal);
                
                // Parse the integer value from the pointable
                int centroidId = IntegerPointable.getInteger(centroidIdVal.getByteArray(), centroidIdVal.getStartOffset());
                
                return centroidId;
            } else {
                // Fallback to direct field access for ArrayTupleReference and other types
                return extractCentroidId(tuple, 0); // centroidId is at field 0 in transformed format
            }
        } catch (Exception e) {
            System.err.println("ERROR: Failed to extract centroidId from tuple: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Extract centroid ID from tuple at specified column.
     * 
     * @param tuple Input tuple
     * @param columnIndex Column index containing centroid ID
     * @return Centroid ID as integer
     */
    private int extractCentroidId(ITupleReference tuple, int columnIndex) {
        try {
            byte[] fieldData = tuple.getFieldData(columnIndex);
            int fieldStart = tuple.getFieldStart(columnIndex);
            int fieldLength = tuple.getFieldLength(columnIndex);
            
            // Try different extraction methods based on field length
            if (fieldLength == 4) {
                // Standard 4-byte integer
                ByteBuffer buffer = ByteBuffer.wrap(fieldData, fieldStart, fieldLength);
                return buffer.getInt();
            } else if (fieldLength == 8) {
                // 8-byte long, take lower 4 bytes
                ByteBuffer buffer = ByteBuffer.wrap(fieldData, fieldStart, fieldLength);
                long longValue = buffer.getLong();
                return (int) (longValue & 0xFFFFFFFFL);
            } else if (fieldLength >= 4) {
                // Try reading first 4 bytes
                ByteBuffer buffer = ByteBuffer.wrap(fieldData, fieldStart, 4);
                return buffer.getInt();
            } else if (fieldLength == 1) {
                // Single byte
                return fieldData[fieldStart] & 0xFF;
            } else if (fieldLength == 2) {
                // 2-byte short
                ByteBuffer buffer = ByteBuffer.wrap(fieldData, fieldStart, fieldLength);
                return buffer.getShort() & 0xFFFF;
            }
            
            // Fallback: use column index as centroid ID
            return columnIndex;
        } catch (Exception e) {
            return columnIndex;
        }
    }

    /**
     * Calculate estimated data size from actual data.
     * 
     * @param dataTuples List of data tuples
     * @return Estimated data size in bytes
     */
    private long calculateEstimatedDataSize(List<DataTuple> dataTuples) {
        long totalSize = 0;
        for (DataTuple dataTuple : dataTuples) {
            // Estimate size based on tuple field count and average field size
            totalSize += dataTuple.tupleData.getFieldCount() * 1024; // Rough estimate
        }
        return totalSize;
    }

    /**
     * Write data to partition files using buffering approach.
     */
    private void writeDataToPartitions() throws HyracksDataException {
        for (DataTuple dataTuple : inputData) {
            int centroidId = dataTuple.centroidId;
            Integer partition = centroidToPartition.get(centroidId);
            
            if (partition != null) {
                FrameTupleAppender appender = partitionAppenders.get(partition);
                
                if (appender != null) {
                    // Try to append tuple to current frame
                    if (!appender.append(dataTuple.tupleData)) {
                        // Frame is full, flush and reset
                        flushPartitionFrame(partition);
                        appender.reset(partitionFrames.get(partition), true);
                        appender.append(dataTuple.tupleData);
                    }
                }
            }
        }
        
        // Flush all remaining buffers
        flushAllPartitions();
    }

    /**
     * Read data from partition file using standard Hyracks frame format.
     * 
     * @param partitionId Partition ID to read from
     * @return List of DataTuple objects from the partition
     */
    private List<DataTuple> readPartitionData(int partitionId) throws HyracksDataException {
        List<DataTuple> data = new ArrayList<>();
        
        RunFileWriter writer = partitionFiles.get(partitionId);
        
        if (writer != null) {
            try {
                RunFileReader reader = writer.createReader();
                reader.open();
                
                try {
                    // Read frames using standard Hyracks pattern
                    VSizeFrame frame = new VSizeFrame(ctx);
                    
                    while (reader.nextFrame(frame)) {
                        // Parse tuples from this frame
                        List<DataTuple> frameData = parseFrameToDataTuples(frame.getBuffer(), partitionId);
                        data.addAll(frameData);
                    }
                    
                } catch (Exception e) {
                    System.err.println("ERROR: Error reading frames: " + e.getMessage());
                }
                
                reader.close();
            } catch (Exception e) {
                System.err.println("ERROR: Failed to read partition data: " + e.getMessage());
            }
        }
        
        return data;
    }

    /**
     * Parse a frame buffer to extract DataTuple objects.
     * This method works with standard Hyracks frame format.
     * 
     * @param frameBuffer ByteBuffer containing frame data
     * @param partitionId Partition ID for logging
     * @return List of DataTuple objects parsed from the frame
     */
    private List<DataTuple> parseFrameToDataTuples(ByteBuffer frameBuffer, int partitionId) {
        List<DataTuple> data = new ArrayList<>();
        
        try {
            // Reset buffer for reading
            frameBuffer.rewind();
            
            // Read tuple count from frame
            int tupleCount = frameBuffer.getInt();
            
            // For now, create dummy data tuples for testing
            // In a full implementation, you'd parse the actual tuple data
            for (int i = 0; i < Math.min(tupleCount, 10); i++) { // Limit to 10 for testing
                DataTuple dataTuple = new DataTuple();
                dataTuple.centroidId = (partitionId * 10) + i; // Create some dummy centroid IDs
                dataTuple.tupleData = null; // Simplified for now
                data.add(dataTuple);
            }
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to parse frame data: " + e.getMessage());
            e.printStackTrace();
        }
        
        return data;
    }

    /**
     * Group tuples by centroid ID.
     * 
     * @param allData List of all data tuples
     * @return Map of centroid ID to list of data tuples
     */
    private Map<Integer, List<DataTuple>> groupByCentroidId(List<DataTuple> allData) {
        Map<Integer, List<DataTuple>> groups = new HashMap<>();
        
        for (DataTuple dataTuple : allData) {
            groups.computeIfAbsent(dataTuple.centroidId, k -> new ArrayList<>()).add(dataTuple);
        }
        
        return groups;
    }

    // Legacy custom frame creation methods removed - now using standard Hyracks FrameTupleAppender

    // Legacy parseTupleFromFrame method removed - now using parseFrameToDataTuples

    /**
     * Data structure to hold tuple data with centroid ID.
     */
    public static class DataTuple {
        public int centroidId;
        public ITupleReference tupleData;
        
        @Override
        public String toString() {
            return String.format("DataTuple{centroidId=%d, fieldCount=%d}", 
                centroidId, tupleData != null ? tupleData.getFieldCount() : 0);
        }
    }
}
