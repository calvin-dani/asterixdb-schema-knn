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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;

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
    private boolean useModuloPartitioning = true; // Flag to enable modulo-based partitioning
    
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
     * Set the partitioning strategy.
     * 
     * @param useModuloPartitioning true for modulo-based partitioning (centroidId % numPartitions),
     *                              false for range-based partitioning (pre-calculated mapping)
     */
    public void setPartitioningStrategy(boolean useModuloPartitioning) {
        this.useModuloPartitioning = useModuloPartitioning;
        System.err.println("Partitioning strategy set to: " + (useModuloPartitioning ? "modulo-based" : "range-based"));
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
     * Base case: separate centroids into individual files using streaming approach.
     * This method processes the partition file directly without loading all data into memory.
     */
    private void separateAndWriteToFiles(int partitionId, List<Integer> centroids, int level)
            throws HyracksDataException {
        System.err.println("=== separateAndWriteToFiles (streaming) for partition " + partitionId + " ===");
        System.err.println("Target centroids: " + centroids);
        
        // Create writers for each centroid
        Map<Integer, RunFileWriter> centroidWriters = new HashMap<>();
        Map<Integer, FrameTupleAppender> centroidAppenders = new HashMap<>();
        
        try {
            // Initialize writers and appenders for each centroid
            for (int centroidId : centroids) {
                FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                        "VCTreeCentroid_" + centroidId + "_unsorted_" + System.currentTimeMillis());
                RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
                writer.open();
                centroidWriters.put(centroidId, writer);
                
                VSizeFrame frame = new VSizeFrame(ctx);
                FrameTupleAppender appender = new FrameTupleAppender(frame);
                centroidAppenders.put(centroidId, appender);
                
                System.err.println("Created writer for centroid " + centroidId);
            }
            
            System.err.println("DEBUG: Target centroids for writers: " + centroidWriters.keySet());
            
            // Stream data from partition file and route to appropriate centroid files
            RunFileWriter partitionWriter = partitionFiles.get(partitionId);
            if (partitionWriter != null) {
                RunFileReader reader = partitionWriter.createReader();
                reader.open();
                
                try {
                    VSizeFrame frame = new VSizeFrame(ctx);
                    int totalTuples = 0;
                    
                    while (reader.nextFrame(frame)) {
                        // Parse tuples from this frame
                        List<DataTuple> frameData = parseFrameToDataTuples(frame.getBuffer(), partitionId);
                        totalTuples += frameData.size();
                        
                        // Route each tuple to its appropriate centroid file
                        for (DataTuple dataTuple : frameData) {
                            int centroidId = dataTuple.centroidId;
                            RunFileWriter centroidWriter = centroidWriters.get(centroidId);
                            FrameTupleAppender centroidAppender = centroidAppenders.get(centroidId);
                            
                            System.err.println("DEBUG: Routing tuple with centroidId " + centroidId + " to writer");
                            
                            if (centroidWriter != null && centroidAppender != null && dataTuple.tupleData != null) {
                                // Append tuple to centroid file
                                if (!centroidAppender.append(dataTuple.tupleData)) {
                                    // Frame is full, flush and reset
                                    System.err.println("DEBUG: Frame full for centroid " + centroidId + ", flushing");
                                    FrameUtils.flushFrame(centroidAppender.getBuffer(), centroidWriter);
                                    centroidAppender.reset(new VSizeFrame(ctx), true);
                                    if (!centroidAppender.append(dataTuple.tupleData)) {
                                        throw new HyracksDataException("Tuple too large for frame in centroid " + centroidId);
                                    }
                                } else {
                                    System.err.println("DEBUG: Successfully appended tuple to centroid " + centroidId);
                                }
                            } else {
                                System.err.println("DEBUG: No writer/appender found for centroid " + centroidId);
                            }
                        }
                    }
                    
                    System.err.println("Processed " + totalTuples + " tuples from partition " + partitionId);
                    
                } finally {
                    reader.close();
                }
            }
            
            // Flush all remaining data and close files
            for (Map.Entry<Integer, RunFileWriter> entry : centroidWriters.entrySet()) {
                int centroidId = entry.getKey();
                RunFileWriter writer = entry.getValue();
                FrameTupleAppender appender = centroidAppenders.get(centroidId);
                
                try {
                    // Flush any remaining data
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(appender.getBuffer(), writer);
                    }
                    
                    writer.close();
                    centroidFiles.put(centroidId, writer.getFileReference());
                    System.err.println("Successfully created file for centroid " + centroidId);
                    
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to finalize centroid " + centroidId + ": " + e.getMessage());
                    writer.fail();
                }
            }
            
        } catch (Exception e) {
            // Clean up on error
            for (RunFileWriter writer : centroidWriters.values()) {
                try {
                    writer.fail();
                } catch (Exception cleanupE) {
                    System.err.println("ERROR: Failed to cleanup writer: " + cleanupE.getMessage());
                }
            }
            throw HyracksDataException.create(e);
        }
    }


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
        System.err.println("=== preInitializePartitioning called ===");
        System.err.println("K: " + K + ", memoryBudget: " + memoryBudget + ", frameSize: " + frameSize);
        
        this.K = K;
        
        // Initialize column accessors first
        initializeColumnAccessors();
        
        // Calculate number of partitions using SHAPIRO formula
        long estimatedDataSize = K * 1024L; // Rough estimate for pre-calculation
        this.numPartitions = calculatePartitionsUsingShapiro(K, estimatedDataSize, frameSize, memoryBudget);
        System.err.println("Calculated numPartitions: " + numPartitions);
        
        // Pre-calculate partition mapping using range partitioning
        calculatePartitionMapping();
        
        // Initialize partition buffers and files
        initializePartitionBuffers(numPartitions);
        System.err.println("=== preInitializePartitioning completed ===");
    }

    /**
     * Write a single tuple to the appropriate partition buffer (streaming mode with buffering).
     * Uses column accessor to extract centroidId from [centroidId, distance, embedding, centroidId] format.
     * 
     * @param tuple Input tuple in format [centroidId, distance, embedding, centroidId]
     * @throws HyracksDataException if writing fails
     */
    public void writeStreamingTuple(ITupleReference tuple) throws HyracksDataException {
        System.err.println("=== writeStreamingTuple called ===");
        try {
            // Extract centroid ID using column accessor (field 0 in [centroidId, distance, embedding, centroidId] format)
            int centroidId = extractCentroidId(tuple);
            System.err.println("DEBUG: Extracted centroid ID: " + centroidId);
            
            // Check if centroid ID is in valid range
//            if (centroidId < 0 || centroidId >= K) {
//                return;
//            }
//
            // Determine target partition
            int partition;
            if (useModuloPartitioning) {
                // Use modulo-based partitioning: centroidId % numPartitions
                partition = Math.abs(centroidId) % numPartitions;
                System.err.println("DEBUG: Centroid ID " + centroidId + " -> Partition " + partition + " (modulo " + numPartitions + ")");
            } else {
                // Use pre-calculated mapping (range partitioning)
                Integer mappedPartition = centroidToPartition.get(centroidId);
                if (mappedPartition == null) {
                    System.err.println("DEBUG: No mapping found for centroid ID " + centroidId + ", skipping");
                    return;
                }
                partition = mappedPartition;
            }
            
            // Get appender for this partition
            FrameTupleAppender appender = partitionAppenders.get(partition);
            
            if (appender == null) {
                return;
            }
            
            // Debug: Check what we're about to write
            System.err.println("DEBUG: About to write tuple to partition " + partition);
            System.err.println("DEBUG: Tuple field count: " + tuple.getFieldCount());
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                System.err.println("DEBUG: Field " + i + " length: " + tuple.getFieldLength(i));
                if (i == 0) { // First field should be centroid ID
                    int fieldCentroidId = extractCentroidId(tuple, i);
                    System.err.println("DEBUG: Field 0 centroid ID before writing: " + fieldCentroidId);
                }
            }
            
            // Try to append tuple to current frame
            if (appender.append(tuple)) {
                System.err.println("DEBUG: Successfully appended tuple to partition " + partition);
                return;
            }
            
            // Frame is full, need to flush
            System.err.println("DEBUG: Frame full, flushing partition " + partition);
            flushPartitionFrame(partition);
            
            // Reset appender and try again
            appender.reset(new VSizeFrame(ctx), true);
            if (!appender.append(tuple)) {
                throw new HyracksDataException("Tuple too large for frame in partition " + partition);
            }
            System.err.println("DEBUG: Successfully appended tuple to partition " + partition + " after flush");
            
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
        System.err.println("=== finalizePartitioning called ===");
        System.err.println("numPartitions: " + numPartitions);
        System.err.println("partitionToCentroids: " + partitionToCentroids);
        System.err.println("useModuloPartitioning: " + useModuloPartitioning);
        
        // Flush all remaining buffers to files
        flushAllPartitions();
        
        // Close all partition files
        closePartitionFiles();
        
        if (useModuloPartitioning) {
            // For modulo partitioning, we need to read the partition files and extract centroids
            System.err.println("Using modulo partitioning - reading partition files to extract centroids");
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                List<Integer> centroidsInPartition = extractCentroidsFromPartition(partitionId);
                System.err.println("Partition " + partitionId + " has centroids: " + centroidsInPartition);
                if (centroidsInPartition != null && !centroidsInPartition.isEmpty()) {
                    recursivelySeparateCentroids(partitionId, centroidsInPartition, 0);
                }
            }
        } else {
            // For range partitioning, use pre-calculated mapping
            System.err.println("Using range partitioning - using pre-calculated mapping");
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                List<Integer> centroidsInPartition = partitionToCentroids.get(partitionId);
                System.err.println("Partition " + partitionId + " has centroids: " + centroidsInPartition);
                if (centroidsInPartition != null && !centroidsInPartition.isEmpty()) {
                    recursivelySeparateCentroids(partitionId, centroidsInPartition, 0);
                }
            }
        }
        
        System.err.println("Final centroidFiles: " + centroidFiles);
        return centroidFiles;
    }

    /**
     * Extract unique centroid IDs from a partition file using streaming approach.
     * 
     * @param partitionId Partition ID to read from
     * @return List of unique centroid IDs found in this partition
     * @throws HyracksDataException if reading fails
     */
    private List<Integer> extractCentroidsFromPartition(int partitionId) throws HyracksDataException {
        System.err.println("=== extractCentroidsFromPartition (streaming) for partition " + partitionId + " ===");
        
        List<Integer> centroids = new ArrayList<>();
        Set<Integer> uniqueCentroids = new HashSet<>();
        
        try {
            // Stream data from partition file
            RunFileWriter partitionWriter = partitionFiles.get(partitionId);
            if (partitionWriter != null) {
                RunFileReader reader = partitionWriter.createReader();
                reader.open();
                
                try {
                    VSizeFrame frame = new VSizeFrame(ctx);
                    int totalTuples = 0;
                    
                    while (reader.nextFrame(frame)) {
                        // Parse tuples from this frame
                        List<DataTuple> frameData = parseFrameToDataTuples(frame.getBuffer(), partitionId);
                        totalTuples += frameData.size();
                        
                        // Extract unique centroid IDs
                        for (DataTuple dataTuple : frameData) {
                            if (dataTuple.tupleData != null) {
                                int centroidId = extractCentroidId(dataTuple.tupleData, 0);
                                if (uniqueCentroids.add(centroidId)) {
                                    centroids.add(centroidId);
                                    System.err.println("Found centroid ID: " + centroidId);
                                }
                            }
                        }
                    }
                    
                    System.err.println("Processed " + totalTuples + " tuples from partition " + partitionId);
                    
                } finally {
                    reader.close();
                }
            }
            
            System.err.println("Extracted " + centroids.size() + " unique centroids from partition " + partitionId);
            return centroids;
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to extract centroids from partition " + partitionId + ": " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    /**
     * Get centroid files created during partitioning.
     */
    public Map<Integer, FileReference> getCentroidFiles() {
        return centroidFiles;
    }

    /**
     * Get the record descriptor used for reading/writing tuples in this partitioner.
     * This ensures consistency between writing and reading operations.
     * 
     * @return RecordDescriptor used for tuple operations
     */
    public RecordDescriptor getTupleRecordDescriptor() {
        // Create the same record descriptor used in parseFrameToDataTuples
        ITypeTraits[] typeTraits = new ITypeTraits[4];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS; // centroidId
        typeTraits[1] = DoublePointable.TYPE_TRAITS; // distance
        typeTraits[2] = DoublePointable.TYPE_TRAITS; // embedding (treated as binary data)
        typeTraits[3] = IntegerPointable.TYPE_TRAITS; // centroidId (duplicate)
        
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[4];
        serdes[0] = IntegerSerializerDeserializer.INSTANCE; // centroidId
        serdes[1] = DoubleSerializerDeserializer.INSTANCE; // distance
        serdes[2] = DoubleSerializerDeserializer.INSTANCE; // embedding (treated as binary data)
        serdes[3] = IntegerSerializerDeserializer.INSTANCE; // centroidId (duplicate)
        
        return new RecordDescriptor(serdes, typeTraits);
    }

    /**
     * Calculate partition mapping using range partitioning.
     */
    private void calculatePartitionMapping() {
        partitionToCentroids = new HashMap<>();
        centroidToPartition = new HashMap<>();
        
        if (useModuloPartitioning) {
            // Modulo-based partitioning: centroidId % numPartitions
            System.err.println("Using modulo-based partitioning: centroidId % " + numPartitions);
            // Note: We don't pre-populate the mapping since we don't know centroid IDs in advance
            // The mapping will be calculated dynamically in writeStreamingTuple()
        } else {
            // Range-based partitioning (original approach)
            System.err.println("Using range-based partitioning");
            int centroidsPerPartition = calculateCentroidsPerPartition(K, numPartitions);
            
            for (int centroidId = 0; centroidId < K; centroidId++) {
                int partition = centroidId / centroidsPerPartition; // Range partitioning
                partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(centroidId);
                centroidToPartition.put(centroidId, partition);
            }
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
                System.err.println("DEBUG: Flushing partition " + partitionId + " with " + appender.getTupleCount() + " tuples");
                
                // Write current frame to file (no rewind needed - FrameUtils.flushFrame handles the buffer correctly)
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
     * Extract centroid ID from tuple at specified column using AsterixDB IntegerPointable.
     * 
     * @param tuple Input tuple
     * @param columnIndex Column index containing centroid ID
     * @return Centroid ID as integer
     */
    private int extractCentroidId(ITupleReference tuple, int columnIndex) {
        try {
            System.err.println("DEBUG: extractCentroidId called with columnIndex: " + columnIndex);
            System.err.println("DEBUG: Tuple field count: " + tuple.getFieldCount());
            System.err.println("DEBUG: Field " + columnIndex + " length: " + tuple.getFieldLength(columnIndex));
            
            // Use AsterixDB IntegerPointable like in VCTreeStaticStructureCreatorOperatorDescriptor
            int centroidId = IntegerPointable.getInteger(tuple.getFieldData(columnIndex), tuple.getFieldStart(columnIndex));
            System.err.println("DEBUG: Extracted centroid ID: " + centroidId);
            return centroidId;
        } catch (Exception e) {
            System.err.println("ERROR: Failed to extract centroid ID from column " + columnIndex + ": " + e.getMessage());
            e.printStackTrace();
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
     * Parse a frame buffer to extract DataTuple objects.
     * This method works with standard Hyracks frame format created by FrameTupleAppender.
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
            
            // Create FrameTupleAccessor to parse the frame properly
            // We need to create a record descriptor that matches the transformed tuple format
            // Format: [centroidId, distance, ...original fields...]
            // The original tuple has variable fields, so we need to handle this dynamically
            
            // For now, let's use a simple approach: just read the first field (centroidId)
            // and create a minimal DataTuple. The actual tuple parsing will be done by the
            // FrameTupleAccessor which can handle variable field counts.
            
            // Create a record descriptor that matches the transformed tuple format
            // Format: [centroidId, distance, embedding, centroidId] - 4 fields total
            // We need to match exactly what was written during the streaming phase
            ITypeTraits[] typeTraits = new ITypeTraits[4];
            typeTraits[0] = IntegerPointable.TYPE_TRAITS; // centroidId
            typeTraits[1] = DoublePointable.TYPE_TRAITS; // distance
            typeTraits[2] = DoublePointable.TYPE_TRAITS; // embedding (treated as binary data)
            typeTraits[3] = IntegerPointable.TYPE_TRAITS; // centroidId (duplicate)
            
            ISerializerDeserializer[] serdes = new ISerializerDeserializer[4];
            serdes[0] = IntegerSerializerDeserializer.INSTANCE; // centroidId
            serdes[1] = DoubleSerializerDeserializer.INSTANCE; // distance
            serdes[2] = DoubleSerializerDeserializer.INSTANCE; // embedding (treated as binary data)
            serdes[3] = IntegerSerializerDeserializer.INSTANCE; // centroidId (duplicate)
            
            RecordDescriptor recordDesc = new RecordDescriptor(serdes, typeTraits);
            FrameTupleAccessor fta = new FrameTupleAccessor(recordDesc);
            fta.reset(frameBuffer);
            
            int tupleCount = fta.getTupleCount();
            System.err.println("Parsing frame for partition " + partitionId + " with " + tupleCount + " tuples");
            
            // Parse each tuple in the frame
            for (int i = 0; i < tupleCount; i++) {
                try {
                    FrameTupleReference tuple = new FrameTupleReference();
                    tuple.reset(fta, i);
                    
                    // Debug: Log tuple field count
                    if (i < 3) { // Only log first 3 tuples to avoid spam
                        System.err.println("DEBUG: Tuple " + i + " field count: " + tuple.getFieldCount());
                        for (int j = 0; j < Math.min(tuple.getFieldCount(), 4); j++) {
                            System.err.println("DEBUG: Field " + j + " length: " + tuple.getFieldLength(j));
                        }
                    }
                    
                    // Extract centroid ID from the first field
                    int centroidId = extractCentroidId(tuple, 0);
                    
                    // Debug: Log centroid ID extraction
                    if (i < 5) { // Only log first 5 tuples to avoid spam
                        System.err.println("DEBUG: Tuple " + i + " has centroidId: " + centroidId);
                    }
                    
                    // Create DataTuple with the actual tuple data
                    DataTuple dataTuple = new DataTuple();
                    dataTuple.centroidId = centroidId;
                    dataTuple.tupleData = tuple; // Store the actual tuple reference
                    
                    data.add(dataTuple);
                    
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to parse tuple " + i + " in partition " + partitionId + ": " + e.getMessage());
                    // Continue with next tuple
                }
            }
            
            System.err.println("Successfully parsed " + data.size() + " tuples from partition " + partitionId);
            
        } catch (Exception e) {
            System.err.println("ERROR: Failed to parse frame data for partition " + partitionId + ": " + e.getMessage());
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
        
        // Debug: Log first few centroid IDs
        int debugCount = 0;
        for (DataTuple dataTuple : allData) {
            if (debugCount < 10) { // Only log first 10 tuples
                System.err.println("DEBUG: Grouping tuple with centroidId: " + dataTuple.centroidId);
                debugCount++;
            }
            groups.computeIfAbsent(dataTuple.centroidId, k -> new ArrayList<>()).add(dataTuple);
        }
        
        // Debug: Log unique centroid IDs found
        System.err.println("DEBUG: Found " + groups.size() + " unique centroid IDs: " + groups.keySet());
        
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
