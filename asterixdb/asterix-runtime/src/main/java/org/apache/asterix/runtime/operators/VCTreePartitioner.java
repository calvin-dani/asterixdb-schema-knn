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

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

/**
 * VCTreePartitioner handles recursive centroid partitioning using SHAPIRO formula.
 * Separates centroids into individual files through recursive partitioning.
 */
public class VCTreePartitioner {

    private final IHyracksTaskContext ctx;
    private final int memoryBudget;
    private final int frameSize;
    private final int maxRecursionLevels;

    // Partition hierarchy tracking
    private final Map<Integer, RunFileWriter> partitionFiles = new HashMap<>();
    private final Map<Integer, List<Integer>> partitionToCentroids = new HashMap<>();
    private final Map<Integer, Integer> centroidToPartition = new HashMap<>();
    private final Map<Integer, FileReference> centroidFiles = new HashMap<>();

    // SHAPIRO formula constants
    private static final double FUDGE_FACTOR = 1.1;
    private static final double NLJ_SWITCH_THRESHOLD = 0.8;

    public VCTreePartitioner(IHyracksTaskContext ctx, int memoryBudget, int frameSize) {
        this.ctx = ctx;
        this.memoryBudget = memoryBudget;
        this.frameSize = frameSize;
        this.maxRecursionLevels = 5;
    }

    /**
     * Main entry point: partition dummy data using recursive SHAPIRO formula.
     */
    public void partitionData(VCTreeStaticStructureCreatorOperatorDescriptor.DummyData dummyData, int K)
            throws HyracksDataException {
        System.err.println("=== VCTreePartitioner: Starting partitioning for K=" + K + " centroids ===");

        // Step 1: Calculate initial partitions using SHAPIRO
        int numPartitions =
                calculatePartitionsUsingShapiro(K, dummyData.tuples.size() * 1024L, frameSize, memoryBudget);
        System.err.println("Initial partitions calculated: " + numPartitions);

        // Step 2: Distribute centroids to partitions
        distributeCentroidsToPartitions(K, numPartitions);

        // Step 3: Create initial partition files
        createInitialPartitionFiles(numPartitions);

        // Step 4: Write dummy data to partition files
        writeDataToPartitions(dummyData);

        // Step 5: Recursively separate centroids
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            List<Integer> centroids = partitionToCentroids.get(partitionId);
            if (centroids != null && !centroids.isEmpty()) {
                recursivelySeparateCentroids(partitionId, centroids, 0);
            }
        }

        System.err.println("=== VCTreePartitioner: Partitioning complete ===");
        System.err.println("Created " + centroidFiles.size() + " centroid files");
    }

    /**
     * Recursively separate centroids until each file contains only 1 centroid.
     */
    private void recursivelySeparateCentroids(int partitionId, List<Integer> centroids, int level)
            throws HyracksDataException {
        System.err.println(
                "Level " + level + ": Processing " + centroids.size() + " centroids in partition " + partitionId);

        if (level >= maxRecursionLevels) {
            System.err.println("Max recursion level reached, forcing separation");
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
            System.err.println("Creating " + subPartitions + " sub-partitions for " + centroids.size() + " centroids");

            // Distribute centroids to sub-partitions
            Map<Integer, List<Integer>> subPartitionToCentroids = new HashMap<>();
            for (int centroidId : centroids) {
                int subPartition = centroidId % subPartitions;
                subPartitionToCentroids.computeIfAbsent(subPartition, k -> new ArrayList<>()).add(centroidId);
            }

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
                writeCentroidDataToPartition(subFileId, subCentroids);

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
        System.err.println("Base case: Separating " + centroids.size() + " centroids into individual files");

        // Read data from partition file
        List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> allData = readPartitionData(partitionId);

        // Group by centroid ID
        Map<Integer, List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple>> centroidGroups =
                groupByCentroidId(allData);

        // Create individual file for each centroid
        for (int centroidId : centroids) {
            List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> centroidData =
                    centroidGroups.get(centroidId);
            if (centroidData != null) {
                // Create file for this centroid
                FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                        "VCTreeCentroid_" + centroidId + "_unsorted_" + System.currentTimeMillis());
                RunFileWriter writer = new RunFileWriter(file, ctx.getIoManager());
                writer.open();

                // Write centroid data
                for (VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple : centroidData) {
                    ByteBuffer frame = createFrameFromTuple(tuple);
                    writer.nextFrame(frame);
                }

                writer.close();
                centroidFiles.put(centroidId, file);
                System.err.println("Created file for centroid " + centroidId + ": " + file);
            }
        }
    }

    /**
     * Group tuples by centroid ID.
     */
    private Map<Integer, List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple>> groupByCentroidId(
            List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> allData) {
        Map<Integer, List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple>> groups = new HashMap<>();

        for (VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple : allData) {
            groups.computeIfAbsent(tuple.centroidId, k -> new ArrayList<>()).add(tuple);
        }

        return groups;
    }

    /**
     * Read data from partition file.
     */
    private List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> readPartitionData(int partitionId)
            throws HyracksDataException {
        List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> data = new ArrayList<>();

        RunFileWriter writer = partitionFiles.get(partitionId);
        if (writer != null) {
            // Create reader from writer
            RunFileReader reader = writer.createReader();
            reader.open();

            IFrame frame = new FixedSizeFrame(ByteBuffer.allocate(frameSize));
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

    /**
     * Write centroid data to partition file.
     */
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

    /**
     * Distribute centroids to partitions.
     */
    private void distributeCentroidsToPartitions(int K, int numPartitions) {
        for (int centroidId = 0; centroidId < K; centroidId++) {
            int partition = centroidId % numPartitions;
            partitionToCentroids.computeIfAbsent(partition, k -> new ArrayList<>()).add(centroidId);
            centroidToPartition.put(centroidId, partition);
        }

        System.err.println("Centroid distribution:");
        for (Map.Entry<Integer, List<Integer>> entry : partitionToCentroids.entrySet()) {
            System.err.println("  Partition " + entry.getKey() + ": " + entry.getValue());
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
     * Write dummy data to partition files.
     */
    private void writeDataToPartitions(VCTreeStaticStructureCreatorOperatorDescriptor.DummyData dummyData)
            throws HyracksDataException {
        for (VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple : dummyData.tuples) {
            int partition = centroidToPartition.get(tuple.centroidId);
            RunFileWriter writer = partitionFiles.get(partition);

            ByteBuffer frame = createFrameFromTuple(tuple);
            writer.nextFrame(frame);
        }
    }

    /**
     * Create frame from tuple (simplified version).
     */
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

    /**
     * Parse tuple from frame (simplified version).
     */
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

    /**
     * Generate dummy embedding vector.
     */
    private float[] generateDummyEmbedding(int dimension) {
        float[] embedding = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            embedding[i] = (float) (Math.random() * 2.0 - 1.0);
        }
        return embedding;
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
     * Get centroid files created during partitioning.
     */
    public Map<Integer, FileReference> getCentroidFiles() {
        return centroidFiles;
    }

    /**
     * Close all partition files.
     */
    public void closeAllFiles() throws HyracksDataException {
        for (RunFileWriter writer : partitionFiles.values()) {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
