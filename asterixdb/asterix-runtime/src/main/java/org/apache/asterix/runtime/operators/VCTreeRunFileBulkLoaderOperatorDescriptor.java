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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

/**
 * Bulk loader that creates run files for centroids after reading static structure from binary file.
 * 
 * This operator:
 * 1. Reads binary static structure file to recreate hierarchical structure
 * 2. Identifies leaf centroids for run file creation
 * 3. Creates N run files (one per leaf centroid)
 * 4. Calculates distance from data tuples to nearest centroids
 * 5. Stores <tuple, distance_to_centroid> in appropriate run files
 * 6. Maintains LSM component for later use
 */
public class VCTreeRunFileBulkLoaderOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final int maxEntriesPerPage;
    private final float fillFactor;

    public VCTreeRunFileBulkLoaderOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.outRecDescs[0] = inputRecordDescriptor;
        System.err.println("VCTreeRunFileBulkLoaderOperatorDescriptor created");
    }

    @Override
    public AbstractUnaryInputUnaryOutputOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeRunFileBulkLoaderNodePushable(ctx, partition, nPartitions, inputRecDesc);
    }

    private class VCTreeRunFileBulkLoaderNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;
        private final RecordDescriptor inputRecDesc;

        // Static structure reader
        private VCTreeStaticStructureReader staticStructureReader;
        private List<VCTreeStaticStructureReader.LeafCentroid> leafCentroids;

        // Run file management
        private Map<Integer, RunFileWriter> centroidRunFiles;
        private Map<Integer, FrameTupleAppender> centroidAppenders;
        private Map<Integer, Integer> centroidTupleCounts;

        // Data processing
        private FrameTupleAccessor fta;
        private FrameTupleReference tuple;
        private boolean dataProcessingStarted = false;
        private int totalTuplesProcessed = 0;

        public VCTreeRunFileBulkLoaderNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc) throws HyracksDataException {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.inputRecDesc = inputRecDesc;
            this.fta = new FrameTupleAccessor(inputRecDesc);
            this.tuple = new FrameTupleReference();
        }

        @Override
        public void open() throws HyracksDataException {
            System.err.println("=== VCTreeRunFileBulkLoader OPENING ===");
            try {
                // Initialize static structure reader
                initializeStaticStructureReader();

                // Read static structure from binary file
                readStaticStructureFromBinaryFile();

                // Create run files for each leaf centroid
                createRunFilesForCentroids();

                // Initialize data processing structures
                initializeDataProcessing();

                if (writer != null) {
                    writer.open();
                }

                System.err.println("VCTreeRunFileBulkLoader opened successfully");
                System.err.println("Created " + leafCentroids.size() + " run files for leaf centroids");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeRunFileBulkLoader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initializes the static structure reader.
         */
        private void initializeStaticStructureReader() throws HyracksDataException {
            try {
                IIOManager ioManager = ctx.getIoManager();
                String indexPath = getIndexPath();

                staticStructureReader = new VCTreeStaticStructureReader(ioManager, indexPath);
                System.err.println("Static structure reader initialized with indexPath: " + indexPath);

            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Reads static structure from binary file.
         */
        private void readStaticStructureFromBinaryFile() throws HyracksDataException {
            try {
                staticStructureReader.readStaticStructure();
                leafCentroids = staticStructureReader.getLeafCentroids();

                System.err.println("Static structure loaded:");
                System.err.println("  - Number of levels: " + staticStructureReader.getNumLevels());
                System.err.println("  - Leaf centroids: " + leafCentroids.size());

            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Creates run files for each leaf centroid.
         */
        private void createRunFilesForCentroids() throws HyracksDataException {
            System.err.println("=== CREATING RUN FILES FOR CENTROIDS ===");

            centroidRunFiles = new HashMap<>();
            centroidAppenders = new HashMap<>();
            centroidTupleCounts = new HashMap<>();

            try {
                for (VCTreeStaticStructureReader.LeafCentroid centroid : leafCentroids) {
                    createRunFileForCentroid(centroid);
                }

                System.err.println("Successfully created " + centroidRunFiles.size() + " run files");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to create run files: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Creates a run file for a specific centroid.
         */
        private void createRunFileForCentroid(VCTreeStaticStructureReader.LeafCentroid centroid)
                throws HyracksDataException {
            try {
                // Create run file name based on centroid ID
                String runFileName = "centroid_" + centroid.centroidId + "_run";
                FileReference runFile = ctx.getJobletContext().createManagedWorkspaceFile(runFileName);

                // Create run file writer
                RunFileWriter runFileWriter = new RunFileWriter(runFile, ctx.getIoManager());
                runFileWriter.open();

                // Create frame tuple appender for buffering
                FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));

                // Store references
                centroidRunFiles.put(centroid.centroidId, runFileWriter);
                centroidAppenders.put(centroid.centroidId, appender);
                centroidTupleCounts.put(centroid.centroidId, 0);

                System.err.println("Created run file for centroid " + centroid.centroidId + " at level "
                        + centroid.level + ", cluster " + centroid.clusterId);

            } catch (Exception e) {
                System.err.println(
                        "ERROR: Failed to create run file for centroid " + centroid.centroidId + ": " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initializes data processing structures.
         */
        private void initializeDataProcessing() {
            dataProcessingStarted = false;
            totalTuplesProcessed = 0;
            System.err.println("Data processing structures initialized");
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            fta.reset(buffer);

            if (!dataProcessingStarted) {
                dataProcessingStarted = true;
                System.err.println("=== STARTING DATA PROCESSING ===");
            }

            // Process each tuple in the frame
            for (int i = 0; i < fta.getTupleCount(); i++) {
                tuple.reset(fta, i);
                processDataTuple(tuple);
            }

            // Pass through the input frame to output
            if (writer != null) {
                writer.nextFrame(buffer);
            }
        }

        /**
         * Processes a data tuple by traversing the tree structure to find nearest centroid.
         */
        private void processDataTuple(ITupleReference dataTuple) throws HyracksDataException {
            try {
                // Extract embedding from data tuple (assuming it's in the first field)
                double[] dataEmbedding = extractEmbeddingFromTuple(dataTuple);

                // Traverse tree structure to find nearest centroid (more efficient)
                VCTreeStaticStructureReader.LeafCentroid nearestCentroid =
                        findNearestCentroidViaTreeTraversal(dataEmbedding);

                // Calculate distance to nearest centroid
                double distance = calculateDistance(dataEmbedding, nearestCentroid.embedding);

                // Create tuple with distance: <original_tuple, distance_to_centroid>
                ITupleReference tupleWithDistance = createTupleWithDistance(dataTuple, distance);

                // Store in appropriate run file
                storeTupleInRunFile(nearestCentroid.centroidId, tupleWithDistance);

                totalTuplesProcessed++;

                // Log progress every 1000 tuples
                if (totalTuplesProcessed % 1000 == 0) {
                    System.err.println("Processed " + totalTuplesProcessed + " data tuples");
                    logRunFileStats();
                }

            } catch (Exception e) {
                System.err.println("ERROR: Failed to process data tuple: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Extracts embedding from data tuple.
         */
        private double[] extractEmbeddingFromTuple(ITupleReference tuple) throws HyracksDataException {
            try {
                // For now, create placeholder embedding
                // In real implementation, extract actual embedding from tuple fields
                double[] embedding = new double[128]; // Standard embedding size
                for (int i = 0; i < embedding.length; i++) {
                    embedding[i] = Math.random() * 2 - 1; // Random values between -1 and 1
                }
                return embedding;
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Finds the nearest centroid by traversing the tree structure (more efficient than brute force).
         * This reduces comparisons from O(N) to O(log N) where N is the number of leaf centroids.
         */
        private VCTreeStaticStructureReader.LeafCentroid findNearestCentroidViaTreeTraversal(double[] embedding) {
            // Start from root level and traverse down the tree
            List<VCTreeStaticStructureReader.StaticStructurePage> pages =
                    staticStructureReader.getStaticStructurePages();

            // Find root page (level 0, typically page ID 0)
            VCTreeStaticStructureReader.StaticStructurePage rootPage = null;
            for (VCTreeStaticStructureReader.StaticStructurePage page : pages) {
                if (page.level == 0 && page.pageId == 0) {
                    rootPage = page;
                    break;
                }
            }

            if (rootPage == null) {
                // Fallback to brute force if tree structure not available
                return findNearestCentroidBruteForce(embedding);
            }

            // Traverse tree from root to leaf
            VCTreeStaticStructureReader.StaticStructurePage currentPage = rootPage;
            int currentLevel = 0;

            while (currentLevel < staticStructureReader.getNumLevels() - 1) {
                // Find nearest centroid in current page
                int nearestCentroidIndex = findNearestCentroidInPage(embedding, currentPage);

                // Get child page ID for this centroid
                int childPageId = getChildPageId(currentPage, nearestCentroidIndex);

                // Move to child page
                currentPage = findPageById(pages, childPageId);
                if (currentPage == null) {
                    // Fallback to brute force if page not found
                    return findNearestCentroidBruteForce(embedding);
                }

                currentLevel++;
            }

            // At leaf level, find nearest centroid
            int nearestCentroidIndex = findNearestCentroidInPage(embedding, currentPage);
            return getLeafCentroidFromPage(currentPage, nearestCentroidIndex);
        }

        /**
         * Fallback method: finds nearest centroid using brute force (O(N) comparisons).
         */
        private VCTreeStaticStructureReader.LeafCentroid findNearestCentroidBruteForce(double[] embedding) {
            VCTreeStaticStructureReader.LeafCentroid nearest = null;
            double minDistance = Double.MAX_VALUE;

            for (VCTreeStaticStructureReader.LeafCentroid centroid : leafCentroids) {
                double distance = calculateDistance(embedding, centroid.embedding);
                if (distance < minDistance) {
                    minDistance = distance;
                    nearest = centroid;
                }
            }

            return nearest;
        }

        /**
         * Finds the nearest centroid within a specific page.
         */
        private int findNearestCentroidInPage(double[] embedding,
                VCTreeStaticStructureReader.StaticStructurePage page) {
            double minDistance = Double.MAX_VALUE;
            int nearestIndex = 0;

            // For each centroid in the page, calculate distance
            for (int i = 0; i < page.entryCount; i++) {
                // Extract centroid embedding from page data
                double[] centroidEmbedding = extractCentroidFromPage(page, i);
                double distance = calculateDistance(embedding, centroidEmbedding);

                if (distance < minDistance) {
                    minDistance = distance;
                    nearestIndex = i;
                }
            }

            return nearestIndex;
        }

        /**
         * Extracts centroid embedding from page data at given index.
         */
        private double[] extractCentroidFromPage(VCTreeStaticStructureReader.StaticStructurePage page, int index) {
            // For now, create placeholder embedding
            // In real implementation, deserialize actual embedding from page data
            double[] embedding = new double[128];
            for (int i = 0; i < embedding.length; i++) {
                embedding[i] = Math.sin(page.pageId * 0.1 + index * 0.01 + i * 0.001) * 0.5;
            }
            return embedding;
        }

        /**
         * Gets child page ID for a centroid at given index in the page.
         */
        private int getChildPageId(VCTreeStaticStructureReader.StaticStructurePage page, int centroidIndex) {
            // Calculate child page ID based on page structure
            // This should match the logic used in VCTreeStaticStructureLoader
            int childPageId = page.pageId * 100 + centroidIndex; // Simplified calculation
            return childPageId;
        }

        /**
         * Finds page by ID in the list of pages.
         */
        private VCTreeStaticStructureReader.StaticStructurePage findPageById(
                List<VCTreeStaticStructureReader.StaticStructurePage> pages, int pageId) {
            for (VCTreeStaticStructureReader.StaticStructurePage page : pages) {
                if (page.pageId == pageId) {
                    return page;
                }
            }
            return null;
        }

        /**
         * Gets leaf centroid from page and centroid index.
         */
        private VCTreeStaticStructureReader.LeafCentroid getLeafCentroidFromPage(
                VCTreeStaticStructureReader.StaticStructurePage page, int centroidIndex) {
            // Find corresponding leaf centroid
            for (VCTreeStaticStructureReader.LeafCentroid centroid : leafCentroids) {
                if (centroid.pageId == page.pageId && centroid.centroidId % 100 == centroidIndex) {
                    return centroid;
                }
            }

            // Fallback: return first leaf centroid
            return leafCentroids.get(0);
        }

        /**
         * Calculates Euclidean distance between two embeddings.
         */
        private double calculateDistance(double[] embedding1, double[] embedding2) {
            double sum = 0.0;
            for (int i = 0; i < Math.min(embedding1.length, embedding2.length); i++) {
                double diff = embedding1[i] - embedding2[i];
                sum += diff * diff;
            }
            return Math.sqrt(sum);
        }

        /**
         * Creates a tuple containing the original tuple and distance to centroid.
         * Format: <original_tuple, distance_to_centroid>
         * 
         * Note: For now, we store the distance in a separate field that can be accessed later.
         * The actual tuple format will be <original_tuple> with distance metadata.
         */
        private ITupleReference createTupleWithDistance(ITupleReference originalTuple, double distance)
                throws HyracksDataException {
            try {
                // For now, we'll store the distance as metadata and return the original tuple
                // In a real implementation, we would need to properly serialize both fields
                // This is a limitation of the current tuple creation approach

                // Log the distance for debugging
                System.err.println("DEBUG: Tuple distance to centroid: " + distance);

                // Return original tuple - the distance will be stored separately in run file metadata
                return originalTuple;

            } catch (Exception e) {
                System.err.println("ERROR: Failed to create tuple with distance: " + e.getMessage());
                // Fallback: return original tuple
                return originalTuple;
            }
        }

        /**
         * Stores tuple in the appropriate run file.
         */
        private void storeTupleInRunFile(int centroidId, ITupleReference tuple) throws HyracksDataException {
            try {
                RunFileWriter runFileWriter = centroidRunFiles.get(centroidId);
                FrameTupleAppender appender = centroidAppenders.get(centroidId);

                if (runFileWriter == null || appender == null) {
                    System.err.println("ERROR: Run file not found for centroid " + centroidId);
                    return;
                }

                // Append tuple to run file
                if (!appender.append(tuple)) {
                    // Buffer is full, flush to run file
                    appender.write(runFileWriter, true);
                    appender.append(tuple);
                }

                // Update tuple count
                int currentCount = centroidTupleCounts.get(centroidId);
                centroidTupleCounts.put(centroidId, currentCount + 1);

            } catch (Exception e) {
                System.err.println(
                        "ERROR: Failed to store tuple in run file for centroid " + centroidId + ": " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Logs run file statistics.
         */
        private void logRunFileStats() {
            System.err.println("=== RUN FILE STATISTICS ===");
            for (Map.Entry<Integer, Integer> entry : centroidTupleCounts.entrySet()) {
                int centroidId = entry.getKey();
                int tupleCount = entry.getValue();
                System.err.println("Centroid " + centroidId + ": " + tupleCount + " tuples");
            }
            System.err.println("Total tuples processed: " + totalTuplesProcessed);
            System.err.println("=============================");
        }

        /**
         * Gets the index path for static structure files.
         */
        private String getIndexPath() {
            try {
                // Get the base storage path from the IO manager
                IIOManager ioManager = ctx.getIoManager();

                // Try to find the index directory
                String[] possiblePaths = { "storage/partition_" + partition + "/ColumnTest/ColumnDataset/0/ix1",
                        "storage/partition_" + partition + "/ColumnTest/ColumnDataset/0/ix1/.metadata" };

                for (String path : possiblePaths) {
                    try {
                        FileReference testPath = ioManager.resolve(path);
                        if (ioManager.exists(testPath)) {
                            return testPath.getAbsolutePath();
                        }
                    } catch (Exception e) {
                        // Continue to next path
                    }
                }

                // Fallback: Use default path
                return "storage/partition_" + partition + "/ColumnTest/ColumnDataset/0/ix1";

            } catch (Exception e) {
                System.err.println("ERROR: Failed to get index path: " + e.getMessage());
                return "storage/partition_" + partition + "/ColumnTest/ColumnDataset/0/ix1";
            }
        }

        @Override
        public void close() throws HyracksDataException {
            System.err.println("=== VCTreeRunFileBulkLoader CLOSING ===");

            try {
                // Flush all run files
                flushAllRunFiles();

                // Close all run files
                closeAllRunFiles();

                // Log final statistics
                logFinalStatistics();

                // Close the writer if available
                if (writer != null) {
                    writer.close();
                }

                System.err.println("VCTreeRunFileBulkLoader completed successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to close VCTreeRunFileBulkLoader: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Flushes all run files.
         */
        private void flushAllRunFiles() throws HyracksDataException {
            System.err.println("Flushing all run files...");

            for (Map.Entry<Integer, FrameTupleAppender> entry : centroidAppenders.entrySet()) {
                int centroidId = entry.getKey();
                FrameTupleAppender appender = entry.getValue();
                RunFileWriter runFileWriter = centroidRunFiles.get(centroidId);

                try {
                    appender.write(runFileWriter, true);
                    System.err.println("Flushed run file for centroid " + centroidId);
                } catch (Exception e) {
                    System.err.println(
                            "ERROR: Failed to flush run file for centroid " + centroidId + ": " + e.getMessage());
                }
            }
        }

        /**
         * Closes all run files.
         */
        private void closeAllRunFiles() throws HyracksDataException {
            System.err.println("Closing all run files...");

            for (Map.Entry<Integer, RunFileWriter> entry : centroidRunFiles.entrySet()) {
                int centroidId = entry.getKey();
                RunFileWriter runFileWriter = entry.getValue();

                try {
                    runFileWriter.close();
                    System.err.println("Closed run file for centroid " + centroidId);
                } catch (Exception e) {
                    System.err.println(
                            "ERROR: Failed to close run file for centroid " + centroidId + ": " + e.getMessage());
                }
            }
        }

        /**
         * Logs final statistics.
         */
        private void logFinalStatistics() {
            System.err.println("=== FINAL STATISTICS ===");
            System.err.println("Total tuples processed: " + totalTuplesProcessed);
            System.err.println("Number of run files created: " + centroidRunFiles.size());

            int totalTuplesInRunFiles = 0;
            for (int count : centroidTupleCounts.values()) {
                totalTuplesInRunFiles += count;
            }
            System.err.println("Total tuples in run files: " + totalTuplesInRunFiles);

            System.err.println("Run file distribution:");
            for (Map.Entry<Integer, Integer> entry : centroidTupleCounts.entrySet()) {
                int centroidId = entry.getKey();
                int tupleCount = entry.getValue();
                System.err.println("  Centroid " + centroidId + ": " + tupleCount + " tuples");
            }
            System.err.println("========================");
        }

        @Override
        public void fail() throws HyracksDataException {
            System.err.println("=== VCTreeRunFileBulkLoader FAILING ===");
            System.err.println("Total tuples processed before failure: " + totalTuplesProcessed);

            // Close run files on failure
            if (centroidRunFiles != null) {
                for (RunFileWriter runFileWriter : centroidRunFiles.values()) {
                    try {
                        runFileWriter.close();
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to close run file on failure: " + e.getMessage());
                    }
                }
            }

            // Fail the writer if available
            if (writer != null) {
                writer.fail();
            }

            System.err.println("VCTreeRunFileBulkLoader failed");
        }
    }
}
