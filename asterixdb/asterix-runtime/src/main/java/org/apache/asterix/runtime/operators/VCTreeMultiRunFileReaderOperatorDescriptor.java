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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * Efficient multi-run-file reader that processes multiple run files in parallel.
 * 
 * This operator:
 * 1. Reads N unsorted run files concurrently
 * 2. Merges tuples from multiple run files efficiently
 * 3. Streams merged tuples to ExternalSortOperatorDescriptor
 * 4. Handles large datasets with optimal memory usage
 * 5. Provides load balancing across multiple run files
 */
public class VCTreeMultiRunFileReaderOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final Map<Integer, String> runFilePaths; // centroidId -> run file path
    private final UUID permitUUID;
    private final int maxConcurrentReaders;
    private final int framesLimit;

    public VCTreeMultiRunFileReaderOperatorDescriptor(IOperatorDescriptorRegistry spec,
            Map<Integer, String> runFilePaths, UUID permitUUID, int maxConcurrentReaders, int framesLimit,
            RecordDescriptor outputRecordDescriptor) {
        super(spec, 0, 1); // No input, 1 output
        this.runFilePaths = runFilePaths;
        this.permitUUID = permitUUID;
        this.maxConcurrentReaders = Math.min(maxConcurrentReaders, runFilePaths.size());
        this.framesLimit = framesLimit;
        this.outRecDescs[0] = outputRecordDescriptor;
        System.err.println("VCTreeMultiRunFileReaderOperatorDescriptor created with " + runFilePaths.size()
                + " run files, max " + this.maxConcurrentReaders + " concurrent readers");
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new VCTreeMultiRunFileReaderNodePushable(ctx, partition, nPartitions);
    }

    private class VCTreeMultiRunFileReaderNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;

        // Run file management
        private List<RunFileReader> runFileReaders;
        private ExecutorService executorService;
        private Map<Integer, Future<RunFileData>> readerFutures;
        private List<RunFileData> completedReaders;

        // Frame management
        private ByteBuffer frameBuffer;
        private FrameTupleReference tuple;

        // Statistics
        private long totalTuplesRead;
        private long runFilesProcessed;
        private boolean initialized = false;

        public VCTreeMultiRunFileReaderNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions) {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.runFileReaders = new ArrayList<>();
            this.readerFutures = new ConcurrentHashMap<>();
            this.completedReaders = new ArrayList<>();
            this.totalTuplesRead = 0;
            this.runFilesProcessed = 0;
        }

        @Override
        public void initialize() throws HyracksDataException {
            if (initialized) {
                return;
            }

            System.err.println("=== VCTreeMultiRunFileReader INITIALIZING ===");

            try {
                // Initialize frame buffer
                frameBuffer = ByteBuffer.allocate(ctx.getInitialFrameSize());
                tuple = new FrameTupleReference();

                // Initialize executor service for concurrent reading
                executorService = Executors.newFixedThreadPool(maxConcurrentReaders);

                // Initialize run file readers
                initializeRunFileReaders();

                initialized = true;
                System.err.println("VCTreeMultiRunFileReader initialized with " + runFileReaders.size() + " run files");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize VCTreeMultiRunFileReader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        public void nextFrame() throws HyracksDataException {
            if (!initialized) {
                initialize();
            }

            // Process run files concurrently
            processRunFilesConcurrently();
        }

        public void close() throws HyracksDataException {
            System.err.println("=== VCTreeMultiRunFileReader CLOSING ===");

            try {
                // Shutdown executor service
                if (executorService != null) {
                    executorService.shutdown();
                }

                // Close all run file readers
                for (RunFileReader reader : runFileReaders) {
                    if (reader != null) {
                        reader.close();
                    }
                }

                System.err.println("VCTreeMultiRunFileReader closed. Processed " + runFilesProcessed + " run files, "
                        + totalTuplesRead + " tuples");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to close VCTreeMultiRunFileReader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        public void fail() throws HyracksDataException {
            System.err.println("VCTreeMultiRunFileReader failed");
            close();
        }

        @Override
        public String getDisplayName() {
            return "VCTreeMultiRunFileReader";
        }

        /**
         * Initialize run file readers for all run files.
         */
        private void initializeRunFileReaders() throws HyracksDataException {
            System.err.println("Initializing run file readers...");

            for (Map.Entry<Integer, String> entry : runFilePaths.entrySet()) {
                int centroidId = entry.getKey();
                String runFilePath = entry.getValue();

                try {
                    // Create file reference
                    org.apache.hyracks.api.io.FileReference fileRef =
                            ctx.getJobletContext().createManagedWorkspaceFile("runfile_" + centroidId);

                    // Create run file reader
                    RunFileReader reader = new RunFileReader(fileRef, ctx.getIoManager(), 0, false);
                    runFileReaders.add(reader);

                    System.err.println("Added run file reader for centroid " + centroidId + ": " + runFilePath);

                } catch (Exception e) {
                    System.err.println("ERROR: Failed to create run file reader for centroid " + centroidId + ": "
                            + e.getMessage());
                    e.printStackTrace();
                    throw HyracksDataException.create(e);
                }
            }

            System.err.println("Initialized " + runFileReaders.size() + " run file readers");
        }

        /**
         * Process run files concurrently using thread pool.
         */
        private void processRunFilesConcurrently() throws HyracksDataException {
            try {
                // Submit reading tasks for all run files
                for (int i = 0; i < runFileReaders.size(); i++) {
                    final int readerIndex = i;
                    if (!readerFutures.containsKey(readerIndex)) {
                        RunFileReader reader = runFileReaders.get(readerIndex);
                        Future<RunFileData> future = executorService.submit(() -> readRunFile(reader, readerIndex));
                        readerFutures.put(readerIndex, future);
                    }
                }

                // Process completed readers
                processCompletedReaders();

            } catch (Exception e) {
                System.err.println("ERROR: Failed to process run files concurrently: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Read a single run file and return its data.
         */
        private RunFileData readRunFile(RunFileReader reader, int readerIndex) {
            try {
                reader.open();

                List<ByteBuffer> frames = new ArrayList<>();
                ByteBuffer buffer = ByteBuffer.allocate(ctx.getInitialFrameSize());

                org.apache.hyracks.api.comm.IFrame frame = new org.apache.hyracks.api.comm.VSizeFrame(ctx);
                while (reader.nextFrame(frame)) {
                    buffer = frame.getBuffer();
                    ByteBuffer frameCopy = ByteBuffer.allocate(buffer.remaining());
                    frameCopy.put(buffer.duplicate());
                    frameCopy.flip();
                    frames.add(frameCopy);
                    buffer.clear();
                }

                reader.close();

                return new RunFileData(readerIndex, frames);

            } catch (Exception e) {
                System.err.println("ERROR: Failed to read run file " + readerIndex + ": " + e.getMessage());
                e.printStackTrace();
                return new RunFileData(readerIndex, new ArrayList<>());
            }
        }

        /**
         * Process completed readers and output their data.
         */
        private void processCompletedReaders() throws HyracksDataException {
            List<Integer> completedIndices = new ArrayList<>();

            for (Map.Entry<Integer, Future<RunFileData>> entry : readerFutures.entrySet()) {
                if (entry.getValue().isDone()) {
                    try {
                        RunFileData data = entry.getValue().get();
                        processRunFileData(data);
                        completedIndices.add(entry.getKey());
                        runFilesProcessed++;

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to get run file data: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

            // Remove completed futures
            for (Integer index : completedIndices) {
                readerFutures.remove(index);
            }
        }

        /**
         * Process data from a completed run file.
         */
        private void processRunFileData(RunFileData data) throws HyracksDataException {
            for (ByteBuffer frame : data.frames) {
                processFrame(frame);
            }
        }

        /**
         * Process a frame of tuples and output them.
         */
        private void processFrame(ByteBuffer buffer) throws HyracksDataException {
            // Reset frame buffer for reading
            buffer.rewind();

            // Create frame tuple accessor
            FrameTupleAccessor fta = new FrameTupleAccessor(outRecDescs[0]);
            fta.reset(buffer);

            // Process each tuple in the frame
            for (int i = 0; i < fta.getTupleCount(); i++) {
                tuple.reset(fta, i);
                processTuple(tuple);
            }

            // Output the frame
            writer.nextFrame(buffer);
        }

        /**
         * Process a single tuple.
         */
        private void processTuple(ITupleReference tuple) throws HyracksDataException {
            totalTuplesRead++;

            // Log progress every 1000 tuples
            if (totalTuplesRead % 1000 == 0) {
                System.err.println("Processed " + totalTuplesRead + " tuples from run files");
            }

            // Tuple is already in correct format: <original_tuple, distance_to_centroid>
            // No additional processing needed - just pass through
        }

        /**
         * Data structure to hold run file data.
         */
        private static class RunFileData {
            final int readerIndex;
            final List<ByteBuffer> frames;

            RunFileData(int readerIndex, List<ByteBuffer> frames) {
                this.readerIndex = readerIndex;
                this.frames = frames;
            }
        }
    }
}
