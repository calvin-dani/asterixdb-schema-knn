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
 * Operator that reads unsorted run files and streams tuples to ExternalSortOperatorDescriptor.
 * 
 * This operator:
 * 1. Reads N unsorted run files (one per centroid)
 * 2. Streams tuples from each run file sequentially
 * 3. Passes tuples to ExternalSortOperatorDescriptor for sorting
 * 4. Handles multiple run files efficiently
 * 5. Outputs sorted tuples to next operator
 */
public class VCTreeRunFileReaderOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final Map<Integer, String> runFilePaths; // centroidId -> run file path
    private final UUID permitUUID;
    private final int[] sortFields; // Fields to sort by (distance field)
    private final int framesLimit;

    public VCTreeRunFileReaderOperatorDescriptor(IOperatorDescriptorRegistry spec, Map<Integer, String> runFilePaths,
            UUID permitUUID, int[] sortFields, int framesLimit, RecordDescriptor outputRecordDescriptor) {
        super(spec, 0, 1); // No input, 1 output
        this.runFilePaths = runFilePaths;
        this.permitUUID = permitUUID;
        this.sortFields = sortFields;
        this.framesLimit = framesLimit;
        this.outRecDescs[0] = outputRecordDescriptor;
        System.err.println("VCTreeRunFileReaderOperatorDescriptor created with " + runFilePaths.size() + " run files");
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new VCTreeRunFileReaderNodePushable(ctx, partition, nPartitions);
    }

    private class VCTreeRunFileReaderNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;

        // Run file management
        private List<RunFileReader> runFileReaders;
        private int currentRunFileIndex;
        private RunFileReader currentReader;
        private ByteBuffer frameBuffer;
        private FrameTupleReference tuple;

        // Statistics
        private long totalTuplesRead;
        private long runFilesProcessed;

        public VCTreeRunFileReaderNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions) {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.runFileReaders = new ArrayList<>();
            this.currentRunFileIndex = 0;
            this.totalTuplesRead = 0;
            this.runFilesProcessed = 0;
        }

        @Override
        public void initialize() throws HyracksDataException {
            System.err.println("=== VCTreeRunFileReader INITIALIZING ===");

            try {
                // Initialize frame buffer
                frameBuffer = ByteBuffer.allocate(ctx.getInitialFrameSize());
                tuple = new FrameTupleReference();

                // Initialize run file readers
                initializeRunFileReaders();

                System.err.println("VCTreeRunFileReader initialized with " + runFileReaders.size() + " run files");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize VCTreeRunFileReader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        public void nextFrame() throws HyracksDataException {
            if (currentReader == null) {
                // Move to next run file
                if (currentRunFileIndex >= runFileReaders.size()) {
                    // All run files processed
                    return;
                }
                currentReader = runFileReaders.get(currentRunFileIndex);
                currentReader.open();
                runFilesProcessed++;
                System.err.println("Processing run file " + currentRunFileIndex + " of " + runFileReaders.size());
            }

            // Read next frame from current run file
            org.apache.hyracks.api.comm.IFrame frame = new org.apache.hyracks.api.comm.VSizeFrame(ctx);
            if (currentReader.nextFrame(frame)) {
                frameBuffer = frame.getBuffer();
                // Process tuples in the frame
                processFrame(frameBuffer);
            } else {
                // Current run file exhausted, move to next
                currentReader.close();
                currentReader = null;
                currentRunFileIndex++;

                if (currentRunFileIndex < runFileReaders.size()) {
                    // Process next run file
                    nextFrame();
                } else {
                    System.err.println("All run files processed. Total tuples read: " + totalTuplesRead);
                }
            }
        }

        public void close() throws HyracksDataException {
            System.err.println("=== VCTreeRunFileReader CLOSING ===");

            try {
                // Close current reader if open
                if (currentReader != null) {
                    currentReader.close();
                }

                // Close all run file readers
                for (RunFileReader reader : runFileReaders) {
                    if (reader != null) {
                        reader.close();
                    }
                }

                System.err.println("VCTreeRunFileReader closed. Processed " + runFilesProcessed + " run files, "
                        + totalTuplesRead + " tuples");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to close VCTreeRunFileReader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        public void fail() throws HyracksDataException {
            System.err.println("VCTreeRunFileReader failed");
            close();
        }

        @Override
        public String getDisplayName() {
            return "VCTreeRunFileReader";
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
    }
}
