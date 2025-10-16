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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.sort.AbstractSorterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.IRunGenerator;

/**
 * Operator that integrates VCTreeRunFileReaderOperatorDescriptor with ExternalSortOperatorDescriptor.
 * 
 * This operator:
 * 1. Reads unsorted run files using VCTreeRunFileReaderOperatorDescriptor
 * 2. Streams tuples to ExternalSortOperatorDescriptor for sorting
 * 3. Outputs sorted tuples to VCTreeStaticStructureBulkLoaderOperatorDescriptor
 * 4. Handles the complete data flow pipeline efficiently
 */
public class VCTreeSortingPipelineOperatorDescriptor extends AbstractSorterOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final Map<Integer, String> runFilePaths; // centroidId -> run file path
    private final UUID permitUUID;
    private final int framesLimit;

    public VCTreeSortingPipelineOperatorDescriptor(IOperatorDescriptorRegistry spec, Map<Integer, String> runFilePaths,
            UUID permitUUID, int framesLimit, int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        super(spec, framesLimit, sortFields, keyNormalizerFactories, comparatorFactories, recordDescriptor);
        this.runFilePaths = runFilePaths;
        this.permitUUID = permitUUID;
        this.framesLimit = framesLimit;
        System.err
                .println("VCTreeSortingPipelineOperatorDescriptor created with " + runFilePaths.size() + " run files");
    }

    @Override
    public AbstractSorterOperatorDescriptor.SortActivity getSortActivity(
            org.apache.hyracks.api.dataflow.ActivityId id) {
        return new VCTreeSortingPipelineSortActivity(id);
    }

    @Override
    public AbstractSorterOperatorDescriptor.MergeActivity getMergeActivity(
            org.apache.hyracks.api.dataflow.ActivityId id) {
        return new VCTreeSortingPipelineMergeActivity(id);
    }

    /**
     * Sort activity that reads from run files and generates sorted runs.
     */
    private class VCTreeSortingPipelineSortActivity extends AbstractSorterOperatorDescriptor.SortActivity {
        private static final long serialVersionUID = 1L;

        protected VCTreeSortingPipelineSortActivity(org.apache.hyracks.api.dataflow.ActivityId id) {
            super(id);
        }

        @Override
        protected IRunGenerator getRunGenerator(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider)
                throws HyracksDataException {
            return new VCTreeRunFileRunGenerator(ctx, runFilePaths, permitUUID, sortFields, keyNormalizerFactories,
                    comparatorFactories, outRecDescs[0], framesLimit);
        }
    }

    /**
     * Merge activity that merges sorted runs and outputs final sorted data.
     */
    private class VCTreeSortingPipelineMergeActivity extends AbstractSorterOperatorDescriptor.MergeActivity {
        private static final long serialVersionUID = 1L;

        protected VCTreeSortingPipelineMergeActivity(org.apache.hyracks.api.dataflow.ActivityId id) {
            super(id);
        }

        @Override
        protected org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunMerger getSortRunMerger(
                IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                List<GeneratedRunFileReader> runs,
                org.apache.hyracks.api.dataflow.value.IBinaryComparator[] comparators,
                org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer nmkComputer, int necessaryFrames) {
            return new org.apache.hyracks.dataflow.std.sort.ExternalSortRunMerger(ctx, runs, sortFields, comparators,
                    nmkComputer, outRecDescs[0], necessaryFrames, Integer.MAX_VALUE);
        }
    }

    /**
     * Run generator that reads from VCTree run files and creates sorted runs.
     */
    private static class VCTreeRunFileRunGenerator implements IRunGenerator {
        private final IHyracksTaskContext ctx;
        private final Map<Integer, String> runFilePaths;
        private final UUID permitUUID;
        private final int[] sortFields;
        private final INormalizedKeyComputerFactory[] keyNormalizerFactories;
        private final IBinaryComparatorFactory[] comparatorFactories;
        private final RecordDescriptor recordDescriptor;
        private final int framesLimit;

        private org.apache.hyracks.dataflow.std.sort.IRunGenerator runGenerator;
        private java.util.List<GeneratedRunFileReader> runs;
        private boolean opened = false;

        public VCTreeRunFileRunGenerator(IHyracksTaskContext ctx, Map<Integer, String> runFilePaths, UUID permitUUID,
                int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
                IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, int framesLimit) {
            this.ctx = ctx;
            this.runFilePaths = runFilePaths;
            this.permitUUID = permitUUID;
            this.sortFields = sortFields;
            this.keyNormalizerFactories = keyNormalizerFactories;
            this.comparatorFactories = comparatorFactories;
            this.recordDescriptor = recordDescriptor;
            this.framesLimit = framesLimit;
            this.runs = new java.util.ArrayList<>();
        }

        @Override
        public void open() throws HyracksDataException {
            if (opened) {
                return;
            }

            System.err.println("=== VCTreeRunFileRunGenerator OPENING ===");

            try {
                // Create run generator for external sorting
                runGenerator = new org.apache.hyracks.dataflow.std.sort.ExternalSortRunGenerator(ctx, sortFields,
                        keyNormalizerFactories, comparatorFactories, recordDescriptor,
                        org.apache.hyracks.dataflow.std.sort.Algorithm.MERGE_SORT,
                        org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy.LAST_FIT, framesLimit,
                        Integer.MAX_VALUE);

                runGenerator.open();
                opened = true;

                System.err.println("VCTreeRunFileRunGenerator opened successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeRunFileRunGenerator: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            if (!opened) {
                throw new HyracksDataException("RunGenerator not opened");
            }

            // Process frame from run files
            runGenerator.nextFrame(buffer);
        }

        @Override
        public void close() throws HyracksDataException {
            if (!opened) {
                return;
            }

            System.err.println("=== VCTreeRunFileRunGenerator CLOSING ===");

            try {
                runGenerator.close();
                runs = runGenerator.getRuns();

                System.err.println("VCTreeRunFileRunGenerator closed. Generated " + runs.size() + " runs");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to close VCTreeRunFileRunGenerator: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            System.err.println("VCTreeRunFileRunGenerator failed");
            if (runGenerator != null) {
                try {
                    runGenerator.fail();
                } catch (Exception e) {
                    System.err.println("Error failing run generator: " + e.getMessage());
                }
            }
        }

        @Override
        public List<GeneratedRunFileReader> getRuns() {
            return runs;
        }

        @Override
        public org.apache.hyracks.dataflow.std.sort.ISorter getSorter() {
            return runGenerator.getSorter();
        }
    }
}
