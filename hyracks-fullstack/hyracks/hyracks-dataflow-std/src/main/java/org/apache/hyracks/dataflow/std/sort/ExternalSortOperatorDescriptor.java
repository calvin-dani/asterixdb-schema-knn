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
package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalSortOperatorDescriptor extends AbstractSorterOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();

    private Algorithm alg = Algorithm.MERGE_SORT;
    private EnumFreeSlotPolicy policy = EnumFreeSlotPolicy.LAST_FIT;
    private final int outputLimit;

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, Algorithm alg) {
        this(spec, framesLimit, sortFields, keyNormalizerFactories, comparatorFactories, recordDescriptor, alg,
                EnumFreeSlotPolicy.LAST_FIT);
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, sortFields, (INormalizedKeyComputerFactory[]) null, comparatorFactories,
                recordDescriptor);
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, sortFields,
                firstKeyNormalizerFactory != null ? new INormalizedKeyComputerFactory[] { firstKeyNormalizerFactory }
                        : null,
                comparatorFactories, recordDescriptor, Algorithm.MERGE_SORT, EnumFreeSlotPolicy.LAST_FIT);
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, sortFields, keyNormalizerFactories, comparatorFactories, recordDescriptor,
                Algorithm.MERGE_SORT, EnumFreeSlotPolicy.LAST_FIT);
    }

    @Override
    public AbstractSorterOperatorDescriptor.SortActivity getSortActivity(ActivityId id) {
        return new AbstractSorterOperatorDescriptor.SortActivity(id) {
            private static final long serialVersionUID = 1L;

            @Override
            protected IRunGenerator getRunGenerator(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider) throws HyracksDataException {
                IRunGenerator runGen = new ExternalSortRunGenerator(ctx, sortFields, keyNormalizerFactories,
                        comparatorFactories, outRecDescs[0], alg, policy, framesLimit, outputLimit);
                return runGen;
            }

            @Override
            public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                    final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
                final IOperatorNodePushable op =
                        super.createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
                return new IOperatorNodePushable() {
                    long openDuration;
                    long nextFrameDuration;
                    long closeDuration;

                    @Override
                    public void initialize() throws HyracksDataException {
                        op.initialize();
                    }

                    @Override
                    public void deinitialize() throws HyracksDataException {
                        op.deinitialize();
                    }

                    @Override
                    public int getInputArity() {
                        return op.getInputArity();
                    }

                    @Override
                    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                            throws HyracksDataException {
                        op.setOutputFrameWriter(index, writer, recordDesc);
                    }

                    @Override
                    public IFrameWriter getInputFrameWriter(int index) {
                        final IFrameWriter writer = op.getInputFrameWriter(index);
                        return new IFrameWriter() {
                            @Override
                            public void open() throws HyracksDataException {
                                long start = System.nanoTime();
                                writer.open();
                                openDuration = System.nanoTime() - start;
                            }

                            @Override
                            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                                long start = System.nanoTime();
                                writer.nextFrame(buffer);
                                nextFrameDuration += System.nanoTime() - start;
                            }

                            @Override
                            public void fail() throws HyracksDataException {
                                writer.fail();
                            }

                            @Override
                            public void close() throws HyracksDataException {
                                long start = System.nanoTime();
                                try {
                                    writer.close();
                                } finally {
                                    closeDuration = System.nanoTime() - start;
                                    if (TaskUtil.get("SAMPLE_OPERATION_IS_GOING", ctx) != null) {
                                        LOGGER.debug("StatsLogging: ExternalSort_SortActivity_Open_Time: {}ns",
                                                openDuration);
                                        LOGGER.debug("StatsLogging: ExternalSort_SortActivity_NextFrame_Time: {}ns",
                                                nextFrameDuration);
                                        LOGGER.debug("StatsLogging: ExternalSort_SortActivity_Close_Time: {}ns",
                                                closeDuration);
                                    }
                                }
                            }

                            @Override
                            public void flush() throws HyracksDataException {
                                writer.flush();
                            }
                        };
                    }

                    @Override
                    public String getDisplayName() {
                        return op.getDisplayName();
                    }
                };
            }
        };
    }

    @Override
    public AbstractSorterOperatorDescriptor.MergeActivity getMergeActivity(ActivityId id) {
        return new AbstractSorterOperatorDescriptor.MergeActivity(id) {
            private static final long serialVersionUID = 1L;

            @Override
            protected AbstractExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
                    IRecordDescriptorProvider recordDescProvider, List<GeneratedRunFileReader> runs,
                    IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer, int necessaryFrames) {
                return new ExternalSortRunMerger(ctx, runs, sortFields, comparators, nmkComputer, outRecDescs[0],
                        necessaryFrames, outputLimit);
            }

            @Override
            public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                    final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
                final IOperatorNodePushable op =
                        super.createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
                return new IOperatorNodePushable() {
                    long initializeDuration;

                    @Override
                    public void initialize() throws HyracksDataException {
                        long start = System.nanoTime();
                        try {
                            op.initialize();
                        } finally {
                            initializeDuration = System.nanoTime() - start;
                            if (TaskUtil.get("SAMPLE_OPERATION_IS_GOING", ctx) != null) {
                                LOGGER.debug("StatsLogging: ExternalSort_MergeActivity_Initialize_Time: {}ns",
                                        initializeDuration);
                            }
                        }
                    }

                    @Override
                    public void deinitialize() throws HyracksDataException {
                        op.deinitialize();
                    }

                    @Override
                    public int getInputArity() {
                        return op.getInputArity();
                    }

                    @Override
                    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                            throws HyracksDataException {
                        op.setOutputFrameWriter(index, writer, recordDesc);
                    }

                    @Override
                    public IFrameWriter getInputFrameWriter(int index) {
                        return op.getInputFrameWriter(index);
                    }

                    @Override
                    public String getDisplayName() {
                        return op.getDisplayName();
                    }
                };
            }
        };
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, Algorithm alg, EnumFreeSlotPolicy policy) {
        this(spec, framesLimit, sortFields, keyNormalizerFactories, comparatorFactories, recordDescriptor, alg, policy,
                Integer.MAX_VALUE);
    }

    public ExternalSortOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, Algorithm alg, EnumFreeSlotPolicy policy, int outputLimit) {
        super(spec, framesLimit, sortFields, keyNormalizerFactories, comparatorFactories, recordDescriptor);
        if (framesLimit <= 1) {
            throw new IllegalStateException();// minimum of 2 frames (1 in,1 out)
        }
        this.alg = alg;
        this.policy = policy;
        this.outputLimit = outputLimit;
    }

}
