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

import static org.apache.hyracks.api.job.profiling.NoOpOperatorStats.INVALID_ODID;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.IndexStats;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Computes total tuple count and total tuple length for all input tuples,
 * and emits these values as operator stats.
 */
public final class DatasetStreamStatsOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 2L;
    private static final Logger LOGGER = LogManager.getLogger();

    private final String operatorName;
    private final IIndexDataflowHelperFactory[] indexes;
    private final String[] indexesNames;
    private final int[][] partitionsMap;
    private final boolean isSampling;

    public DatasetStreamStatsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            String operatorName, IIndexDataflowHelperFactory[] indexes, String[] indexesNames, int[][] partitionsMap,
            boolean isSampling) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.operatorName = operatorName;
        this.indexes = indexes;
        this.indexesNames = indexesNames;
        this.partitionsMap = partitionsMap;
        this.isSampling = isSampling;
    }

    public DatasetStreamStatsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            String operatorName, IIndexDataflowHelperFactory[] indexes, String[] indexesNames, int[][] partitionsMap) {
        this(spec, rDesc, operatorName, indexes, indexesNames, partitionsMap, false);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private FrameTupleAccessor fta;
            private long totalTupleCount;
            private long totalTupleLength;
            private Map<String, IndexStats> indexesStats;
            private long openDuration;
            private long nextFrameDuration;
            private long closeDuration;

            @Override
            public void open() throws HyracksDataException {
                long start = System.nanoTime();
                fta = new FrameTupleAccessor(outRecDescs[0]);
                totalTupleCount = 0;
                writer.open();
                IStatsCollector coll = ctx.getStatsCollector();
                if (coll != null) {
                    coll.add(new OperatorStats(operatorName, INVALID_ODID));
                }
                INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
                indexesStats = new HashMap<>();
                if (indexes.length > 0) {
                    gatherIndexesStats(serviceCtx, partitionsMap[partition]);
                }
                openDuration = System.nanoTime() - start;
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                long start = System.nanoTime();
                //                fta.reset(buffer, " DATASET STREAM STATS " + partition);
                fta.reset(buffer);
                computeStats();
                FrameUtils.flushFrame(buffer, writer);
                nextFrameDuration += System.nanoTime() - start;
            }

            private void computeStats() {
                int n = fta.getTupleCount();
                totalTupleCount += n;
                for (int i = 0; i < n; i++) {
                    totalTupleLength += fta.getTupleLength(i);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                long start = System.nanoTime();
                IStatsCollector statsCollector = ctx.getStatsCollector();
                if (statsCollector != null) {
                    IOperatorStats stats = statsCollector.getOperatorStats(operatorName);
                    DatasetStreamStats.update(stats, totalTupleCount, totalTupleLength, indexesStats);
                }
                writer.close();
                closeDuration = System.nanoTime() - start;

                if (TaskUtil.get("SAMPLE_OPERATION_IS_GOING", ctx) != null) {
                    LOGGER.debug("StatsLogging: DatasetStreamStats_Open_Time: {}ns", openDuration);
                    LOGGER.debug("StatsLogging: DatasetStreamStats_NextFrame_Time: {}ns", nextFrameDuration);
                    LOGGER.debug("StatsLogging: DatasetStreamStats_Close_Time: {}ns", closeDuration);
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                writer.flush();
            }

            @Override
            public String getDisplayName() {
                return operatorName;
            }

            private void gatherIndexesStats(INCServiceContext srcCtx, int[] partitions) throws HyracksDataException {
                for (int p : partitions) {
                    for (int i = 0; i < indexes.length; i++) {
                        IIndexDataflowHelper idxFlowHelper = indexes[i].create(srcCtx, p);
                        try {
                            idxFlowHelper.open();
                            ILSMIndex indexInstance = (ILSMIndex) idxFlowHelper.getIndexInstance();
                            long numPages = 0;
                            synchronized (indexInstance.getOperationTracker()) {
                                for (ILSMDiskComponent component : indexInstance.getDiskComponents()) {
                                    long componentSize = component.getComponentSize();
                                    if (component instanceof AbstractLSMWithBloomFilterDiskComponent) {
                                        componentSize -= ((AbstractLSMWithBloomFilterDiskComponent) component)
                                                .getBloomFilter().getFileReference().getFile().length();
                                    }
                                    numPages += componentSize / indexInstance.getBufferCache().getPageSize();
                                }
                            }
                            IndexStats indexStats = indexesStats.computeIfAbsent(indexesNames[i],
                                    idxName -> new IndexStats(idxName, 0));
                            indexStats.updateNumPages(numPages);
                        } finally {
                            idxFlowHelper.close();
                        }
                    }
                }
            }
        };
    }
}
