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

import org.apache.asterix.common.storage.OptimizedScalarQuantizationSampleFile;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IIndex;

/**
 * Operator that receives sorted scalar values (doubles) and computes quantiles (min, max) and alpha.
 * Since data is sorted, we can compute quantiles frame-by-frame by tracking position.
 * 
 * This operator:
 * 1. Receives sorted double values frame-by-frame
 * 2. Tracks total count and current position
 * 3. Extracts minQ and maxQ at quantile positions
 * 4. Calculates alpha after all frames
 * 5. Stores results in .metadata file
 * 6. Passes through all data to sink
 */
public class QuantileCalculatorOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final float confidenceInterval;
    private final int bits;
    private final IIndexDataflowHelperFactory indexHelperFactory;

    public QuantileCalculatorOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDesc,
            float confidenceInterval, int bits, IIndexDataflowHelperFactory indexHelperFactory) {
        super(spec, 1, 1);
        this.confidenceInterval = confidenceInterval;
        this.bits = bits;
        this.indexHelperFactory = indexHelperFactory;
        this.outRecDescs[0] = recordDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new QuantileCalculatorNodePushable(ctx, partition);
    }

    private class QuantileCalculatorNodePushable extends AbstractUnaryInputOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final int partition;
        private final float confidenceInterval;
        private final int bits;
        private final IIndexDataflowHelperFactory indexHelperFactory;
        private FrameTupleAccessor fta;
        private FrameTupleReference tuple;
        private int totalCount;
        private double minQ;
        private double maxQ;
        private RunFileWriter sortedDataWriter;
        private boolean sortedDataMaterialized;

        public QuantileCalculatorNodePushable(IHyracksTaskContext ctx, int partition) {
            this.ctx = ctx;
            this.partition = partition;
            this.confidenceInterval = QuantileCalculatorOperatorDescriptor.this.confidenceInterval;
            this.bits = QuantileCalculatorOperatorDescriptor.this.bits;
            this.indexHelperFactory = QuantileCalculatorOperatorDescriptor.this.indexHelperFactory;
            this.totalCount = 0;
            this.minQ = Double.NaN;
            this.maxQ = Double.NaN;
            this.sortedDataMaterialized = false;
        }

        @Override
        public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
            if (index != 0) {
                throw new IllegalStateException();
            }
            this.writer = writer;
            this.recordDesc = recordDesc;
        }

        @Override
        public void open() throws HyracksDataException {
            writer.open();
            fta = new FrameTupleAccessor(outRecDescs[0]);
            tuple = new FrameTupleReference();

            // Create run file to materialize sorted data for quantile extraction
            FileReference sortedDataFile =
                    ctx.getJobletContext().createManagedWorkspaceFile("QuantileCalculatorSortedData_" + partition);
            sortedDataWriter = new RunFileWriter(sortedDataFile, ctx.getIoManager());
            sortedDataWriter.open();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            fta.reset(buffer);
            int tupleCount = fta.getTupleCount();

            // Count tuples and materialize sorted data
            for (int i = 0; i < tupleCount; i++) {
                tuple.reset(fta, i);
                totalCount++;
            }

            // Materialize sorted frame to run file for later quantile extraction
            sortedDataWriter.nextFrame(buffer);
            sortedDataMaterialized = true;

            // Pass through frame to sink
            writer.nextFrame(buffer);
        }

        @Override
        public void close() throws HyracksDataException {
            try {
                // Close sorted data writer
                if (sortedDataWriter != null) {
                    sortedDataWriter.close();
                }

                // Calculate quantiles from sorted data if we have data
                if (totalCount > 0 && sortedDataMaterialized) {
                    calculateQuantiles();
                }
            } finally {
                writer.close();
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            if (sortedDataWriter != null) {
                try {
                    sortedDataWriter.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
            writer.fail();
        }

        /**
         * Calculate quantiles by reading the materialized sorted run file.
         * Since data is sorted, we can extract values at quantile positions.
         */
        private void calculateQuantiles() throws HyracksDataException {
            try {
                // Calculate quantile indices
                float half = (1.0f - confidenceInterval) / 2.0f;
                int lowerIdx = (int) Math.floor(half * (totalCount - 1));
                int upperIdx = (int) Math.ceil((1.0f - half) * (totalCount - 1));

                // Clamp indices to valid range
                lowerIdx = Math.max(0, Math.min(lowerIdx, totalCount - 1));
                upperIdx = Math.max(0, Math.min(upperIdx, totalCount - 1));

                System.err.println("Calculating quantiles: totalCount=" + totalCount + ", lowerIdx=" + lowerIdx
                        + ", upperIdx=" + upperIdx);

                // Read sorted run file and extract values at quantile positions
                GeneratedRunFileReader reader = sortedDataWriter.createReader();
                reader.open();

                try {
                    int currentPosition = 0;
                    VSizeFrame frame = new VSizeFrame(ctx);
                    FrameTupleAccessor readFta = new FrameTupleAccessor(outRecDescs[0]);
                    FrameTupleReference readTuple = new FrameTupleReference();

                    while (reader.nextFrame(frame)) {
                        ByteBuffer buffer = frame.getBuffer();
                        readFta.reset(buffer);
                        int tupleCount = readFta.getTupleCount();

                        for (int i = 0; i < tupleCount; i++) {
                            readTuple.reset(readFta, i);

                            // Extract value at quantile positions
                            if (currentPosition == lowerIdx) {
                                minQ = DoublePointable.getDouble(readTuple.getFieldData(0), readTuple.getFieldStart(0));
                                System.err.println("Extracted minQ at position " + currentPosition + ": " + minQ);
                            }
                            if (currentPosition == upperIdx) {
                                maxQ = DoublePointable.getDouble(readTuple.getFieldData(0), readTuple.getFieldStart(0));
                                System.err.println("Extracted maxQ at position " + currentPosition + ": " + maxQ);
                            }

                            currentPosition++;

                            // Early exit if we've found both quantiles
                            if (!Double.isNaN(minQ) && !Double.isNaN(maxQ)) {
                                break;
                            }
                        }

                        // Early exit if we've found both quantiles
                        if (!Double.isNaN(minQ) && !Double.isNaN(maxQ)) {
                            break;
                        }
                    }

                    // Calculate alpha
                    if (!Double.isNaN(minQ) && !Double.isNaN(maxQ)) {
                        // Avoid division by zero
                        double eps = 1e-12;
                        if (maxQ <= minQ + eps) {
                            maxQ = minQ + 1e-6;
                        }

                        int levels = 1 << bits; // 2^bits
                        float alpha = (float) ((levels - 1) / (maxQ - minQ));

                        System.err.println("Quantiles calculated: minQ=" + minQ + ", maxQ=" + maxQ + ", alpha=" + alpha
                                + ", totalCount=" + totalCount);

                        // Store quantiles in .metadata file
                        storeQuantilesInMetadata((float) minQ, (float) maxQ, alpha);
                    }

                } finally {
                    reader.close();
                }

            } catch (Exception e) {
                System.err.println("ERROR: Failed to calculate quantiles: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Store quantiles and alpha in .metadata file similar to quantization params.
         */
        private void storeQuantilesInMetadata(float minQ, float maxQ, float alpha) throws HyracksDataException {
            if (indexHelperFactory == null) {
                System.err.println("WARNING: indexHelperFactory is null, cannot store quantiles");
                return;
            }

            IIndexDataflowHelper indexHelper = null;
            try {
                indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                indexHelper.open();
                IIndex indexInstance = indexHelper.getIndexInstance();
                if (!(indexInstance instanceof ILSMIndex)) {
                    System.err.println("WARNING: Index is not LSM index, skipping quantile storage");
                    return;
                }

                ILSMIndex lsmIndex = (ILSMIndex) indexInstance;
                // Get index directory from LocalResource
                org.apache.hyracks.storage.common.LocalResource resource = indexHelper.getResource();
                String resourcePath = resource.getPath();
                IIOManager ioManager = ctx.getJobletContext().getServiceContext().getIoManager();
                FileReference indexDir = ioManager.resolve(resourcePath);

                // Create Params object with quantiles
                // Note: vectorDimensions=1 since we're dealing with scalar values
                OptimizedScalarQuantizationSampleFile.Params params = new OptimizedScalarQuantizationSampleFile.Params(
                        bits, 1, totalCount, confidenceInterval, minQ, maxQ, alpha);

                // Write to .metadata file
                OptimizedScalarQuantizationSampleFile.write(ctx.getIoManager(), indexDir, params);

                System.err.println("Stored quantiles in .metadata file: minQ=" + minQ + ", maxQ=" + maxQ + ", alpha="
                        + alpha + ", bits=" + bits + ", sampleCount=" + totalCount);

            } catch (Exception e) {
                System.err.println("WARNING: Failed to store quantiles in metadata: " + e.getMessage());
                e.printStackTrace();
                // Don't fail the job - quantile storage is optional
            } finally {
                if (indexHelper != null) {
                    try {
                        indexHelper.close();
                    } catch (Exception e) {
                        // Ignore
                    }
                }
            }
        }
    }
}
