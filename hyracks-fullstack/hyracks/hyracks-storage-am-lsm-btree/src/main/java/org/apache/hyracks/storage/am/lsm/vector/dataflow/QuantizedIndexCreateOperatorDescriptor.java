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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.api.IIndexBuilderFactory;
import org.apache.hyracks.storage.am.common.build.IndexBuilder;

public class QuantizedIndexCreateOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexBuilderFactory[][] indexBuilderFactories;
    private final int[][] partitionsMap;

    public QuantizedIndexCreateOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexBuilderFactory[][] indexBuilderFactories, int[][] partitionsMap, RecordDescriptor recordDesc) {
        super(spec, 1, 0);
        this.indexBuilderFactories = indexBuilderFactories;
        this.partitionsMap = partitionsMap;

    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        int[] storagePartitions = partitionsMap[partition];
        IIndexBuilderFactory[] partitionIndexBuilderFactories = indexBuilderFactories[partition];
        IIndexBuilder[] indexBuilders = new IIndexBuilder[storagePartitions.length];
        for (int i = 0; i < storagePartitions.length; i++) {
            indexBuilders[i] = partitionIndexBuilderFactories[i].create(ctx, storagePartitions[i]);
        }
        RecordDescriptor inputRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new QuantizedIndexCreateOperatorNodePushable(ctx, indexBuilders, inputRecordDesc);
    }

    private static class QuantizedIndexCreateOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final IIndexBuilder[] indexBuilders;
        private final FrameTupleAccessor tupleAccessor;
        private final FrameTupleReference tupleHelper;
        private Map<String, Object> quantizationParams;

        public QuantizedIndexCreateOperatorNodePushable(IHyracksTaskContext ctx, IIndexBuilder[] indexBuilders,
                RecordDescriptor inputRecordDesc) {
            this.ctx = ctx;
            this.indexBuilders = indexBuilders;
            this.tupleAccessor = new FrameTupleAccessor(inputRecordDesc);
            this.tupleHelper = new FrameTupleReference();
        }

        @Override
        public void open() throws HyracksDataException {
            // Nothing to initialize here
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            tupleAccessor.reset(buffer);
            int tupleCount = tupleAccessor.getTupleCount();
            System.err.println("[QuantizedIndexCreateOp] nextFrame() - received frame with " + tupleCount + " tuples");
            if (tupleCount > 0) {
                // We expect only 1 tuple containing the quantization constants
                // But since it's broadcasted, we might receive it multiple times or in batches
                // We just need to read it once.
                if (quantizationParams == null) {
                    System.err.println("[QuantizedIndexCreateOp] nextFrame() - extracting params (first time)");
                    tupleHelper.reset(tupleAccessor, 0);
                    quantizationParams = extractQuantizationParams(tupleHelper);
                    if (quantizationParams != null) {
                        System.err
                                .println("[QuantizedIndexCreateOp] nextFrame() - calling setQuantizationParameters on "
                                        + indexBuilders.length + " builders");
                        for (IIndexBuilder indexBuilder : indexBuilders) {
                            System.err.println("[QuantizedIndexCreateOp] nextFrame() - builder class: "
                                    + indexBuilder.getClass().getName());
                            if (indexBuilder instanceof IndexBuilder) {
                                System.err.println("[QuantizedIndexCreateOp] nextFrame() - calling setter");
                                ((IndexBuilder) indexBuilder).setQuantizationParameters(quantizationParams);
                            } else {
                                System.err.println(
                                        "[QuantizedIndexCreateOp] nextFrame() - WARNING: not an IndexBuilder!");
                            }
                        }
                    } else {
                        System.err.println("[QuantizedIndexCreateOp] nextFrame() - WARNING: extracted params is NULL");
                    }
                } else {
                    System.err.println("[QuantizedIndexCreateOp] nextFrame() - params already extracted");
                }
            }
        }

        private Map<String, Object> extractQuantizationParams(FrameTupleReference tuple) {
            // Field 0: ABINARY containing serialized QuantizationConstants
            byte[] data = tuple.getFieldData(0);
            int start = tuple.getFieldStart(0);
            int length = tuple.getFieldLength(0);
            System.err.println("[QuantizedIndexCreateOp] extractQuantizationParams() - field length=" + length);

            // Skip Type Tag (1 byte) + Length (2 bytes for simple binary, or VarLen)
            // Using ByteArrayPointable to handle the length descriptor automatically
            ByteArrayPointable ptr = new ByteArrayPointable();
            // Skip the type tag (1 byte)
            ptr.set(data, start + 1, length - 1);

            byte[] content = ptr.getByteArray();
            int offset = ptr.getContentStartOffset();

            Map<String, Object> params = new HashMap<>();

            // Format: [minQ][maxQ][alpha][bits][conf][count]
            // Float (4) + Float (4) + Float (4) + Int (4) + Float (4) + Int (4) = 24 bytes

            params.put("minQuantile", Float.intBitsToFloat(getInt(content, offset)));
            offset += 4;

            params.put("maxQuantile", Float.intBitsToFloat(getInt(content, offset)));
            offset += 4;

            params.put("alpha", Float.intBitsToFloat(getInt(content, offset)));
            offset += 4;

            params.put("bits", getInt(content, offset));
            offset += 4;

            params.put("confidenceInterval", Float.intBitsToFloat(getInt(content, offset)));
            offset += 4;

            params.put("sampleCount", getInt(content, offset));

            return params;
        }

        private int getInt(byte[] bytes, int offset) {
            return ((bytes[offset] & 0xff) << 24) | ((bytes[offset + 1] & 0xff) << 16)
                    | ((bytes[offset + 2] & 0xff) << 8) | (bytes[offset + 3] & 0xff);
        }

        @Override
        public void fail() throws HyracksDataException {
            // cleanup if needed
        }

        @Override
        public void close() throws HyracksDataException {
            // Once we have processed the input (quantization constants), proceed to build
            // the indexes
            System.err.println("[QuantizedIndexCreateOp] close() - quantizationParams=" + quantizationParams);
            if (quantizationParams != null) {
                System.err.println("[QuantizedIndexCreateOp] close() - building " + indexBuilders.length + " indexes");
                for (IIndexBuilder indexBuilder : indexBuilders) {
                    indexBuilder.build();
                }
            } else {
                throw new HyracksDataException("QuantizedIndexCreateOperator: No quantization constants received.");
            }
        }
    }
}
