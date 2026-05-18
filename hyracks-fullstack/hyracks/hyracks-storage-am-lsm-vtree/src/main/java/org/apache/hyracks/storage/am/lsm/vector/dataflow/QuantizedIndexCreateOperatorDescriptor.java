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

/**
 * Operator descriptor for creating a quantized vector index.
 *
 * Input: a single tuple containing the serialized quantization parameters
 *        (minQuantile, maxQuantile, alpha, bits, confidenceInterval, sampleCount).
 * Output: none (sink). On close() the underlying {@link IIndexBuilder}s are built,
 *         propagating the extracted quantization parameters to each storage partition.
 */
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
        private final IIndexBuilder[] indexBuilders;
        private final FrameTupleAccessor tupleAccessor;
        private final FrameTupleReference tupleHelper;
        private Map<String, Object> quantizationParams;

        public QuantizedIndexCreateOperatorNodePushable(IHyracksTaskContext ctx, IIndexBuilder[] indexBuilders,
                RecordDescriptor inputRecordDesc) {
            this.indexBuilders = indexBuilders;
            this.tupleAccessor = new FrameTupleAccessor(inputRecordDesc);
            this.tupleHelper = new FrameTupleReference();
        }

        @Override
        public void open() throws HyracksDataException {
            // No-op: index builders are constructed eagerly; build() is deferred until close().
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            tupleAccessor.reset(buffer);
            if (tupleAccessor.getTupleCount() > 0 && quantizationParams == null) {
                tupleHelper.reset(tupleAccessor, 0);
                quantizationParams = extractQuantizationParams(tupleHelper);
                if (quantizationParams != null) {
                    for (IIndexBuilder indexBuilder : indexBuilders) {
                        if (indexBuilder instanceof IndexBuilder) {
                            ((IndexBuilder) indexBuilder).setQuantizationParameters(quantizationParams);
                        }
                    }
                }
            }
        }

        private Map<String, Object> extractQuantizationParams(FrameTupleReference tuple) {
            byte[] data = tuple.getFieldData(0);
            int start = tuple.getFieldStart(0);
            int length = tuple.getFieldLength(0);

            ByteArrayPointable ptr = new ByteArrayPointable();
            ptr.set(data, start + 1, length - 1);

            byte[] content = ptr.getByteArray();
            int offset = ptr.getContentStartOffset();

            Map<String, Object> params = new HashMap<>();

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
        }

        @Override
        public void close() throws HyracksDataException {
            if (quantizationParams != null) {
                for (IIndexBuilder indexBuilder : indexBuilders) {
                    indexBuilder.build();
                }
            } else {
                throw new HyracksDataException("QuantizedIndexCreateOperator: No quantization constants received.");
            }
        }
    }
}
