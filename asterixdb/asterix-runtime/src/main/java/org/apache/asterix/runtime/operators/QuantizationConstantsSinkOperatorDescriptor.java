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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.storage.QuantizationConstants;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

/**
 * Sink operator that extracts QuantizationConstants from the input tuple
 * (which contains a BINARY field with serialized QuantizationConstants)
 * and stores them in the Hyracks task context using TaskUtil.
 */
public class QuantizationConstantsSinkOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final String quantizationKey;
    private final RecordDescriptor inputRecDesc;

    public QuantizationConstantsSinkOperatorDescriptor(IOperatorDescriptorRegistry spec, String quantizationKey,
            RecordDescriptor inputRecDesc) {
        super(spec, 1, 0); // 1 input, 0 outputs
        this.quantizationKey = quantizationKey;
        this.inputRecDesc = inputRecDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        RecordDescriptor inputRecordDescriptor = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new QuantizationConstantsSinkNodePushable(ctx, inputRecordDescriptor, quantizationKey);
    }

    private static class QuantizationConstantsSinkNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final FrameTupleAccessor frameTupleAccessor;
        private final FrameTupleReference frameTupleReference;
        private final String quantizationKey;
        private QuantizationConstants quantizationConstants;

        private QuantizationConstantsSinkNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecordDescriptor,
                String quantizationKey) {
            this.ctx = ctx;
            this.frameTupleAccessor = new FrameTupleAccessor(inputRecordDescriptor);
            this.frameTupleReference = new FrameTupleReference();
            this.quantizationKey = quantizationKey;
        }

        @Override
        public void open() throws HyracksDataException {
            // No-op
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            frameTupleAccessor.reset(buffer);
            int tupleCount = frameTupleAccessor.getTupleCount();

            // Expect only one tuple from global aggregate
            if (tupleCount > 0 && quantizationConstants == null) {
                frameTupleReference.reset(frameTupleAccessor, 0);
                quantizationConstants = extractQuantizationConstants(frameTupleReference);
            }
        }

        /**
         * Extracts QuantizationConstants from the input tuple.
         * The tuple contains a BINARY field (field 0) with serialized QuantizationConstants.
         * Format: [typeTag (byte)] [serialized ABinary content]
         * ABinary content format: [length (int)] [minQ (float)] [maxQ (float)] [alpha (float)] [bits (int)] [confidenceInterval (float)] [sampleCount (int)]
         * 
         * @return QuantizationConstants or null if the input is SYSTEM_NULL (empty dataset)
         */
        private QuantizationConstants extractQuantizationConstants(ITupleReference tuple) throws HyracksDataException {
            try {
                // Get BINARY field (field 0)
                byte[] fieldData = tuple.getFieldData(0);
                int fieldStart = tuple.getFieldStart(0);
                int fieldLength = tuple.getFieldLength(0);

                if (fieldLength == 0) {
                    System.err.println("[QuantizationConstantsSink] Empty field - no data");
                    return null;
                }

                // Check type tag
                ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(fieldData[fieldStart]);
                
                // Handle SYSTEM_NULL (empty dataset case)
                if (typeTag == ATypeTag.SYSTEM_NULL || typeTag == ATypeTag.NULL || typeTag == ATypeTag.MISSING) {
                    System.err.println("[QuantizationConstantsSink] Received " + typeTag + " - empty dataset");
                    return null;
                }
                
                if (typeTag != ATypeTag.BINARY) {
                    throw HyracksDataException.create(new IOException("Expected BINARY type, got: " + typeTag));
                }

                // Skip type tag (1 byte) and deserialize ABinary
                int dataOffset = fieldStart + 1; // Skip type tag
                ByteArrayInputStream bais = new ByteArrayInputStream(fieldData, dataOffset, fieldLength - 1);
                DataInput in = new DataInputStream(bais);

                // Deserialize ABinary using ABinarySerializerDeserializer
                @SuppressWarnings("unchecked")
                ISerializerDeserializer<ABinary> binarySerde =
                        SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);
                ABinary binary = binarySerde.deserialize(in);

                // Extract bytes from ABinary and deserialize QuantizationConstants
                byte[] binaryBytes = binary.getBytes();
                int binaryStart = binary.getStart();
                int binaryLength = binary.getLength();

                ByteArrayInputStream constantsBais = new ByteArrayInputStream(binaryBytes, binaryStart, binaryLength);
                DataInput constantsIn = new DataInputStream(constantsBais);

                // Deserialize QuantizationConstants from the data bytes
                // Format: [minQ (float)] [maxQ (float)] [alpha (float)] [bits (int)] [confidenceInterval (float)] [sampleCount (int)]
                float minQ = FloatSerializerDeserializer.read(constantsIn);
                float maxQ = FloatSerializerDeserializer.read(constantsIn);
                float alpha = FloatSerializerDeserializer.read(constantsIn);
                int bits = IntegerSerializerDeserializer.read(constantsIn);
                float confidenceInterval = FloatSerializerDeserializer.read(constantsIn);
                int sampleCount = IntegerSerializerDeserializer.read(constantsIn);

                System.err.println("[QuantizationConstantsSink] Extracted: minQ=" + minQ + ", maxQ=" + maxQ + 
                        ", alpha=" + alpha + ", bits=" + bits + ", sampleCount=" + sampleCount);
                return new QuantizationConstants(minQ, maxQ, alpha, bits, confidenceInterval, sampleCount);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            // Store quantization constants in task context (if available)
            if (quantizationConstants == null) {
                // Empty dataset - no samples were found
                System.err.println("[QuantizationConstantsSink] WARNING: No quantization constants computed (empty dataset or no samples)");
                // Still store null to indicate completion without error
                TaskUtil.put(quantizationKey, null, ctx);
            } else {
                System.err.println("[QuantizationConstantsSink] Storing quantization constants with key=" + quantizationKey);
                TaskUtil.put(quantizationKey, quantizationConstants, ctx);
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            // No-op
        }
    }
}
