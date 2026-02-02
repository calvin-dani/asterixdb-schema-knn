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
import org.apache.asterix.common.storage.QuantizationConstantsFileManager;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

/**
 * Sink operator that extracts QuantizationConstants from the input tuple
 * (which contains a BINARY field with serialized QuantizationConstants)
 * and writes them to a sidecar file for retrieval by subsequent jobs.
 * 
 * The sidecar file is written to the dataset directory with the name:
 * .quantization_<indexName>
 * 
 * This file-based approach is used because IOperatorStats aggregation
 * only copies a limited set of counters (tupleCounter, timeCounter, pageReads),
 * which is insufficient for passing all quantization parameters.
 */
public class QuantizationConstantsSinkOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 3L;
    
    private final String quantizationKey;
    private final RecordDescriptor inputRecDesc;
    private final FileSplit[] datasetFileSplits;
    private final String indexName;

    public QuantizationConstantsSinkOperatorDescriptor(IOperatorDescriptorRegistry spec, String quantizationKey,
            RecordDescriptor inputRecDesc, FileSplit[] datasetFileSplits, String indexName) {
        super(spec, 1, 0); // 1 input, 0 outputs
        this.quantizationKey = quantizationKey;
        this.inputRecDesc = inputRecDesc;
        this.datasetFileSplits = datasetFileSplits;
        this.indexName = indexName;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        RecordDescriptor inputRecordDescriptor = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        // Pass the FileSplit for this partition - FileReference will be resolved in close() using IIOManager
        FileSplit fileSplit = datasetFileSplits[partition];
        return new QuantizationConstantsSinkNodePushable(ctx, inputRecordDescriptor, quantizationKey, 
                fileSplit, indexName);
    }

    private static class QuantizationConstantsSinkNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final FrameTupleAccessor frameTupleAccessor;
        private final FrameTupleReference frameTupleReference;
        private final String quantizationKey;
        private final FileSplit fileSplit;
        private final String indexName;
        private QuantizationConstants quantizationConstants;

        private QuantizationConstantsSinkNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecordDescriptor,
                String quantizationKey, FileSplit fileSplit, String indexName) {
            this.ctx = ctx;
            this.frameTupleAccessor = new FrameTupleAccessor(inputRecordDescriptor);
            this.frameTupleReference = new FrameTupleReference();
            this.quantizationKey = quantizationKey;
            this.fileSplit = fileSplit;
            this.indexName = indexName;
        }

        @Override
        public void open() throws HyracksDataException {
            // No initialization needed - we write to sidecar file in close()
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
         * Format: [typeTag (byte)] [variable-length encoded length prefix (1-5 bytes)] [content (24 bytes)]
         * Content format: [minQ (float)] [maxQ (float)] [alpha (float)] [bits (int)] [confidenceInterval (float)] [sampleCount (int)]
         * 
         * Uses ByteArrayPointable to correctly handle the variable-length encoded length prefix,
         * similar to how RangeMap's SortForwardOperatorDescriptor handles binary data.
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

                // Use ByteArrayPointable to directly access content (skips length prefix automatically)
                // Similar to how RangeMap's SortForwardOperatorDescriptor handles binary data
                ByteArrayPointable pointable = new ByteArrayPointable();
                pointable.set(fieldData, fieldStart + 1, fieldLength - 1); // Skip type tag
                
                // Get content start offset (after length prefix) and content length (24 bytes)
                int contentStartOffset = pointable.getContentStartOffset();
                int contentLength = pointable.getContentLength();
                
                System.err.println("[QuantizationConstantsSink] Binary content: startOffset=" + contentStartOffset + 
                        ", contentLength=" + contentLength + ", expected=24");
                
                // Create stream from the actual content bytes (skipping length prefix)
                ByteArrayInputStream constantsBais = new ByteArrayInputStream(
                    pointable.getByteArray(),
                    contentStartOffset,
                    contentLength);
                DataInput constantsIn = new DataInputStream(constantsBais);

                // Deserialize QuantizationConstants from the content bytes
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
            if (quantizationConstants == null) {
                System.err.println("[QuantizationConstantsSink] WARNING: No quantization constants computed (empty dataset or no samples)");
            } else {
                // Write quantization constants to sidecar file for retrieval by subsequent jobs
                IIOManager ioManager = ctx.getIoManager();
                
                // Resolve FileReference from FileSplit using IIOManager (must be done here, not in createPushRuntime)
                // The FileSplit points to the PRIMARY INDEX path: .../datasetName/rebalanceCount/datasetName
                // We need to go up 2 levels to get the actual DATASET directory: .../datasetName
                FileReference primaryIndexDir = fileSplit.getFileReference(ioManager);
                FileReference rebalanceDir = primaryIndexDir.getParent();  // .../datasetName/rebalanceCount
                FileReference datasetDir = rebalanceDir.getParent();       // .../datasetName
                
                System.err.println("[QuantizationConstantsSink] Writing quantization constants to sidecar file");
                System.err.println("[QuantizationConstantsSink] Primary index dir: " + primaryIndexDir.getAbsolutePath());
                System.err.println("[QuantizationConstantsSink] Dataset dir: " + datasetDir.getAbsolutePath());
                System.err.println("[QuantizationConstantsSink] Index name: " + indexName);
                
                QuantizationConstantsFileManager.write(ioManager, datasetDir, indexName, quantizationConstants);
                
                System.err.println("[QuantizationConstantsSink] Successfully wrote sidecar file: " + quantizationConstants);
            }
            
            // Also store in TaskUtil for backward compatibility (within same job)
            TaskUtil.put(quantizationKey, quantizationConstants, ctx);
        }

        @Override
        public void fail() throws HyracksDataException {
            // No-op
        }
    }
}
