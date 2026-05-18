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

package org.apache.hyracks.storage.am.vector.impls;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleCreator;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

/**
 * Transforms input tuples from operator format to VTree data page storage format.
 *
 * Input tuple format: [vector, include_fields..., pk]
 *
 * Non-quantized output: [distance, centroidId, pk, include_fields...]
 * Quantized output:     [distance, centroidId, quantized_distance, quantized_embedding, pk, include_fields...]
 *
 * The returned ITupleReference is valid until the next call to {@link #createDataTuple}.
 */
public class VTreeDataTupleCreator implements IVTreeDataTupleCreator {

    // Indices into the raw quantizationParams float[] produced by OptimizedScalarQuantization:
    // {minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}. Index 3
    // (confidenceInterval) and 5 (sampleCount) are not consumed here.
    private static final int OSQ_MIN_QUANTILE = 0;
    private static final int OSQ_MAX_QUANTILE = 1;
    private static final int OSQ_ALPHA = 2;
    private static final int OSQ_BITS = 4;

    // Number of leading (non-PK, non-include) fields written per data tuple.
    // Non-quantized: [distance, centroidId]. Quantized adds [quantizedDistance, quantizedEmbedding].
    private static final int NUM_LEADING_FIELDS_NON_QUANTIZED = 2;
    private static final int NUM_LEADING_FIELDS_QUANTIZED = 4;

    private final int numIncludeFields;

    private final boolean isQuantized;
    private final float[] quantizationParams;
    private final ArrayTupleBuilder tupleBuilder;
    private final ArrayTupleReference tupleRef;

    public VTreeDataTupleCreator(int numIncludeFields, boolean isQuantized, float[] quantizationParams) {
        this.numIncludeFields = numIncludeFields;
        this.isQuantized = isQuantized;
        this.quantizationParams = quantizationParams;
        // Leading fields + 1 primary-key field + include fields.
        int leadingFields = isQuantized ? NUM_LEADING_FIELDS_QUANTIZED : NUM_LEADING_FIELDS_NON_QUANTIZED;
        int fieldCount = leadingFields + 1 + numIncludeFields;

        this.tupleBuilder = new ArrayTupleBuilder(fieldCount);
        this.tupleRef = new ArrayTupleReference();
    }

    @Override
    public ITupleReference createDataTuple(double[] vector, double distance, int centroidId,
            ITupleReference originalTuple) throws HyracksDataException {
        try {
            tupleBuilder.reset();
            DataOutput dos = tupleBuilder.getDataOutput();

            // Field 0: distance (raw double)
            dos.writeDouble(distance);
            tupleBuilder.addFieldEndOffset();

            // Field 1: centroidId (raw int)
            dos.writeInt(centroidId);
            tupleBuilder.addFieldEndOffset();

            if (isQuantized) {
                writeQuantizedFields(dos, vector, distance);
            }

            // PK field (at position 1 + numIncludeFields in input)
            int pkFieldIndex = 1 + numIncludeFields;
            tupleBuilder.addField(originalTuple.getFieldData(pkFieldIndex), originalTuple.getFieldStart(pkFieldIndex),
                    originalTuple.getFieldLength(pkFieldIndex));

            // Include fields (from input fields 1..numIncludeFields)
            for (int i = 0; i < numIncludeFields; i++) {
                int srcFieldIndex = 1 + i;
                tupleBuilder.addField(originalTuple.getFieldData(srcFieldIndex),
                        originalTuple.getFieldStart(srcFieldIndex), originalTuple.getFieldLength(srcFieldIndex));
            }

            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tupleRef;

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void writeQuantizedFields(DataOutput dos, double[] vector, double distance) throws Exception {
        // Field 2: quantized distance (raw double)
        dos.writeDouble(distance);
        tupleBuilder.addFieldEndOffset();

        // Field 3: quantized embedding (VarLen length prefix + content bytes)
        byte[] quantizedEmbedding = quantizeVector(vector);
        int metaLen = ByteArrayPointable.getNumberBytesToStoreMeta(quantizedEmbedding.length);
        byte[] meta = new byte[metaLen];
        VarLenIntEncoderDecoder.encode(quantizedEmbedding.length, meta, 0);
        dos.write(meta);
        dos.write(quantizedEmbedding);
        tupleBuilder.addFieldEndOffset();
    }

    private byte[] quantizeVector(double[] vector) {
        if (quantizationParams != null) {
            float minQ = quantizationParams[OSQ_MIN_QUANTILE];
            float maxQ = quantizationParams[OSQ_MAX_QUANTILE];
            float alpha = quantizationParams[OSQ_ALPHA];
            int bits = (int) quantizationParams[OSQ_BITS];

            int levels = 1 << bits;
            byte[] result = new byte[vector.length];
            for (int i = 0; i < vector.length; i++) {
                double value = Math.max(minQ, Math.min(maxQ, vector[i]));
                int quantizedValue = Math.toIntExact(Math.round((value - minQ) * alpha));
                quantizedValue = Math.max(0, Math.min(levels - 1, quantizedValue));
                result[i] = (byte) quantizedValue;
            }
            return result;
        }
        // Fallback: serialize full-precision vector as raw big-endian doubles (tests)
        ByteBuffer buf = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double d : vector) {
            buf.putDouble(d);
        }
        return buf.array();
    }
}
