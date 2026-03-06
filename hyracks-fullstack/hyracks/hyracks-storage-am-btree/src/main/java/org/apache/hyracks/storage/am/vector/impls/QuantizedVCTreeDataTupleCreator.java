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
import org.apache.hyracks.storage.am.vector.api.IVCTreeDataTupleCreator;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

/**
 * Quantized data tuple creator for VectorClusteringTree.
 *
 * Includes quantized distance and quantized embedding in the data tuple so that
 * search cursors (e.g., LSMVCTreeBlockedCursor) can compute approximate D(q, x)
 * from the stored quantized data without needing to fetch from the primary index.
 *
 * In the unit test framework, the actual vector embedding is used as the
 * "quantized embedding" and the full distance as the "quantized distance"
 * (no real quantization is performed). The embedding is serialized as raw
 * big-endian doubles, consistent with the production byte[] format.
 *
 * Input tuple format: [vector, include_fields..., pk]
 * - Field 0: vector
 * - Fields 1 to numIncludeFields: include fields (optional)
 * - Field (1 + numIncludeFields): primary key (single field)
 *
 * Output data tuple format: <distance, centroidId, quantized_distance, quantized_embedding, pk, include_fields...>
 * - Field 0: distance (raw double, 8 bytes, no type tag)
 * - Field 1: centroidId (raw int, 4 bytes, no type tag)
 * - Field 2: quantized_distance (raw double, 8 bytes, no type tag)
 * - Field 3: quantized_embedding (ByteArrayPointable: VarLen length prefix + content bytes)
 * - Field 4: primary key
 * - Fields 5+: include fields
 */
public class QuantizedVCTreeDataTupleCreator implements IVCTreeDataTupleCreator {

    private static final int PK_FIELD_INDEX = 4;
    private final int numIncludeFields;

    // Quantization parameters: {minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}
    // null = not yet initialized (falls back to full-precision serialization for unit tests)
    private float[] quantizationParams;

    public QuantizedVCTreeDataTupleCreator(int numIncludeFields) {
        this.numIncludeFields = numIncludeFields;
    }

    @Override
    public void setQuantizationParams(float[] quantizationParams) {
        this.quantizationParams = quantizationParams;
    }

    @Override
    public ITupleReference createDataTuple(double[] vector, double distance, int centroidId,
            ITupleReference originalTuple) throws HyracksDataException {
        byte[] quantizedEmbedding;
        if (quantizationParams != null) {
            // Production path: perform actual scalar quantization
            float minQ = quantizationParams[0];
            float maxQ = quantizationParams[1];
            float alpha = quantizationParams[2];
            int bits = (int) quantizationParams[4];
            int levels = 1 << bits;
            quantizedEmbedding = new byte[vector.length];
            for (int i = 0; i < vector.length; i++) {
                double value = Math.max(minQ, Math.min(maxQ, vector[i]));
                int quantizedValue = Math.toIntExact(Math.round((value - minQ) * alpha));
                quantizedValue = Math.max(0, Math.min(levels - 1, quantizedValue));
                quantizedEmbedding[i] = (byte) quantizedValue;
            }
        } else {
            // Test mode fallback: serialize the full-precision vector as raw big-endian doubles
            ByteBuffer buf = ByteBuffer.allocate(vector.length * Double.BYTES);
            for (double d : vector) {
                buf.putDouble(d);
            }
            quantizedEmbedding = buf.array();
        }
        return createDataTuple(vector, distance, centroidId, originalTuple, distance, quantizedEmbedding);
    }

    @Override
    public ITupleReference createDataTuple(double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, double quantizedDistance, byte[] quantizedEmbedding)
            throws HyracksDataException {
        try {
            // Total fields: distance + centroidId + quantized_distance + quantized_embedding + pk + include_fields
            int dataFieldCount = 5 + numIncludeFields;
            ArrayTupleBuilder dataTupleBuilder = new ArrayTupleBuilder(dataFieldCount);
            DataOutput dos = dataTupleBuilder.getDataOutput();

            // Field 0: distance as raw double (8 bytes, no type tag)
            dos.writeDouble(distance);
            dataTupleBuilder.addFieldEndOffset();

            // Field 1: centroidId as raw int (4 bytes, no type tag)
            dos.writeInt(centroidId);
            dataTupleBuilder.addFieldEndOffset();

            // Field 2: quantized_distance as raw double (8 bytes, no type tag)
            dos.writeDouble(quantizedDistance);
            dataTupleBuilder.addFieldEndOffset();

            // Field 3: quantized_embedding with ByteArrayPointable serialization
            // (VarLen length prefix + content bytes, consistent with production bulk load format)
            int metaLen = ByteArrayPointable.getNumberBytesToStoreMeta(quantizedEmbedding.length);
            byte[] meta = new byte[metaLen];
            VarLenIntEncoderDecoder.encode(quantizedEmbedding.length, meta, 0);
            dos.write(meta);
            dos.write(quantizedEmbedding);
            dataTupleBuilder.addFieldEndOffset();

            // Field 4: primary key (at position 1 + numIncludeFields in input)
            int pkFieldIndex = 1 + numIncludeFields;
            dataTupleBuilder.addField(originalTuple.getFieldData(pkFieldIndex),
                    originalTuple.getFieldStart(pkFieldIndex), originalTuple.getFieldLength(pkFieldIndex));

            // Fields 5+: include fields (from originalTuple fields 1 to numIncludeFields)
            for (int i = 0; i < numIncludeFields; i++) {
                int srcFieldIndex = 1 + i;
                dataTupleBuilder.addField(originalTuple.getFieldData(srcFieldIndex),
                        originalTuple.getFieldStart(srcFieldIndex), originalTuple.getFieldLength(srcFieldIndex));
            }

            ArrayTupleReference dataTupleRef = new ArrayTupleReference();
            dataTupleRef.reset(dataTupleBuilder.getFieldEndOffsets(), dataTupleBuilder.getByteArray());

            return dataTupleRef;

        } catch (Exception e) {
            throw new HyracksDataException("Failed to create quantized data tuple", e);
        }
    }

    @Override
    public int getPrimaryKeyFieldIndex() {
        return PK_FIELD_INDEX;
    }
}
