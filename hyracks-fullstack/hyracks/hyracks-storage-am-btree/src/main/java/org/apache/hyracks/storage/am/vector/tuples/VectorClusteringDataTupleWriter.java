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

package org.apache.hyracks.storage.am.vector.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;

/**
 * Tuple writer for vector clustering data frames.
 * Handles tuples with format: <distance_to_centroid, cos(θ), quantized_vector, include_fields, PK>
 */
public class VectorClusteringDataTupleWriter extends TypeAwareTupleWriter implements ITreeIndexTupleWriter {

    private static final int DISTANCE_FIELD = 0;
    private static final int COSINE_FIELD = 1;
    private static final int QUANTIZED_VECTOR_FIELD = 2;
    private static final int INCLUDE_FIELDS_FIELD = 3;
    private static final int PRIMARY_KEY_FIELD = 4;

    public VectorClusteringDataTupleWriter(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        super(typeTraits, nullTypeTraits, nullIntrospector);
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        // Calculate required bytes for all fields
        int totalBytes = 0;

        for (int i = 0; i < tuple.getFieldCount(); i++) {
            totalBytes += tuple.getFieldLength(i);
        }

        // Add field offset array overhead
        totalBytes += (tuple.getFieldCount() + 1) * 4; // 4 bytes per offset

        return totalBytes;
    }

    @Override
    public int bytesRequired(ITupleReference tuple, int startField, int numFields) {
        int totalBytes = 0;

        for (int i = startField; i < startField + numFields; i++) {
            totalBytes += tuple.getFieldLength(i);
        }

        // Add field offset array overhead
        totalBytes += (numFields + 1) * 4;

        return totalBytes;
    }

    /**
     * Get the distance to centroid from the tuple
     */
    public float getDistanceToCentroid(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(DISTANCE_FIELD);
        int offset = tuple.getFieldStart(DISTANCE_FIELD);

        int bits = (data[offset] << 24) | ((data[offset + 1] & 0xFF) << 16) | ((data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);

        return Float.intBitsToFloat(bits);
    }

    /**
     * Get the cosine similarity from the tuple
     */
    public float getCosineSimilarity(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(COSINE_FIELD);
        int offset = tuple.getFieldStart(COSINE_FIELD);

        int bits = (data[offset] << 24) | ((data[offset + 1] & 0xFF) << 16) | ((data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);

        return Float.intBitsToFloat(bits);
    }

    /**
     * Get the quantized vector from the tuple
     */
    public byte[] getQuantizedVector(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(QUANTIZED_VECTOR_FIELD);
        int offset = tuple.getFieldStart(QUANTIZED_VECTOR_FIELD);
        int length = tuple.getFieldLength(QUANTIZED_VECTOR_FIELD);

        byte[] quantizedVector = new byte[length];
        System.arraycopy(data, offset, quantizedVector, 0, length);

        return quantizedVector;
    }

    /**
     * Get the include fields from the tuple
     */
    public byte[] getIncludeFields(ITupleReference tuple) {
        if (tuple.getFieldCount() <= INCLUDE_FIELDS_FIELD) {
            return new byte[0];
        }

        byte[] data = tuple.getFieldData(INCLUDE_FIELDS_FIELD);
        int offset = tuple.getFieldStart(INCLUDE_FIELDS_FIELD);
        int length = tuple.getFieldLength(INCLUDE_FIELDS_FIELD);

        byte[] includeFields = new byte[length];
        System.arraycopy(data, offset, includeFields, 0, length);

        return includeFields;
    }

    /**
     * Get the primary key from the tuple
     */
    public byte[] getPrimaryKey(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(PRIMARY_KEY_FIELD);
        int offset = tuple.getFieldStart(PRIMARY_KEY_FIELD);
        int length = tuple.getFieldLength(PRIMARY_KEY_FIELD);

        byte[] primaryKey = new byte[length];
        System.arraycopy(data, offset, primaryKey, 0, length);

        return primaryKey;
    }

    /**
     * Create a tuple with all vector data fields
     */
    public void createTuple(byte[] targetBuffer, int targetOffset, float distance, float cosine, byte[] quantizedVector,
            byte[] includeFields, byte[] primaryKey) {

        int fieldCount = (includeFields != null && includeFields.length > 0) ? 5 : 4;

        // Write field count
        writeInt(targetBuffer, targetOffset, fieldCount);
        int currentOffset = targetOffset + 4;

        // Write field offsets
        int fieldOffsetArrayStart = currentOffset;
        currentOffset += (fieldCount + 1) * 4; // field count + 1 offsets

        // Write distance field
        writeInt(targetBuffer, fieldOffsetArrayStart + 4, currentOffset - targetOffset);
        writeFloat(targetBuffer, currentOffset, distance);
        currentOffset += 4;

        // Write cosine field
        writeInt(targetBuffer, fieldOffsetArrayStart + 8, currentOffset - targetOffset);
        writeFloat(targetBuffer, currentOffset, cosine);
        currentOffset += 4;

        // Write quantized vector field
        writeInt(targetBuffer, fieldOffsetArrayStart + 12, currentOffset - targetOffset);
        System.arraycopy(quantizedVector, 0, targetBuffer, currentOffset, quantizedVector.length);
        currentOffset += quantizedVector.length;

        // Write include fields if present
        if (includeFields != null && includeFields.length > 0) {
            writeInt(targetBuffer, fieldOffsetArrayStart + 16, currentOffset - targetOffset);
            System.arraycopy(includeFields, 0, targetBuffer, currentOffset, includeFields.length);
            currentOffset += includeFields.length;

            // Write primary key field
            writeInt(targetBuffer, fieldOffsetArrayStart + 20, currentOffset - targetOffset);
        } else {
            // Write primary key field directly
            writeInt(targetBuffer, fieldOffsetArrayStart + 16, currentOffset - targetOffset);
        }

        System.arraycopy(primaryKey, 0, targetBuffer, currentOffset, primaryKey.length);
        currentOffset += primaryKey.length;
    }

    private void writeInt(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) (value >>> 24);
        buffer[offset + 1] = (byte) (value >>> 16);
        buffer[offset + 2] = (byte) (value >>> 8);
        buffer[offset + 3] = (byte) value;
    }

    private void writeFloat(byte[] buffer, int offset, float value) {
        writeInt(buffer, offset, Float.floatToIntBits(value));
    }
}
