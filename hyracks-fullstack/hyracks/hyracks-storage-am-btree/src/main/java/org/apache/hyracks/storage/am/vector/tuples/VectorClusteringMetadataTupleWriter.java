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

package org.apache.hyracks.storage.am.lsm.vector.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;

/**
 * Tuple writer for vector clustering metadata frames.
 * Handles tuples with format: <max_distance, pointer_to_data_page>
 */
public class VectorClusteringMetadataTupleWriter extends TypeAwareTupleWriter implements ITreeIndexTupleWriter {

    private static final int MAX_DISTANCE_FIELD = 0;
    private static final int DATA_POINTER_FIELD = 1;

    public VectorClusteringMetadataTupleWriter(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        super(typeTraits, nullTypeTraits, nullIntrospector);
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        // Calculate required bytes for max_distance + data pointer
        int totalBytes = 0;

        // Max distance (typically 4 bytes for float)
        totalBytes += tuple.getFieldLength(MAX_DISTANCE_FIELD);

        // Data pointer (typically 8 bytes for long)
        totalBytes += tuple.getFieldLength(DATA_POINTER_FIELD);

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
     * Get the maximum distance from the tuple
     */
    public float getMaxDistance(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(MAX_DISTANCE_FIELD);
        int offset = tuple.getFieldStart(MAX_DISTANCE_FIELD);

        int bits = (data[offset] << 24) | ((data[offset + 1] & 0xFF) << 16) | ((data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);

        return Float.intBitsToFloat(bits);
    }

    /**
     * Get the data page pointer from the tuple
     */
    public long getDataPagePointer(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(DATA_POINTER_FIELD);
        int offset = tuple.getFieldStart(DATA_POINTER_FIELD);

        long pointer = 0;
        for (int i = 0; i < 8; i++) {
            pointer = (pointer << 8) | (data[offset + i] & 0xFF);
        }

        return pointer;
    }

    /**
     * Create a tuple with max distance and data page pointer
     */
    public void createTuple(byte[] targetBuffer, int targetOffset, float maxDistance, long dataPagePointer) {
        // Write field count
        writeInt(targetBuffer, targetOffset, 2);
        int currentOffset = targetOffset + 4;

        // Write field offsets
        int fieldOffsetArrayStart = currentOffset;
        currentOffset += 3 * 4; // 3 offsets (start + 2 field ends)

        // Write max distance field
        writeInt(targetBuffer, fieldOffsetArrayStart + 4, currentOffset - targetOffset);
        writeFloat(targetBuffer, currentOffset, maxDistance);
        currentOffset += 4;

        // Write data page pointer field
        writeInt(targetBuffer, fieldOffsetArrayStart + 8, currentOffset - targetOffset);
        writeLong(targetBuffer, currentOffset, dataPagePointer);
        currentOffset += 8;
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

    private void writeLong(byte[] buffer, int offset, long value) {
        buffer[offset] = (byte) (value >>> 56);
        buffer[offset + 1] = (byte) (value >>> 48);
        buffer[offset + 2] = (byte) (value >>> 40);
        buffer[offset + 3] = (byte) (value >>> 32);
        buffer[offset + 4] = (byte) (value >>> 24);
        buffer[offset + 5] = (byte) (value >>> 16);
        buffer[offset + 6] = (byte) (value >>> 8);
        buffer[offset + 7] = (byte) value;
    }
}
