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
 * Tuple writer for vector clustering interior frames.
 * Handles tuples with format: <cid, full_precision_centroid, pointer_to_clustered_page>
 */
public class VectorClusteringInteriorTupleWriter extends TypeAwareTupleWriter implements ITreeIndexTupleWriter {

    private static final int CID_FIELD = 0;
    private static final int CENTROID_FIELD = 1;
    private static final int POINTER_FIELD = 2;

    public VectorClusteringInteriorTupleWriter(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        super(typeTraits, nullTypeTraits, nullIntrospector);
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        // Calculate required bytes for CID + centroid + pointer
        int totalBytes = 0;

        // CID (typically 4 bytes for int)
        totalBytes += tuple.getFieldLength(CID_FIELD);

        // Centroid (vector of floats)
        totalBytes += tuple.getFieldLength(CENTROID_FIELD);

        // Pointer (typically 8 bytes for long)
        totalBytes += tuple.getFieldLength(POINTER_FIELD);

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
     * Get the cluster ID from the tuple
     */
    public int getClusterId(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(CID_FIELD);
        int offset = tuple.getFieldStart(CID_FIELD);
        return (data[offset] << 24) | ((data[offset + 1] & 0xFF) << 16) | ((data[offset + 2] & 0xFF) << 8)
                | (data[offset + 3] & 0xFF);
    }

    /**
     * Get the centroid vector from the tuple
     */
    public float[] getCentroid(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(CENTROID_FIELD);
        int offset = tuple.getFieldStart(CENTROID_FIELD);
        int length = tuple.getFieldLength(CENTROID_FIELD);

        // Assuming centroid is stored as array of floats
        int numDimensions = length / 4; // 4 bytes per float
        float[] centroid = new float[numDimensions];

        for (int i = 0; i < numDimensions; i++) {
            int floatOffset = offset + (i * 4);
            int bits = (data[floatOffset] << 24) | ((data[floatOffset + 1] & 0xFF) << 16)
                    | ((data[floatOffset + 2] & 0xFF) << 8) | (data[floatOffset + 3] & 0xFF);
            centroid[i] = Float.intBitsToFloat(bits);
        }

        return centroid;
    }

    /**
     * Get the page pointer from the tuple
     */
    public long getPagePointer(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(POINTER_FIELD);
        int offset = tuple.getFieldStart(POINTER_FIELD);

        long pointer = 0;
        for (int i = 0; i < 8; i++) {
            pointer = (pointer << 8) | (data[offset + i] & 0xFF);
        }

        return pointer;
    }
}
