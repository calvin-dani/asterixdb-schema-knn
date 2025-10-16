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
 * Tuple writer for vector clustering leaf frames.
 * Handles tuples with format: <cid, full_precision_centroid, pointer_to_first_metadata_page>
 */
public class VectorClusteringLeafTupleWriter extends TypeAwareTupleWriter implements ITreeIndexTupleWriter {

    private static final int CID_FIELD = 0;
    private static final int CENTROID_FIELD = 1;
    private static final int METADATA_POINTER_FIELD = 2;

    public VectorClusteringLeafTupleWriter(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        super(typeTraits, nullTypeTraits, nullIntrospector);
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        // Calculate required bytes for CID + centroid + metadata pointer
        int totalBytes = 0;

        // CID (typically 4 bytes for int)
        totalBytes += tuple.getFieldLength(CID_FIELD);

        // Centroid (vector of doubles)
        totalBytes += tuple.getFieldLength(CENTROID_FIELD);

        // Metadata pointer (typically 8 bytes for long)
        totalBytes += tuple.getFieldLength(METADATA_POINTER_FIELD);

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
    public double[] getCentroid(ITupleReference tuple) {
        System.err.println("=== VectorClusteringLeafTupleWriter.getCentroid ===");
        
        byte[] data = tuple.getFieldData(CENTROID_FIELD);
        int offset = tuple.getFieldStart(CENTROID_FIELD);
        int length = tuple.getFieldLength(CENTROID_FIELD);

        System.err.println("Centroid field data: length=" + length + ", offset=" + offset + ", data.length=" + data.length);
        
        // Show first few bytes for analysis
        int bytesToShow = Math.min(16, length);
        System.err.print("First " + bytesToShow + " bytes: ");
        for (int i = 0; i < bytesToShow; i++) {
            System.err.printf("%02X ", data[offset + i] & 0xFF);
        }
        System.err.println();

        // Assuming centroid is stored as array of doubles
        int numDimensions = length / 8; // 8 bytes per double
        System.err.println("Calculated dimensions (double): " + numDimensions);
        
        if (numDimensions <= 0) {
            System.err.println("ERROR: Invalid dimensions for double array, trying float interpretation");
            int floatDimensions = length / 4; // 4 bytes per float
            System.err.println("Calculated dimensions (float): " + floatDimensions);
            
            if (floatDimensions > 0) {
                return getCentroidAsFloatArray(data, offset, length, floatDimensions);
            } else {
                throw new RuntimeException("Invalid centroid dimensions: double=" + numDimensions + 
                                         ", float=" + floatDimensions + ", length=" + length);
            }
        }
        
        double[] centroid = new double[numDimensions];

        for (int i = 0; i < numDimensions; i++) {
            int doubleOffset = offset + (i * 8);
            long bits = ((long) data[doubleOffset] << 56) | (((long) data[doubleOffset + 1] & 0xFF) << 48)
                    | (((long) data[doubleOffset + 2] & 0xFF) << 40) | (((long) data[doubleOffset + 3] & 0xFF) << 32)
                    | (((long) data[doubleOffset + 4] & 0xFF) << 24) | (((long) data[doubleOffset + 5] & 0xFF) << 16)
                    | (((long) data[doubleOffset + 6] & 0xFF) << 8) | ((long) data[doubleOffset + 7] & 0xFF);
            centroid[i] = Double.longBitsToDouble(bits);
            
            if (i < 3) { // Show first 3 values
                System.err.println("Double[" + i + "] = " + centroid[i]);
            }
        }

        System.err.println("Successfully extracted " + numDimensions + " dimensions as double array");
        return centroid;
    }
    
    private double[] getCentroidAsFloatArray(byte[] data, int offset, int length, int numDimensions) {
        System.err.println("Extracting as FLOAT array with " + numDimensions + " dimensions");
        double[] centroid = new double[numDimensions];

        for (int i = 0; i < numDimensions; i++) {
            int floatOffset = offset + (i * 4);
            int bits = (data[floatOffset] << 24) | ((data[floatOffset + 1] & 0xFF) << 16)
                    | ((data[floatOffset + 2] & 0xFF) << 8) | (data[floatOffset + 3] & 0xFF);
            float floatValue = Float.intBitsToFloat(bits);
            centroid[i] = (double) floatValue; // Convert float to double
            
            if (i < 3) { // Show first 3 values
                System.err.println("Float[" + i + "] = " + floatValue + " (as double: " + centroid[i] + ")");
            }
        }

        System.err.println("Successfully extracted " + numDimensions + " dimensions as float array");
        return centroid;
    }

    /**
     * Get the metadata page pointer from the tuple
     */
    public long getMetadataPagePointer(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(METADATA_POINTER_FIELD);
        int offset = tuple.getFieldStart(METADATA_POINTER_FIELD);

        long pointer = 0;
        for (int i = 0; i < 8; i++) {
            pointer = (pointer << 8) | (data[offset + i] & 0xFF);
        }

        return pointer;
    }
}
