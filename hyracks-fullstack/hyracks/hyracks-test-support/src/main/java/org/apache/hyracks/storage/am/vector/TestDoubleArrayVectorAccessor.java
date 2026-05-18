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

package org.apache.hyracks.storage.am.vector;

import java.io.DataInputStream;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Test implementation of IVTreeBinaryAccessor for reading double[] vectors
 * serialized with DoubleArraySerializerDeserializer.
 *
 * This accessor is used in unit tests where vectors are stored as plain double arrays
 * (not ADM-tagged AOrderedList format).
 */
public class TestDoubleArrayVectorAccessor implements IVTreeBinaryAccessor {

    private byte[] data;
    private int start;
    private int length;

    @Override
    public void reset(byte[] data, int start, int length) throws HyracksDataException {
        this.data = data;
        this.start = start;
        this.length = length;
    }

    @Override
    public double[] getVector() throws HyracksDataException {
        try {
            // Use DoubleArraySerializerDeserializer to deserialize the vector
            // Format: [int:length][double:v0][double:v1]...[double:vN-1]
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data, start, length);
            DataInputStream dis = new DataInputStream(bais);
            return DoubleArraySerializerDeserializer.INSTANCE.deserialize(dis);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public int getDimension() throws HyracksDataException {
        // Read the dimension from the first int in the serialized format
        // Format: [int:length][double:v0]...
        if (length >= 4) {
            int dim = ((data[start] & 0xFF) << 24) | ((data[start + 1] & 0xFF) << 16) | ((data[start + 2] & 0xFF) << 8)
                    | (data[start + 3] & 0xFF);
            return dim;
        }
        throw new HyracksDataException("Invalid vector data: too short to read dimension");
    }

    /**
     * Factory for creating TestDoubleArrayVectorAccessor instances.
     */
    public static class Factory implements IVTreeBinaryAccessorFactory {
        private static final long serialVersionUID = 1L;

        public static final Factory INSTANCE = new Factory();

        private Factory() {
        }

        @Override
        public IVTreeBinaryAccessor createAccessor() {
            return new TestDoubleArrayVectorAccessor();
        }

        @Override
        public JsonNode toJson(org.apache.hyracks.api.io.IPersistedResourceRegistry registry)
                throws HyracksDataException {
            ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
            return json;
        }

        public static IVTreeBinaryAccessorFactory fromJson(
                org.apache.hyracks.api.io.IPersistedResourceRegistry registry, JsonNode json) {
            return INSTANCE;
        }
    }
}
