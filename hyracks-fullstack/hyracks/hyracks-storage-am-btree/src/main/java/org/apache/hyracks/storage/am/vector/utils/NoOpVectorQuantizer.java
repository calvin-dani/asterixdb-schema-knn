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
package org.apache.hyracks.storage.am.vector.utils;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVectorQuantizer;

/**
 * No-op vector quantizer for unit tests.
 *
 * In unit tests, the actual full-precision embedding is stored as the "quantized embedding"
 * (no real quantization is performed). This quantizer provides identity semantics:
 * - {@code quantize(vector)} returns the input vector unchanged
 * - {@code dequantize(bytes)} deserializes raw big-endian double bytes back to double[]
 *
 * The byte format is consistent with production: raw content bytes (no length prefix).
 * ByteArrayPointable prefix stripping is handled by the caller before invoking dequantize.
 */
public class NoOpVectorQuantizer implements IVectorQuantizer {

    public static final NoOpVectorQuantizer INSTANCE = new NoOpVectorQuantizer();

    @Override
    public double[] quantize(double[] vector) throws HyracksDataException {
        // Identity: return the input vector as-is
        return vector;
    }

    @Override
    public double[] dequantize(byte[] quantizedBytes) throws HyracksDataException {
        // Raw big-endian doubles: dimension count inferred from byte array length
        int count = quantizedBytes.length / Double.BYTES;
        ByteBuffer buf = ByteBuffer.wrap(quantizedBytes);
        double[] vector = new double[count];
        for (int i = 0; i < count; i++) {
            vector[i] = buf.getDouble();
        }
        return vector;
    }
}
