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
package org.apache.hyracks.storage.am.vector.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Interface for vector quantization/dequantization at the Hyracks layer.
 * Allows Hyracks-layer code (VTreeNavigationUtils, LSMVTreeBlockedCursor, etc.)
 * to quantize/dequantize vectors without depending on AsterixDB implementations.
 *
 * Both methods return double[] so existing distanceFunction.apply(double[], double[])
 * works unchanged.
 */
public interface IVTreeQuantizer {

    /**
     * Quantize a full-precision vector into a quantized representation,
     * then immediately dequantize back to double[] for distance computation.
     *
     * @param vector the full-precision vector to quantize
     * @return the dequantized double[] (lossy representation suitable for approximate distance)
     * @throws HyracksDataException if quantization fails
     */
    double[] quantize(double[] vector) throws HyracksDataException;

    /**
     * Dequantize a stored quantized byte representation back to double[].
     *
     * @param quantizedBytes the quantized byte representation
     * @return the dequantized double[] suitable for distance computation
     * @throws HyracksDataException if dequantization fails
     */
    double[] dequantize(byte[] quantizedBytes) throws HyracksDataException;
}
