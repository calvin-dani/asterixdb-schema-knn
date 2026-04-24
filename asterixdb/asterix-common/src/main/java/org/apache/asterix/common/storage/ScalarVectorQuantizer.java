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
package org.apache.asterix.common.storage;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;

/**
 * AsterixDB implementation of {@link IVTreeQuantizer} that wraps
 * {@link OptimizedScalarQuantizationSampleFile} for scalar quantization.
 *
 * <p>
 * Does not L2-normalize vectors; callers must satisfy the index similarity contract
 * (e.g. unit-length vectors for cosine indexes).
 *
 * <p>
 * Both {@link #quantize(double[])} and {@link #dequantize(byte[])} return double[]
 * so that existing distance functions (which operate on double[]) work unchanged.
 */
public class ScalarVectorQuantizer implements IVTreeQuantizer {

    private final OptimizedScalarQuantizationSampleFile.Params params;
    private final OptimizedScalarQuantizationSampleFile.SimilarityFunction similarityFunction;

    public ScalarVectorQuantizer(OptimizedScalarQuantizationSampleFile.Params params,
            OptimizedScalarQuantizationSampleFile.SimilarityFunction similarityFunction) {
        this.params = params;
        this.similarityFunction = similarityFunction;
    }

    // TODO change, added support for byte size vector calculation.
    @Override
    public double[] quantize(double[] vector) throws HyracksDataException {
        // Quantize then immediately dequantize -> lossy double[] for distance computation
        OptimizedScalarQuantizationSampleFile.QuantizedVector qv =
                OptimizedScalarQuantizationSampleFile.quantizeVector(vector, params, similarityFunction);
        return OptimizedScalarQuantizationSampleFile.dequantizeToDoubleArray(qv.quantizedBytes, params);
    }

    @Override
    public double[] dequantize(byte[] quantizedBytes) throws HyracksDataException {
        // 8-bit only: byte[] from leaf page passes directly to dequantize
        return OptimizedScalarQuantizationSampleFile.dequantizeToDoubleArray(quantizedBytes, params);
    }
}
