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
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizerFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;

/**
 * AsterixDB-side factory that constructs a {@link ScalarVectorQuantizer} given the
 * {@link VTreeQuantizationParams} stored on the VTree index.
 * <p>
 * Replaces the {@code Class.forName} reflection block previously living in
 * {@code VTree#search}, which had to look up
 * {@link OptimizedScalarQuantizationSampleFile.Params},
 * {@link OptimizedScalarQuantizationSampleFile.SimilarityFunction}, and
 * {@link ScalarVectorQuantizer} reflectively because the {@code hyracks-storage-am-vtree}
 * module cannot import AsterixDB types directly.
 */
public class OptimizedScalarQuantizerFactory implements IVTreeQuantizerFactory {

    private static final long serialVersionUID = 1L;

    // The distance metric is fixed at index creation and baked into this factory (it selects the
    // symmetric vs asymmetric similarity function); the storage layer builds the quantizer without it.
    private final String distanceMetric;

    public OptimizedScalarQuantizerFactory(String distanceMetric) {
        this.distanceMetric = distanceMetric;
    }

    @Override
    public IVTreeQuantizer createQuantizer(int vectorDimensions, VTreeQuantizationParams params)
            throws HyracksDataException {
        if (params == null) {
            throw HyracksDataException.create(new IllegalArgumentException("Quantization params must not be null"));
        }

        OptimizedScalarQuantizationSampleFile.Params p =
                new OptimizedScalarQuantizationSampleFile.Params(params.bits(), vectorDimensions, params.sampleCount(),
                        params.confidenceInterval(), params.minQuantile(), params.maxQuantile(), params.alpha());

        OptimizedScalarQuantizationSampleFile.SimilarityFunction sim =
                OptimizedScalarQuantizationSampleFile.fromDistanceMetric(distanceMetric);

        return new ScalarVectorQuantizer(p, sim);
    }
}
