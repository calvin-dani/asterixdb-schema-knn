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

import java.io.Serializable;

import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Factory for creating {@link IVTreeDataTupleCreator} instances.
 * <p>
 * This factory is not actually round-tripped through Java serialization: rather than serializing the
 * object, {@code LSMVTreeLocalResource.fromJson} re-derives it from primitive fields
 * ({@code new VTreeDataTupleCreatorFactory(numIncludeFields, isQuantized)}). The {@code Serializable}
 * super-interface is retained only for API compatibility with the resource-metadata contract.
 */
public interface IVTreeDataTupleCreatorFactory extends Serializable {

    /**
     * Creates a new data tuple creator.
     *
     * @param quantizationParams the {@code float[6]} scalar-quantization params
     *        {@code {minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}}
     *        (same layout as {@link IVTreeQuantizerFactory#createQuantizer}); {@code null} when the
     *        index is non-quantized, in which case the quantized fields are not written
    
     * @return a new IVTreeDataTupleCreator
     */
    IVTreeDataTupleCreator createDataTupleCreator(float[] quantizationParams);

    /**
     * Whether the tuples produced by this factory use the quantized data-tuple layout
     * {@code [distance, centroidId, quantized_distance, quantized_embedding, PK..., includes...]}
     * (pkStartField=4) rather than the non-quantized {@code [distance, centroidId, PK...,
     * includes...]} layout (pkStartField=2).
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Fix: merge cancellation key must skip quantized fields (pkStartField)")
    default boolean isQuantized() {
        return false;
    }
}
