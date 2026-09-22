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
import java.util.Arrays;

import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * OptimizedScalarQuantization parameters for a VTree index: the min/max clipping quantiles, the OSQ
 * anisotropic weight ({@code alpha}), the confidence interval, the quantization bit width, and the
 * training sample count. Carried across the Hyracks/asterix layer boundary as named, typed fields.
 *
 * <p>
 * {@code minPerDim}/{@code maxPerDim} are optional FAISS {@code QT_8bit} ranges. Null means every
 * dimension uses the global {@code minQuantile}/{@code maxQuantile}/{@code alpha}. Residual DOT
 * indexes fill the arrays after k-means; Job 1 raw-x SQ stays global.
 */
public record VTreeQuantizationParams(float minQuantile, float maxQuantile, float alpha, float confidenceInterval,
        int bits, int sampleCount, float[] minPerDim, float[] maxPerDim) implements Serializable {
    private static final long serialVersionUID = 1L;

    public VTreeQuantizationParams {
        minPerDim = minPerDim == null ? null : Arrays.copyOf(minPerDim, minPerDim.length);
        maxPerDim = maxPerDim == null ? null : Arrays.copyOf(maxPerDim, maxPerDim.length);
    }

    public VTreeQuantizationParams(float minQuantile, float maxQuantile, float alpha, float confidenceInterval,
            int bits, int sampleCount) {
        this(minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount, null, null);
    }

    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Optional per-dim residual SQ ranges")
    public boolean hasPerDimRanges() {
        return minPerDim != null && maxPerDim != null && minPerDim.length == maxPerDim.length && minPerDim.length > 0;
    }

    public float dimMin(int i) {
        return hasPerDimRanges() ? minPerDim[i] : minQuantile;
    }

    public float dimMax(int i) {
        return hasPerDimRanges() ? maxPerDim[i] : maxQuantile;
    }

    public float dimAlpha(int i) {
        int levels = 1 << bits;
        if (!hasPerDimRanges()) {
            return alpha;
        }
        float span = maxPerDim[i] - minPerDim[i];
        if (span <= 1e-12f) {
            span = 1e-6f;
        }
        return (levels - 1) / span;
    }
}
