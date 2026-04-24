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

/**
 * Represents quantization constants computed from scalar value samples.
 * Contains the minimum quantile, maximum quantile, alpha scaling factor,
 * quantization bits, confidence interval, and sample count.
 */
public final class QuantizationConstants {
    private final float minQ;
    private final float maxQ;
    private final float alpha;
    private final int bits;
    private final float confidenceInterval;
    private final int sampleCount;

    /**
     * Creates a new QuantizationConstants instance.
     *
     * @param minQ minimum quantile value
     * @param maxQ maximum quantile value
     * @param alpha scaling factor: (levels - 1) / (maxQ - minQ) where levels = 2^bits
     * @param bits number of quantization bits
     * @param confidenceInterval confidence interval used for quantile calculation (e.g., 0.99)
     * @param sampleCount number of samples used to compute these constants
     */
    public QuantizationConstants(float minQ, float maxQ, float alpha, int bits, float confidenceInterval,
            int sampleCount) {
        this.minQ = minQ;
        this.maxQ = maxQ;
        this.alpha = alpha;
        this.bits = bits;
        this.confidenceInterval = confidenceInterval;
        this.sampleCount = sampleCount;
    }

    /**
     * @return minimum quantile value
     */
    public float getMinQ() {
        return minQ;
    }

    /**
     * @return maximum quantile value
     */
    public float getMaxQ() {
        return maxQ;
    }

    /**
     * @return alpha scaling factor
     */
    public float getAlpha() {
        return alpha;
    }

    /**
     * @return number of quantization bits
     */
    public int getBits() {
        return bits;
    }

    /**
     * @return confidence interval used for quantile calculation
     */
    public float getConfidenceInterval() {
        return confidenceInterval;
    }

    /**
     * @return number of samples used to compute these constants
     */
    public int getSampleCount() {
        return sampleCount;
    }

    @Override
    public String toString() {
        return "QuantizationConstants{" + "minQ=" + minQ + ", maxQ=" + maxQ + ", alpha=" + alpha + ", bits=" + bits
                + ", confidenceInterval=" + confidenceInterval + ", sampleCount=" + sampleCount + '}';
    }
}
