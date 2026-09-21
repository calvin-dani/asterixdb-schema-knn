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
package org.apache.asterix.common.vector;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * DOT residual SQ8: field 3 stores SQ(x − c) so magnitude is not collapsed by a global clamp on raw x.
 */
@AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "DOT residual SQ codec round-trip and 8-D clamp example")
public class OptimizedScalarQuantizationCodecResidualTest {

    @Test
    public void residualRoundTripIsXMinusC() throws Exception {
        float minQ = -1f;
        float maxQ = 1f;
        int bits = 8;
        float alpha = (float) ((1 << bits) - 1) / (maxQ - minQ);
        OptimizedScalarQuantizationCodec.Params params =
                new OptimizedScalarQuantizationCodec.Params(bits, 4, 1, 0.99f, minQ, maxQ, alpha);
        double[] x = { 0.4, -0.2, 0.1, 0.0 };
        double[] c = { 0.1, -0.1, 0.0, 0.05 };
        OptimizedScalarQuantizationCodec.QuantizedVector qv = OptimizedScalarQuantizationCodec.quantizeResidual(x, c,
                params, OptimizedScalarQuantizationCodec.SimilarityFunction.DOT_PRODUCT);
        double[] rHat = OptimizedScalarQuantizationCodec.dequantizeToDoubleArray(qv.quantizedBytes, params);
        double[] r = OptimizedScalarQuantizationCodec.subtract(x, c);
        for (int i = 0; i < r.length; i++) {
            Assert.assertEquals(r[i], rHat[i], 1.0 / alpha);
        }
    }

    @Test
    public void residualRecoversInnerProductWhenRawSqClamps() throws Exception {
        // Job 1 raw quantiles on unit-ish coords: maxQ=1. High-norm DOT vectors clamp to 1 and
        // lose magnitude. Residual x−c stays near zero when x is near its centroid.
        float minQ = -1f;
        float maxQ = 1f;
        int bits = 8;
        float alpha = (float) ((1 << bits) - 1) / (maxQ - minQ);
        OptimizedScalarQuantizationCodec.Params params =
                new OptimizedScalarQuantizationCodec.Params(bits, 8, 1, 0.99f, minQ, maxQ, alpha);

        double[] c = { 4, 4, 4, 4, 4, 4, 4, 4 };
        double[] x = { 4.2, 3.8, 4.1, 3.9, 4.05, 3.95, 4.0, 4.0 };
        double[] q = x;

        OptimizedScalarQuantizationCodec.QuantizedVector raw = OptimizedScalarQuantizationCodec.quantizeVector(x,
                params, OptimizedScalarQuantizationCodec.SimilarityFunction.DOT_PRODUCT);
        double[] xHat = OptimizedScalarQuantizationCodec.dequantizeToDoubleArray(raw.quantizedBytes, params);
        // Every raw coord is above maxQ, so SQ(x) saturates.
        for (double v : xHat) {
            Assert.assertEquals(maxQ, v, 1.0 / alpha);
        }

        OptimizedScalarQuantizationCodec.QuantizedVector residual = OptimizedScalarQuantizationCodec.quantizeResidual(x,
                c, params, OptimizedScalarQuantizationCodec.SimilarityFunction.DOT_PRODUCT);
        double[] rHat = OptimizedScalarQuantizationCodec.dequantizeToDoubleArray(residual.quantizedBytes, params);

        double qDotX = dot(q, x);
        double qDotC = dot(q, c);
        double qDotRHat = dot(q, rHat);
        double reconstructed = qDotC + qDotRHat;
        double qDotXHat = dot(q, xHat);

        Assert.assertTrue("residual reconstruction should beat clamped SQ(x)",
                Math.abs(qDotX - reconstructed) < Math.abs(qDotX - qDotXHat));
        Assert.assertEquals(qDotX, reconstructed, 8.0 * (1.0 / alpha) * 4.2);
    }

    @Test
    public void computeParamsFromScalarsMatchesQuantileFormula() throws Exception {
        List<Double> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add(i + 1.0);
        }
        OptimizedScalarQuantizationCodec.Params params =
                OptimizedScalarQuantizationCodec.computeParamsFromScalars(values, 8, 1, 0.9f);
        Assert.assertEquals(5.0f, params.minQuantile, 0.0f);
        Assert.assertEquals(96.0f, params.maxQuantile, 0.0f);
        Assert.assertEquals(255.0f / 91.0f, params.alpha, 1e-6f);
        Assert.assertEquals(100, params.sampleCount);
    }

    private static double dot(double[] a, double[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) {
            s += a[i] * b[i];
        }
        return s;
    }
}
