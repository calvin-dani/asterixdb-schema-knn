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
package org.apache.asterix.runtime.utils;

import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.utils.VTreeNavigationUtils;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * VTree DOT hops are {@code 1 - a·b / (|a||b|)}, so ε identity matches cosine for unit and scaled
 * queries. SQL++ {@code dotDistance} stays {@code -dot}.
 */
@AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "DOT hops are cosine distance; SQL++ stays -dot")
public class VTreeDotEpsilonWindowTest {

    private static final double EPS = 0.75;
    private static final IVTreeDistanceFunction DOT = VectorDistanceCalculation.DOT_DISTANCE_FN;
    private static final IVTreeDistanceFunction COSINE = VectorDistanceCalculation.COSINE_DISTANCE_FN;

    @Test
    public void treeDotHopIsCosineDistanceNotNegatedDot() throws Exception {
        double[] a = { 3.0, 4.0 };
        double[] b = { 0.0, 1.0 };
        Assert.assertEquals(VectorDistanceCalculation.cosineDistance(a, b), DOT.apply(a, b), 0.0);
        Assert.assertEquals(-4.0, VectorDistanceCalculation.dotDistance(a, b), 0.0);
    }

    @Test
    public void unitAndScaledQueriesShareCosineHopsAndKeepOnlyA() throws Exception {
        double[] cA = unit(0.99, Math.sqrt(1.0 - 0.99 * 0.99));
        double[] cB = unit(0.97, Math.sqrt(1.0 - 0.97 * 0.97));
        double[] cC = unit(0.40, Math.sqrt(1.0 - 0.40 * 0.40));
        double[] q1 = { 1.0, 0.0 };
        double[] q2 = { 2.0, 0.0 };

        double dA1 = DOT.apply(q1, cA);
        double dB1 = DOT.apply(q1, cB);
        double dC1 = DOT.apply(q1, cC);
        Assert.assertEquals(0.01, dA1, 1e-12);
        Assert.assertEquals(dA1, DOT.apply(q2, cA), 1e-12);
        Assert.assertEquals(dB1, DOT.apply(q2, cB), 1e-12);
        Assert.assertEquals(dC1, DOT.apply(q2, cC), 1e-12);
        Assert.assertEquals(dA1, COSINE.apply(q1, cA), 0.0);

        Assert.assertTrue(VTreeNavigationUtils.isWithinEpsilonWindow(dA1, dA1, EPS, DOT, 1.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(dB1, dA1, EPS, DOT, 1.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(dC1, dA1, EPS, DOT, 1.0));
        Assert.assertTrue(VTreeNavigationUtils.isWithinEpsilonWindow(dA1, dA1, EPS, DOT, 2.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(dC1, dA1, EPS, DOT, 2.0));
    }

    private static double[] unit(double x, double y) {
        double n = Math.hypot(x, y);
        return new double[] { x / n, y / n };
    }
}
