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
 * DOT ε must keep the cosine neighborhood (only list A) for both unit and scaled queries. Raw
 * {@code -dot} ε keeps C as well.
 */
@AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "DOT epsilon window A/B/C vs cosine")
public class VTreeDotEpsilonWindowTest {

    private static final double EPS = 0.75;
    private static final IVTreeDistanceFunction DOT = VectorDistanceCalculation.DOT_DISTANCE_FN;
    private static final IVTreeDistanceFunction COSINE = VectorDistanceCalculation.COSINE_DISTANCE_FN;

    @Test
    public void unitQueryDotWindowMatchesCosineAndDropsFarList() {
        // A 0.99, B 0.97, C 0.40 — hops are -IP for DOT and 1-IP for cosine.
        double closestDot = -0.99;
        Assert.assertTrue(VTreeNavigationUtils.isWithinEpsilonWindow(-0.99, closestDot, EPS, DOT, 1.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(-0.97, closestDot, EPS, DOT, 1.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(-0.40, closestDot, EPS, DOT, 1.0));

        double closestCos = 0.01;
        Assert.assertTrue(VTreeNavigationUtils.isWithinEpsilonWindow(0.01, closestCos, EPS, COSINE, 1.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(0.03, closestCos, EPS, COSINE, 1.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(0.60, closestCos, EPS, COSINE, 1.0));
    }

    @Test
    public void scaledQueryDotWindowStillDropsFarList() {
        // Same angles, |q|=2: hop = -|q|cosθ. Naive -dot ε would keep C; 1-cos ε does not.
        double closestDot = -1.98;
        Assert.assertEquals(0.01, VectorDistanceCalculation.dotHopToEpsilonDistance(-1.98, 2.0), 1e-12);
        Assert.assertTrue(VTreeNavigationUtils.isWithinEpsilonWindow(-1.98, closestDot, EPS, DOT, 2.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(-1.94, closestDot, EPS, DOT, 2.0));
        Assert.assertFalse(VTreeNavigationUtils.isWithinEpsilonWindow(-0.80, closestDot, EPS, DOT, 2.0));
        Assert.assertTrue("raw -dot window must still contain C — that is the bug being fixed",
                -0.80 <= closestDot + Math.abs(closestDot) * EPS);
    }
}
