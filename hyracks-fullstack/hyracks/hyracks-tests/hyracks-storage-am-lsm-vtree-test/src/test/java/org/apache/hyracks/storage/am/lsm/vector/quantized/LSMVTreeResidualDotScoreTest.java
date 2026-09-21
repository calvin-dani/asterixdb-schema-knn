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
package org.apache.hyracks.storage.am.lsm.vector.quantized;

import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeTopKSearchCursor;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * DOT residual TopK heap key is {@code 1 - (q·c + q·r̂)} with raw q, not {@code −q̂·x̂}.
 */
@AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "DOT residual TopK score equals 1-(q·c + q·r̂)")
public class LSMVTreeResidualDotScoreTest {

    @Test
    public void residualScoreIsOneMinusExactPlusResidualInnerProduct() {
        double[] q = { 1.0, 0.5, -0.25, 2.0 };
        double[] c = { 4.0, 4.0, 4.0, 4.0 };
        double[] rHat = { 0.2, -0.2, 0.1, 0.0 };
        double qDotC = 0;
        double qDotR = 0;
        for (int i = 0; i < q.length; i++) {
            qDotC += q[i] * c[i];
            qDotR += q[i] * rHat[i];
        }
        double expected = 1.0 - (qDotC + qDotR);
        Assert.assertEquals(expected, LSMVTreeTopKSearchCursor.residualDotScore(q, rHat, qDotC), 0.0);

        double[] qHat = { 1.0, 0.5, -0.25, 1.0 };
        double[] xHat = { 1.0, 1.0, 1.0, 1.0 };
        double clampedScore = 0;
        for (int i = 0; i < qHat.length; i++) {
            clampedScore -= qHat[i] * xHat[i];
        }
        Assert.assertNotEquals(clampedScore, expected, 0.0);
    }
}
