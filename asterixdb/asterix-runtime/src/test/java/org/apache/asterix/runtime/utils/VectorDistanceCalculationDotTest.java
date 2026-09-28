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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.junit.Assert;
import org.junit.Test;

/**
 * The dot-product index distance {@code 1 - q·v/|q|} must be cosine-shaped against unit centroids (so the
 * epsilon window matches cosine), rank candidates exactly as {@code -q·v} does, and map back to {@code -q·v}.
 */
public class VectorDistanceCalculationDotTest {

    private static final IVTreeDistanceFunction DOT = VectorDistanceCalculation.DOT_DISTANCE_FN;
    private static final double TOLERANCE = 1e-12;

    @Test
    public void hopToUnitCentroidIsCosineDistanceForAnyQueryNorm() throws Exception {
        double[] unitCentroid = { 0.6, 0.8 };
        double[] unitQuery = { 1.0, 0.0 };
        double[] scaledQuery = { 3.0, 0.0 };
        double cosineDistance = VectorDistanceCalculation.cosineDistance(unitQuery, unitCentroid);
        Assert.assertEquals(cosineDistance, DOT.apply(unitQuery, unitCentroid), TOLERANCE);
        Assert.assertEquals(cosineDistance, DOT.apply(scaledQuery, unitCentroid), TOLERANCE);
    }

    @Test
    public void ranksNonUnitCandidatesByInnerProduct() {
        Random random = new Random(7);
        double[] query = randomVector(random, 16, 5.0);
        double[][] candidates = new double[50][];
        for (int i = 0; i < candidates.length; i++) {
            candidates[i] = randomVector(random, 16, 1.0 + random.nextDouble() * 10.0);
        }
        Integer[] byIndexDistance = order(candidates.length, i -> DOT.apply(query, candidates[i]));
        Integer[] byNegatedDot =
                order(candidates.length, i -> VectorDistanceCalculation.dotDistance(query, candidates[i]));
        Assert.assertArrayEquals(byNegatedDot, byIndexDistance);
    }

    @Test
    public void toQueryDistanceRecoversNegatedDot() throws Exception {
        double[] query = { 2.0, -1.0, 0.5 };
        double[] candidate = { 4.0, 3.0, -2.0 };
        double queryNorm = Math.sqrt(2.0 * 2.0 + 1.0 + 0.25);
        double reported = DOT.toQueryDistance(DOT.apply(query, candidate), queryNorm);
        Assert.assertEquals(VectorDistanceCalculation.dotDistance(query, candidate), reported, TOLERANCE);
    }

    @Test
    public void zeroQueryIsNaN() throws Exception {
        Assert.assertTrue(Double.isNaN(DOT.apply(new double[] { 0.0, 0.0 }, new double[] { 1.0, 0.0 })));
    }

    private static double[] randomVector(Random random, int dim, double scale) {
        double[] v = new double[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = random.nextGaussian() * scale;
        }
        return v;
    }

    private interface IndexDistance {
        double of(int index) throws Exception;
    }

    private static Integer[] order(int n, IndexDistance distance) {
        Integer[] indexes = new Integer[n];
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            indexes[i] = i;
            try {
                values[i] = distance.of(i);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        Arrays.sort(indexes, Comparator.comparingDouble(i -> values[i]));
        return indexes;
    }
}
