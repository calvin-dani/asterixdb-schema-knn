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

public class VectorBitmapDistanceArrCalculation {

    public static double euclidean(VectorWithBitmap a, VectorWithBitmap b) {
        final double[] sum = new double[1];
        a.forEachNonZeroPair(b, (i, va, vb) -> {
            double d = va - vb;
            sum[0] += d * d;
        });
        if (Double.isNaN(sum[0])) {
            return Double.NaN; // Handle NaN case
        }
        return Math.sqrt(sum[0]);
    }

    public static double manhattan(VectorWithBitmap a, VectorWithBitmap b) {
        final double[] sum = new double[1];
        a.forEachNonZeroPair(b, (i, va, vb) -> {
            sum[0] += Math.abs(va - vb);
        });
        if (Double.isNaN(sum[0])) {
            return Double.NaN; // Handle NaN case
        }
        return sum[0];
    }

    public static double cosine(VectorWithBitmap a, VectorWithBitmap b) {
        final double[] dot = new double[1];
        final double[] normA = new double[1];
        final double[] normB = new double[1];

        a.forEachNonZeroPair(b, (i, va, vb) -> {
            dot[0] += va * vb;
            normA[0] += va * va;
            normB[0] += vb * vb;
        });

        if (normA[0] == 0 || normB[0] == 0 || Double.isNaN(normA[0]) || Double.isNaN(normB[0])
                || Double.isNaN(dot[0])) {
            return Double.NaN;
        }
        return dot[0] / (Math.sqrt(normA[0]) * Math.sqrt(normB[0]));
    }

    public static double dot(VectorWithBitmap a, VectorWithBitmap b) {
        final double[] sum = new double[1];
        a.forEachNonZeroPair(b, (i, va, vb) -> {
            sum[0] += va * vb;
        });
        if (Double.isNaN(sum[0])) {
            return Double.NaN; // Handle NaN case
        }
        return sum[0];
    }

}
