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

import java.util.BitSet;

public class VectorBitmapDistanceArrCalculation {


    public static double euclidean(VectorWithBitmap a, VectorWithBitmap b) {
        double sum = 0.0;
        BitSet union = (BitSet) a.bitset.clone();
        union.or(b.bitset); // union of non-zero indices
        for (int i = union.nextSetBit(0); i >= 0; i = union.nextSetBit(i + 1)) {
            double diff = a.values[i] - b.values[i];
            sum += diff * diff;
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return Math.sqrt(sum);
    }

    public static double manhattan(VectorWithBitmap a, VectorWithBitmap b) {
        double sum = 0.0;
        BitSet union = (BitSet) a.bitset.clone();
        union.or(b.bitset);

        for (int i = union.nextSetBit(0); i >= 0; i = union.nextSetBit(i + 1)) {
            sum += Math.abs(a.values[i] - b.values[i]);
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

       public static double cosine(VectorWithBitmap a, VectorWithBitmap b) {
        double dot = 0.0, normA = 0.0, normB = 0.0;

        BitSet common = (BitSet) a.bitset.clone();
        common.and(b.bitset);

        for (int i = common.nextSetBit(0); i >= 0; i = common.nextSetBit(i + 1)) {
            dot += a.values[i] * b.values[i];
        }

        for (int i = a.bitset.nextSetBit(0); i >= 0; i = a.bitset.nextSetBit(i + 1)) {
            normA += a.values[i] * a.values[i];
        }

        for (int i = b.bitset.nextSetBit(0); i >= 0; i = b.bitset.nextSetBit(i + 1)) {
            normB += b.values[i] * b.values[i];
        }

        if (normA == 0 || normB == 0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Double.NaN;
        }

        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }


    public static double dot(VectorWithBitmap a, VectorWithBitmap b) {
        double sum = 0.0;
        BitSet common = (BitSet) a.bitset.clone();
        common.and(b.bitset); // intersection = positions where both are non-zero

        for (int i = common.nextSetBit(0); i >= 0; i = common.nextSetBit(i + 1)) {
            sum += a.values[i] * b.values[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

}
