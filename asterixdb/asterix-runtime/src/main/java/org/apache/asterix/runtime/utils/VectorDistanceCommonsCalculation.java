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

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.ml.distance.ManhattanDistance;

public class VectorDistanceCommonsCalculation {
    private static final EuclideanDistance euclidean_distance = new EuclideanDistance();
    private static final ManhattanDistance manhattan_distance = new ManhattanDistance();
    //   Euclidean Distance
    public static double euclidean(double[] a, double[] b) {
        return Math.sqrt(euclidean_distance.compute(a, b));
    }

    // Manhattan Distance
    public static double manhattan(double[] a, double[] b) {
        return manhattan_distance.compute(a, b);
    }

    // Cosine Similarity
    public static double cosine(double[] a, double[] b) {
        ArrayRealVector v1 = new ArrayRealVector(a);
        ArrayRealVector v2 = new ArrayRealVector(b);
        return v1.dotProduct(v2) / (v1.getNorm() * v2.getNorm());
    }

    // Dot Product
    public static double dot(double[] a, double[] b) {
        ArrayRealVector v1 = new ArrayRealVector(a);
        ArrayRealVector v2 = new ArrayRealVector(b);
        return v1.dotProduct(v2);
    }

}
