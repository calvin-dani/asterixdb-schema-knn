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

import smile.math.MathEx;
import smile.math.distance.EuclideanDistance;
import smile.math.distance.ManhattanDistance;

public class VectorDistanceCalculationSMILE {

    // Euclidean Distance
    public static double euclidean(double[] a, double[] b) {
        EuclideanDistance distance = new EuclideanDistance();
        return distance.d(a, b);
    }

    // Manhattan Distance
    public static double manhattan(double[] a, double[] b) {
        ManhattanDistance distance = new ManhattanDistance();
        return distance.d(a, b);
    }

    // Cosine Similarity
    public static double cosine(double[] a, double[] b) {
        double dotCal = MathEx.dot(a, b);
        return dotCal / (MathEx.norm2(a) * MathEx.norm2(b));

    }

    // Dot Product
    public static double dot(double[] a, double[] b) {
        return MathEx.dot(a, b);
    }

}
