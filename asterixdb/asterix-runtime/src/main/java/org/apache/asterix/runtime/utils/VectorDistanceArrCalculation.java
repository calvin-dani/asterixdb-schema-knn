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

public class VectorDistanceArrCalculation {

    public static double euclidean(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vectors must be of the same length");
        }
        double sum = euclideanSquared(a, b);
        return Math.sqrt(sum);
    }

    public static double euclideanSquared(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vectors must be of the same length");
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double manhattan(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vectors must be of the same length");
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.abs(a[i] - b[i]);
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

    /**
     * Cosine distance = 1 - cosine similarity. Range [0, 2]; lower = more similar.
     * Aligns with Spark and common vector DBs so "minimize distance" = "maximize similarity".
     * Returns NaN if cosine(a,b) is NaN (e.g. zero norm).
     */
    public static double cosineDistance(double[] a, double[] b) {
        double sim = cosine(a, b);
        return Double.isNaN(sim) ? Double.NaN : (1.0 - sim);
    }

    public static double cosine(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vectors must be of the same length");
        }
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Float.NaN; // or throw exception for zero vector
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * L2 norm of a double vector: sqrt(sum_i a[i]^2).
     * Returns 0.0 for zero vector or NaN if any component is NaN.
     */
    public static double l2Norm(double[] a) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double v = a[i];
            sum += v * v;
        }
        return Double.isNaN(sum) ? Double.NaN : Math.sqrt(sum);
    }

    /**
     * Normalize vector in place to unit L2 norm.
     * No-op if norm is zero or NaN (avoids division by zero).
     */
    public static void normalizeL2(double[] a) {
        double norm = l2Norm(a);
        if (norm > 0.0 && !Double.isNaN(norm)) {
            for (int i = 0; i < a.length; i++) {
                a[i] /= norm;
            }
        }
    }

    public static double dot(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vectors must be of the same length");
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

}
