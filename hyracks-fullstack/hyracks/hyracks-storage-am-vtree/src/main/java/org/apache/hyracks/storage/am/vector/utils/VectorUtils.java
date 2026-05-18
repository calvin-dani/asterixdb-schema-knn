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
package org.apache.hyracks.storage.am.vector.utils;

import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Storage-layer distance-function helpers. The canonical implementations live in
 * {@code asterix-runtime}'s {@code VectorDistanceArrCalculation}; the lookup below is the
 * fallback used only when the access parameters do not carry a
 * {@code VECTOR_DISTANCE_FUNCTION_FACTORY}. The math here must stay numerically consistent
 * with the canonical versions.
 */
public final class VectorUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    private VectorUtils() {
    }

    /**
     * Look up a fallback distance function by metric name.
     *
     * @param distanceMetric metric name; null, blank, or unknown values default to Euclidean.
     */
    public static IVTreeDistanceFunction forMetric(String distanceMetric) {
        if (distanceMetric == null || distanceMetric.trim().isEmpty()) {
            return VectorUtils::calculateEuclideanDistance;
        }
        switch (distanceMetric.toLowerCase().trim()) {
            case "euclidean":
            case "l2":
                return VectorUtils::calculateEuclideanDistance;
            case "euclidean_squared":
            case "l2_squared":
                return VectorUtils::calculateSquaredEuclideanDistance;
            case "manhattan":
            case "l1":
                return (a, b) -> {
                    double sum = 0.0;
                    for (int i = 0; i < a.length; i++) {
                        sum += Math.abs(a[i] - b[i]);
                    }
                    return sum;
                };
            case "cosine":
                return (a, b) -> {
                    // Returned as 1 - cos(a, b) so smaller = more similar.
                    double dotProduct = 0.0;
                    double normA = 0.0;
                    double normB = 0.0;
                    for (int i = 0; i < a.length; i++) {
                        dotProduct += a[i] * b[i];
                        normA += a[i] * a[i];
                        normB += b[i] * b[i];
                    }
                    if (normA == 0.0 || normB == 0.0) {
                        return 1.0;
                    }
                    return 1.0 - dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
                };
            case "dot":
                return (a, b) -> {
                    // Negated so smaller = more similar (MIPS convention).
                    double dotProduct = 0.0;
                    for (int i = 0; i < a.length; i++) {
                        dotProduct += a[i] * b[i];
                    }
                    return -dotProduct;
                };
            default:
                LOGGER.log(Level.TRACE, "Unsupported distance function: {}, defaulting to euclidean", distanceMetric);
                return VectorUtils::calculateEuclideanDistance;
        }
    }

    /**
     * Calculate Euclidean distance between two double vectors. Kept as a direct entry point
     * for existing callers; new code should prefer {@link #forMetric(String)}.
     */
    public static double calculateEuclideanDistance(double[] a, double[] b) {
        return Math.sqrt(calculateSquaredEuclideanDistance(a, b));
    }

    private static double calculateSquaredEuclideanDistance(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }
}
