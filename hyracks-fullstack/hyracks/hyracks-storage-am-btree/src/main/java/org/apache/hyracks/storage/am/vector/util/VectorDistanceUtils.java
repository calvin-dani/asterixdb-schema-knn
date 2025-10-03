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
package org.apache.hyracks.storage.am.lsm.vector.utils;

/**
 * Utility class for vector distance calculations in ANN search operations.
 */
public class VectorDistanceUtils {

    /**
     * Distance metric enumeration for type-safe distance calculations.
     */
    public enum DistanceMetric {
        EUCLIDEAN("euclidean"),
        EUCLIDEAN_SQUARED("euclidean_squared"),
        COSINE("cosine"),
        MANHATTAN("manhattan");

        private final String name;

        DistanceMetric(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Calculates Euclidean distance between two vectors.
     * 
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Euclidean distance
     */
    public static float euclideanDistance(float[] vector1, float[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }

        float sum = 0.0f;
        for (int i = 0; i < vector1.length; i++) {
            float diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }
        return (float) Math.sqrt(sum);
    }

    /**
     * Calculates squared Euclidean distance between two vectors.
     * More efficient than euclideanDistance when only relative ordering matters.
     * 
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Squared Euclidean distance
     */
    public static float euclideanDistanceSquared(float[] vector1, float[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }

        float sum = 0.0f;
        for (int i = 0; i < vector1.length; i++) {
            float diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }
        return sum;
    }

    /**
     * Calculates cosine similarity between two vectors.
     * 
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Cosine similarity (1 - cosine distance)
     */
    public static float cosineSimilarity(float[] vector1, float[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }

        float dotProduct = 0.0f;
        float norm1 = 0.0f;
        float norm2 = 0.0f;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            norm1 += vector1[i] * vector1[i];
            norm2 += vector2[i] * vector2[i];
        }

        if (norm1 == 0.0f || norm2 == 0.0f) {
            return 0.0f;
        }

        return dotProduct / (float) (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    /**
     * Calculates cosine distance between two vectors.
     * 
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Cosine distance (1 - cosine similarity)
     */
    public static float cosineDistance(float[] vector1, float[] vector2) {
        return 1.0f - cosineSimilarity(vector1, vector2);
    }

    /**
     * Calculates Manhattan distance between two vectors.
     * 
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Manhattan distance
     */
    public static float manhattanDistance(float[] vector1, float[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }

        float sum = 0.0f;
        for (int i = 0; i < vector1.length; i++) {
            sum += Math.abs(vector1[i] - vector2[i]);
        }
        return sum;
    }

    /**
     * Calculates distance between two vectors using the specified metric.
     * 
     * @param vector1 First vector
     * @param vector2 Second vector
     * @param metric Distance metric ("euclidean", "cosine", "manhattan")
     * @return Distance value
     */
    public static float calculateDistance(float[] vector1, float[] vector2, String metric) {
        switch (metric.toLowerCase()) {
            case "euclidean":
                return euclideanDistance(vector1, vector2);
            case "euclidean_squared":
                return euclideanDistanceSquared(vector1, vector2);
            case "cosine":
                return cosineDistance(vector1, vector2);
            case "manhattan":
                return manhattanDistance(vector1, vector2);
            default:
                throw new IllegalArgumentException("Unsupported distance metric: " + metric);
        }
    }

    /**
     * Calculates distance between two vectors using the specified metric enum.
     * 
     * @param vector1 First vector
     * @param vector2 Second vector
     * @param metric Distance metric enum
     * @return Distance value
     */
    public static float calculateDistance(float[] vector1, float[] vector2, DistanceMetric metric) {
        return calculateDistance(vector1, vector2, metric.getName());
    }
}
