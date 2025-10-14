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

package org.apache.hyracks.storage.am.vector.util;

import java.util.Arrays;

/**
 * Utility class for vector operations including distance calculations, 
 * similarity measures, and vector quantization.
 */
public class VectorUtils {

    /**
     * Calculate Euclidean distance between two vectors.
     */
    public static double calculateEuclideanDistance(float[] vector1, float[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }

        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Calculate Euclidean distance between two vectors with mixed types.
     */
    public static double calculateEuclideanDistance(float[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }

        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Calculate Euclidean distance between two double vectors.
     */
    public static double calculateEuclideanDistance(double[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }

        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Calculate cosine similarity between two vectors.
     * Returns a value between -1 and 1, where 1 means identical direction.
     */
    public static double calculateCosineSimilarity(float[] vector1, float[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }

        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            normA += vector1[i] * vector1[i];
            normB += vector2[i] * vector2[i];
        }

        if (normA == 0.0 || normB == 0.0) {
            return 0.0; // Handle zero vectors
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * Calculate cosine similarity between two vectors with mixed types.
     * Returns a value between -1 and 1, where 1 means identical direction.
     */
    public static double calculateCosineSimilarity(float[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }

        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            normA += vector1[i] * vector1[i];
            normB += vector2[i] * vector2[i];
        }

        if (normA == 0.0 || normB == 0.0) {
            return 0.0; // Handle zero vectors
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * Calculate cosine similarity between two double vectors.
     * Returns a value between -1 and 1, where 1 means identical direction.
     */
    public static double calculateCosineSimilarity(double[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }

        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            normA += vector1[i] * vector1[i];
            normB += vector2[i] * vector2[i];
        }

        if (normA == 0.0 || normB == 0.0) {
            return 0.0; // Handle zero vectors
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /**
     * Calculate Manhattan (L1) distance between two vectors.
     */
    public static double calculateManhattanDistance(float[] vector1, float[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }

        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            sum += Math.abs(vector1[i] - vector2[i]);
        }
        return sum;
    }

    /**
     * Calculate Manhattan (L1) distance between two vectors with mixed types.
     */
    public static double calculateManhattanDistance(float[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimensionality");
        }

        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            sum += Math.abs(vector1[i] - vector2[i]);
        }
        return sum;
    }

    /**
     * Normalize a vector to unit length (L2 normalization).
     */
    public static float[] normalizeVector(float[] vector) {
        double norm = 0.0;
        for (float value : vector) {
            norm += value * value;
        }
        norm = Math.sqrt(norm);

        if (norm == 0.0) {
            return Arrays.copyOf(vector, vector.length);
        }

        float[] normalized = new float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            normalized[i] = (float) (vector[i] / norm);
        }
        return normalized;
    }

    /**
     * Quantize a vector using binary quantization.
     * Each dimension is converted to 1 bit: 1 if positive, 0 if negative or zero.
     */
    public static byte[] binaryQuantize(float[] vector) {
        int numBytes = (vector.length + 7) / 8; // Round up to nearest byte
        byte[] quantized = new byte[numBytes];

        for (int i = 0; i < vector.length; i++) {
            if (vector[i] > 0) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                quantized[byteIndex] |= (1 << (7 - bitIndex));
            }
        }

        return quantized;
    }

    /**
     * Dequantize a binary quantized vector back to float representation.
     * 1 bit becomes +1.0, 0 bit becomes -1.0.
     */
    public static float[] binaryDequantize(byte[] quantized, int dimensions) {
        float[] vector = new float[dimensions];

        for (int i = 0; i < dimensions; i++) {
            int byteIndex = i / 8;
            int bitIndex = i % 8;

            if (byteIndex < quantized.length) {
                boolean bit = (quantized[byteIndex] & (1 << (7 - bitIndex))) != 0;
                vector[i] = bit ? 1.0f : -1.0f;
            } else {
                vector[i] = -1.0f; // Default for out-of-bounds
            }
        }

        return vector;
    }

    /**
     * Quantize a vector using scalar quantization with specified number of bits per dimension.
     */
    public static byte[] scalarQuantize(float[] vector, int bitsPerDimension, float minValue, float maxValue) {
        if (bitsPerDimension > 8) {
            throw new IllegalArgumentException("Bits per dimension must be <= 8");
        }

        int levels = 1 << bitsPerDimension; // 2^bitsPerDimension
        float range = maxValue - minValue;
        float step = range / (levels - 1);

        byte[] quantized = new byte[vector.length];
        for (int i = 0; i < vector.length; i++) {
            float value = Math.max(minValue, Math.min(maxValue, vector[i]));
            int quantizedValue = Math.round((value - minValue) / step);
            quantized[i] = (byte) quantizedValue;
        }

        return quantized;
    }

    /**
     * Dequantize a scalar quantized vector.
     */
    public static float[] scalarDequantize(byte[] quantized, int bitsPerDimension, float minValue, float maxValue) {
        int levels = 1 << bitsPerDimension;
        float range = maxValue - minValue;
        float step = range / (levels - 1);

        float[] vector = new float[quantized.length];
        for (int i = 0; i < quantized.length; i++) {
            int quantizedValue = quantized[i] & ((1 << bitsPerDimension) - 1);
            vector[i] = minValue + quantizedValue * step;
        }

        return vector;
    }

    /**
     * Calculate approximate Euclidean distance using binary quantized vectors.
     * Uses Hamming distance as approximation.
     */
    public static double approximateDistanceBinary(byte[] quantized1, byte[] quantized2, int dimensions) {
        int hammingDistance = 0;
        int minBytes = Math.min(quantized1.length, quantized2.length);

        for (int i = 0; i < minBytes; i++) {
            byte xor = (byte) (quantized1[i] ^ quantized2[i]);
            hammingDistance += Integer.bitCount(xor & 0xFF);
        }

        // Handle remaining bits if arrays have different lengths
        if (quantized1.length != quantized2.length) {
            byte[] longer = quantized1.length > quantized2.length ? quantized1 : quantized2;
            for (int i = minBytes; i < longer.length; i++) {
                hammingDistance += Integer.bitCount(longer[i] & 0xFF);
            }
        }

        // Convert Hamming distance to approximate Euclidean distance
        // This is a rough approximation based on the assumption that 
        // Hamming distance correlates with Euclidean distance for binary vectors
        return Math.sqrt(hammingDistance);
    }

    /**
     * Compute centroid of a set of vectors.
     */
    public static float[] computeCentroid(float[][] vectors) {
        if (vectors.length == 0) {
            return new float[0];
        }

        int dimensions = vectors[0].length;
        float[] centroid = new float[dimensions];

        for (float[] vector : vectors) {
            if (vector.length != dimensions) {
                throw new IllegalArgumentException("All vectors must have the same dimensionality");
            }
            for (int i = 0; i < dimensions; i++) {
                centroid[i] += vector[i];
            }
        }

        for (int i = 0; i < dimensions; i++) {
            centroid[i] /= vectors.length;
        }

        return centroid;
    }

    /**
     * Check if two vectors are approximately equal within a tolerance.
     */
    public static boolean approximatelyEqual(float[] vector1, float[] vector2, float tolerance) {
        if (vector1.length != vector2.length) {
            return false;
        }

        for (int i = 0; i < vector1.length; i++) {
            if (Math.abs(vector1[i] - vector2[i]) > tolerance) {
                return false;
            }
        }

        return true;
    }

    /**
     * Convert float array to byte array (for serialization).
     */
    public static byte[] floatArrayToBytes(float[] array) {
        byte[] bytes = new byte[array.length * 4];
        for (int i = 0; i < array.length; i++) {
            int bits = Float.floatToIntBits(array[i]);
            int byteOffset = i * 4;
            bytes[byteOffset] = (byte) (bits >>> 24);
            bytes[byteOffset + 1] = (byte) (bits >>> 16);
            bytes[byteOffset + 2] = (byte) (bits >>> 8);
            bytes[byteOffset + 3] = (byte) bits;
        }
        return bytes;
    }

    /**
     * Convert byte array to float array (for deserialization).
     */
    public static float[] bytesToFloatArray(byte[] bytes) {
        if (bytes.length % 4 != 0) {
            throw new IllegalArgumentException("Byte array length must be multiple of 4");
        }

        float[] array = new float[bytes.length / 4];
        for (int i = 0; i < array.length; i++) {
            int byteOffset = i * 4;
            int bits = (bytes[byteOffset] << 24) | ((bytes[byteOffset + 1] & 0xFF) << 16)
                    | ((bytes[byteOffset + 2] & 0xFF) << 8) | (bytes[byteOffset + 3] & 0xFF);
            array[i] = Float.intBitsToFloat(bits);
        }
        return array;
    }

    /**
     * Convert byte array to double array.
     */
    public static double[] bytesToDoubleArray(byte[] bytes) {
        if (bytes.length % 8 != 0) {
            throw new IllegalArgumentException("Byte array length must be multiple of 8");
        }

        double[] array = new double[bytes.length / 8];
        for (int i = 0; i < array.length; i++) {
            int byteOffset = i * 8;
            long bits = ((long) bytes[byteOffset] << 56) | (((long) bytes[byteOffset + 1] & 0xFF) << 48)
                    | (((long) bytes[byteOffset + 2] & 0xFF) << 40) | (((long) bytes[byteOffset + 3] & 0xFF) << 32)
                    | (((long) bytes[byteOffset + 4] & 0xFF) << 24) | (((long) bytes[byteOffset + 5] & 0xFF) << 16)
                    | (((long) bytes[byteOffset + 6] & 0xFF) << 8) | ((long) bytes[byteOffset + 7] & 0xFF);
            array[i] = Double.longBitsToDouble(bits);
        }
        return array;
    }
}
