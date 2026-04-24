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
package org.apache.asterix.common.storage;

/**
 * Optimized scalar quantization (OSQ) utilities for vector indexes.
 *
 * <p>
 * Global quantization parameters ({@link Params}) are computed at index creation
 * (via {@code QuantizationConstantsAggregate}) and stored on
 * {@code LSMVTreeLocalResource}. This class applies those parameters to quantize and
 * dequantize vectors during bulk load, static structure build, and query.
 *
 * <p>
 * For index {@code WITH similarity} values that map to cosine ({@code "cosine"},
 * {@code "cosine similarity"}), embedding and query vectors must be L2-normalized to
 * unit length before insert and search. The engine does not re-normalize during
 * quantization.
 */
public final class OptimizedScalarQuantizationSampleFile {

    /**
     * When false, corrective multiplier is not computed (stored as 0.0f).
     * Set to true when Lucene-style integer dot plus per-vector correction is implemented and persisted.
     */
    private static final boolean ENABLE_CORRECTIVE_MULTIPLIER = false;

    private OptimizedScalarQuantizationSampleFile() {
    }

    /**
     * Similarity function types for vector quantization.
     * Determines similarity metadata on {@link QuantizedVector}; corrective multipliers are computed only when
     * {@link #ENABLE_CORRECTIVE_MULTIPLIER} is true.
     */
    public enum SimilarityFunction {
        /** Dot product similarity */
        DOT_PRODUCT,
        /**
         * Cosine similarity. Vectors must already be L2-normalized to unit length by the
         * caller; the engine does not normalize during quantization.
         */
        COSINE,
        /** Euclidean distance (L2) */
        EUCLIDEAN,
        /** Squared Euclidean distance (L2 squared) */
        EUCLIDEAN_SQUARED
    }

    /**
     * Result of vector quantization: per-dimension codes plus optional Lucene-style corrective term.
     * Corrective multiplier is 0.0f while {@link #ENABLE_CORRECTIVE_MULTIPLIER} is false.
     */
    public static final class QuantizedVector {
        /** Quantized vector as byte[], short[], or int[] depending on bits */
        public final Object quantizedBytes;
        /** Per-vector corrective multiplier for offset correction (0.0f when corrective computation is disabled) */
        public final float correctiveMultiplier;
        /** The similarity function used for quantization */
        public final SimilarityFunction similarityFunction;

        public QuantizedVector(Object quantizedBytes, float correctiveMultiplier,
                SimilarityFunction similarityFunction) {
            this.quantizedBytes = quantizedBytes;
            this.correctiveMultiplier = correctiveMultiplier;
            this.similarityFunction = similarityFunction;
        }
    }

    /**
     * Converts a distance metric string to SimilarityFunction enum.
     * 
     * @param distanceMetric Distance metric string (e.g., "euclidean", "cosine similarity", "dot product")
     * @return Corresponding SimilarityFunction enum, or DOT_PRODUCT as default
     */
    public static SimilarityFunction fromDistanceMetric(String distanceMetric) {
        if (distanceMetric == null || distanceMetric.trim().isEmpty()) {
            return SimilarityFunction.DOT_PRODUCT; // Default
        }

        String metricLower = distanceMetric.toLowerCase().trim();
        switch (metricLower) {
            case "euclidean":
            case "l2":
                return SimilarityFunction.EUCLIDEAN;
            case "euclidean_squared":
            case "l2_squared":
                return SimilarityFunction.EUCLIDEAN_SQUARED;
            case "cosine":
            case "cosine similarity":
                return SimilarityFunction.COSINE;
            case "dot":
            case "dot product":
                return SimilarityFunction.DOT_PRODUCT;
            default:
                // Default to DOT_PRODUCT for unknown metrics
                return SimilarityFunction.DOT_PRODUCT;
        }
    }

    /**
     * Global scalar-quantization parameters computed at index creation and reused for encode/decode.
     *
     * <p>
     * {@link #minQuantile} and {@link #maxQuantile} are the lower/upper sample quantiles over all
     * vector dimensions. {@link #alpha} is {@code (2^bits - 1) / (maxQuantile - minQuantile)} (see
     * {@code QuantizationConstantsAggregate} at index creation). In practice {@link #bits} is 4 (SQ4)
     * or 8 (SQ8).
     */
    public static final class Params {
        /** Number of bits per dimension (e.g. 4 or 8); determines code range {@code [0, 2^bits - 1]}. */
        public final int bits;
        public final int vectorDimensions;
        public final int sampleCount;
        public final float confidenceInterval;
        /** Global minimum scalar value (minQ in encode/decode formulas). */
        public final float minQuantile;
        /** Global maximum scalar value (maxQ in encode clamp). */
        public final float maxQuantile;
        /** Scale factor: {@code (2^bits - 1) / (maxQuantile - minQuantile)}. */
        public final float alpha;

        // Derived optimization constants for faster similarity computation (Lucene-style)
        public final float alphaSquared;
        public final float minQuantileAlpha;
        public final float minQuantileSquared;

        public Params(int bits, int vectorDimensions, int sampleCount, float confidenceInterval, float minQuantile,
                float maxQuantile, float alpha) {
            this.bits = bits;
            this.vectorDimensions = vectorDimensions;
            this.sampleCount = sampleCount;
            this.confidenceInterval = confidenceInterval;
            this.minQuantile = minQuantile;
            this.maxQuantile = maxQuantile;
            this.alpha = alpha;

            // Compute derived constants for optimized similarity calculations
            this.alphaSquared = alpha * alpha;
            this.minQuantileAlpha = minQuantile * alpha;
            this.minQuantileSquared = minQuantile * minQuantile;
        }
    }

    /**
     * Quantizes a vector using optimized scalar quantization with similarity-function awareness.
     *
     * <p>
     * Vectors are quantized as provided. For cosine indexes, callers must supply L2-normalized unit vectors.
     * Per-vector corrective multiplier is computed only when {@link #ENABLE_CORRECTIVE_MULTIPLIER} is true;
     * otherwise it is 0.0f (current default).
     *
     * <p>
     * Encoding uses {@code levels = 2^bits} and delegates to {@link #quantizeToByte},
     * {@link #quantizeToShort}, or {@link #quantizeToInt} depending on {@code bits}. See those methods
     * for the per-dimension encode formula and {@link #dequantizeToDoubleArray} for the inverse.
     *
     * @param vector The input vector to quantize (double array)
     * @param params Quantization parameters including bits, minQuantile, maxQuantile, alpha
     * @param similarityFunction The similarity function type (recorded on the result; used for corrective when enabled)
     * @return QuantizedVector containing quantized bytes and metadata
     * @throws IllegalArgumentException if vector is null or params are invalid
     */
    public static QuantizedVector quantizeVector(double[] vector, Params params,
            SimilarityFunction similarityFunction) {
        if (vector == null) {
            throw new IllegalArgumentException("vector must be non-null");
        }
        if (params == null) {
            throw new IllegalArgumentException("params must be non-null");
        }
        if (vector.length != params.vectorDimensions) {
            throw new IllegalArgumentException(
                    "vector dimension mismatch: expected " + params.vectorDimensions + ", got " + vector.length);
        }

        final int bits = params.bits;
        final float minQ = params.minQuantile;
        final float maxQ = params.maxQuantile;
        final float alpha = params.alpha;
        final int levels = 1 << bits; // 2^bits

        Object quantizedBytes;
        if (bits <= 8) {
            quantizedBytes = quantizeToByte(vector, minQ, maxQ, alpha, levels);
        } else if (bits <= 16) {
            quantizedBytes = quantizeToShort(vector, minQ, maxQ, alpha, levels);
        } else if (bits <= 32) {
            quantizedBytes = quantizeToInt(vector, minQ, maxQ, alpha, levels);
        } else {
            throw new IllegalArgumentException("bits must be <= 32, got " + bits);
        }

        float correctiveMultiplier = ENABLE_CORRECTIVE_MULTIPLIER
                ? calculateCorrectiveMultiplier(vector, quantizedBytes, params, similarityFunction) : 0.0f;

        return new QuantizedVector(quantizedBytes, correctiveMultiplier, similarityFunction);
    }

    /**
     * Legacy method for backward compatibility.
     * Quantizes a vector without similarity function awareness.
     * 
     * @param vector The input vector to quantize
     * @param params Quantization parameters
     * @return Quantized vector as Object (byte[], short[], or int[] based on bits)
     * @deprecated Use {@link #quantizeVector(double[], Params, SimilarityFunction)} instead
     */
    @Deprecated
    public static Object quantizeVector(double[] vector, Params params) {
        QuantizedVector result = quantizeVector(vector, params, SimilarityFunction.DOT_PRODUCT);
        return result.quantizedBytes;
    }

    /*
     * Per-dimension scalar encode/decode contract (used by quantizeToByte, quantizeToShort, quantizeToInt).
     *
     * Parameter source: minQ, maxQ, alpha, and bits come from Params (minQuantile, maxQuantile, alpha, bits),
     * populated at index creation by QuantizationConstantsAggregate:
     *   levels = 2^bits
     *   alpha = (levels - 1) / (maxQ - minQ)
     *
     * Encode (dimension i):
     *   v = clamp(x[i], minQ, maxQ)
     *   q = clamp(round((v - minQ) * alpha), 0, levels - 1)
     *
     * Decode (inverse, see dequantizeToDoubleArray):
     *   x_hat[i] = q / alpha + minQ   (minQ = Params.minQuantile)
     *
     * Endpoints: v = minQ -> q = 0; v = maxQ -> q = levels - 1 (after clamp and round).
     * Rounding: Math.round selects the nearest integer code (standard nearest-bin scalar quant).
     *
     * Storage by bits: bits <= 8 -> byte[] (SQ4 uses codes 0..15 in byte[]); <= 16 -> short[]; <= 32 -> int[].
     * Java byte/short are signed; codes above 127 or 32767 appear negative unless read with & 0xFF / & 0xFFFF.
     *
     * Debugging: log params.bits, levels, minQ, maxQ, alpha; check min/max q across dims; compare
     * dequantizeToDoubleArray(quantizeVector(x)) against x for round-trip error on sample vectors.
     */

    /**
     * Encodes a vector to {@code byte[]} when {@code bits <= 8} (including SQ4 with 16 levels).
     *
     * <p>
     * Per dimension: clamp to {@code [minQ, maxQ]}, then {@code q = clamp(round((v - minQ) * alpha), 0, levels - 1)}.
     * Caller must pass {@code levels == 1 << bits}. Inverse: {@link #dequantizeToDoubleArray}.
     *
     * <p>
     * Codes 0..255 are stored in signed {@code byte}; consumers must read with {@code & 0xFF}.
     *
     * @see #quantizeToShort
     * @see #quantizeToInt
     */
    private static byte[] quantizeToByte(double[] vector, float minQ, float maxQ, float alpha, int levels) {
        byte[] quantized = new byte[vector.length];
        for (int i = 0; i < vector.length; i++) {
            // clamp to global quantile range, then map to integer code in [0, levels - 1]
            double value = Math.max(minQ, Math.min(maxQ, vector[i]));
            int quantizedValue = Math.toIntExact(Math.round((value - minQ) * alpha));
            quantizedValue = Math.max(0, Math.min(levels - 1, quantizedValue));
            quantized[i] = (byte) quantizedValue;
        }
        return quantized;
    }

    /**
     * Encodes a vector to {@code short[]} when {@code 8 < bits <= 16}.
     *
     * <p>
     * Same encode formula as {@link #quantizeToByte}. Caller must pass {@code levels == 1 << bits}.
     * Inverse: {@link #dequantizeToDoubleArray}. Codes above 32767 require unsigned read ({@code & 0xFFFF}).
     *
     * @see #quantizeToByte
     * @see #quantizeToInt
     */
    private static short[] quantizeToShort(double[] vector, float minQ, float maxQ, float alpha, int levels) {
        short[] quantized = new short[vector.length];
        for (int i = 0; i < vector.length; i++) {
            // clamp to global quantile range, then map to integer code in [0, levels - 1]
            double value = Math.max(minQ, Math.min(maxQ, vector[i]));
            int quantizedValue = Math.toIntExact(Math.round((value - minQ) * alpha));
            quantizedValue = Math.max(0, Math.min(levels - 1, quantizedValue));
            quantized[i] = (short) quantizedValue;
        }
        return quantized;
    }

    /**
     * Encodes a vector to {@code int[]} when {@code 16 < bits <= 32}.
     *
     * <p>
     * Same encode formula as {@link #quantizeToByte}. Caller must pass {@code levels == 1 << bits}.
     * Inverse: {@link #dequantizeToDoubleArray}. Uses {@code long} for {@code Math.round} (equivalent
     * to the {@code Math.toIntExact} path after clamp for practical code ranges).
     *
     * @see #quantizeToByte
     * @see #quantizeToShort
     */
    private static int[] quantizeToInt(double[] vector, float minQ, float maxQ, float alpha, int levels) {
        int[] quantized = new int[vector.length];
        for (int i = 0; i < vector.length; i++) {
            // clamp to global quantile range, then map to integer code in [0, levels - 1]
            double value = Math.max(minQ, Math.min(maxQ, vector[i]));
            long quantizedValue = Math.round((value - minQ) * alpha);
            quantizedValue = Math.max(0, Math.min(levels - 1, quantizedValue));
            quantized[i] = (int) quantizedValue;
        }
        return quantized;
    }

    /**
     * Calculates the per-vector corrective multiplier using Lucene's exact formula.
     * 
     * <p>
     * For DOT_PRODUCT and COSINE similarity:
     *   correctiveMultiplier = Σ((original[i] - minQuantile) - quantized[i] * α) * quantized[i] * α
     * 
     * <p>
     * For EUCLIDEAN and EUCLIDEAN_SQUARED:
     *   Returns 0.0f (no correction needed, offset terms cancel out)
     * 
     * @param originalVector The original vector before quantization
     * @param quantizedVector The quantized vector (byte[], short[], or int[])
     * @param params Quantization parameters
     * @param similarityFunction The similarity function type
     * @return Corrective multiplier float value
     */
    private static float calculateCorrectiveMultiplier(double[] originalVector, Object quantizedVector, Params params,
            SimilarityFunction similarityFunction) {
        // For Euclidean distance, offset terms cancel out - no correction needed
        if (similarityFunction == SimilarityFunction.EUCLIDEAN
                || similarityFunction == SimilarityFunction.EUCLIDEAN_SQUARED) {
            return 0.0f;
        }

        // For DOT_PRODUCT and COSINE, use Lucene's exact formula
        // correctiveMultiplier = Σ((original[i] - minQuantile) - quantized[i] * α) * quantized[i] * α
        final float minQ = params.minQuantile;
        final float alpha = params.alpha;
        final int dims = params.vectorDimensions;

        float correctiveSum = 0.0f;

        if (quantizedVector instanceof byte[]) {
            byte[] bytes = (byte[]) quantizedVector;
            for (int i = 0; i < dims; i++) {
                int quantizedVal = bytes[i] & 0xFF; // unsigned byte
                double originalVal = originalVector[i];
                // Lucene's formula: ((original[i] - minQuantile) - quantized[i] * α) * quantized[i] * α
                float error = (float) ((originalVal - minQ) - quantizedVal * alpha);
                correctiveSum += error * quantizedVal * alpha;
            }
        } else if (quantizedVector instanceof short[]) {
            short[] shorts = (short[]) quantizedVector;
            for (int i = 0; i < dims; i++) {
                int quantizedVal = shorts[i] & 0xFFFF; // unsigned short
                double originalVal = originalVector[i];
                float error = (float) ((originalVal - minQ) - quantizedVal * alpha);
                correctiveSum += error * quantizedVal * alpha;
            }
        } else if (quantizedVector instanceof int[]) {
            int[] ints = (int[]) quantizedVector;
            for (int i = 0; i < dims; i++) {
                long quantizedVal = ints[i] & 0xFFFFFFFFL; // unsigned int
                double originalVal = originalVector[i];
                float error = (float) ((originalVal - minQ) - quantizedVal * alpha);
                correctiveSum += error * quantizedVal * alpha;
            }
        } else {
            throw new IllegalArgumentException("Unsupported quantized vector type: " + quantizedVector.getClass());
        }

        return correctiveSum;
    }

    /**
     * Converts quantized vector data to a double array for use with distance
     * functions.
     * Inverse of the encode path in {@link #quantizeToByte}, {@link #quantizeToShort}, and
     * {@link #quantizeToInt}.
     *
     * <p>
     * This is a simple type conversion that preserves the quantized integer values
     * as doubles,
     * allowing standard distance functions that expect double[] to work on
     * quantized data.
     * 
     * <p>
     * Example: quantized [71, 9, 88, 63] with alpha=2.0, minQuantile=-1.0
     *          → dequantized [34.5, 3.5, 43.0, 30.5]
     *
     * @param quantizedBytes The quantized vector as byte[], short[], or int[]
     * @param params         Quantization parameters (alpha and minQuantile used for
     *                       inverse mapping; bits and vectorDimensions for validation)
     * @return double[] array where each element approximates the original value via
     *         inverse quantization: value = quantizedInt / alpha + minQuantile
     * @throws IllegalArgumentException if inputs are null or array length doesn't
     *                                  match params
     */
    public static double[] dequantizeToDoubleArray(Object quantizedBytes, Params params) {
        if (quantizedBytes == null) {
            throw new IllegalArgumentException("quantizedBytes must be non-null");
        }
        if (params == null) {
            throw new IllegalArgumentException("params must be non-null");
        }

        final int dims = params.vectorDimensions;
        final int bits = params.bits;
        double[] result = new double[dims];

        // Determine data type based on bits and convert to double
        if (bits <= 8) {
            // byte[] - treat as unsigned
            if (!(quantizedBytes instanceof byte[])) {
                throw new IllegalArgumentException(
                        "Expected byte[] for " + bits + " bits, got " + quantizedBytes.getClass().getName());
            }
            byte[] bytes = (byte[]) quantizedBytes;
            if (bytes.length != dims) {
                throw new IllegalArgumentException("Array length mismatch: expected " + dims + ", got " + bytes.length);
            }
            for (int i = 0; i < dims; i++) {
                result[i] = ((double) (bytes[i] & 0xFF)) / params.alpha + params.minQuantile;
            }
        } else if (bits <= 16) {
            // short[] - treat as unsigned
            if (!(quantizedBytes instanceof short[])) {
                throw new IllegalArgumentException(
                        "Expected short[] for " + bits + " bits, got " + quantizedBytes.getClass().getName());
            }
            short[] shorts = (short[]) quantizedBytes;
            if (shorts.length != dims) {
                throw new IllegalArgumentException(
                        "Array length mismatch: expected " + dims + ", got " + shorts.length);
            }
            for (int i = 0; i < dims; i++) {
                result[i] = ((double) (shorts[i] & 0xFFFF)) / params.alpha + params.minQuantile;
            }
        } else if (bits <= 32) {
            // int[] - treat as unsigned
            if (!(quantizedBytes instanceof int[])) {
                throw new IllegalArgumentException(
                        "Expected int[] for " + bits + " bits, got " + quantizedBytes.getClass().getName());
            }
            int[] ints = (int[]) quantizedBytes;
            if (ints.length != dims) {
                throw new IllegalArgumentException("Array length mismatch: expected " + dims + ", got " + ints.length);
            }
            for (int i = 0; i < dims; i++) {
                result[i] = ((double) (ints[i] & 0xFFFFFFFFL)) / params.alpha + params.minQuantile;
            }
        } else {
            throw new IllegalArgumentException("bits must be <= 32, got " + bits);
        }

        return result;
    }
}
