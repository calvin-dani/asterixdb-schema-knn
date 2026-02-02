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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;

/**
 * Stores quantization parameters computed from sampled vectors (Job 1),
 * as a small sidecar file inside the index directory.
 *
 * <p>
 * File format (little-endian):
 * <pre>
 *   u32 magic               // 'OSQF' = 0x4F535146
 *   u16 version             // current = 1
 *   u16 bits                // quantization bits per dimension (<= 8)
 *   u32 vectorDimensions
 *   u32 sampleCount         // number of sampled vectors used
 *   f32 confidenceInterval  // e.g. 0.99
 *   f32 minQuantile
 *   f32 maxQuantile
 *   f32 alpha               // (levels-1)/(maxQuantile-minQuantile)
 * </pre>
 *
 * <p>
 * Note: This is only the "first part": compute + persist params.
 * Wiring into Job 1 and reading in Job 2 is intentionally left out for now.
 */
public final class OptimizedScalarQuantizationSampleFile {

    /** Must start with "." to avoid being treated as an LSM component file. */
    public static final String FILE_NAME = ".osq_samples";

    private static final int MAGIC = 0x4F535146; // 'OSQF'
    private static final short VERSION = 1;

    private OptimizedScalarQuantizationSampleFile() {
    }

    /**
     * Similarity function types for vector quantization.
     * Determines how vectors are processed and how corrective multipliers are calculated.
     */
    public enum SimilarityFunction {
        /** Dot product similarity */
        DOT_PRODUCT,
        /** Cosine similarity (requires L2 normalization before quantization) */
        COSINE,
        /** Euclidean distance (L2) */
        EUCLIDEAN,
        /** Squared Euclidean distance (L2 squared) */
        EUCLIDEAN_SQUARED
    }

    /**
     * Result of vector quantization, containing quantized bytes and corrective multiplier.
     * Inspired by Lucene's ScalarQuantizer implementation.
     */
    public static final class QuantizedVector {
        /** Quantized vector as byte[], short[], or int[] depending on bits */
        public final Object quantizedBytes;
        /** Per-vector corrective multiplier for offset correction (Lucene-style) */
        public final float correctiveMultiplier;
        /** Whether the vector was normalized before quantization */
        public final boolean isNormalized;
        /** The similarity function used for quantization */
        public final SimilarityFunction similarityFunction;

        public QuantizedVector(Object quantizedBytes, float correctiveMultiplier, boolean isNormalized,
                SimilarityFunction similarityFunction) {
            this.quantizedBytes = quantizedBytes;
            this.correctiveMultiplier = correctiveMultiplier;
            this.isNormalized = isNormalized;
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

    public static final class Params {
        public final int bits;
        public final int vectorDimensions;
        public final int sampleCount;
        public final float confidenceInterval;
        public final float minQuantile;
        public final float maxQuantile;
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
     * Computes quantization parameters from sampled vectors using a confidence interval
     * to clamp outliers.
     *
     * <p>
     * The quantile range is:
     * lower = (1 - CI) / 2
     * upper = 1 - lower
     */
    public static Params computeFromSamples(List<float[]> samples, float confidenceInterval, int bits) {
        if (samples == null || samples.isEmpty()) {
            throw new IllegalArgumentException("samples must be non-null and non-empty");
        }
        if (!(confidenceInterval > 0.0f && confidenceInterval <= 1.0f)) {
            throw new IllegalArgumentException("confidenceInterval must be in (0, 1]");
        }
        if (bits <= 0 || bits > 32) {
            throw new IllegalArgumentException("bits must be in [1, 32]");
        }

        final int sampleCount = samples.size();
        final int vectorDimensions = samples.get(0).length;
        for (int i = 1; i < sampleCount; i++) {
            if (samples.get(i).length != vectorDimensions) {
                throw new IllegalArgumentException("all sampled vectors must have the same dimensionality");
            }
        }

        // Flatten components for quantile estimation.
        final int n = sampleCount * vectorDimensions;
        final float[] values = new float[n];
        int p = 0;
        for (float[] v : samples) {
            System.arraycopy(v, 0, values, p, vectorDimensions);
            p += vectorDimensions;
        }
        Arrays.sort(values);

        final float half = (1.0f - confidenceInterval) / 2.0f;
        final int lowerIdx = clampIndex((int) Math.floor(half * (n - 1)), n);
        final int upperIdx = clampIndex((int) Math.ceil((1.0f - half) * (n - 1)), n);

        float minQ = values[lowerIdx];
        float maxQ = values[upperIdx];

        // Avoid division by zero / extremely small range.
        final float eps = 1e-12f;
        if (!(maxQ > minQ + eps)) {
            maxQ = minQ + 1e-6f;
        }

        final int levels = 1 << bits; // 2^bits
        final float alpha = (levels - 1) / (maxQ - minQ);

        return new Params(bits, vectorDimensions, sampleCount, confidenceInterval, minQ, maxQ, alpha);
    }

    /**
     * Writes params to a binary sidecar file under {@code indexDir}.
     */
    public static void write(IIOManager ioManager, FileReference indexDir, Params params) throws HyracksDataException {
        if (ioManager == null || indexDir == null || params == null) {
            throw new IllegalArgumentException("ioManager, indexDir, and params must be non-null");
        }
        try {
            FileReference file = indexDir.getChild(FILE_NAME);
            ByteBuffer buf = ByteBuffer.allocate(4 + 2 + 2 + 4 + 4 + 4 + 4 + 4 + 4 + 4);
            buf.order(ByteOrder.LITTLE_ENDIAN);

            buf.putInt(MAGIC);
            buf.putShort(VERSION);
            buf.putShort((short) params.bits);
            buf.putInt(params.vectorDimensions);
            buf.putInt(params.sampleCount);
            buf.putFloat(params.confidenceInterval);
            buf.putFloat(params.minQuantile);
            buf.putFloat(params.maxQuantile);
            buf.putFloat(params.alpha);

            ioManager.overwrite(file, buf.array());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Reads params from a binary sidecar file under {@code indexDir}.
     */
    public static Params read(IIOManager ioManager, FileReference indexDir) throws HyracksDataException {
        if (ioManager == null || indexDir == null) {
            throw new IllegalArgumentException("ioManager and indexDir must be non-null");
        }
        try {
            FileReference file = indexDir.getChild(FILE_NAME);
            byte[] data = ioManager.readAllBytes(file);
            ByteBuffer buf = ByteBuffer.wrap(data);
            buf.order(ByteOrder.LITTLE_ENDIAN);

            int magic = buf.getInt();
            if (magic != MAGIC) {
                throw new IllegalStateException("Invalid quantization sample file (magic mismatch)");
            }
            short version = buf.getShort();
            if (version != VERSION) {
                throw new IllegalStateException("Unsupported quantization sample file version: " + version);
            }

            int bits = Short.toUnsignedInt(buf.getShort());
            int dims = buf.getInt();
            int sampleCount = buf.getInt();
            float ci = buf.getFloat();
            float minQ = buf.getFloat();
            float maxQ = buf.getFloat();
            float alpha = buf.getFloat();

            return new Params(bits, dims, sampleCount, ci, minQ, maxQ, alpha);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Checks for presence of the sidecar file under {@code indexDir}.
     */
    public static boolean exists(IIOManager ioManager, FileReference indexDir) throws HyracksDataException {
        if (ioManager == null || indexDir == null) {
            throw new IllegalArgumentException("ioManager and indexDir must be non-null");
        }
        try {
            return ioManager.exists(indexDir.getChild(FILE_NAME));
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Normalizes a vector to unit length using L2 normalization.
     * Required for cosine similarity before quantization.
     * 
     * @param vector The input vector to normalize
     * @return Normalized vector with L2 norm = 1, or original vector if norm is zero
     */
    public static float[] normalizeVectorL2(float[] vector) {
        if (vector == null) {
            throw new IllegalArgumentException("vector must be non-null");
        }

        // Calculate L2 norm
        double normSquared = 0.0;
        for (float v : vector) {
            normSquared += v * v;
        }
        double norm = Math.sqrt(normSquared);

        // Handle zero vector
        if (norm == 0.0 || Double.isNaN(norm) || Double.isInfinite(norm)) {
            return Arrays.copyOf(vector, vector.length);
        }

        // Normalize to unit length
        float[] normalized = new float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            normalized[i] = (float) (vector[i] / norm);
        }
        return normalized;
    }

    /**
     * Quantizes a vector using optimized scalar quantization with similarity-function awareness.
     * Returns a QuantizedVector containing quantized bytes and corrective multiplier.
     * 
     * <p>
     * For cosine similarity, vectors are normalized to unit length before quantization.
     * For dot product and euclidean, vectors are quantized directly.
     * 
     * @param vector The input vector to quantize (double array)
     * @param params Quantization parameters including bits, minQuantile, maxQuantile, alpha
     * @param similarityFunction The similarity function type (determines normalization and correction)
     * @return QuantizedVector containing quantized bytes, corrective multiplier, and metadata
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

        // Convert double[] to float[] for normalization
        float[] floatVector = new float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            floatVector[i] = (float) vector[i];
        }

        // Normalize for cosine similarity
        boolean isNormalized = false;
        float[] vectorToQuantize = floatVector;
        if (similarityFunction == SimilarityFunction.COSINE) {
            vectorToQuantize = normalizeVectorL2(floatVector);
            isNormalized = true;
        }

        // Convert back to double[] for quantization (quantization methods expect double[])
        double[] vectorForQuantization = new double[vectorToQuantize.length];
        for (int i = 0; i < vectorToQuantize.length; i++) {
            vectorForQuantization[i] = vectorToQuantize[i];
        }

        // Quantize the vector
        final int bits = params.bits;
        final float minQ = params.minQuantile;
        final float maxQ = params.maxQuantile;
        final float alpha = params.alpha;
        final int levels = 1 << bits; // 2^bits

        Object quantizedBytes;
        // Select data type based on bits
        if (bits <= 8) {
            quantizedBytes = quantizeToByte(vectorForQuantization, minQ, maxQ, alpha, levels);
        } else if (bits <= 16) {
            quantizedBytes = quantizeToShort(vectorForQuantization, minQ, maxQ, alpha, levels);
        } else if (bits <= 32) {
            quantizedBytes = quantizeToInt(vectorForQuantization, minQ, maxQ, alpha, levels);
        } else {
            throw new IllegalArgumentException("bits must be <= 32, got " + bits);
        }

        // Calculate corrective multiplier using Lucene's exact formula
        float correctiveMultiplier =
                calculateCorrectiveMultiplier(vectorForQuantization, quantizedBytes, params, similarityFunction);

        return new QuantizedVector(quantizedBytes, correctiveMultiplier, isNormalized, similarityFunction);
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

    /**
     * Quantizes vector to byte array (for bits <= 8).
     * Uses formula: quantizedValue = round((value - minQ) * alpha)
     */
    private static byte[] quantizeToByte(double[] vector, float minQ, float maxQ, float alpha, int levels) {
        byte[] quantized = new byte[vector.length];
        for (int i = 0; i < vector.length; i++) {
            double value = Math.max(minQ, Math.min(maxQ, vector[i]));
            //TODO CALVIN DANI Check, math round
            int quantizedValue = Math.toIntExact(Math.round((value - minQ) * alpha));
            // Clamp to valid range [0, levels-1]
            quantizedValue = Math.max(0, Math.min(levels - 1, quantizedValue));
            quantized[i] = (byte) quantizedValue;
        }
        return quantized;
    }

    /**
     * Quantizes vector to short array (for bits <= 16).
     * Uses formula: quantizedValue = round((value - minQ) * alpha)
     */
    private static short[] quantizeToShort(double[] vector, float minQ, float maxQ, float alpha, int levels) {
        short[] quantized = new short[vector.length];
        for (int i = 0; i < vector.length; i++) {
            double value = Math.max(minQ, Math.min(maxQ, vector[i]));

            //TODO: CALVIN DANI check
            int quantizedValue = Math.toIntExact(Math.round((value - minQ) * alpha));
            // Clamp to valid range [0, levels-1]
            quantizedValue = Math.max(0, Math.min(levels - 1, quantizedValue));
            quantized[i] = (short) quantizedValue;
        }
        return quantized;
    }

    /**
     * Quantizes vector to int array (for bits <= 32).
     * Uses formula: quantizedValue = round((value - minQ) * alpha)
     */
    private static int[] quantizeToInt(double[] vector, float minQ, float maxQ, float alpha, int levels) {
        int[] quantized = new int[vector.length];
        for (int i = 0; i < vector.length; i++) {
            double value = Math.max(minQ, Math.min(maxQ, vector[i]));
            long quantizedValue = Math.round((value - minQ) * alpha);
            // Clamp to valid range [0, levels-1]
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

    private static int clampIndex(int idx, int length) {
        if (idx < 0) {
            return 0;
        }
        if (idx >= length) {
            return length - 1;
        }
        return idx;
    }
}
