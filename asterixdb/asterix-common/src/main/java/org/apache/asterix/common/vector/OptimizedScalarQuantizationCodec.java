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
package org.apache.asterix.common.vector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.util.annotations.AiProvenance;

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
 * For an index whose {@code WITH similarity} is {@code "cosine"}, embedding and query vectors must
 * be L2-normalized to unit length before insert and search. The engine does not re-normalize during
 * quantization.
 */
public final class OptimizedScalarQuantizationCodec {

    private OptimizedScalarQuantizationCodec() {
    }

    /**
     * Similarity function types for vector quantization.
     * Determines similarity metadata on {@link QuantizedVector}.
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
     * Result of vector quantization: per-dimension codes plus similarity metadata.
     */
    public static final class QuantizedVector {
        /** Quantized vector as byte[], short[], or int[] depending on bits */
        public final Object quantizedBytes;
        /** The similarity function used for quantization */
        public final SimilarityFunction similarityFunction;

        public QuantizedVector(Object quantizedBytes, SimilarityFunction similarityFunction) {
            this.quantizedBytes = quantizedBytes;
            this.similarityFunction = similarityFunction;
        }
    }

    /**
     * Converts a distance metric string to SimilarityFunction enum.
     * 
     * @param distanceMetric Distance metric string (e.g., "euclidean", "cosine", "dot")
     * @return Corresponding SimilarityFunction enum, or DOT_PRODUCT as default
     */
    public static SimilarityFunction fromDistanceMetric(String distanceMetric) {
        // Resolve the metric name through VectorSimilarityMetric, the single source of truth for vector
        // metric aliases (it handles null/blank/casing). A null (unrecognized or blank) metric falls back
        // to DOT_PRODUCT, preserving the previous default for unknown metrics.
        VectorSimilarityMetric metric = VectorSimilarityMetric.fromAlias(distanceMetric);
        if (metric == null) {
            return SimilarityFunction.DOT_PRODUCT;
        }
        return switch (metric) {
            case EUCLIDEAN -> SimilarityFunction.EUCLIDEAN;
            case EUCLIDEAN_SQUARED -> SimilarityFunction.EUCLIDEAN_SQUARED;
            case COSINE -> SimilarityFunction.COSINE;
            case DOT -> SimilarityFunction.DOT_PRODUCT;
        };
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
        /** Per-dimension min when FAISS {@code QT_8bit} residual ranges are set; null = use global. */
        public final float[] minPerDim;
        /** Per-dimension max when FAISS {@code QT_8bit} residual ranges are set; null = use global. */
        public final float[] maxPerDim;

        public Params(int bits, int vectorDimensions, int sampleCount, float confidenceInterval, float minQuantile,
                float maxQuantile, float alpha) {
            this(bits, vectorDimensions, sampleCount, confidenceInterval, minQuantile, maxQuantile, alpha, null, null);
        }

        @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Optional per-dim residual SQ ranges")
        public Params(int bits, int vectorDimensions, int sampleCount, float confidenceInterval, float minQuantile,
                float maxQuantile, float alpha, float[] minPerDim, float[] maxPerDim) {
            this.bits = bits;
            this.vectorDimensions = vectorDimensions;
            this.sampleCount = sampleCount;
            this.confidenceInterval = confidenceInterval;
            this.minQuantile = minQuantile;
            this.maxQuantile = maxQuantile;
            this.alpha = alpha;
            this.minPerDim = minPerDim == null ? null : minPerDim.clone();
            this.maxPerDim = maxPerDim == null ? null : maxPerDim.clone();
        }

        @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Optional per-dim residual SQ ranges")
        public boolean hasPerDimRanges() {
            return minPerDim != null && maxPerDim != null && minPerDim.length == vectorDimensions
                    && maxPerDim.length == vectorDimensions;
        }
    }

    /**
     * Quantizes a vector using optimized scalar quantization with similarity-function awareness.
     *
     * <p>
     * Vectors are quantized as provided. For cosine indexes, callers must supply L2-normalized unit vectors.
     *
     * <p>
     * Encoding uses {@code levels = 2^bits} and delegates to {@link #quantizeToByte},
     * {@link #quantizeToShort}, or {@link #quantizeToInt} depending on {@code bits}. See those methods
     * for the per-dimension encode formula and {@link #dequantizeToDoubleArray} for the inverse.
     *
     * @param vector The input vector to quantize (double array)
     * @param params Quantization parameters including bits, minQuantile, maxQuantile, alpha
     * @param similarityFunction The similarity function type (recorded on the result)
     * @return QuantizedVector containing quantized bytes and metadata
     * @throws HyracksDataException if vector is null or params are invalid
     */
    public static QuantizedVector quantizeVector(double[] vector, Params params, SimilarityFunction similarityFunction)
            throws HyracksDataException {
        if (vector == null) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "A null vector reached the quantizer");
        }
        if (params == null) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "Null quantization params reached the quantizer");
        }
        if (vector.length != params.vectorDimensions) {
            throw new RuntimeDataException(ErrorCode.VECTOR_DIMENSION_MISMATCH, params.vectorDimensions, vector.length);
        }

        final int bits = params.bits;
        final int levels = 1 << bits; // 2^bits

        Object quantizedBytes;
        if (bits <= 8) {
            quantizedBytes = quantizeToByte(vector, params, levels);
        } else if (bits <= 16) {
            quantizedBytes = quantizeToShort(vector, params, levels);
        } else if (bits <= 32) {
            quantizedBytes = quantizeToInt(vector, params, levels);
        } else {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                    "Unsupported quantization bit width " + bits + "; the maximum is 32");
        }

        return new QuantizedVector(quantizedBytes, similarityFunction);
    }

    /**
     * SQ-encodes the residual {@code x − c} with the same clamp/alpha as {@link #quantizeVector}.
     * Used for DOT quantized indexes so field 3 stores residual codes rather than globally clamped
     * raw coordinates (which collapse magnitude). {@code centroid == null} falls back to encoding
     * {@code x} unchanged.
     */
    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "DOT residual SQ8: encode x-c with existing clamp/alpha")
    public static QuantizedVector quantizeResidual(double[] vector, double[] centroid, Params params,
            SimilarityFunction similarityFunction) throws HyracksDataException {
        if (centroid == null) {
            return quantizeVector(vector, params, similarityFunction);
        }
        if (vector == null) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "A null vector reached the residual quantizer");
        }
        if (vector.length != centroid.length) {
            throw new RuntimeDataException(ErrorCode.VECTOR_DIMENSION_MISMATCH, centroid.length, vector.length);
        }
        return quantizeVector(subtract(vector, centroid), params, similarityFunction);
    }

    /** Per-dimension {@code r[i] = x[i] − c[i]}. */
    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "DOT residual subtract")
    public static double[] subtract(double[] vector, double[] centroid) {
        double[] residual = new double[vector.length];
        for (int i = 0; i < vector.length; i++) {
            residual[i] = vector[i] - centroid[i];
        }
        return residual;
    }

    /**
     * Quantile clip bounds and OSQ {@code alpha} from a list of scalar samples, matching
     * {@code QuantizationConstantsAggregate} (confidence-interval tails, then
     * {@code alpha = (2^bits − 1) / (maxQ − minQ)}). Used to retrain minQ/maxQ on DOT residuals
     * {@code x − c} after k-means assignment.
     */
    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Residual quantile params from train-list x-c scalars")
    public static Params computeParamsFromScalars(List<Double> values, int bits, int vectorDimensions,
            float confidenceInterval) throws HyracksDataException {
        if (values == null || values.isEmpty()) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                    "Cannot compute quantization params from an empty scalar sample");
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);

        float half = (1.0f - confidenceInterval) / 2.0f;
        int totalCount = sorted.size();
        int lowerIdx = (int) Math.floor(half * (totalCount - 1));
        int upperIdx = (int) Math.ceil((1.0f - half) * (totalCount - 1));
        lowerIdx = Math.max(0, Math.min(lowerIdx, totalCount - 1));
        upperIdx = Math.max(0, Math.min(upperIdx, totalCount - 1));

        float minQ = sorted.get(lowerIdx).floatValue();
        float maxQ = sorted.get(upperIdx).floatValue();
        double eps = 1e-12;
        if (maxQ <= minQ + eps) {
            maxQ = minQ + 1e-6f;
        }
        int levels = 1 << bits;
        float alpha = (levels - 1) / (maxQ - minQ);
        return new Params(bits, vectorDimensions, totalCount, confidenceInterval, minQ, maxQ, alpha);
    }

    /**
     * FAISS {@code QT_8bit} {@code RS_minmax} ranges: one {@code [vmin, vmax]} per dimension from the
     * training extrema, no confidence-interval drop. Global minQ/maxQ/alpha remain the envelope so
     * callers that only read those scalars still have a defined codebook.
     */
    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Per-dim residual minmax SQ params")
    public static Params computeParamsFromPerDimMinMax(float[] minPerDim, float[] maxPerDim, int bits,
            int vectorDimensions, int sampleCount, float confidenceInterval) throws HyracksDataException {
        if (minPerDim == null || maxPerDim == null || minPerDim.length != vectorDimensions
                || maxPerDim.length != vectorDimensions) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                    "Per-dimension residual ranges must match the vector dimension");
        }
        float[] mins = minPerDim.clone();
        float[] maxs = maxPerDim.clone();
        float envMin = Float.POSITIVE_INFINITY;
        float envMax = Float.NEGATIVE_INFINITY;
        double eps = 1e-12;
        for (int i = 0; i < vectorDimensions; i++) {
            if (maxs[i] <= mins[i] + eps) {
                maxs[i] = mins[i] + 1e-6f;
            }
            envMin = Math.min(envMin, mins[i]);
            envMax = Math.max(envMax, maxs[i]);
        }
        int levels = 1 << bits;
        float alpha = (levels - 1) / (envMax - envMin);
        return new Params(bits, vectorDimensions, sampleCount, confidenceInterval, envMin, envMax, alpha, mins, maxs);
    }

    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Build codec Params from persisted VTreeQuantizationParams")
    public static Params fromVTreeParams(VTreeQuantizationParams params, int vectorDimensions) {
        return new Params(params.bits(), vectorDimensions, params.sampleCount(), params.confidenceInterval(),
                params.minQuantile(), params.maxQuantile(), params.alpha(), params.minPerDim(), params.maxPerDim());
    }

    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Per-dim or global SQ clamp bounds")
    static float dimMin(Params params, int i) {
        return params.hasPerDimRanges() ? params.minPerDim[i] : params.minQuantile;
    }

    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Per-dim or global SQ clamp bounds")
    static float dimMax(Params params, int i) {
        return params.hasPerDimRanges() ? params.maxPerDim[i] : params.maxQuantile;
    }

    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Per-dim or global SQ alpha")
    static float dimAlpha(Params params, int i, int levels) {
        if (!params.hasPerDimRanges()) {
            return params.alpha;
        }
        float span = params.maxPerDim[i] - params.minPerDim[i];
        if (span <= 1e-12f) {
            span = 1e-6f;
        }
        return (levels - 1) / span;
    }

    private static long encodeScalar(double x, float minQ, float maxQ, float alpha, int levels) {
        double value = Math.max(minQ, Math.min(maxQ, x));
        long quantizedValue = Math.round((value - minQ) * alpha);
        return Math.max(0, Math.min(levels - 1, quantizedValue));
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
    private static byte[] quantizeToByte(double[] vector, Params params, int levels) {
        byte[] quantized = new byte[vector.length];
        for (int i = 0; i < vector.length; i++) {
            quantized[i] = (byte) encodeScalar(vector[i], dimMin(params, i), dimMax(params, i),
                    dimAlpha(params, i, levels), levels);
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
    private static short[] quantizeToShort(double[] vector, Params params, int levels) {
        short[] quantized = new short[vector.length];
        for (int i = 0; i < vector.length; i++) {
            quantized[i] = (short) encodeScalar(vector[i], dimMin(params, i), dimMax(params, i),
                    dimAlpha(params, i, levels), levels);
        }
        return quantized;
    }

    /**
     * Encodes a vector to {@code int[]} when {@code 16 < bits <= 32}.
     *
     * <p>
     * Same encode formula as {@link #quantizeToByte}. Caller must pass {@code levels == 1 << bits}.
     * Inverse: {@link #dequantizeToDoubleArray}. All three encoders round into a {@code long}, clamp to
     * {@code [0, levels - 1]}, then narrow to the target type; {@code long} is required here because for
     * {@code bits} near 32 the pre-clamp code {@code (value - minQ) * alpha} can exceed
     * {@code Integer.MAX_VALUE}.
     *
     * @see #quantizeToByte
     * @see #quantizeToShort
     */
    private static int[] quantizeToInt(double[] vector, Params params, int levels) {
        int[] quantized = new int[vector.length];
        for (int i = 0; i < vector.length; i++) {
            quantized[i] = (int) encodeScalar(vector[i], dimMin(params, i), dimMax(params, i),
                    dimAlpha(params, i, levels), levels);
        }
        return quantized;
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
     * @throws HyracksDataException if inputs are null or array length doesn't
     *                              match params
     */
    public static double[] dequantizeToDoubleArray(Object quantizedBytes, Params params) throws HyracksDataException {
        if (quantizedBytes == null) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "A null quantized vector reached the dequantizer");
        }
        if (params == null) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "Null quantization params reached the dequantizer");
        }

        final int dims = params.vectorDimensions;
        final int bits = params.bits;
        double[] result = new double[dims];

        // Determine data type based on bits and convert to double
        if (bits <= 8) {
            // byte[] - treat as unsigned
            if (!(quantizedBytes instanceof byte[])) {
                throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "Expected a byte[] quantized vector for " + bits
                        + " bits, but got a " + quantizedBytes.getClass().getName());
            }
            byte[] bytes = (byte[]) quantizedBytes;
            if (bytes.length != dims) {
                throw new RuntimeDataException(ErrorCode.VECTOR_DIMENSION_MISMATCH, dims, bytes.length);
            }
            for (int i = 0; i < dims; i++) {
                result[i] = ((double) (bytes[i] & 0xFF)) / dimAlpha(params, i, 1 << bits) + dimMin(params, i);
            }
        } else if (bits <= 16) {
            // short[] - treat as unsigned
            if (!(quantizedBytes instanceof short[])) {
                throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "Expected a short[] quantized vector for "
                        + bits + " bits, but got a " + quantizedBytes.getClass().getName());
            }
            short[] shorts = (short[]) quantizedBytes;
            if (shorts.length != dims) {
                throw new RuntimeDataException(ErrorCode.VECTOR_DIMENSION_MISMATCH, dims, shorts.length);
            }
            for (int i = 0; i < dims; i++) {
                result[i] = ((double) (shorts[i] & 0xFFFF)) / dimAlpha(params, i, 1 << bits) + dimMin(params, i);
            }
        } else if (bits <= 32) {
            // int[] - treat as unsigned
            if (!(quantizedBytes instanceof int[])) {
                throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "Expected an int[] quantized vector for " + bits
                        + " bits, but got a " + quantizedBytes.getClass().getName());
            }
            int[] ints = (int[]) quantizedBytes;
            if (ints.length != dims) {
                throw new RuntimeDataException(ErrorCode.VECTOR_DIMENSION_MISMATCH, dims, ints.length);
            }
            for (int i = 0; i < dims; i++) {
                result[i] = ((double) (ints[i] & 0xFFFFFFFFL)) / dimAlpha(params, i, 1 << bits) + dimMin(params, i);
            }
        } else {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                    "Unsupported quantization bit width " + bits + "; the maximum is 32");
        }

        return result;
    }
}
