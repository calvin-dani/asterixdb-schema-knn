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
package org.apache.asterix.lang.common.util;

import java.util.Locale;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Validates the {@code WITH} clause of a {@code CREATE INDEX ... TYPE VTREE} statement and
 * produces the canonical {@link AdmObjectNode} stored on the {@code CreateIndexStatement}.
 */
public class VectorIndexDeclUtil {

    public static final String VECTOR_INDEX_PARAMETER_DIMENSION = "dimension";
    public static final String VECTOR_INDEX_PARAMETER_QUANTIZATION = "quantization";
    public static final String VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION = "train_list_fraction";
    public static final String VECTOR_INDEX_PARAMETER_SIMILARITY = "similarity";
    public static final String VECTOR_INDEX_PARAMETER_NUM_K = "num_clusters";
    /**
     * Clustering/TRAINING process used to build the static structure (Job 2): {@code "bottom-up"}
     * (default, hierarchical k-means++ trained leaves-first) or {@code "top-down"} (SPANN-style
     * SelectHead/BKT recursive splitting). This selects the <em>training</em> algorithm only — both
     * modes must still emit {@code (treeLevel, centroidId, parentClusterId, embedding)} tuples
     * bottom-up (leaves first, root last, BFS-from-root centroid ids) as required by the storage
     * layer's {@code VTreeStaticStructureBuilder}, whose page emission is always bottom-up.
     */
    public static final String VECTOR_INDEX_PARAMETER_CREATION_MODE = "creation_mode";
    public static final String VECTOR_INDEX_CREATION_MODE_BOTTOM_UP = "bottom-up";
    public static final String VECTOR_INDEX_CREATION_MODE_TOP_DOWN = "top-down";
    public static final String VECTOR_INDEX_DEFAULT_CREATION_MODE = VECTOR_INDEX_CREATION_MODE_BOTTOM_UP;
    public static final String VECTOR_INDEX_PARAMETER_EPSILON = "epsilon";
    /**
     * Cross-pollination factor: each record is written into the M closest leaf centroids at bulk-load,
     * not just the closest one. M=1 reproduces the legacy (no cross-pollination) behavior.
     */
    public static final String VECTOR_INDEX_PARAMETER_CROSS_POLLINATION_M = "cross_pollination_m";
    /**
     * RNG (relative neighborhood graph) factor for SPTAG-style cross-pollination diversity. A candidate
     * centroid {@code c_i} is rejected iff some already-accepted replica {@code r} satisfies
     * {@code rng_factor * dist(c_i, r) < dist(x, c_i)}. The canonical SPTAG rule is {@code 1.0}; larger
     * values loosen the rule (more replicas survive), smaller values tighten. A non-finite value
     * effectively disables RNG, degrading to the pure top-{@code cross_pollination_m} slice.
     */
    public static final String VECTOR_INDEX_PARAMETER_RNG_FACTOR = "rng_factor";
    public static final String VECTOR_INDEX_DEFAULT_QUANTIZATION = "SQ8";
    /** Default for level-wise centroid search and ANN search predicate (matches VectorSearchPredicate). */
    public static final double VECTOR_INDEX_DEFAULT_TRAIN_LIST = 0.1;
    public static final double VECTOR_INDEX_DEFAULT_EPSILON = 0.25;
    public static final long VECTOR_INDEX_DEFAULT_CROSS_POLLINATION_M = 1L;
    /** Sanity cap on cross_pollination_m: anything larger almost certainly indicates a user error. */
    public static final long VECTOR_INDEX_MAX_CROSS_POLLINATION_M = 1024L;
    /** Default RNG factor: canonical SPTAG rule. */
    public static final double VECTOR_INDEX_DEFAULT_RNG_FACTOR = 1.0;

    /**
     * Canonical names for {@code similarity}, aligned with
     * {@link org.apache.asterix.runtime.utils.VectorDistanceFunctionFactory} (after {@code toLowerCase()}).
     */
    private static final Set<String> ALLOWED_VECTOR_DISTANCE_METRICS =
            Set.of("euclidean", "l2", "euclidean_squared", "l2_squared", "cosine", "dot");

    private static final Set<String> ALLOWED_VECTOR_INDEX_QUANTIZATION = Set.of("SQ4", "SQ8");

    /**
     * Only these keys may appear in the vector index {@code WITH} clause (unknown keys are a compile error).
     */
    private static final Set<String> ALLOWED_VECTOR_INDEX_WITH_FIELDS =
            Set.of(VECTOR_INDEX_PARAMETER_DIMENSION, VECTOR_INDEX_PARAMETER_SIMILARITY,
                    VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION, VECTOR_INDEX_PARAMETER_QUANTIZATION,
                    VECTOR_INDEX_PARAMETER_EPSILON, VECTOR_INDEX_PARAMETER_NUM_K, VECTOR_INDEX_PARAMETER_CREATION_MODE,
                    VECTOR_INDEX_PARAMETER_CROSS_POLLINATION_M, VECTOR_INDEX_PARAMETER_RNG_FACTOR);

    private static final Set<String> ALLOWED_CREATION_MODE_VALUES =
            Set.of(VECTOR_INDEX_CREATION_MODE_BOTTOM_UP, VECTOR_INDEX_CREATION_MODE_TOP_DOWN);

    private VectorIndexDeclUtil() {
    }

    public static AdmObjectNode validateAndGetWithObjectNode(RecordConstructor withRecord) throws CompilationException {
        if (withRecord == null) {
            return null;
        }
        return validateAndGetWithObjectNode(withRecord, withRecord.getSourceLocation());
    }

    public static AdmObjectNode validateAndGetWithObjectNode(RecordConstructor withRecord, SourceLocation sourceLoc)
            throws CompilationException {
        if (withRecord == null) {
            return null;
        }
        final AdmObjectNode node = ExpressionUtils.toNode(withRecord);

        validateWithClauseFieldNames(node);
        validateDimension(node);
        validateTrainList(node);
        validateSimilarity(node);
        validateQuantization(node);
        validateEpsilon(node);
        validateNumClusters(node);
        validateCreationMode(node);
        validateCrossPollinationM(node);
        validateRngFactor(node);

        return node;
    }

    private static void validateWithClauseFieldNames(AdmObjectNode node) throws CompilationException {
        for (String name : node.getFieldNames()) {
            if (!ALLOWED_VECTOR_INDEX_WITH_FIELDS.contains(name)) {
                throw new CompilationException("Failed to create vector index. Unknown field `" + name
                        + "` in WITH clause. Allowed fields: dimension, similarity, train_list_fraction, quantization, "
                        + "epsilon, num_clusters, creation_mode, cross_pollination_m, rng_factor");
            }
        }
    }

    private static void validateDimension(AdmObjectNode node) throws CompilationException {
        IAdmNode dimNode = node.get(VECTOR_INDEX_PARAMETER_DIMENSION);
        if (dimNode == null) {
            throw new CompilationException(
                    "Failed to create vector index. Missing required parameter `dimension` in WITH clause.");
        }
        long dimValue;
        switch (dimNode.getType()) {
            case BIGINT:
                long lv = ((AdmBigIntNode) dimNode).get();
                if (lv <= 0 || lv > Integer.MAX_VALUE) {
                    throw new CompilationException(
                            "Failed to create vector index. Invalid `dimension` parameter value. It must be an integer greater than 0");
                }
                dimValue = lv;
                break;
            default:
                throw new CompilationException(
                        "Failed to create vector index. Invalid `dimension` parameter value. It must be an integer greater than 0");
        }
        node.remove(VECTOR_INDEX_PARAMETER_DIMENSION);
        node.set(VECTOR_INDEX_PARAMETER_DIMENSION, new AdmBigIntNode(dimValue));
    }

    private static void validateSimilarity(AdmObjectNode node) throws CompilationException {
        IAdmNode simNode = node.get(VECTOR_INDEX_PARAMETER_SIMILARITY);
        if (simNode == null) {
            throw new CompilationException(
                    "Failed to create vector index. Missing required parameter `similarity` in WITH clause.");
        }
        switch (simNode.getType()) {
            case STRING:
                String similarity = ((AdmStringNode) simNode).get();
                if (similarity == null || similarity.trim().isEmpty()) {
                    throw new CompilationException(
                            "Failed to create vector index. Invalid `similarity` parameter value. Allowed values: EUCLIDEAN, L2, EUCLIDEAN_SQUARED, L2_SQUARED, COSINE and DOT");
                }
                String normalizedSimilarity = similarity.trim().toLowerCase(Locale.ROOT);
                if (!ALLOWED_VECTOR_DISTANCE_METRICS.contains(normalizedSimilarity)) {
                    throw new CompilationException(
                            "Failed to create vector index. Invalid `similarity` parameter value. Allowed values: EUCLIDEAN, L2, EUCLIDEAN_SQUARED, L2_SQUARED, COSINE and DOT");
                }
                break;
            default:
                throw new CompilationException(
                        "Failed to create vector index. Invalid `similarity` parameter value. Allowed values: EUCLIDEAN, L2, EUCLIDEAN_SQUARED, L2_SQUARED, COSINE and DOT");
        }
    }

    private static void validateQuantization(AdmObjectNode node) throws CompilationException {
        IAdmNode qNode = node.get(VECTOR_INDEX_PARAMETER_QUANTIZATION);
        if (qNode == null) {
            node.set(VECTOR_INDEX_PARAMETER_QUANTIZATION, new AdmStringNode(VECTOR_INDEX_DEFAULT_QUANTIZATION));
            return;
        }
        switch (qNode.getType()) {
            case STRING:
                String quantization = ((AdmStringNode) qNode).get();
                String qNorm = quantization.trim().toUpperCase(Locale.ROOT);
                if (!ALLOWED_VECTOR_INDEX_QUANTIZATION.contains(qNorm)) {
                    throw new CompilationException(
                            "Failed to create vector index. Invalid `quantization` parameter value. Allowed values: SQ4 and SQ8");
                } else {
                    node.set(VECTOR_INDEX_PARAMETER_QUANTIZATION, new AdmStringNode(qNorm));
                }
                break;
            default:
                throw new CompilationException(
                        "Failed to create vector index. Invalid `quantization` parameter value. Allowed values: SQ4 and SQ8");
        }
    }

    /**
     * Training list size is specified only via {@code train_list_fraction} (with ANALYZE/cardinality at build time).
     */
    private static void validateTrainList(AdmObjectNode node) throws CompilationException {
        IAdmNode fn = node.get(VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION);
        if (fn == null) {
            node.set(VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION, new AdmDoubleNode(VECTOR_INDEX_DEFAULT_TRAIN_LIST));
            return;
        }
        double trainListFractionValue = parseDoubleOrBigInt(fn,
                "Failed to create vector index. Invalid `train_list_fraction` parameter value. It must be in the range of (0,1]");
        if (trainListFractionValue <= 0 || trainListFractionValue > 1) {
            throw new CompilationException(
                    "Failed to create vector index. Invalid `train_list_fraction` parameter value. It must be in the range of (0,1]");
        }
        if (fn.getType() == ATypeTag.BIGINT) {
            node.remove(VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION);
            node.set(VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION, new AdmDoubleNode(trainListFractionValue));
        }
    }

    private static void validateEpsilon(AdmObjectNode node) throws CompilationException {
        IAdmNode epsNode = node.get(VECTOR_INDEX_PARAMETER_EPSILON);
        if (epsNode == null) {
            node.set(VECTOR_INDEX_PARAMETER_EPSILON, new AdmDoubleNode(VECTOR_INDEX_DEFAULT_EPSILON));
            return;
        }
        double v = parseDoubleOrBigInt(epsNode,
                "Failed to create vector index. Invalid `epsilon` parameter value. It must be in the range of [0,1]");
        if (v < 0 || v > 1) {
            throw new CompilationException(
                    "Failed to create vector index. Invalid `epsilon` parameter value. It must be in the range of [0,1]");
        }
        if (epsNode.getType() == ATypeTag.BIGINT) {
            node.remove(VECTOR_INDEX_PARAMETER_EPSILON);
            node.set(VECTOR_INDEX_PARAMETER_EPSILON, new AdmDoubleNode(v));
        }
    }

    /**
     * Validates {@code creation_mode}: which clustering/training process builds the static structure
     * (Job 2). Not to be confused with the storage layer's page emission, which is always bottom-up
     * (leaves at low page ids, root highest) regardless of this setting. Missing or blank defaults to
     * {@code "bottom-up"}; the stored value is normalized to lowercase.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Renamed from structure_build (bottom_up/spann) to creation_mode (bottom-up/top-down)")
    private static void validateCreationMode(AdmObjectNode node) throws CompilationException {
        IAdmNode cmNode = node.get(VECTOR_INDEX_PARAMETER_CREATION_MODE);
        if (cmNode == null) {
            node.set(VECTOR_INDEX_PARAMETER_CREATION_MODE, new AdmStringNode(VECTOR_INDEX_DEFAULT_CREATION_MODE));
            return;
        }
        switch (cmNode.getType()) {
            case STRING:
                String creationMode = ((AdmStringNode) cmNode).get();
                if (creationMode == null || creationMode.trim().isEmpty()) {
                    node.set(VECTOR_INDEX_PARAMETER_CREATION_MODE,
                            new AdmStringNode(VECTOR_INDEX_DEFAULT_CREATION_MODE));
                    return;
                }
                String normalized = creationMode.trim().toLowerCase(Locale.ROOT);
                if (!ALLOWED_CREATION_MODE_VALUES.contains(normalized)) {
                    throw new CompilationException("Failed to create vector index. Invalid `creation_mode` parameter "
                            + "value. Allowed values: bottom-up and top-down");
                }
                node.remove(VECTOR_INDEX_PARAMETER_CREATION_MODE);
                node.set(VECTOR_INDEX_PARAMETER_CREATION_MODE, new AdmStringNode(normalized));
                break;
            default:
                throw new CompilationException("Failed to create vector index. Invalid `creation_mode` parameter "
                        + "value. Allowed values: bottom-up and top-down");
        }
    }

    /**
     * Validates {@code cross_pollination_m}: each record is written into the M closest leaf centroids at
     * bulk-load (M=1 means no cross-pollination, matching legacy behavior). Must be a positive integer;
     * capped at {@link #VECTOR_INDEX_MAX_CROSS_POLLINATION_M} as a sanity check.
     */
    private static void validateCrossPollinationM(AdmObjectNode node) throws CompilationException {
        IAdmNode mNode = node.get(VECTOR_INDEX_PARAMETER_CROSS_POLLINATION_M);
        if (mNode == null) {
            node.set(VECTOR_INDEX_PARAMETER_CROSS_POLLINATION_M,
                    new AdmBigIntNode(VECTOR_INDEX_DEFAULT_CROSS_POLLINATION_M));
            return;
        }
        if (mNode.getType() != ATypeTag.BIGINT) {
            throw new CompilationException("Failed to create vector index. Invalid `cross_pollination_m` parameter "
                    + "value. It must be an integer in [1, " + VECTOR_INDEX_MAX_CROSS_POLLINATION_M + "]");
        }
        long v = ((AdmBigIntNode) mNode).get();
        if (v < 1 || v > VECTOR_INDEX_MAX_CROSS_POLLINATION_M) {
            throw new CompilationException("Failed to create vector index. Invalid `cross_pollination_m` parameter "
                    + "value. It must be an integer in [1, " + VECTOR_INDEX_MAX_CROSS_POLLINATION_M + "]");
        }
    }

    /**
     * Validates {@code num_clusters}: the target number of leaf clusters per storage partition. Optional —
     * when absent the builder derives it as sqrt(cardinality / numPartitions), so no default is injected
     * here. When present it must be a positive BIGINT that fits in an int (it is later read via
     * {@code getOptionalInt}); a wrong-typed or non-positive value previously slipped past DDL and either
     * threw a ClassCastException during metadata serialization or was silently dropped.
     */
    private static void validateNumClusters(AdmObjectNode node) throws CompilationException {
        IAdmNode kNode = node.get(VECTOR_INDEX_PARAMETER_NUM_K);
        if (kNode == null) {
            return;
        }
        if (kNode.getType() != ATypeTag.BIGINT) {
            throw new CompilationException("Failed to create vector index. Invalid `num_clusters` parameter value. "
                    + "It must be an integer greater than 0.");
        }
        long v = ((AdmBigIntNode) kNode).get();
        if (v < 1 || v > Integer.MAX_VALUE) {
            throw new CompilationException("Failed to create vector index. Invalid `num_clusters` parameter value. "
                    + "It must be an integer greater than 0.");
        }
    }

    /**
     * Validates {@code rng_factor}: SPTAG-style RNG acceptance multiplier applied at bulk-load after the
     * eps-filtered candidate list (see {@code RngAcceptanceFilter}). Must be a positive finite double. A
     * BIGINT literal is coerced to DOUBLE. To effectively disable RNG, use a value much larger than the
     * maximum expected centroid-to-centroid distance.
     */
    private static void validateRngFactor(AdmObjectNode node) throws CompilationException {
        IAdmNode rngNode = node.get(VECTOR_INDEX_PARAMETER_RNG_FACTOR);
        if (rngNode == null) {
            node.set(VECTOR_INDEX_PARAMETER_RNG_FACTOR, new AdmDoubleNode(VECTOR_INDEX_DEFAULT_RNG_FACTOR));
            return;
        }
        double v = parseDoubleOrBigInt(rngNode, "Failed to create vector index. Invalid `rng_factor` parameter "
                + "value. It must be a positive finite number.");
        if (!(v > 0.0) || !Double.isFinite(v)) {
            throw new CompilationException("Failed to create vector index. Invalid `rng_factor` parameter value. "
                    + "It must be a positive finite number.");
        }
        if (rngNode.getType() == ATypeTag.BIGINT) {
            node.remove(VECTOR_INDEX_PARAMETER_RNG_FACTOR);
            node.set(VECTOR_INDEX_PARAMETER_RNG_FACTOR, new AdmDoubleNode(v));
        }
    }

    private static double parseDoubleOrBigInt(IAdmNode n, String errorMsg) throws CompilationException {
        switch (n.getType()) {
            case DOUBLE:
                return ((AdmDoubleNode) n).get();
            case BIGINT:
                return ((AdmBigIntNode) n).get();
            default:
                throw new CompilationException(errorMsg);
        }
    }
}
