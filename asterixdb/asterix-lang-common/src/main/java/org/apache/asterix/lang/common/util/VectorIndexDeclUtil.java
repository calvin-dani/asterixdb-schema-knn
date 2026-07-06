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
    public static final String VECTOR_INDEX_PARAMETER_STRUCTURE_BUILD = "structure_build";
    public static final String VECTOR_INDEX_DEFAULT_STRUCTURE_BUILD = "bottom_up";
    public static final String VECTOR_INDEX_PARAMETER_EPSILON = "epsilon";
    public static final String VECTOR_INDEX_DEFAULT_QUANTIZATION = "SQ8";
    /** Default for level-wise centroid search and ANN search predicate (matches VectorSearchPredicate). */
    public static final double VECTOR_INDEX_DEFAULT_TRAIN_LIST = 0.1;
    public static final double VECTOR_INDEX_DEFAULT_EPSILON = 0.25;

    /**
     * Canonical names for {@code similarity}, aligned with
     * {@link org.apache.asterix.runtime.utils.VectorDistanceFunctionFactory} (after {@code toLowerCase()}).
     */
    private static final Set<String> ALLOWED_VECTOR_DISTANCE_METRICS =
            Set.of("euclidean", "l2", "euclidean_squared", "l2_squared", "manhattan_distance", "cosine", "dot");

    private static final Set<String> ALLOWED_VECTOR_INDEX_QUANTIZATION = Set.of("SQ4", "SQ8");

    /**
     * Only these keys may appear in the vector index {@code WITH} clause (unknown keys are a compile error).
     */
    private static final Set<String> ALLOWED_VECTOR_INDEX_WITH_FIELDS = Set.of(VECTOR_INDEX_PARAMETER_DIMENSION,
            VECTOR_INDEX_PARAMETER_SIMILARITY, VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION,
            VECTOR_INDEX_PARAMETER_QUANTIZATION, VECTOR_INDEX_PARAMETER_EPSILON, VECTOR_INDEX_PARAMETER_NUM_K,
            VECTOR_INDEX_PARAMETER_STRUCTURE_BUILD);

    private static final Set<String> ALLOWED_STRUCTURE_BUILD_VALUES = Set.of("bottom_up", "spann");

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
        validateStructureBuild(node);

        return node;
    }

    private static void validateWithClauseFieldNames(AdmObjectNode node) throws CompilationException {
        for (String name : node.getFieldNames()) {
            if (!ALLOWED_VECTOR_INDEX_WITH_FIELDS.contains(name)) {
                throw new CompilationException("Failed to create vector index. Unknown field `" + name
                        + "` in WITH clause. Allowed fields: dimension, similarity, train_list_fraction, quantization, epsilon, num_clusters, structure_build");
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

    private static void validateStructureBuild(AdmObjectNode node) throws CompilationException {
        IAdmNode sbNode = node.get(VECTOR_INDEX_PARAMETER_STRUCTURE_BUILD);
        if (sbNode == null) {
            node.set(VECTOR_INDEX_PARAMETER_STRUCTURE_BUILD, new AdmStringNode(VECTOR_INDEX_DEFAULT_STRUCTURE_BUILD));
            return;
        }
        switch (sbNode.getType()) {
            case STRING:
                String structureBuild = ((AdmStringNode) sbNode).get();
                if (structureBuild == null || structureBuild.trim().isEmpty()) {
                    node.set(VECTOR_INDEX_PARAMETER_STRUCTURE_BUILD,
                            new AdmStringNode(VECTOR_INDEX_DEFAULT_STRUCTURE_BUILD));
                    return;
                }
                String normalized = structureBuild.trim().toLowerCase(Locale.ROOT);
                if (!ALLOWED_STRUCTURE_BUILD_VALUES.contains(normalized)) {
                    throw new CompilationException(
                            "Failed to create vector index. Invalid `structure_build` parameter value. Allowed values: bottom_up and spann");
                }
                node.remove(VECTOR_INDEX_PARAMETER_STRUCTURE_BUILD);
                node.set(VECTOR_INDEX_PARAMETER_STRUCTURE_BUILD, new AdmStringNode(normalized));
                break;
            default:
                throw new CompilationException(
                        "Failed to create vector index. Invalid `structure_build` parameter value. Allowed values: bottom_up and spann");
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
