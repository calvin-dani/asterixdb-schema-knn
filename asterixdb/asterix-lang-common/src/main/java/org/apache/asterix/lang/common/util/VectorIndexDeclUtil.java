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
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class VectorIndexDeclUtil {
    /* ***********************************************
     * Vector Index Policy Parameters
     * ***********************************************
     */
    public static final String VECTOR_INDEX_PARAMETER_DIMENSION = "dimension";
    public static final String VECTOR_INDEX_PARAMETER_QUANTIZATION = "quantization";
    public static final String VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION = "train_list_fraction";
    public static final String VECTOR_INDEX_PARAMETER_SIMILARITY = "similarity";
    public static final String VECTOR_INDEX_PARAMETER_NUM_K = "num_clusters";
    public static final String VECTOR_INDEX_PARAMETER_EPSILON = "epsilon";
    public static final String VECTOR_INDEX_DEFAULT_QUANTIZATION = "SQ8";
    /** Default for level-wise centroid search and ANN search predicate (matches VectorSearchPredicate). */
    public static final double VECTOR_INDEX_DEFAULT_EPSILON = 0.25;

    /**
     * Canonical names for {@code similarity}, aligned with
     * {@link org.apache.asterix.runtime.utils.VectorDistanceFunctionFactory} (after {@code toLowerCase()}).
     */
    private static final Set<String> ALLOWED_VECTOR_DISTANCE_METRICS = Set.of("euclidean", "l2", "euclidean_squared",
            "l2_squared", "manhattan_distance", "cosine_similarity", "dot");

    private static final Set<String> ALLOWED_VECTOR_INDEX_QUANTIZATION = Set.of("SQ4", "SQ8");

    private static final String DISALLOWED_TRAIN_LIST_NUMBER = "train_list_number";
    private static final String DISALLOWED_TRAIN_LIST_PERCENTAGE = "train_list_percentage";

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

        validateDimension(node, sourceLoc);
        validateTrainList(node);

        String similarity = node.getOptionalString(VECTOR_INDEX_PARAMETER_SIMILARITY, null);
        if (similarity == null || similarity.trim().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_SIMILARITY_REQUIRED);
        }

        String normalizedSimilarity = similarity.trim().toLowerCase(Locale.ROOT);
        if (!ALLOWED_VECTOR_DISTANCE_METRICS.contains(normalizedSimilarity)) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_SIMILARITY_UNSUPPORTED,
                    similarity.trim());
        }

        String quantization = node.getOptionalString(VECTOR_INDEX_PARAMETER_QUANTIZATION, null);
        if (quantization == null) {
            node.set(VECTOR_INDEX_PARAMETER_QUANTIZATION, new AdmStringNode(VECTOR_INDEX_DEFAULT_QUANTIZATION));
        } else {
            String qNorm = quantization.trim().toUpperCase(Locale.ROOT);
            if (qNorm.isEmpty()) {
                node.set(VECTOR_INDEX_PARAMETER_QUANTIZATION, new AdmStringNode(VECTOR_INDEX_DEFAULT_QUANTIZATION));
            } else if (!ALLOWED_VECTOR_INDEX_QUANTIZATION.contains(qNorm)) {
                throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_QUANTIZATION_UNSUPPORTED,
                        quantization.trim());
            } else {
                node.set(VECTOR_INDEX_PARAMETER_QUANTIZATION, new AdmStringNode(qNorm));
            }
        }

        validateEpsilon(node);

        return node;
    }

    private static void validateDimension(AdmObjectNode node, SourceLocation sourceLoc) throws CompilationException {
        IAdmNode dimNode = node.get(VECTOR_INDEX_PARAMETER_DIMENSION);
        if (dimNode == null || dimNode.getType() == ATypeTag.NULL) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_DIMENSION_REQUIRED, sourceLoc,
                    locationSuffix(sourceLoc));
        }
    }

    private static String locationSuffix(SourceLocation sourceLoc) {
        if (sourceLoc == null) {
            return "";
        }
        return String.format(" (in line %d, at column %d)", sourceLoc.getLine(), sourceLoc.getColumn());
    }

    /**
     * Training list size is specified only via {@code train_list_fraction} (with ANALYZE/cardinality at build time).
     */
    private static void validateTrainList(AdmObjectNode node) throws CompilationException {
        if (node.contains(DISALLOWED_TRAIN_LIST_NUMBER) || node.contains(DISALLOWED_TRAIN_LIST_PERCENTAGE)) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_TRAIN_LIST_CONFLICT);
        }
        if (!node.contains(VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION)) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_TRAIN_LIST_UNSPECIFIED);
        }
        IAdmNode fn = node.get(VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION);
        if (fn == null || fn.getType() == ATypeTag.NULL) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_TRAIN_LIST_UNSPECIFIED);
        }
        double trainListFractionValue =
                parseDoubleOrBigInt(fn, ErrorCode.COMPILATION_VECTOR_INDEX_TRAIN_LIST_FRACTION_RANGE);
        if (trainListFractionValue <= 0 || trainListFractionValue > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_TRAIN_LIST_FRACTION_RANGE);
        }
        if (fn.getType() == ATypeTag.BIGINT) {
            node.remove(VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION);
            node.set(VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION, new AdmDoubleNode(trainListFractionValue));
        }
    }

    private static void validateEpsilon(AdmObjectNode node) throws CompilationException {
        IAdmNode epsNode = node.get(VECTOR_INDEX_PARAMETER_EPSILON);
        if (epsNode == null || epsNode.getType() == ATypeTag.NULL) {
            node.set(VECTOR_INDEX_PARAMETER_EPSILON, new AdmDoubleNode(VECTOR_INDEX_DEFAULT_EPSILON));
            return;
        }
        double v = parseDoubleOrBigInt(epsNode, ErrorCode.COMPILATION_VECTOR_INDEX_EPSILON_RANGE);
        if (v < 0 || v > 1) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_EPSILON_RANGE);
        }
        if (epsNode.getType() == ATypeTag.BIGINT) {
            node.remove(VECTOR_INDEX_PARAMETER_EPSILON);
            node.set(VECTOR_INDEX_PARAMETER_EPSILON, new AdmDoubleNode(v));
        }
    }

    private static double parseDoubleOrBigInt(IAdmNode n, ErrorCode invalidTypeCode) throws CompilationException {
        switch (n.getType()) {
            case DOUBLE:
                return ((AdmDoubleNode) n).get();
            case BIGINT:
                return ((AdmBigIntNode) n).get();
            default:
                throw new CompilationException(invalidTypeCode);
        }
    }

}
