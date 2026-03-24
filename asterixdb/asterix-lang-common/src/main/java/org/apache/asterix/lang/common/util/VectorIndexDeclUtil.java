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
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class VectorIndexDeclUtil {
    /* ***********************************************
     * Vector Index Policy Parameters
     * ***********************************************
     */
    public static final String VECTOR_INDEX_PARAMETER_DIMENSION = "dimension";
    public static final String VECTOR_INDEX_PARAMETER_QUANTIZATION = "quantization";
    public static final String VECTOR_INDEX_PARAMETER_TRAIN_LIST = "train_list_number";
    public static final String VECTOR_INDEX_PARAMETER_TRAIN_LIST_PERCENTAGE = "train_list_percentage";
    public static final String VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION = "train_list_fraction";
    public static final String VECTOR_INDEX_PARAMETER_SIMILARITY = "similarity";
    public static final String VECTOR_INDEX_PARAMETER_NUM_K = "num_clusters";
    public static final String VECTOR_INDEX_PARAMETER_EPSILON = "epsilon";
    public static final String VECTOR_INDEX_DEFAULT_QUANTIZATION = "SQ8";
    /** Default for level-wise centroid search and ANN search predicate (matches VectorSearchPredicate). */
    public static final double VECTOR_INDEX_DEFAULT_EPSILON = 0.3;

    /**
     * Canonical names for {@code similarity}, aligned with
     * {@link org.apache.asterix.runtime.utils.VectorDistanceFunctionFactory} (after {@code toLowerCase()}).
     */
    private static final Set<String> ALLOWED_VECTOR_DISTANCE_METRICS = Set.of("euclidean", "l2", "euclidean_squared",
            "l2_squared", "manhattan distance", "cosine similarity", "dot");

    private static final ARecordType WITH_OBJECT_TYPE = getWithObjectType();

    private VectorIndexDeclUtil() {
    }

    public static AdmObjectNode validateAndGetWithObjectNode(RecordConstructor withRecord) throws CompilationException {
        if (withRecord == null) {
            return null;
        }
        final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
        final AdmObjectNode node = ExpressionUtils.toNode(withRecord);
        validator.validateType(WITH_OBJECT_TYPE, node);

        String similarity = node.getOptionalString(VECTOR_INDEX_PARAMETER_SIMILARITY, null);
        if (similarity == null || similarity.trim().isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_SIMILARITY_REQUIRED);
        }

        String normalizedSimilarity = similarity.trim().toLowerCase(Locale.ROOT);
        if (!ALLOWED_VECTOR_DISTANCE_METRICS.contains(normalizedSimilarity)) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_SIMILARITY_UNSUPPORTED,
                    similarity.trim());
        }

        // Default quantization to SQ8 if not specified
        String quantization = node.getOptionalString(VECTOR_INDEX_PARAMETER_QUANTIZATION, null);
        if (quantization == null) {
            node.set(VECTOR_INDEX_PARAMETER_QUANTIZATION, new AdmStringNode(VECTOR_INDEX_DEFAULT_QUANTIZATION));
        }

        // Default epsilon for level-wise search / ANN if not specified
        if (node.get(VECTOR_INDEX_PARAMETER_EPSILON) == null) {
            node.set(VECTOR_INDEX_PARAMETER_EPSILON, new AdmDoubleNode(VECTOR_INDEX_DEFAULT_EPSILON));
        }

        return node;
    }

    private static ARecordType getWithObjectType() {
        final String[] withNames = { VECTOR_INDEX_PARAMETER_DIMENSION, VECTOR_INDEX_PARAMETER_QUANTIZATION,
                VECTOR_INDEX_PARAMETER_TRAIN_LIST, VECTOR_INDEX_PARAMETER_TRAIN_LIST_PERCENTAGE,
                VECTOR_INDEX_PARAMETER_TRAIN_LIST_FRACTION, VECTOR_INDEX_PARAMETER_SIMILARITY,
                VECTOR_INDEX_PARAMETER_NUM_K, VECTOR_INDEX_PARAMETER_EPSILON };
        final IAType[] withTypes = { BuiltinType.AINT64, AUnionType.createUnknownableType(BuiltinType.ASTRING),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE),
                AUnionType.createUnknownableType(BuiltinType.ASTRING),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE) };
        return new ARecordType("withObject", withNames, withTypes, false);
    }

}
