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
package org.apache.hyracks.api.util;

public class HyracksConstants {
    public static final String KEY_MESSAGE = "HYX:MSG";
    public static final String HYRACKS_LOGGER_NAME = "org.apache.hyracks";
    // A frame manager that manages all inverted index searches
    public static final String INVERTED_INDEX_SEARCH_FRAME_MANAGER = "INVERTED_INDEX_SEARCH_FRAME_MANAGER";
    // Hyracks task context
    public static final String HYRACKS_TASK_CONTEXT = "HYRACKS_TASK_CONTEXT";

    public static final String INDEX_CURSOR_STATS = "INDEX_CURSOR_STATS";

    public static final String TUPLE_PROJECTOR = "TUPLE_PROJECTOR";
    public static final String SAMPLE_CARDINALITY = "SAMPLE_CARDINALITY";
    public static final String SAMPLE_SEED = "SAMPLE_SEED";

    public static final String ATOMIC_OP_CONTEXT = "ATOMIC_OP_CONTEXT";

    // Vector search query vector (extracted double[] from input tuple)
    public static final String VECTOR_QUERY = "VECTOR_QUERY";

    // Vector search K parameter (number of nearest neighbors to return)
    public static final String VECTOR_K = "VECTOR_K";

    // Vector distance function factory (wraps VectorDistanceArrCalculation from AsterixDB)
    public static final String VECTOR_DISTANCE_FUNCTION_FACTORY = "VECTOR_DISTANCE_FUNCTION_FACTORY";

    // Flag to enable optimized search with bidirectional traversal and triangle inequality
    public static final String USE_OPTIMIZED_SEARCH = "USE_OPTIMIZED_SEARCH";

    // Flag to enable naive blocked search (top-K window with quantized distance, no bidirectional pruning)
    public static final String USE_NAIVE_BLOCKED_SEARCH = "USE_NAIVE_BLOCKED_SEARCH";

    // Flag to enable sequential scan of all clusters (index-driven KNN with bidirectional pruning)
    public static final String USE_SEQUENTIAL_SCAN = "USE_SEQUENTIAL_SCAN";

    // Pre-configured vector quantizer (IVTreeQuantizer) for direct injection — used by test
    // fixtures (e.g. NoOpVectorQuantizer.INSTANCE). Production code paths should use
    // VECTOR_QUANTIZER_FACTORY below and let the storage layer construct the quantizer per query.
    public static final String VECTOR_QUANTIZER = "VECTOR_QUANTIZER";

    // Vector quantizer factory (IVTreeQuantizerFactory). Replaces the previous reflective
    // construction of ScalarVectorQuantizer in VTree#search.
    public static final String VECTOR_QUANTIZER_FACTORY = "VECTOR_QUANTIZER_FACTORY";

    private HyracksConstants() {
    }
}
