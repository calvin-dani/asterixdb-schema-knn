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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.impls.VectorSearchPredicate;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runtime operator for vector index search (ANN search).
 * Extends IndexSearchOperatorNodePushable which handles the heavy lifting:
 * - Opening/closing indexes
 * - Frame/tuple iteration
 * - Output buffering
 * - Transaction callbacks
 *
 * This class implements the schema-agnostic pattern using IVectorBinaryAccessor
 * to abstract over different vector serialization formats (e.g., AOrderedList).
 *
 * This class only needs to implement:
 * 1. createSearchPredicate() - Create VectorPointPredicate with accessor
 * 2. resetSearchPredicate() - Set query tuple reference (no deserialization!)
 * 3. getFieldCount() - Return number of output fields
 * 4. addAdditionalIndexAccessorParams() - Add vector-specific params (if any)
 */
public class VectorSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();

    // Field indexes in input tuple: [query_vector_field, k_field, metric_field]
    protected final int[] queryFields;

    // Factory for creating vector accessors (passed from AsterixDB layer)
    protected final IVectorBinaryAccessorFactory vectorAccessorFactory;

    // Factory for creating distance functions (passed from AsterixDB layer, wraps VectorDistanceArrCalculation)
    protected final java.io.Serializable distanceFunctionFactory;

    // Tuple reference for extracting query parameters
    protected PermutingFrameTupleReference queryParamsTuple;

    // Reusable pointable for extracting string values
    private final UTF8StringPointable stringPointable = new UTF8StringPointable();

    // Factory for creating tuple filters for INCLUDE field predicates (e.g., year > 2000)
    // When set, the filter is pushed down to the cursor level for proper K counting
    protected final ITupleFilterFactory tupleFilterFactory;

    // The actual tuple filter, created from the factory
    protected ITupleFilter tupleFilter;

    // Search approach: 0 = naive (LSMVCTreeSearchCursor), 1 = optimized (LSMVCTreeBlockedCursor)
    // Compile-time constant passed from descriptor for cursor selection at open() time
    protected final int searchApproach;

    // Number of secondary key fields before PKs in data tuples (2 for non-quantized, 4 for quantized)
    protected final int numSecondaryKeys;

    // Multiplier for candidate limit: K * kMultiplier candidates sent to PK for reranking
    protected final int kMultiplier;

    /** Epsilon from vector index metadata (default 0.3 when absent in catalog). */
    protected final double indexEpsilon;

    public VectorSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] queryFields, IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput,
            ISearchOperationCallbackFactory searchCallbackFactory, ITupleProjectorFactory projectorFactory,
            IVectorBinaryAccessorFactory vectorAccessorFactory, java.io.Serializable distanceFunctionFactory,
            int[][] partitionsMap, ITupleFilterFactory tupleFilterFactory, int searchApproach, int numSecondaryKeys,
            int kMultiplier, double indexEpsilon) throws HyracksDataException {
        // Call parent constructor
        // Note: Vector search doesn't need min/max filter fields (pass null)
        // Note: Vector search doesn't need missing writer (pass null for retainMissing)
        // Note: No index filter for now (pass false for appendIndexFilter)
        // Note: We pass null for tupleFilterFactory to parent - we handle filtering in cursor
        // Note: No output limit for now (pass -1)
        // Note: No search callback result needed (pass false)
        super(ctx, inputRecDesc, partition, null, // minFilterFieldIndexes
                null, // maxFilterFieldIndexes
                indexHelperFactory, retainInput, false, // retainMissing
                null, // nonMatchWriterFactory
                searchCallbackFactory, false, // appendIndexFilter
                null, // nonFilterWriterFactory
                null, // tupleFilterFactory - we handle this at cursor level, not operator level
                -1, // outputLimit
                false, // appendOpCallbackProceedResult
                null, // searchCallbackProceedResultFalseValue
                null, // searchCallbackProceedResultTrueValue
                projectorFactory, // ← PKOnlyTupleProjectorFactory (extracts only PK fields)
                null, // tuplePartitionerFactory
                partitionsMap);

        this.queryFields = queryFields;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.tupleFilterFactory = tupleFilterFactory;
        this.searchApproach = searchApproach;
        this.numSecondaryKeys = numSecondaryKeys;
        this.kMultiplier = kMultiplier;
        this.indexEpsilon = indexEpsilon;

        // Setup permuting tuple reference to extract query parameters
        if (queryFields != null && queryFields.length > 0) {
            queryParamsTuple = new PermutingFrameTupleReference();
            queryParamsTuple.setFieldPermutation(queryFields);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();

        // Create tuple filter from factory if available
        // This filter is pushed down to the cursor level for proper K counting
        if (tupleFilterFactory != null) {
            tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
        }
    }

    @Override
    protected ISearchPredicate createSearchPredicate(IIndex index) {
        // Create simple marker predicate
        // The actual query vector is passed via IIndexAccessParameters in addAdditionalIndexAccessorParams()
        return new VectorSearchPredicate();
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        // Update queryParamsTuple to point to current input tuple
        if (queryParamsTuple != null) {
            queryParamsTuple.reset(accessor, tupleIndex);

            // Update predicate with current tuple reference
            // Following RTree pattern: predicate holds reference, updated per-tuple
            VectorSearchPredicate vectorPred = (VectorSearchPredicate) searchPred;
            vectorPred.setQueryTuple(queryParamsTuple);
            vectorPred.setQueryFieldIndex(0); // Field 0 is the vector field
            vectorPred.setPkStartField(numSecondaryKeys);

            // Extract K value from field 1 if available
            if (queryFields.length > 1) {
                // K is at field index 1 in queryParamsTuple (after permutation)
                // Skip type tag (1 byte) to get the actual integer value
                int k = IntegerPointable.getInteger(queryParamsTuple.getFieldData(1),
                        queryParamsTuple.getFieldStart(1) + 1 // +1 to skip type tag
                );
                vectorPred.setK(k);
            }

            // Extract distance metric from field index 2 (after query vector at 0, k at 1)
            if (queryFields != null && queryFields.length > 2) {
                String distanceMetric = extractDistanceMetricFromTuple(queryParamsTuple, 2);
                vectorPred.setDistanceMetric(distanceMetric);
            }

            // Extract min_probe_fraction from field 3 (double, +1 to skip type tag)
            // Fraction of leaf clusters to probe (0.0-1.0). 0 means use default (0.1).
            if (queryFields.length > 3) {
                double minProbeFraction = DoublePointable.getDouble(queryParamsTuple.getFieldData(3),
                        queryParamsTuple.getFieldStart(3) + 1);
                if (minProbeFraction > 0.0) {
                    vectorPred.setMinProbeFraction(minProbeFraction);
                }
            }

            // Extract k_multiplier from field 4 (int, +1 to skip type tag)
            if (queryFields.length > 4) {
                int queryKMult = IntegerPointable.getInteger(queryParamsTuple.getFieldData(4),
                        queryParamsTuple.getFieldStart(4) + 1);
                vectorPred.setKMultiplier(Math.max(1, queryKMult));
            }

            // Set tuple filter for INCLUDE field predicates (e.g., year > 2000)
            // This filter is applied at cursor level for proper K counting
            if (tupleFilter != null) {
                vectorPred.setTupleFilter(tupleFilter);
            }

            // Session config compiler.vector.kmultiplier overrides query arg if set (kMultiplier > 1 from constructor)
            if (kMultiplier > 1) {
                vectorPred.setKMultiplier(kMultiplier);
            }

            vectorPred.setEpsilon(indexEpsilon);
        }
    }

    /**
     * Extract distance metric string from tuple field.
     *
     * @param tuple Input tuple containing the distance metric
     * @param fieldIndex Field index containing the distance metric string
     * @return Distance metric string, or "euclidean" as default
     */
    private String extractDistanceMetricFromTuple(ITupleReference tuple, int fieldIndex) {
        try {
            if (tuple.getFieldCount() > fieldIndex) {
                byte[] fieldData = tuple.getFieldData(fieldIndex);
                int fieldStart = tuple.getFieldStart(fieldIndex);
                int fieldLength = tuple.getFieldLength(fieldIndex);

                // Reset pointable to read the string value
                stringPointable.set(fieldData, fieldStart + 1, fieldLength);
                return stringPointable.toString();
            }
        } catch (Exception e) {
            // If extraction fails, default to euclidean
            LOGGER.trace("Failed to extract distance metric from tuple, defaulting to euclidean: {}", e.getMessage());
        }
        return "euclidean"; // Default fallback
    }

    @Override
    protected int getFieldCount(IIndex index) {
        // For vector index, we only output primary keys (no secondary keys/embeddings)
        // The number of fields is determined by the dataset's primary key count
        //
        // TODO: Get actual PK count from index metadata
        // For now, assume single PK field (common case)
        return 1;

        // When implementing properly:
        // LSMVCTree lsmvcTree = (LSMVCTree) index;
        // return lsmvcTree.getNumPrimaryKeys();  // Or similar method
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) {
        // Store the vector accessor factory in parameters
        // The VCTree accessor will extract the query vector from the predicate during search()
        // This maintains layer separation: extraction happens in storage layer using the factory
        iap.getParameters().put(HyracksConstants.VECTOR_QUERY, vectorAccessorFactory);

        // Store the K field index (field 1 in queryFields: [vector, k, metric])
        // The cursor will extract K from the query tuple using this index
        if (queryFields != null && queryFields.length > 1) {
            iap.getParameters().put(HyracksConstants.VECTOR_K, 1); // K is at field index 1
        }

        // Store the distance function factory in parameters
        // The VCTree will use this factory to create IVectorDistanceFunction implementations
        // that wrap VectorDistanceArrCalculation from AsterixDB
        iap.getParameters().put(HyracksConstants.VECTOR_DISTANCE_FUNCTION_FACTORY, distanceFunctionFactory);

        // Set cursor selection flags based on compile-time searchApproach constant
        // 0 = naive streaming (LSMVCTreeSearchCursor)
        // 1 = optimized bidirectional (LSMVCTreeBlockedCursor)
        // 2 = optimized bidirectional with inline filtering (LSMVCTreeBlockedCursor + ITupleFilter)
        // 3 = naive blocked (LSMVCTreeBlockedCursorNaive - top-K window, quantized distance, no pruning)
        // 4 = index-driven KNN (LSMVCTreeBlockedCursor + SequentialClusterSelectionStrategy)
        if (searchApproach == 1 || searchApproach == 2 || searchApproach == 4) {
            iap.getParameters().put(HyracksConstants.USE_OPTIMIZED_SEARCH, Boolean.TRUE);
        }
        if (searchApproach == 4) {
            iap.getParameters().put(HyracksConstants.USE_SEQUENTIAL_SCAN, Boolean.TRUE);
        }
        if (searchApproach == 3) {
            iap.getParameters().put(HyracksConstants.USE_NAIVE_BLOCKED_SEARCH, Boolean.TRUE);
        }
    }
}
