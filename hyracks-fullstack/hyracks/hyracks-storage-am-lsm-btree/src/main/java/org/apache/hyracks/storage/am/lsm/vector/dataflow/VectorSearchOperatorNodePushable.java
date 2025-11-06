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
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

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

    // Field indexes in input tuple: [query_vector_field, k_field, metric_field]
    protected final int[] queryFields;

    // Factory for creating vector accessors (passed from AsterixDB layer)
    protected final IVectorBinaryAccessorFactory vectorAccessorFactory;

    // Tuple reference for extracting query parameters
    protected PermutingFrameTupleReference queryParamsTuple;

    public VectorSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] queryFields, IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput,
            ISearchOperationCallbackFactory searchCallbackFactory, ITupleProjectorFactory projectorFactory,
            IVectorBinaryAccessorFactory vectorAccessorFactory, int[][] partitionsMap) throws HyracksDataException {
        // Call parent constructor
        // Note: Vector search doesn't need min/max filter fields (pass null)
        // Note: Vector search doesn't need missing writer (pass null for retainMissing)
        // Note: No index filter for now (pass false for appendIndexFilter)
        // Note: No tuple filter for now (pass null)
        // Note: No output limit for now (pass -1)
        // Note: No search callback result needed (pass false)
        super(ctx, inputRecDesc, partition, null, // minFilterFieldIndexes
                null, // maxFilterFieldIndexes
                indexHelperFactory, retainInput, false, // retainMissing
                null, // nonMatchWriterFactory
                searchCallbackFactory, false, // appendIndexFilter
                null, // nonFilterWriterFactory
                null, // tupleFilterFactory
                -1, // outputLimit
                false, // appendOpCallbackProceedResult
                null, // searchCallbackProceedResultFalseValue
                null, // searchCallbackProceedResultTrueValue
                projectorFactory, // ← PKOnlyTupleProjectorFactory (extracts only PK fields)
                null, // tuplePartitionerFactory
                partitionsMap);

        this.queryFields = queryFields;
        this.vectorAccessorFactory = vectorAccessorFactory;

        // Setup permuting tuple reference to extract query parameters
        if (queryFields != null && queryFields.length > 0) {
            queryParamsTuple = new PermutingFrameTupleReference();
            queryParamsTuple.setFieldPermutation(queryFields);
        }
    }

    @Override
    protected ISearchPredicate createSearchPredicate(IIndex index) {
        // Create simple marker predicate
        // The actual query vector is passed via IIndexAccessParameters in addAdditionalIndexAccessorParams()
        return new VectorPointPredicate();
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        // Update queryParamsTuple to point to current input tuple
        if (queryParamsTuple != null) {
            queryParamsTuple.reset(accessor, tupleIndex);

            // Update predicate with current tuple reference
            // Following RTree pattern: predicate holds reference, updated per-tuple
            VectorPointPredicate vectorPred = (VectorPointPredicate) searchPred;
            vectorPred.setQueryTuple(queryParamsTuple);
            vectorPred.setQueryFieldIndex(0); // Field 0 is the vector field
        }
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
    }
}
