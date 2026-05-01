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
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.vector.impls.PKOnlyTupleProjectorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

/**
 * Operator descriptor for vector index search (ANN search).
 * This creates the runtime operator (VectorSearchOperatorNodePushable) that performs
 * the actual search on each node controller.
 */
public class VectorSearchOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    // Field indexes in input tuple: [query_vector_field, k_field, metric_field]
    protected final int[] queryFields;

    // Factory to open LSMVTree index
    protected final IIndexDataflowHelperFactory indexHelperFactory;

    // Whether to retain input tuples in output
    protected final boolean retainInput;

    // Transaction callback factory
    protected final ISearchOperationCallbackFactory searchCallbackFactory;

    // Partition mapping (compute nodes to storage nodes)
    protected final int[][] partitionsMap;

    // Number of primary and secondary keys for tuple projection
    protected final int numPrimaryKeys;
    protected final int numSecondaryKeys;

    // Tuple projector factory (extracts only PKs from index results)
    protected final ITupleProjectorFactory tupleProjectorFactory;

    // Factory for creating vector binary accessors (for extracting AOrderedList<ADouble>)
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;

    // Factory for creating distance functions (wraps VectorDistanceArrCalculation from AsterixDB)
    // This is passed from AsterixDB layer to avoid circular dependencies
    protected final java.io.Serializable distanceFunctionFactory;

    // Factory for creating tuple filters for INCLUDE field predicates (e.g., year > 2000)
    // When set, the cursor will only return tuples that pass this filter
    protected final ITupleFilterFactory tupleFilterFactory;

    // Search approach: 0 = naive (LSMVTreeSearchCursor), 1 = optimized (LSMVTreeBlockedCursor)
    // This is a compile-time constant extracted from the ANN_DISTANCE function arguments
    protected final int searchApproach;

    // Multiplier for candidate limit: K * kMultiplier candidates sent to PK for reranking
    protected final int kMultiplier;

    /** Epsilon from vector index WITH metadata (ANN / cluster search). */
    protected final double indexEpsilon;

    public VectorSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] queryFields, IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput,
            ISearchOperationCallbackFactory searchCallbackFactory, IVTreeBinaryAccessorFactory vectorAccessorFactory,
            java.io.Serializable distanceFunctionFactory, int[][] partitionsMap, int numPrimaryKeys,
            int numSecondaryKeys, ITupleFilterFactory tupleFilterFactory, int searchApproach, int kMultiplier,
            double indexEpsilon) {
        super(spec, 1, 1); // 1 input, 1 output
        this.queryFields = queryFields;
        this.indexHelperFactory = indexHelperFactory;
        this.retainInput = retainInput;
        this.searchCallbackFactory = searchCallbackFactory;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.partitionsMap = partitionsMap;
        this.numPrimaryKeys = numPrimaryKeys;
        this.numSecondaryKeys = numSecondaryKeys;
        this.tupleFilterFactory = tupleFilterFactory;
        this.searchApproach = searchApproach;
        this.kMultiplier = kMultiplier;
        this.indexEpsilon = indexEpsilon;
        this.outRecDescs[0] = outRecDesc;

        // Create tuple projector factory that extracts only PK fields
        // This avoids writing large embedding vectors (4KB-16KB) to output frames
        this.tupleProjectorFactory = new PKOnlyTupleProjectorFactory(numSecondaryKeys, numPrimaryKeys);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new VectorSearchOperatorNodePushable(ctx, partition,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), queryFields, indexHelperFactory,
                retainInput, searchCallbackFactory, tupleProjectorFactory, vectorAccessorFactory,
                distanceFunctionFactory, partitionsMap, tupleFilterFactory, searchApproach, numSecondaryKeys,
                kMultiplier, indexEpsilon);
    }
}
