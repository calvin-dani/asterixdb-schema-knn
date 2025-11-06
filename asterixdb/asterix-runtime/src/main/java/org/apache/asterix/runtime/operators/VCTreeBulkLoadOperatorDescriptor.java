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
package org.apache.asterix.runtime.operators;

import java.util.List;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;

/**
 * Bulk load operator descriptor for VCTree that handles centroid transitions.
 * 
 * This operator extends LSMIndexBulkLoadOperatorDescriptor to add support for
 * detecting centroid changes in sorted tuples and calling loadToNextLeafCluster()
 * on the underlying VCTreeBulkLoader when centroids change.
 */
public class VCTreeBulkLoadOperatorDescriptor extends LSMIndexBulkLoadOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    public VCTreeBulkLoadOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IIndexDataflowHelperFactory indexHelperFactory,
            IIndexDataflowHelperFactory primaryIndexHelperFactory, BulkLoadUsage usage, int datasetId,
            ITupleFilterFactory tupleFilterFactory, ITuplePartitionerFactory partitionerFactory,
            int[][] partitionsMap) {
        this(spec, outRecDesc, fieldPermutation, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                indexHelperFactory, primaryIndexHelperFactory, usage, datasetId, tupleFilterFactory, partitionerFactory,
                partitionsMap, null, null, null, null);
    }

    public VCTreeBulkLoadOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] fieldPermutation, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IIndexDataflowHelperFactory indexHelperFactory,
            IIndexDataflowHelperFactory primaryIndexHelperFactory, BulkLoadUsage usage, int datasetId,
            ITupleFilterFactory tupleFilterFactory, ITuplePartitionerFactory partitionerFactory, int[][] partitionsMap,
            Integer numLevels, List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster,
            Integer maxEntriesPerPage) {
        super(spec, outRecDesc, fieldPermutation, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                indexHelperFactory, primaryIndexHelperFactory, usage, datasetId, tupleFilterFactory, partitionerFactory,
                partitionsMap, numLevels, clustersPerLevel, centroidsPerCluster, maxEntriesPerPage);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new VCTreeBulkLoadOperatorNodePushable(indexHelperFactory, primaryIndexHelperFactory, ctx, partition,
                fieldPermutation, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex,
                recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0), usage, datasetId,
                tupleFilterFactory, partitionerFactory, partitionsMap, numLevels, clustersPerLevel, centroidsPerCluster,
                maxEntriesPerPage);
    }
}
