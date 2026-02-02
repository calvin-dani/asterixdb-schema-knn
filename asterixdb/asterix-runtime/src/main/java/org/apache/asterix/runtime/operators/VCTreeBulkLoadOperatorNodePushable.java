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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.ChainedLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.IChainedComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.vector.impls.VCTreeBulkLoader;
import org.apache.hyracks.storage.common.IIndexBulkLoader;

/**
 * Operator node pushable for bulk loading VCTree with centroid transition support.
 * 
 * This class extends LSMIndexBulkLoadOperatorNodePushable to add support for
 * detecting centroid changes in sorted tuples and calling loadToNextLeafCluster()
 * on the underlying VCTreeBulkLoader when centroids change.
 */
public class VCTreeBulkLoadOperatorNodePushable extends LSMIndexBulkLoadOperatorNodePushable {

    // Centroid tracking state
    private int[] currentCentroidIds; // Track current centroid per partition
    private boolean[] firstTuple; // Track if this is the first tuple per partition
    private VCTreeBulkLoader[] vcTreeBulkLoaders; // Cache VCTreeBulkLoader instances per partition

    public VCTreeBulkLoadOperatorNodePushable(IIndexDataflowHelperFactory indexDataflowHelperFactory,
            IIndexDataflowHelperFactory priamryIndexDataflowHelperFactory, IHyracksTaskContext ctx, int partition,
            int[] fieldPermutation, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, RecordDescriptor recDesc, BulkLoadUsage usage, int datasetId,
            ITupleFilterFactory tupleFilterFactory, ITuplePartitionerFactory partitionerFactory, int[][] partitionsMap,
            Integer numLevels, List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster,
            Integer maxEntriesPerPage) throws HyracksDataException {
        super(indexDataflowHelperFactory, priamryIndexDataflowHelperFactory, ctx, partition, fieldPermutation,
                fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, recDesc, usage, datasetId,
                tupleFilterFactory, partitionerFactory, partitionsMap, numLevels, clustersPerLevel, centroidsPerCluster,
                maxEntriesPerPage);
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        // Initialize centroid tracking arrays
        currentCentroidIds = new int[bulkLoaders.length];
        firstTuple = new boolean[bulkLoaders.length];
        vcTreeBulkLoaders = new VCTreeBulkLoader[bulkLoaders.length];

        // Initialize all to "first tuple" state
        for (int i = 0; i < bulkLoaders.length; i++) {
            currentCentroidIds[i] = -1;
            firstTuple[i] = true;
            vcTreeBulkLoaders[i] = extractVCTreeBulkLoader(bulkLoaders[i]);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        FrameTupleReference frameTuple = new FrameTupleReference();

        for (int i = 0; i < tupleCount; i++) {
            if (tupleFilter != null) {
                frameTuple.reset(accessor, i);
                if (!tupleFilter.accept(frameTuple)) {
                    continue;
                }
            }
            int storagePartition = tuplePartitioner.partition(accessor, i);
            int storageIdx = storagePartitionId2Index.get(storagePartition);
            tuple.reset(accessor, i);

            // Extract centroidId from tuple (field 1 based on VCTreeSortedDataBulkLoaderOperatorDescriptor)
            int centroidId = extractCentroidId(tuple);

            // Check if we've moved to a new centroid
            if (currentCentroidIds[storageIdx] != centroidId) {
                // New centroid - call loadToNextLeafCluster if not first tuple
                if (!firstTuple[storageIdx] && vcTreeBulkLoaders[storageIdx] != null) {
                    try {
                        vcTreeBulkLoaders[storageIdx].loadToNextLeafCluster(0);
                    } catch (HyracksDataException e) {
                        System.err.println("ERROR: Failed to transition to next leaf cluster: " + e.getMessage());
                        throw e;
                    }
                }
                firstTuple[storageIdx] = false;
                currentCentroidIds[storageIdx] = centroidId;
            }

            // Add tuple to bulk loader
            bulkLoaders[storageIdx].add(tuple);
        }

        FrameUtils.flushFrame(buffer, writer);
    }

    /**
     * Extract centroidId from tuple.
     * Based on VCTreeSortedDataBulkLoaderOperatorDescriptor, centroidId is in field 1.
     */
    private int extractCentroidId(ITupleReference tuple) throws HyracksDataException {
        try {
            // Use TupleUtils to deserialize the integer field
            Object[] fieldValues = TupleUtils.deserializeTuple(tuple, recDesc.getFields());
            return ((AInt32) fieldValues[1]).getIntegerValue();
        } catch (Exception e) {
            System.err.println("ERROR: Failed to extract centroidId from tuple: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Extract VCTreeBulkLoader from the bulk loader chain.
     * 
     * Chain: LSMIndexDiskComponentBulkLoader -> ChainedLSMDiskComponentBulkLoader -> LSMIndexBulkLoader -> VCTreeBulkLoader
     */
    private VCTreeBulkLoader extractVCTreeBulkLoader(IIndexBulkLoader bulkLoader) {
        if (bulkLoader == null) {
            return null;
        }

        try {
            // Check if it's LSMIndexDiskComponentBulkLoader
            if (bulkLoader instanceof LSMIndexDiskComponentBulkLoader) {
                LSMIndexDiskComponentBulkLoader lsmBulkLoader = (LSMIndexDiskComponentBulkLoader) bulkLoader;

                // Access componentBulkLoader via reflection or if there's a getter
                // For now, we'll try to get it from the operation context
                // The componentBulkLoader is a ChainedLSMDiskComponentBulkLoader

                // Try to access via reflection as a fallback
                try {
                    java.lang.reflect.Field field =
                            LSMIndexDiskComponentBulkLoader.class.getDeclaredField("componentBulkLoader");
                    field.setAccessible(true);
                    Object componentBulkLoader = field.get(lsmBulkLoader);

                    if (componentBulkLoader instanceof ChainedLSMDiskComponentBulkLoader) {
                        ChainedLSMDiskComponentBulkLoader chained =
                                (ChainedLSMDiskComponentBulkLoader) componentBulkLoader;

                        // Get the bulkloaderChain and find LSMIndexBulkLoader
                        java.lang.reflect.Field chainField =
                                ChainedLSMDiskComponentBulkLoader.class.getDeclaredField("bulkloaderChain");
                        chainField.setAccessible(true);
                        @SuppressWarnings("unchecked")
                        List<IChainedComponentBulkLoader> chain =
                                (List<IChainedComponentBulkLoader>) chainField.get(chained);

                        for (IChainedComponentBulkLoader chainLoader : chain) {
                            if (chainLoader instanceof LSMIndexBulkLoader) {
                                LSMIndexBulkLoader lsmIndexBulkLoader = (LSMIndexBulkLoader) chainLoader;
                                IIndexBulkLoader wrappedLoader = lsmIndexBulkLoader.getBulkLoader();

                                if (wrappedLoader instanceof VCTreeBulkLoader) {
                                    return (VCTreeBulkLoader) wrappedLoader;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("WARNING: Could not extract VCTreeBulkLoader via reflection: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("WARNING: Failed to extract VCTreeBulkLoader: " + e.getMessage());
        }

        return null;
    }
}
