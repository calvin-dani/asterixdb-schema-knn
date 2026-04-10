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

package org.apache.hyracks.storage.am.lsm.vector;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorIndexTestDriver;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * LSMVTree bulk load test.
 * Tests the bulk loading functionality of LSMVTree with static structure and data records.
 */
public class LSMVTreeBulkLoadTest extends VectorIndexTestDriver {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords) throws Exception {

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("LSMVTree Bulk Load Test: {} levels, {} centroids, {} leaf clusters, {} dimension vectors",
                    numClustersPerLevel.size(), centroids.size(), leafRecords.size(), vectorDimension);
        }

        // Create test context
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory());

        // Set test data in context
        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);
        ctx.setDataRecords(leafRecords);

        try {
            // 1. Create and activate index
            ctx.getIndex().create();
            ctx.getIndex().activate();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Index created and activated");
            }

            // 2. Build static structure (hierarchical centroids)
            testUtils.buildStaticStructure(ctx);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Static structure built with {} centroids", centroids.size());
            }

            // 3. Bulk load data records
            testUtils.bulkLoadRecords(ctx);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Bulk loaded {} clusters with data records", leafRecords.size());
            }

            // 4. Validate: scan closest leaf cluster
            testUtils.scanClosestLeafCluster(ctx);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Validation: scan closest leaf cluster succeeded");
            }

            // 5. Validate: top-K search
            testUtils.topKSearch(ctx);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Validation: top-K search succeeded");
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Index validation succeeded");
            }

        } finally {
            // Cleanup
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Index deactivated and destroyed");
            }
        }
    }
}
