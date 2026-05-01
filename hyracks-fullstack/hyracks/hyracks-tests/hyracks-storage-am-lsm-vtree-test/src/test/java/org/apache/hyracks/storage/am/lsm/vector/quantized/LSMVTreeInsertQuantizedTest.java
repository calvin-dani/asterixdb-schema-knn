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

package org.apache.hyracks.storage.am.lsm.vector.quantized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.QuantizedSearchTestDriver;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * LSMVTree insert test with quantized vector embedding.
 * Tests insert operations into memory component after bulk loading the first disk component.
 *
 * Inherits two test cases from QuantizedSearchTestDriver:
 * - optimizedSearchThreeDimension(): 3D single-centroid structure (1 level, 20 bulk-loaded records)
 * - twoDimensionTwoLevels(): 2D two-layer structure (2 levels, 16 leaf centroids, 800 bulk-loaded records)
 *
 * Each test case bulk loads the dataset, then inserts additional records into the memory component,
 * and verifies that records from both components are retrievable via search.
 *
 * Data tuple format (quantized): <distance, centroid_id, vector, primary_key>
 * The vector field is included for computing D(q,x) during optimized search.
 */

public class LSMVTreeInsertQuantizedTest extends QuantizedSearchTestDriver {

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

    /**
     * Implementation of runTest from QuantizedSearchTestDriver.
     * Performs: build static structure → bulk load → insert → verify with insert-aware query cases.
     *
     * The driver provides query cases for bulk-load-only scenarios. After inserting additional records,
     * this method builds insert-aware query cases with updated expected PKs that account for
     * interleaved results from both bulk-loaded (disk) and inserted (memory) components.
     */
    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords, List<QueryCase> queryCases) throws Exception {

        LOGGER.info("LSMVTree Insert Quantized Test: {} levels, {} centroids, {} leaf clusters, {}D vectors",
                numClustersPerLevel.size(), centroids.size(), leafRecords.size(), vectorDimension);

        // Create test context with quantized data tuple creator factory
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(),
                harness.getDataTupleCreatorFactory());

        // Set test data in context (quantized format: PK starts at field 4)
        ctx.setPkStartField(VTreeDataTupleConstants.Q_PK_START_FIELD);
        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);
        ctx.setDataRecords(leafRecords);

        try {
            // 1. Create and activate index
            ctx.getIndex().create();
            ctx.getIndex().activate();
            LOGGER.info("Index created and activated");

            // 2. Build static structure
            testUtils.buildStaticStructure(ctx);
            LOGGER.info("Static structure built with {} centroids", centroids.size());

            // 3. Bulk load data records (first disk component)
            testUtils.bulkLoadRecords(ctx);
            int bulkLoadedCount = leafRecords.stream().mapToInt(List::size).sum();
            LOGGER.info("Bulk loaded {} records across {} clusters", bulkLoadedCount, leafRecords.size());

            // 4. Generate and insert additional records into memory component
            VectorTestStructure struct = vectorDimension == 2 ? STRUCT_2D : STRUCT_3D;
            List<List<ITupleReference>> insertRecords = struct.generateInsertRecords(INSERT_RECORDS_PER_CLUSTER);
            int insertedCount = testUtils.insertRecordsIntoMemoryComponent(ctx, insertRecords);
            LOGGER.info("Inserted {} records into memory component", insertedCount);

            // 5. Verify records using LSMVTreeSearchCursor (large K to find both component types)
            QueryCase firstQuery = queryCases.get(0);
            testUtils.verifyRecordsWithSearch(ctx, firstQuery.queryVector, 100);
            LOGGER.info("Verification: Found records from both bulk-loaded and inserted components");

            // 6. Verify with optimized search (query cases already built by overridden build*QueryCases)
            for (int i = 0; i < queryCases.size(); i++) {
                QueryCase qc = queryCases.get(i);
                ctx.setQueryVector(qc.queryVector);
                ctx.setQueryK(qc.queryK);
                ctx.setExpectedPrimaryKeys(qc.expectedPrimaryKeys);
                testUtils.naiveBlockedSearch(ctx);
                LOGGER.info("Query case {}/{} (naive blocked) succeeded: K={}", i + 1, queryCases.size(), qc.queryK);
            }

        } finally {
            // Cleanup
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
            LOGGER.info("Index deactivated and destroyed");
        }
    }

    /**
     * Override 3D query cases for insert test.
     * Inserts don't affect top-5 (closest insert is pk_ins_c0_0 at [0.30,0,0], D(q,x)=4.70 > bulk top-5 at D≤2.0).
     * Same expected results as bulk-load-only test.
     */
    @Override
    protected List<QueryCase> build3DQueryCases() {
        return super.build3DQueryCases();
    }

    /**
     * Override 2D query cases for insert test.
     * Insert records (baseDistance=0.30, max d=3.0) extend beyond bulk max (d=2.6),
     * so insert records rank higher for queries near centroid boundaries.
     */
    @Override
    protected List<QueryCase> build2DQueryCases() {
        List<QueryCase> queryCases = new ArrayList<>();

        // Query 1: [30, 30] K=5 — insert records from c4 are closest
        // pk_ins_c4_36: [28.0, 25], D(q,x) = sqrt(29) ≈ 5.39
        // pk_ins_c4_38: [25, 28.0], D(q,x) = sqrt(29) ≈ 5.39
        // pk_ins_c4_32: [27.7, 25], D(q,x) = sqrt(30.29) ≈ 5.50
        // pk_ins_c4_34: [25, 27.7], D(q,x) = sqrt(30.29) ≈ 5.50
        // pk_2d_c4_48:  [27.6, 25], D(q,x) = sqrt(30.76) ≈ 5.55
        queryCases.add(new QueryCase(new double[] { 30.0, 30.0 }, 5,
                Arrays.asList("pk_ins_c4_36", "pk_ins_c4_38", "pk_ins_c4_32", "pk_ins_c4_34", "pk_2d_c4_48")));

        // Query 2: [50, 25] K=10 multi-cluster (equidistant from c4[25,25] and c5[75,25])
        // With nprobe=2, probes both c4 and c5
        // Symmetrical results interleaved from both clusters:
        // D=22.0: pk_ins_c4_36 [28,25], pk_ins_c5_37 [72,25]
        // D=22.3: pk_ins_c4_32 [27.7,25], pk_ins_c5_33 [72.3,25]
        // D=22.4: pk_2d_c4_48 [27.6,25], pk_2d_c5_49 [72.4,25]
        // D=22.6: pk_ins_c4_28 [27.4,25], pk_2d_c4_44 [27.4,25],
        //         pk_ins_c5_29 [72.6,25], pk_2d_c5_45 [72.6,25]
        queryCases.add(new QueryCase(new double[] { 50.0, 25.0 }, 10,
                Arrays.asList("pk_ins_c4_36", "pk_ins_c5_37", "pk_ins_c4_32", "pk_ins_c5_33", "pk_2d_c4_48",
                        "pk_2d_c5_49", "pk_ins_c4_28", "pk_2d_c4_44", "pk_ins_c5_29", "pk_2d_c5_45")));

        return queryCases;
    }

}
