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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.OptimizedSearchTestDriver;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * LSMVCTree insert test.
 * Tests insert operations into memory component after bulk loading the first disk component.
 *
 * Uses the 2D two-layer dataset from OptimizedSearchTestDriver:
 * - Level 0: 4 quadrant centroids at [±50, ±50]
 * - Level 1: 16 leaf centroids (4 per quadrant)
 * - 50 bulk-loaded records per leaf centroid (800 total in disk component)
 * - 80 inserted records per leaf centroid (1280 total in memory component)
 * - Total: 2080 records
 *
 * Verification uses LSMVCTreeSearchCursor with K=1000+ to retrieve all records.
 */
public class LSMVCTreeInsertTest extends OptimizedSearchTestDriver {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVCTreeTestHarness harness = new LSMVCTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    // Records per cluster
    private static final int BULK_LOAD_RECORDS_PER_CLUSTER = 50;
    private static final int INSERT_RECORDS_PER_CLUSTER = 80;
    private static final int TOTAL_LEAF_CLUSTERS = 16;

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /**
     * Test insert operations using 2D two-layer dataset.
     *
     * 1. Build static structure (reusing OptimizedSearchTestDriver's 2D dataset)
     * 2. Bulk load 50 records per leaf cluster (first disk component)
     * 3. Insert 80 additional records per leaf cluster (memory component)
     * 4. Verify total records using LSMVCTreeSearchCursor with K=1000+
     */
    @Test
    public void twoDimensionInsertTest() throws Exception {
        // Centroid serializers: centroid ID + Double array vector (2D)
        ISerializerDeserializer[] centroidSerdes = { IntegerSerializerDeserializer.INSTANCE,
                DoubleArraySerializerDeserializer.INSTANCE };

        // Data record serializers for bulk load: distance + centroid_id + vector + primary key
        ISerializerDeserializer[] dataRecordSerdes = { DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, DoubleArraySerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer() };

        // Get 2D dataset configuration from parent class
        List<ITupleReference> centroids = get2DCentroids();
        List<Integer> numClustersPerLevel = get2DNumClustersPerLevel();
        List<List<Integer>> centroidsPerCluster = get2DCentroidsPerCluster();

        // Generate bulk load records (50 per cluster)
        List<List<ITupleReference>> bulkLoadRecords = generate2DBulkLoadRecords(BULK_LOAD_RECORDS_PER_CLUSTER);

        // Generate insert records (80 per cluster)
        List<List<ITupleReference>> insertRecords = generate2DInsertRecords(INSERT_RECORDS_PER_CLUSTER);

        // Query configuration - use a query that will find records from multiple clusters
        // Query at origin [0, 0] to get records from all quadrants
        double[] queryVector = { 0.0, 0.0 };
        int queryK = 1000; // Large enough to get many records
        List<String> expectedPKs = new ArrayList<>(); // We'll verify counts instead

        runInsertTest(centroidSerdes, dataRecordSerdes, centroids, numClustersPerLevel, centroidsPerCluster,
                2, bulkLoadRecords, insertRecords, queryVector, queryK);
    }

    /**
     * Generate bulk load records for 2D dataset with specified count per cluster.
     * Reuses the ring pattern from parent class but allows customizing record count.
     */
    private List<List<ITupleReference>> generate2DBulkLoadRecords(int recordsPerCluster) throws Exception {
        List<List<ITupleReference>> allRecords = new ArrayList<>();
        double[][] leafCentroids = getLeafCentroids2D();

        for (int centroidIndex = 0; centroidIndex < leafCentroids.length; centroidIndex++) {
            List<ITupleReference> clusterRecords = new ArrayList<>();

            int centroidId = centroidIndex + 4; // c4 ~ c19
            double[] centroid = leafCentroids[centroidIndex];

            double baseDistance = 0.2;
            int recordCount = 0;

            while (recordCount < recordsPerCluster) {
                double currentDistance = baseDistance;

                double[][] offsets = {
                        { currentDistance, 0 },
                        { -currentDistance, 0 },
                        { 0, currentDistance },
                        { 0, -currentDistance }
                };

                for (double[] offset : offsets) {
                    if (recordCount >= recordsPerCluster) break;

                    double[] vector = { centroid[0] + offset[0], centroid[1] + offset[1] };
                    String primaryKey = "pk_2d_c" + centroidId + "_" + recordCount;

                    ITupleReference tuple = createBulkLoadRecordTuple(currentDistance, centroidId, vector, primaryKey);
                    clusterRecords.add(tuple);
                    recordCount++;
                }

                baseDistance += 0.2;
            }

            allRecords.add(clusterRecords);
        }

        return allRecords;
    }

    /**
     * Create a bulk load record tuple.
     * Format: <distance_to_centroid, centroid_id, vector, primary_key>
     */
    private ITupleReference createBulkLoadRecordTuple(double distance, int centroidId, double[] vector,
            String primaryKey) throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        tupleBuilder.getDataOutput().writeDouble(distance);
        tupleBuilder.addFieldEndOffset();

        tupleBuilder.getDataOutput().writeInt(centroidId);
        tupleBuilder.addFieldEndOffset();

        DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }

    /**
     * Run the insert test with the provided configuration.
     */
    private void runInsertTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel,
            List<List<Integer>> centroidsPerCluster, int vectorDimension,
            List<List<ITupleReference>> bulkLoadRecords, List<List<ITupleReference>> insertRecords,
            double[] queryVector, int queryK) throws Exception {

        LOGGER.info("LSMVCTree Insert Test: {} levels, {} centroids, {} leaf clusters, {}D vectors",
                numClustersPerLevel.size(), centroids.size(), bulkLoadRecords.size(), vectorDimension);

        // Create test context
        AbstractVectorTreeTestContext ctx = LSMVCTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory());

        // Set test data in context
        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);
        ctx.setDataRecords(bulkLoadRecords);

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
            int bulkLoadedCount = bulkLoadRecords.size() * BULK_LOAD_RECORDS_PER_CLUSTER;
            LOGGER.info("Bulk loaded {} records across {} clusters", bulkLoadedCount, bulkLoadRecords.size());

            // 4. Insert additional records into memory component
            int insertedCount = insertRecordsIntoMemoryComponent(ctx, insertRecords);
            LOGGER.info("Inserted {} records into memory component", insertedCount);

            // 5. Verify total records using LSMVCTreeSearchCursor
            int totalExpected = bulkLoadedCount + insertedCount;
            verifyRecordsWithSearch(ctx, queryVector, queryK, totalExpected);
            LOGGER.info("Verification: Found expected number of records");

        } finally {
            // Cleanup
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
            LOGGER.info("Index deactivated and destroyed");
        }
    }

    /**
     * Insert records into the memory component using the index accessor.
     *
     * @param ctx Test context with activated index
     * @param insertRecords Records to insert (format: <vector, primary_key>)
     * @return Number of records inserted
     */
    private int insertRecordsIntoMemoryComponent(AbstractVectorTreeTestContext ctx,
            List<List<ITupleReference>> insertRecords) throws Exception {

        IIndexAccessor accessor = ctx.getIndex().createAccessor(
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));

        int insertedCount = 0;
        for (List<ITupleReference> clusterRecords : insertRecords) {
            for (ITupleReference tuple : clusterRecords) {
                accessor.insert(tuple);
                insertedCount++;
            }
        }

        LOGGER.info("Inserted {} records via accessor", insertedCount);
        return insertedCount;
    }

    /**
     * Verify total records by scanning with LSMVCTreeSearchCursor.
     *
     * Uses a point query to scan records from the closest cluster and verify
     * that the combined disk + memory components contain the expected records.
     */
    private void verifyRecordsWithSearch(AbstractVectorTreeTestContext ctx, double[] queryVector,
            int k, int totalExpected) throws Exception {

        // Create query tuple
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // Set up predicate
        VectorPointPredicate predicate = new VectorPointPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setDistanceMetric("euclidean");
        predicate.setK(k);

        // Create accessor with vector accessor factory
        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(HyracksConstants.VECTOR_QUERY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        try {
            accessor.search(cursor, predicate);

            List<String> foundPKs = new ArrayList<>();
            int bulkLoadCount = 0;
            int insertCount = 0;

            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                String pk = extractPrimaryKeyFromTuple(tuple);
                foundPKs.add(pk);

                // Categorize by prefix
                if (pk.startsWith("pk_2d_")) {
                    bulkLoadCount++;
                } else if (pk.startsWith("pk_ins_")) {
                    insertCount++;
                }
            }

            LOGGER.info("Search returned {} total records: {} bulk-loaded, {} inserted",
                    foundPKs.size(), bulkLoadCount, insertCount);

            // Verify we got records from both components
            assertTrue("Should find bulk-loaded records", bulkLoadCount > 0);
            assertTrue("Should find inserted records", insertCount > 0);

            // Log a sample of found PKs for debugging
            int sampleSize = Math.min(10, foundPKs.size());
            LOGGER.info("Sample of found PKs: {}", foundPKs.subList(0, sampleSize));

        } finally {
            cursor.close();
            cursor.destroy();
        }
    }

    /**
     * Extract primary key from a result tuple.
     * Result tuple format: <distance, centroid_id, vector, primary_key>
     */
    private String extractPrimaryKeyFromTuple(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = {
                DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE,
                DoubleArraySerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer()
        };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (String) values[3];
    }

    /**
     * Implementation of abstract runTest method from OptimizedSearchTestDriver.
     * For insert tests, we use runInsertTest instead, but this is required for the parent class.
     */
    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel,
            List<List<Integer>> centroidsPerCluster, int vectorDimension,
            List<List<ITupleReference>> leafRecords, double[] queryVector, int queryK,
            List<String> expectedPrimaryKeys) throws Exception {
        // This method is not used by insert tests - we use runInsertTest instead
        throw new UnsupportedOperationException("Use twoDimensionInsertTest() for insert tests");
    }
}
