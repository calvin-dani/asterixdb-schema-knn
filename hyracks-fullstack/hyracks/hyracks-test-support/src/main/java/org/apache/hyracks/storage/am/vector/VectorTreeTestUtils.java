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

package org.apache.hyracks.storage.am.vector;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
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
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTreeDiskComponent;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings({ "rawtypes", "deprecation" })
public class VectorTreeTestUtils extends TreeIndexTestUtils {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int VECTOR_DIMENSIONS = 4;

    // Static initializer for creating predictable test structures
    private static VectorClusteringTreeStaticInitializer staticInitializer;

    private static class TestClusterData {
        final double[] clusterCentroid;
        final List<double[]> insertedVectors;
        final String clusterId;

        TestClusterData(double[] centroid, String id) {
            this.clusterCentroid = centroid.clone();
            this.insertedVectors = new ArrayList<>();
            this.clusterId = id;
        }
    }

    public void buildStaticStructure(AbstractVectorTreeTestContext ctx) throws Exception {
        int numLevels = ctx.getNumCentroidsPerLevel().size();
        List<Integer> clustersPerLevel = ctx.getNumClustersPerLevel();
        List<List<Integer>> centroidsPerCluster = ctx.getNumCentroidsPerLevel();
        List<ITupleReference> centroids = ctx.getStaticStructureCentroids();

        LSMVCTree lsmvcTree = (LSMVCTree) ctx.getIndex();

        // Create parameters map for static structure bulk load
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("numLevels", numLevels);
        parameters.put("clustersPerLevel", clustersPerLevel);
        parameters.put("centroidsPerCluster", centroidsPerCluster);
        parameters.put("maxEntriesPerPage", 100); // Default max entries per page

        // Create static structure bulk loader with parameters
        IIndexBulkLoader ssBuilder = lsmvcTree.createBulkLoader(1.0f, false, centroids.size(), parameters);

        // Add centroids to the builder level by level (BFS order)
        for (ITupleReference tuple : centroids) {
            ssBuilder.add(tuple);
        }

        // Finalize the static structure
        ssBuilder.end();

        LOGGER.info("Static structure built successfully with {} centroids across {} levels", centroids.size(),
                numLevels);
    }

    public void bulkLoadRecords(AbstractVectorTreeTestContext ctx) throws Exception {
        LSMVCTree lsmvcTree = (LSMVCTree) ctx.getIndex();
        List<List<ITupleReference>> dataRecords = ctx.getDataRecords();

        if (dataRecords == null || dataRecords.isEmpty()) {
            LOGGER.warn("No data records to bulk load");
            return;
        }

        // Create empty parameters map - createBulkLoader will add static_structure_component automatically
        // when it detects this is NOT a static structure load (no numLevels, clustersPerLevel, centroidsPerCluster)
        Map<String, Object> parameters = new HashMap<>();

        // Calculate total number of records for hint
        long totalRecords = 0;
        for (List<ITupleReference> clusterRecords : dataRecords) {
            totalRecords += clusterRecords.size();
        }

        // Create data bulk loader (not static structure)
        // The LSMVCTree.createBulkLoader will automatically detect this is a data load
        // because parameters don't contain numLevels/clustersPerLevel/centroidsPerCluster
        IIndexBulkLoader bulkLoader = lsmvcTree.createBulkLoader(1.0f, false, totalRecords, parameters);

        // Add all data records from all clusters
        for (List<ITupleReference> clusterRecords : dataRecords) {
            for (ITupleReference record : clusterRecords) {
                bulkLoader.add(record);
            }
        }

        // Finalize the data component
        bulkLoader.end();

        LOGGER.info("Bulk loaded {} records across {} clusters", totalRecords, dataRecords.size());
    }

    /**
     * Test cursor iteration and validate it returns expected records.
     * Validates that the cursor can find records with the correct tuple structure.
     *
     * Following RTree test pattern: use ctx.getIndexAccessor() for LSM-level searches
     * that coordinate across all components (memory + disk).
     *
     * Sets up the environment properly like the production VectorSearchOperatorNodePushable:
     * 1. Creates a query tuple containing the vector
     * 2. Sets up the predicate with the query tuple and field index
     * 3. Creates an accessor with IVectorBinaryAccessorFactory in its parameters
     */
    public void scanClosestLeafCluster(AbstractVectorTreeTestContext ctx) throws Exception {
        double[] queryVector = { 20.0d, 20.0d, 15.0d };

        // 1. Create query tuple containing the vector (like VectorSearchOperatorNodePushable)
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // 2. Set up predicate with query tuple reference (following RTree pattern)
        VectorPointPredicate predicate = new VectorPointPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0); // Vector is at field 0
        predicate.setDistanceMetric("euclidean");

        // 3. Create accessor with IVectorBinaryAccessorFactory in parameters
        // This is what VectorSearchOperatorNodePushable does in addAdditionalIndexAccessorParams()
        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(HyracksConstants.VECTOR_QUERY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);
        assertNotNull("Cursor should be created", cursor);

        try {
            // Open cursor with predicate (positions cursor at first result)
            accessor.search(cursor, predicate);

            try {
                // Collect all results from cursor
                List<ITupleReference> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference tuple = cursor.getTuple();
                    assertNotNull("Tuple should not be null", tuple);
                    results.add(tuple);
                }

                // Validate we got at least some results
                assertFalse("Should find some records in the cluster", results.isEmpty());

                LOGGER.info("Found {} records in cluster for query vector [{}, {}, {}]", results.size(), queryVector[0],
                        queryVector[1], queryVector[2]);

                printTupleResults(results);

                // Validate cursor state after iteration
                assertFalse("Cursor should not have more results after iteration", cursor.hasNext());

            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
    }

    /**
     * Print tuple results from the data pages.
     * Data record format (from VectorIndexTestDriver.createBulkLoadRecordTuple):
     *   <distance_to_centroid: raw double (8 bytes),
     *    centroid_id: raw int (4 bytes),
     *    primary_key: UTF8String>
     *
     * Note: Fields 0 and 1 use raw types (no ADM type tags).
     */
    private void printTupleResults(List<ITupleReference> results) throws HyracksDataException {
        // Field serializers matching the raw tuple format
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE, // Field 0: distance (raw double)
                IntegerSerializerDeserializer.INSTANCE, // Field 1: centroidId (raw int)
                new UTF8StringSerializerDeserializer() // Field 2: primary_key
        };

        for (ITupleReference tuple : results) {
            try {
                Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
                double distance = (Double) values[0];
                int centroidId = (Integer) values[1];
                String primaryKey = (String) values[2];

                System.out.println(
                        " Record: pk='" + primaryKey + "', centroidId=" + centroidId + ", distance=" + distance);
            } catch (Exception e) {
                LOGGER.error("Failed to deserialize tuple: {}", e.getMessage());
            }
        }
    }

    /**
     * Test top-K search with LSM-level accessor.
     * Following RTree test pattern for proper cursor lifecycle management.
     *
     * Sets up the environment properly like the production VectorSearchOperatorNodePushable.
     *
     * Query vector {20.0, 30.0, 20.0} matches c10 from VectorIndexTestDriver.LEAF_CENTROIDS.
     * With K=100, we expect to get all 100 records from c10's cluster.
     */
    public void topKSearch(AbstractVectorTreeTestContext ctx) throws Exception {
        // Query vector matching c10: {20.0, 30.0, 20.0} from VectorIndexTestDriver
        double[] queryVector = { 20.0d, 30.0d, 20.0d };
        int k = 100; // Number of nearest neighbors to return

        // 1. Create query tuple containing the vector
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // 2. Set up predicate with query tuple reference and K value
        VectorPointPredicate predicate = new VectorPointPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setDistanceMetric("euclidean");
        predicate.setK(k); // Set K for top-K ANN search

        // 3. Create accessor with IVectorBinaryAccessorFactory in parameters
        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(HyracksConstants.VECTOR_QUERY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        try {
            // Open cursor with predicate
            accessor.search(cursor, predicate);

            try {
                // Collect all results from cursor
                List<ITupleReference> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference tuple = cursor.getTuple();
                    results.add(tuple);
                }

                LOGGER.info("Top-K Search: Found {} records for query vector [{}, {}, {}] with K={}", results.size(),
                        queryVector[0], queryVector[1], queryVector[2], k);

                // Validate we got the expected number of results
                // c10 cluster has 100 records, so we should get exactly 100 results
                assertEquals("Top-K search should return K=" + k + " records from c10's cluster", k, results.size());

                printTupleResults(results);

                // Validate all results are from c10 (centroidId = 10)
                for (ITupleReference tuple : results) {
                    int centroidId = extractCentroidIdFromTuple(tuple);
                    assertEquals("All results should be from c10 (centroidId=10)", 10, centroidId);
                }

                LOGGER.info("Top-K Search: All {} records verified to be from c10 cluster", results.size());

            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
    }

    /**
     * Extract centroid ID from a data record tuple.
     * Data record format: <distance (raw double), centroid_id (raw int), primary_key>
     */
    private int extractCentroidIdFromTuple(ITupleReference tuple) throws HyracksDataException {
        // Field serializers - only need first two fields to extract centroidId
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE, // Field 0: distance (raw double)
                IntegerSerializerDeserializer.INSTANCE // Field 1: centroidId (raw int)
        };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (Integer) values[1];
    }

    /**
     * Test optimized search using LSMVCTreeBlockedCursor with bidirectional traversal
     * and triangle inequality termination.
     *
     * Uses test data from VectorIndexTestDriver.optimizedSearchThreeDimension():
     * - Centroid at origin [0, 0, 0]
     * - Query vector at [5, 0, 0], giving D(q, C) = 5.0
     * - 20 records at distances 1-20 along x-axis
     *
     * Tuple format: <distance_to_centroid, centroid_id, vector, primary_key>
     *
     * Expected top-5 results (by D(q, x)):
     * - pk_opt_5:  D(q,x) = 0.0  (vector [5,0,0])
     * - pk_opt_4:  D(q,x) = 1.0  (vector [4,0,0])
     * - pk_opt_6:  D(q,x) = 1.0  (vector [6,0,0])
     * - pk_opt_3:  D(q,x) = 2.0  (vector [3,0,0])
     * - pk_opt_7:  D(q,x) = 2.0  (vector [7,0,0])
     */
    public void optimizedSearch(AbstractVectorTreeTestContext ctx) throws Exception {
        // Get query configuration from context
        double[] queryVector = ctx.getQueryVector();
        int k = ctx.getQueryK();
        List<String> expectedPKs = ctx.getExpectedPrimaryKeys();

        if (queryVector == null) {
            throw new IllegalStateException("Query vector must be set in context via ctx.setQueryVector()");
        }

        // 1. Create query tuple containing the vector
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // 2. Set up predicate with query tuple reference and K value
        VectorPointPredicate predicate = new VectorPointPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setDistanceMetric("euclidean");
        predicate.setK(k);

        // 3. Create accessor with IVectorBinaryAccessorFactory in parameters
        // Also set USE_OPTIMIZED_SEARCH flag to enable LSMVCTreeBlockedCursor
        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(HyracksConstants.VECTOR_QUERY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);
        iap.getParameters().put(HyracksConstants.USE_OPTIMIZED_SEARCH, Boolean.TRUE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        // Verify we got the optimized cursor
        LOGGER.info("Created cursor type: {}", cursor.getClass().getSimpleName());

        try {
            // Open cursor with predicate
            accessor.search(cursor, predicate);

            try {
                // Collect all results from cursor
                List<ITupleReference> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference tuple = cursor.getTuple();
                    results.add(tuple);
                }

                LOGGER.info("Optimized Search: Found {} records for query vector {} with K={}", results.size(),
                        Arrays.toString(queryVector), k);

                // Print PKs immediately for debugging
                System.err.println("[optimizedSearch] Results PKs:");
                for (int i = 0; i < results.size(); i++) {
                    ITupleReference tuple = results.get(i);
                    String pk = extractPrimaryKeyFromOptimizedTuple(tuple);
                    double dxc = extractDistanceFromTuple(tuple);
                    double[] vec = extractVectorFromOptimizedTuple(tuple);
                    double dqx = computeEuclideanDistance(queryVector, vec);
                    System.err.println(String.format("  [%d] pk=%s, D(x,C)=%.2f, D(q,x)=%.2f, vec=%s", i, pk, dxc, dqx,
                            Arrays.toString(vec)));
                }

                // Validate we got the expected number of results
                assertEquals("Optimized search should return K=" + k + " records", k, results.size());

                // Print results with details
                printOptimizedSearchResults(results, queryVector);

                // Validate expected primary keys are in results (if provided)
                List<String> actualPKs = new ArrayList<>();
                for (ITupleReference tuple : results) {
                    actualPKs.add(extractPrimaryKeyFromOptimizedTuple(tuple));
                }

                if (expectedPKs != null && !expectedPKs.isEmpty()) {
                    for (String expectedPK : expectedPKs) {
                        assertTrue("Expected " + expectedPK + " in results, but got: " + actualPKs,
                                actualPKs.contains(expectedPK));
                    }
                }

                // Validate excluded primary keys are NOT in results (for delete tests)
                List<String> excludedPKs = ctx.getExcludedPrimaryKeys();
                if (excludedPKs != null && !excludedPKs.isEmpty()) {
                    for (String excludedPK : excludedPKs) {
                        assertFalse("Deleted " + excludedPK + " should NOT be in results, but got: " + actualPKs,
                                actualPKs.contains(excludedPK));
                    }
                }

                LOGGER.info("Optimized Search: All {} results verified correctly", results.size());

            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
    }

    /**
     * Test naive blocked search using LSMVCTreeBlockedCursorNaive with sequential cluster scanning
     * and top-K window collection.
     *
     * This method mirrors optimizedSearch() but uses USE_NAIVE_BLOCKED_SEARCH flag
     * instead of USE_OPTIMIZED_SEARCH, which routes to LSMVCTreeBlockedCursorNaive
     * (searchApproach=3) instead of LSMVCTreeBlockedCursor (searchApproach=1).
     *
     * Both cursors should return the same top-K results for the same query data,
     * since the difference is in pruning strategy (bidirectional + triangle inequality
     * vs sequential scan), not in correctness.
     */
    public void naiveBlockedSearch(AbstractVectorTreeTestContext ctx) throws Exception {
        // Get query configuration from context
        double[] queryVector = ctx.getQueryVector();
        int k = ctx.getQueryK();
        List<String> expectedPKs = ctx.getExpectedPrimaryKeys();

        if (queryVector == null) {
            throw new IllegalStateException("Query vector must be set in context via ctx.setQueryVector()");
        }

        // 1. Create query tuple containing the vector
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // 2. Set up predicate with query tuple reference and K value
        VectorPointPredicate predicate = new VectorPointPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setDistanceMetric("euclidean");
        predicate.setK(k);

        // 3. Create accessor with IVectorBinaryAccessorFactory in parameters
        // Set USE_NAIVE_BLOCKED_SEARCH flag to enable LSMVCTreeBlockedCursorNaive
        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(HyracksConstants.VECTOR_QUERY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);
        iap.getParameters().put(HyracksConstants.USE_NAIVE_BLOCKED_SEARCH, Boolean.TRUE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        // Verify we got the naive blocked cursor
        LOGGER.info("Created cursor type: {}", cursor.getClass().getSimpleName());

        try {
            // Open cursor with predicate
            accessor.search(cursor, predicate);

            try {
                // Collect all results from cursor
                List<ITupleReference> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference tuple = cursor.getTuple();
                    results.add(tuple);
                }

                LOGGER.info("Naive Blocked Search: Found {} records for query vector {} with K={}", results.size(),
                        Arrays.toString(queryVector), k);

                // Print PKs for debugging
                LOGGER.info("[naiveBlockedSearch] Results PKs:");
                for (int i = 0; i < results.size(); i++) {
                    ITupleReference tuple = results.get(i);
                    String pk = extractPrimaryKeyFromOptimizedTuple(tuple);
                    double dxc = extractDistanceFromTuple(tuple);
                    double[] vec = extractVectorFromOptimizedTuple(tuple);
                    double dqx = computeEuclideanDistance(queryVector, vec);
                    LOGGER.info("  [{}] pk={}, D(x,C)={}, D(q,x)={}, vec={}", i, pk, dxc, dqx,
                            Arrays.toString(vec));
                }

                // Validate we got the expected number of results
                assertEquals("Naive blocked search should return K=" + k + " records", k, results.size());

                // Print results with details
                printOptimizedSearchResults(results, queryVector);

                // Validate expected primary keys are in results (if provided)
                List<String> actualPKs = new ArrayList<>();
                for (ITupleReference tuple : results) {
                    actualPKs.add(extractPrimaryKeyFromOptimizedTuple(tuple));
                }

                if (expectedPKs != null && !expectedPKs.isEmpty()) {
                    for (String expectedPK : expectedPKs) {
                        assertTrue("Expected " + expectedPK + " in results, but got: " + actualPKs,
                                actualPKs.contains(expectedPK));
                    }
                }

                // Validate excluded primary keys are NOT in results (for delete tests)
                List<String> excludedPKs = ctx.getExcludedPrimaryKeys();
                if (excludedPKs != null && !excludedPKs.isEmpty()) {
                    for (String excludedPK : excludedPKs) {
                        assertFalse("Deleted " + excludedPK + " should NOT be in results, but got: " + actualPKs,
                                actualPKs.contains(excludedPK));
                    }
                }

                LOGGER.info("Naive Blocked Search: All {} results verified correctly", results.size());

            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
    }

    /**
     * Insert records into the memory component using the index accessor.
     *
     * @param ctx Test context with active index
     * @param insertRecords Records grouped by cluster, tuple format: <vector, primary_key>
     * @return Number of records inserted
     */
    public int insertRecordsIntoMemoryComponent(AbstractVectorTreeTestContext ctx,
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
     * Delete records from the index using the index accessor.
     * Delete tuple format: <vector, primary_key> (same as insert).
     *
     * @param ctx Test context with active index
     * @param deleteTuples Tuples to delete, format: <vector, primary_key>
     * @return Number of records deleted
     */
    public int deleteRecordsFromIndex(AbstractVectorTreeTestContext ctx, List<ITupleReference> deleteTuples)
            throws Exception {

        IIndexAccessor accessor = ctx.getIndex().createAccessor(
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));

        int deletedCount = 0;
        for (ITupleReference tuple : deleteTuples) {
            accessor.delete(tuple);
            deletedCount++;
        }

        LOGGER.info("Deleted {} records via accessor", deletedCount);
        return deletedCount;
    }

    /**
     * Verify records by scanning with LSMVCTreeSearchCursor (regular, non-optimized).
     * Checks that records from both disk (bulk-loaded, "pk_2d_" or "pk_opt_" prefix)
     * and memory (inserted, "pk_ins_" prefix) components are found.
     *
     * @param ctx Test context with active index
     * @param queryVector Query vector for search
     * @param k Number of results to retrieve
     */
    public void verifyRecordsWithSearch(AbstractVectorTreeTestContext ctx, double[] queryVector, int k)
            throws Exception {

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
                String pk = extractPrimaryKeyFromOptimizedTuple(tuple);
                foundPKs.add(pk);

                if (pk.startsWith("pk_ins_")) {
                    insertCount++;
                } else {
                    bulkLoadCount++;
                }
            }

            LOGGER.info("Search returned {} total records: {} bulk-loaded, {} inserted", foundPKs.size(), bulkLoadCount,
                    insertCount);

            // Verify we got records from both components
            assertTrue("Should find bulk-loaded records", bulkLoadCount > 0);
            assertTrue("Should find inserted records", insertCount > 0);

            int sampleSize = Math.min(10, foundPKs.size());
            LOGGER.info("Sample of found PKs: {}", foundPKs.subList(0, sampleSize));

        } finally {
            cursor.close();
            cursor.destroy();
        }
    }

    /**
     * Print detailed results from optimized search.
     * Tuple format: <distance_to_centroid, centroid_id, vector, primary_key>
     */
    private void printOptimizedSearchResults(List<ITupleReference> results, double[] queryVector)
            throws HyracksDataException {
        LOGGER.info("Optimized Search Results:");
        for (int i = 0; i < results.size(); i++) {
            ITupleReference tuple = results.get(i);

            // Extract fields
            double dxc = extractDistanceFromTuple(tuple);
            int centroidId = extractCentroidIdFromTuple(tuple);
            double[] vector = extractVectorFromOptimizedTuple(tuple);
            String pk = extractPrimaryKeyFromOptimizedTuple(tuple);

            // Compute actual D(q, x)
            double dqx = computeEuclideanDistance(queryVector, vector);

            LOGGER.info("  [{}] pk={}, D(x,C)={}, D(q,x)={}, vector={}", i, pk, dxc, dqx, Arrays.toString(vector));
        }
    }

    /**
     * Extract distance_to_centroid (field 0) from tuple.
     */
    private double extractDistanceFromTuple(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (Double) values[0];
    }

    /**
     * Extract vector (field 2) from optimized search tuple.
     * Tuple format: <distance, centroid_id, vector, primary_key>
     */
    private double[] extractVectorFromOptimizedTuple(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE, // Field 0: distance
                IntegerSerializerDeserializer.INSTANCE, // Field 1: centroidId
                DoubleArraySerializerDeserializer.INSTANCE // Field 2: vector
        };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (double[]) values[2];
    }

    /**
     * Extract primary_key (field 3) from optimized search tuple.
     * Tuple format: <distance, centroid_id, vector, primary_key>
     */
    public String extractPrimaryKeyFromOptimizedTuple(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE, // Field 0: distance
                IntegerSerializerDeserializer.INSTANCE, // Field 1: centroidId
                DoubleArraySerializerDeserializer.INSTANCE, // Field 2: vector
                new UTF8StringSerializerDeserializer() // Field 3: primary_key
        };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (String) values[3];
    }

    /**
     * Compute Euclidean distance between two vectors.
     */
    private double computeEuclideanDistance(double[] v1, double[] v2) {
        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public void clusterRecords(AbstractVectorTreeTestContext ctx) throws Exception {
        LSMVCTree lsmvcTree = (LSMVCTree) ctx.getIndex();
        LSMVCTreeDiskComponent staticStructure = lsmvcTree.getStaticStructure();
        IIndexAccessor accessor = staticStructure.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        VectorClusteringTree.VectorClusteringTreeAccessor vcTreeAccessor =
                (VectorClusteringTree.VectorClusteringTreeAccessor) accessor;
        IVectorDistanceFunction defaultDistanceFunction = VectorUtils::calculateEuclideanDistance;
        ClusterSearchResult searchResult =
                vcTreeAccessor.findClosestLeafCentroid(new double[] { 21.0d, 31.0d, 21.0d }, defaultDistanceFunction);
    }

    public List<TestClusterData> insertRecordsIntoMultipleClusters(AbstractVectorTreeTestContext ctx) throws Exception {
        List<TestClusterData> clusterData = new ArrayList<>();

        // Test clusters from different regions of the hierarchical structure
        double[][] testCentroids = { { 22.0d, 22.0d, 15.0d, 10.0d }, // Root region 0, Interior 0, Leaf 0
                { 17.0d, 19.5d, 20.0d, 10.5d }, // Root region 0, Interior 0, Leaf 0 (variation)
                { -22.0d, -22.0d, -22.0d, -10.0d }, // Root region 1, Interior 2, Leaf 4
                { -19.0d, -19.5d, -20.0d, -9.5d }, // Root region 1, Interior 2, Leaf 4 (variation)
                { 25.0d, -17.0d, 22.0d, 14.0d }, // Root region 0, Interior 1, Leaf 2
                { -17.0d, 23.0d, -18.0d, -6.0d } // Root region 1, Interior 3, Leaf 6
        };

        String[] clusterIds =
                { "cluster_0_0", "cluster_0_1", "cluster_4_0", "cluster_4_1", "cluster_2_0", "cluster_6_0" };

        for (int i = 0; i < 1; i++) {
            TestClusterData cluster = new TestClusterData(testCentroids[i], clusterIds[i]);

            // Insert 20 records near each test centroid
            List<double[]> insertedVectors = insertRecordsIntoCluster(ctx, testCentroids[i], 20);
            cluster.insertedVectors.addAll(insertedVectors);

            clusterData.add(cluster);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Inserted {} records into cluster '{}' with centroid [{}, {}, {}, {}]",
                        insertedVectors.size(), clusterIds[i], testCentroids[i][0], testCentroids[i][1],
                        testCentroids[i][2], testCentroids[i][3]);
            }
        }

        return clusterData;
    }

    /**
     * Insert records into a cluster around the specified centroid.
     * Each record contains a vector and a primary key (string).
     */
    private List<double[]> insertRecordsIntoCluster(AbstractVectorTreeTestContext ctx, double[] centroid, int count)
            throws Exception {
        List<double[]> vectors = new ArrayList<>();
        IIndexAccessor accessor = ctx.getIndexAccessor();
        /* TODO: replace arbitrary random */
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            // Generate vector near the centroid with some noise
            double[] vector = new double[VECTOR_DIMENSIONS];
            for (int j = 0; j < VECTOR_DIMENSIONS; j++) {
                vector[j] = centroid[j] + (random.nextFloat() - 0.5f) * 0.5f; // Small noise around centroid
            }

            // Create tuple with vector and primary key: <vector, primary_key>
            String primaryKey = "pk_" + i;
            ITupleReference tuple = VectorTreeTestUtils.createVectorTuple(vector, primaryKey);
            System.out.println(
                    " Inserting tuple with primary key: " + primaryKey + " vector: " + Arrays.toString(vector));
            accessor.insert(tuple);
            vectors.add(vector);
        }

        return vectors;
    }

    @Override
    protected CheckTuple createCheckTuple(int numFields, int numKeyFields) {
        return new VectorCheckTuple(numFields, numKeyFields);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CheckTuple createIntCheckTuple(int[] fieldValues, int numKeyFields) {
        VectorCheckTuple checkTuple = new VectorCheckTuple(fieldValues.length, numKeyFields);
        for (int v : fieldValues) {
            checkTuple.appendField((Comparable) Integer.valueOf(v));
        }
        return checkTuple;
    }

    @Override
    protected void setIntKeyFields(int[] fieldValues, int numKeyFields, int maxValue, Random rnd) {
        for (int j = 0; j < numKeyFields; j++) {
            fieldValues[j] = rnd.nextInt() % maxValue;
        }
    }

    @Override
    protected void setIntPayloadFields(int[] fieldValues, int numKeyFields, int numFields) {
        for (int j = numKeyFields; j < numFields; j++) {
            fieldValues[j] = j;
        }
    }

    @Override
    protected Collection createCheckTuplesCollection() {
        return new TreeSet<>();
    }

    @Override
    protected ArrayTupleBuilder createDeleteTupleBuilder(IIndexTestContext ctx) {
        return new ArrayTupleBuilder(ctx.getKeyFieldCount());
    }

    @Override
    protected ISearchPredicate createNullSearchPredicate() {
        return null;
    }

    @Override
    public void checkExpectedResults(IIndexCursor cursor, Collection checkTuples, ISerializerDeserializer[] fieldSerdes,
            int keyFieldCount, Iterator<CheckTuple> checkIter) throws Exception {
        // Implementation will be added based on vector-specific requirements
        throw new UnsupportedOperationException("Vector-specific implementation needed");
    }

    @Override
    protected boolean checkDiskOrderScanResult(ITupleReference tuple, CheckTuple checkTuple, IIndexTestContext ctx) {
        // Vector-specific disk order scan result checking
        // For now, just return true as a placeholder
        return true;
    }

    /**
     * Generate random vector tuples and insert them into the index
     */
    @SuppressWarnings("unchecked")
    public void insertVectorTuples(AbstractVectorTreeTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int vectorDimensions = ctx.getVectorDimensions();

        for (int i = 0; i < numTuples; i++) {
            if (LOGGER.isInfoEnabled()) {
                if ((i + 1) % (numTuples / Math.min(10, numTuples)) == 0) {
                    LOGGER.info("Inserting Vector Tuple " + (i + 1) + "/" + numTuples);
                }
            }

            // Create random vector data
            Object[] fieldValues = new Object[fieldCount];

            // Set vector fields
            for (int j = 0; j < numKeyFields; j++) {
                if (ctx.getFieldSerdes()[j] instanceof DoubleArraySerializerDeserializer) {
                    double[] vector = generateRandomVector(vectorDimensions, rnd);
                    fieldValues[j] = vector;
                } else {
                    // String field
                    fieldValues[j] = generateRandomString(5 + rnd.nextInt(10), rnd);
                }
            }

            // Set metadata fields
            for (int j = numKeyFields; j < fieldCount; j++) {
                if (ctx.getFieldSerdes()[j] instanceof DoubleArraySerializerDeserializer) {
                    double[] vector = generateRandomVector(vectorDimensions, rnd);
                    fieldValues[j] = vector;
                } else {
                    // String metadata
                    fieldValues[j] = "metadata_" + i + "_" + j;
                }
            }

            // Create tuple and insert
            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), fieldValues);

            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());

                // Create check tuple for validation
                VectorCheckTuple checkTuple = new VectorCheckTuple(fieldCount, numKeyFields);
                for (Object value : fieldValues) {
                    if (value instanceof double[]) {
                        checkTuple.appendField(new VectorCheckTuple.DoubleArrayWrapper((double[]) value));
                    } else {
                        checkTuple.appendField((Comparable) value);
                    }
                }
                ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());

            } catch (HyracksDataException e) {
                // Ignore duplicate key insertions
                if (!e.matches(ErrorCode.DUPLICATE_KEY)) {
                    throw e;
                }
            }
        }
    }

    /**
     * Insert mixed vector and string tuples
     */
    public void insertMixedTuples(AbstractVectorTreeTestContext ctx, int numTuples, Random rnd) throws Exception {
        insertVectorTuples(ctx, numTuples, rnd);
    }

    /**
     * Insert edge case vectors (zero vectors, unit vectors, etc.)
     */
    @SuppressWarnings("unchecked")
    public void insertEdgeCaseVectors(AbstractVectorTreeTestContext ctx, int numTuples, Random rnd) throws Exception {
        int fieldCount = ctx.getFieldCount();
        int numKeyFields = ctx.getKeyFieldCount();
        int vectorDimensions = ctx.getVectorDimensions();

        for (int i = 0; i < numTuples; i++) {
            Object[] fieldValues = new Object[fieldCount];

            // Set vector fields with edge cases
            for (int j = 0; j < numKeyFields; j++) {
                if (ctx.getFieldSerdes()[j] instanceof DoubleArraySerializerDeserializer) {
                    double[] vector;
                    int caseType = i % 4;
                    switch (caseType) {
                        case 0: // Zero vector
                            vector = new double[vectorDimensions];
                            break;
                        case 1: // Unit vector
                            vector = generateUnitVector(vectorDimensions, rnd);
                            break;
                        case 2: // Large values
                            vector = generateLargeVector(vectorDimensions, rnd);
                            break;
                        default: // Small values
                            vector = generateSmallVector(vectorDimensions, rnd);
                            break;
                    }
                    fieldValues[j] = vector;
                } else {
                    fieldValues[j] = "edge_case_" + i + "_" + j;
                }
            }

            // Set metadata fields
            for (int j = numKeyFields; j < fieldCount; j++) {
                fieldValues[j] = "edge_metadata_" + i + "_" + j;
            }

            TupleUtils.createTuple(ctx.getTupleBuilder(), ctx.getTuple(), ctx.getFieldSerdes(), fieldValues);

            try {
                ctx.getIndexAccessor().insert(ctx.getTuple());

                VectorCheckTuple checkTuple = new VectorCheckTuple(fieldCount, numKeyFields);
                for (Object value : fieldValues) {
                    if (value instanceof double[]) {
                        checkTuple.appendField(new VectorCheckTuple.DoubleArrayWrapper((double[]) value));
                    } else {
                        checkTuple.appendField((Comparable) value);
                    }
                }
                ctx.insertCheckTuple(checkTuple, ctx.getCheckTuples());

            } catch (HyracksDataException e) {
                if (!e.matches(ErrorCode.DUPLICATE_KEY)) {
                    throw e;
                }
            }
        }
    }

    /**
     * Placeholder implementations for required abstract methods from TreeIndexTestUtils
     */
    public void checkPointSearches(AbstractVectorTreeTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Point Searches (placeholder).");
        }
        // TODO: Implement vector-specific point searches
    }

    public void checkScan(AbstractVectorTreeTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Scan (placeholder).");
        }
        // TODO: Implement vector-specific scan validation
    }

    public void checkDiskOrderScan(AbstractVectorTreeTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Disk Order Scan (placeholder).");
        }
        // TODO: Implement vector-specific disk order scan
    }

    public void checkRangeSearch(AbstractVectorTreeTestContext ctx, ITupleReference lowKey, ITupleReference highKey,
            boolean lowKeyInclusive, boolean highKeyInclusive) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Range Search (placeholder).");
        }
        // TODO: Implement vector-specific range searches
    }

    public void checkVectorSimilaritySearches(AbstractVectorTreeTestContext ctx) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Vector Similarity Searches (placeholder).");
        }
        // TODO: Implement vector similarity searches (k-NN, etc.)
    }

    // Utility methods for vector generation
    private double[] generateRandomVector(int dimensions, Random rnd) {
        double[] vector = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = rnd.nextFloat() * 100.0f - 50.0f; // Range [-50, 50]
        }
        return vector;
    }

    private double[] generateUnitVector(int dimensions, Random rnd) {
        double[] vector = new double[dimensions];
        int nonZeroIndex = rnd.nextInt(dimensions);
        vector[nonZeroIndex] = 1.0f;
        return vector;
    }

    private double[] generateLargeVector(int dimensions, Random rnd) {
        double[] vector = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = rnd.nextFloat() * 10000.0f;
        }
        return vector;
    }

    private double[] generateSmallVector(int dimensions, Random rnd) {
        double[] vector = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = rnd.nextFloat() * 0.01f;
        }
        return vector;
    }

    private String generateRandomString(int length, Random rnd) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = (char) ('a' + rnd.nextInt(26));
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Initialize static tree structure using VectorClusteringTreeStaticInitializer
     * Fixed to use proper cluster tuple format for leaf frames
     */
    public static void initializeStaticStructure(AbstractVectorTreeTestContext ctx,
            org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig config)
            throws Exception {

        // Create cluster tuples for leaf frames instead of data tuples  
        // Leaf frames expect format: <cid, centroid, metadata_pointer>
        List<ITupleReference> clusterTuples = new ArrayList<>();

        int totalTuples = config.numLeafPages * config.tuplesPerLeaf;
        for (int i = 0; i < totalTuples; i++) {
            int clusterId = 200 + i; // Arbitrary cluster IDs starting from 200
            double[] centroid = generatePredictableVector(4, i); // 4D vectors
            int metadataPointer = 1000 + i; // Arbitrary metadata pointers

            clusterTuples.add(createClusterTuple(clusterId, centroid, metadataPointer));
        }

        // Initialize the static structure
        VectorClusteringTree vectorTree = (VectorClusteringTree) ctx.getIndex();

        VectorClusteringTreeStaticInitializer initializer = new VectorClusteringTreeStaticInitializer(vectorTree);

        initializer.initializeStaticStructure(config, clusterTuples);
        staticInitializer = initializer;
    }

    /**
     * Generate predictable vector for testing
     */
    private static double[] generatePredictableVector(int dimensions, int index) {
        double[] vector = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = (float) (index + i * 0.1);
        }
        return vector;
    }

    /**
     * Get the current static initializer
     */
    public static VectorClusteringTreeStaticInitializer getStaticInitializer() {
        return staticInitializer;
    }

    /**
     * Clean up static initializer
     */
    public static void cleanupStaticInitializer() throws Exception {
        if (staticInitializer != null) {
            staticInitializer = null;
        }
    }

    /**
     * Create a tuple reference containing a vector and metadata
     * This is a utility method for creating test tuples in vector cursor tests
     */
    public static ITupleReference createVectorTuple(double[] vector, String metadata) throws HyracksDataException {
        // Create field serializers for vector and metadata
        ISerializerDeserializer[] fieldSerdes =
                new ISerializerDeserializer[] { DoubleArraySerializerDeserializer.INSTANCE,
                        new org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer() };

        // Create tuple builder
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tuple = new ArrayTupleReference();

        // Create field values
        Object[] fieldValues = new Object[] { vector, metadata };

        // Build the tuple
        TupleUtils.createTuple(tupleBuilder, tuple, fieldSerdes, fieldValues);

        return tuple;
    }

    /**
     * Create a cluster tuple for leaf frames with format: <cid, centroid, metadata_pointer>
     */
    public static ITupleReference createClusterTuple(int clusterId, double[] centroid, int metadataPointer)
            throws HyracksDataException {
        try {
            // Use ArrayTupleBuilder to create proper cluster tuple
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);

            // Add CID field (field 0) - using IntegerSerializerDeserializer.INSTANCE
            tupleBuilder.addField(
                    org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer.INSTANCE,
                    clusterId);

            // Add centroid field (field 1) - using DoubleArraySerializerDeserializer.INSTANCE
            tupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, centroid);

            // Add metadata pointer field (field 2)
            tupleBuilder.addField(
                    org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer.INSTANCE,
                    metadataPointer);

            // Create the tuple reference
            ArrayTupleReference tupleRef = new ArrayTupleReference();
            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

            return tupleRef;
        } catch (Exception e) {
            throw new HyracksDataException("Failed to create cluster tuple", e);
        }
    }

    /**
     * Initialize the default 3-level tree structure for comprehensive testing:
     * - Root: 2 centroids  
     * - Interior: 4 centroids (2 per root)
     * - Leaf: 8 clusters (2 per interior)
     * Each level uses 4D centroids
     */
    public static void initializeThreeLevelStructure(AbstractVectorTreeTestContext ctx) throws Exception {
        VectorClusteringTree vectorTree = (VectorClusteringTree) ctx.getIndex();

        VectorClusteringTreeStaticInitializer initializer = new VectorClusteringTreeStaticInitializer(vectorTree);

        // Use the specialized 3-level structure directly
        initializer.initializeThreeLevelStructure();
        staticInitializer = initializer;
    }
}
