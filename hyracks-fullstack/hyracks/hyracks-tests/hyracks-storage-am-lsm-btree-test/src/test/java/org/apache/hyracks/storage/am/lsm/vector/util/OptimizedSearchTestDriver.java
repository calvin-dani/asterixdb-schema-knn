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
package org.apache.hyracks.storage.am.lsm.vector.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.junit.Test;

/**
 * Test driver for optimized search (LSMVCTreeBlockedCursor) tests.
 *
 * Generates test data with tuple format: <distance_to_centroid, centroid_id, vector, primary_key>
 * This format includes the vector field required for computing D(q, x) during optimized search.
 *
 * Separated from VectorIndexTestDriver to avoid tuple format conflicts with bulk load tests.
 */
public abstract class OptimizedSearchTestDriver {

    /**
     * A single query case: query vector, K, expected primary keys, and optionally excluded primary keys.
     * Multiple query cases can be passed to runTest() to verify different search scenarios
     * (e.g., single-cluster vs multi-cluster, post-delete) within the same test dataset.
     */
    protected static class QueryCase {
        public final double[] queryVector;
        public final int queryK;
        public final List<String> expectedPrimaryKeys;
        public final List<String> excludedPrimaryKeys; // null = no exclusion check

        public QueryCase(double[] queryVector, int queryK, List<String> expectedPrimaryKeys) {
            this(queryVector, queryK, expectedPrimaryKeys, null);
        }

        public QueryCase(double[] queryVector, int queryK, List<String> expectedPrimaryKeys,
                List<String> excludedPrimaryKeys) {
            this.queryVector = queryVector;
            this.queryK = queryK;
            this.expectedPrimaryKeys = expectedPrimaryKeys;
            this.excludedPrimaryKeys = excludedPrimaryKeys;
        }
    }

    // Centroid at origin for simple distance calculations
    protected static final double[] OPTIMIZED_SEARCH_CENTROID = { 0.0, 0.0, 0.0 };

    // ==================== 2D Two-Layer Dataset ====================
    //
    // Level 0 (Root): 1 cluster with 4 centroids - quadrant centers
    //   c0: [50, 50]   → Q1 (+x, +y)
    //   c1: [-50, 50]  → Q2 (-x, +y)
    //   c2: [-50, -50] → Q3 (-x, -y)
    //   c3: [50, -50]  → Q4 (+x, -y)
    //
    // Level 1 (Leaf): 4 clusters with 4 centroids each = 16 leaf centroids
    //   Q1 (under c0): c4[25,25], c5[75,25], c6[25,75], c7[75,75]
    //   Q2 (under c1): c8[-75,25], c9[-25,25], c10[-75,75], c11[-25,75]
    //   Q3 (under c2): c12[-75,-75], c13[-25,-75], c14[-75,-25], c15[-25,-25]
    //   Q4 (under c3): c16[25,-75], c17[75,-75], c18[25,-25], c19[75,-25]
    //
    // Visual layout:
    //       y
    //       │
    //   100 ┼─────────────────────────────────
    //       │ c10    c11  │  c6     c7
    //       │[-75,75][-25,75]│[25,75][75,75]
    //    50 ┼──────c1──────┼──────c0─────────
    //       │ c8     c9   │  c4     c5
    //       │[-75,25][-25,25]│[25,25][75,25]
    //     0 ┼───────────────┼────────────────x
    //       │ c14    c15  │  c18    c19
    //       │[-75,-25][-25,-25]│[25,-25][75,-25]
    //   -50 ┼──────c2──────┼──────c3─────────
    //       │ c12    c13  │  c16    c17
    //       │[-75,-75][-25,-75]│[25,-75][75,-75]
    //  -100 ┼─────────────────────────────────
    //      -100   -50     0      50    100

    // 16 leaf centroids for 2D dataset (c4 ~ c19)
    private static final double[][] LEAF_CENTROIDS_2D = {
            // Q1 (under c0): c4, c5, c6, c7
            { 25.0, 25.0 }, { 75.0, 25.0 }, { 25.0, 75.0 }, { 75.0, 75.0 },
            // Q2 (under c1): c8, c9, c10, c11
            { -75.0, 25.0 }, { -25.0, 25.0 }, { -75.0, 75.0 }, { -25.0, 75.0 },
            // Q3 (under c2): c12, c13, c14, c15
            { -75.0, -75.0 }, { -25.0, -75.0 }, { -75.0, -25.0 }, { -25.0, -25.0 },
            // Q4 (under c3): c16, c17, c18, c19
            { 25.0, -75.0 }, { 75.0, -75.0 }, { 25.0, -25.0 }, { 75.0, -25.0 } };

    protected abstract void runTest(ISerializerDeserializer[] centroidSerdes,
            ISerializerDeserializer[] dataRecordSerdes, List<ITupleReference> centroids,
            List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster, int vectorDimension,
            List<List<ITupleReference>> leafRecords, List<QueryCase> queryCases) throws Exception;

    /**
     * Build query cases for the 3D single-centroid test.
     * Default: query at [5,0,0], K=5, expect records at distances 3-7 from origin.
     * Subclasses override to provide insert-aware or alternative query cases.
     */
    protected List<QueryCase> build3DQueryCases() {
        List<QueryCase> queryCases = new ArrayList<>();
        queryCases.add(new QueryCase(new double[] { 5.0, 0.0, 0.0 }, 5,
                Arrays.asList("pk_opt_5", "pk_opt_4", "pk_opt_6", "pk_opt_3", "pk_opt_7")));
        return queryCases;
    }

    /**
     * Build query cases for the 2D two-layer test.
     * Default: query at [30,30], K=5, expect top-5 bulk-loaded records from c4.
     * Subclasses override to provide insert-aware or multi-cluster query cases.
     */
    protected List<QueryCase> build2DQueryCases() {
        List<QueryCase> queryCases = new ArrayList<>();
        // Query at [30, 30] closest to centroid c4 at [25, 25]
        // D(q, c4) = sqrt((30-25)^2 + (30-25)^2) = sqrt(50) ≈ 7.07
        // Expected top-5: records from c4 farthest from centroid in +x/+y directions (closest to query)
        // pk_2d_c4_48: [27.6,25], d=2.6, D(q,x)=√30.76≈5.55
        // pk_2d_c4_44: [27.4,25], d=2.4, D(q,x)=√31.76≈5.64
        // pk_2d_c4_46: [25,27.4], d=2.4, D(q,x)=√31.76≈5.64
        // pk_2d_c4_40: [27.2,25], d=2.2, D(q,x)=√32.84≈5.73
        // pk_2d_c4_42: [25,27.2], d=2.2, D(q,x)=√32.84≈5.73
        queryCases.add(new QueryCase(new double[] { 30.0, 30.0 }, 5,
                Arrays.asList("pk_2d_c4_48", "pk_2d_c4_44", "pk_2d_c4_46", "pk_2d_c4_40", "pk_2d_c4_42")));
        return queryCases;
    }

    /**
     * Test data for optimized search (LSMVCTreeBlockedCursor).
     * Uses a simpler single-cluster structure for testing bidirectional traversal
     * and triangle inequality termination.
     *
     * Centroid at origin [0, 0, 0].
     * Records placed at integer distances 1-20 along x-axis.
     * Query at [5, 0, 0] gives D(q, C) = 5.0, pivot between records at D(x,C)=5 and D(x,C)=6.
     *
     * Tuple format: <distance_to_centroid, centroid_id, vector, primary_key>
     */
    @Test
    public void optimizedSearchThreeDimension() throws Exception {
        // Centroid serializers: centroid ID + Double array vector (3D)
        ISerializerDeserializer[] centroidSerdes =
                { IntegerSerializerDeserializer.INSTANCE, DoubleArraySerializerDeserializer.INSTANCE };

        // Data record serializers for optimized search: distance + centroid_id + vector + primary key
        ISerializerDeserializer[] dataRecordSerdes =
                { DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        DoubleArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        // Single centroid at origin
        List<ITupleReference> centroids = new ArrayList<>();
        centroids.add(createCentroidTuple(0, OPTIMIZED_SEARCH_CENTROID));

        // Single-level structure: 1 cluster with 1 centroid
        List<Integer> numClustersPerLevel = Arrays.asList(1);
        List<List<Integer>> centroidsPerCluster = Arrays.asList(Arrays.asList(1));

        // Generate records with controlled distances
        List<List<ITupleReference>> dataRecords = generateOptimizedSearchRecords();

        // Query cases - overridable by subclasses
        List<QueryCase> queryCases = build3DQueryCases();

        runTest(centroidSerdes, dataRecordSerdes, centroids, numClustersPerLevel, centroidsPerCluster, 3, dataRecords,
                queryCases);
    }

    /**
     * Test data for 2D two-layer optimized search.
     * Uses a grid structure with 4 quadrants, each subdivided into 4 sub-squares.
     *
     * Structure:
     * - Level 0: 1 cluster with 4 centroids (quadrant centers)
     * - Level 1: 4 clusters with 4 centroids each (16 leaf centroids total)
     *
     * Data records: 50 records per leaf centroid = 800 total records
     * Tuple format: <distance_to_centroid, centroid_id, vector, primary_key>
     */
    @Test
    public void twoDimensionTwoLevels() throws Exception {
        // Centroid serializers: centroid ID + Double array vector (2D)
        ISerializerDeserializer[] centroidSerdes =
                { IntegerSerializerDeserializer.INSTANCE, DoubleArraySerializerDeserializer.INSTANCE };

        // Data record serializers for optimized search: distance + centroid_id + vector + primary key
        ISerializerDeserializer[] dataRecordSerdes =
                { DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        DoubleArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        // Generate centroids
        List<ITupleReference> centroids = new ArrayList<>();

        // Level 0: 4 quadrant centroids (c0-c3)
        centroids.add(createCentroidTuple(0, new double[] { 50.0, 50.0 })); // Q1
        centroids.add(createCentroidTuple(1, new double[] { -50.0, 50.0 })); // Q2
        centroids.add(createCentroidTuple(2, new double[] { -50.0, -50.0 })); // Q3
        centroids.add(createCentroidTuple(3, new double[] { 50.0, -50.0 })); // Q4

        // Level 1: 16 leaf centroids (c4-c19)
        for (int i = 0; i < LEAF_CENTROIDS_2D.length; i++) {
            centroids.add(createCentroidTuple(i + 4, LEAF_CENTROIDS_2D[i]));
        }

        // Structure: 2 levels
        // Level 0: 1 cluster with 4 centroids
        // Level 1: 4 clusters with 4 centroids each
        List<Integer> numClustersPerLevel = Arrays.asList(1, 4);
        List<List<Integer>> centroidsPerCluster = Arrays.asList(Arrays.asList(4), // Level 0: 1 cluster with 4 centroids
                Arrays.asList(4, 4, 4, 4) // Level 1: 4 clusters with 4 centroids each
        );

        // Generate 50 records per leaf centroid
        List<List<ITupleReference>> dataRecords = generate2DDataRecords();

        // Query cases - overridable by subclasses
        List<QueryCase> queryCases = build2DQueryCases();

        runTest(centroidSerdes, dataRecordSerdes, centroids, numClustersPerLevel, centroidsPerCluster, 2, dataRecords,
                queryCases);
    }

    /**
     * Generate records for 2D two-layer dataset.
     * 50 records per leaf centroid, arranged in concentric rings around each centroid.
     *
     * For each leaf centroid, records are placed at:
     * - 4 records per distance ring (±x, ±y directions)
     * - Distances: 0.2, 0.4, 0.6, ... (increment of 0.2)
     *
     * Example for c4 at [25, 25]:
     * - Ring 1 (d=0.2): [25.2,25], [24.8,25], [25,25.2], [25,24.8]
     * - Ring 2 (d=0.4): [25.4,25], [24.6,25], [25,25.4], [25,24.6]
     * - ...
     */
    private List<List<ITupleReference>> generate2DDataRecords() throws Exception {
        List<List<ITupleReference>> allRecords = new ArrayList<>();

        // Generate 50 records for each of the 16 leaf centroids (c4-c19)
        for (int centroidIndex = 0; centroidIndex < LEAF_CENTROIDS_2D.length; centroidIndex++) {
            List<ITupleReference> clusterRecords = new ArrayList<>();

            int centroidId = centroidIndex + 4; // c4 ~ c19
            double[] centroid = LEAF_CENTROIDS_2D[centroidIndex];

            double baseDistance = 0.2;
            int recordCount = 0;

            // Generate records in increasing distance rings
            while (recordCount < 50) {
                double currentDistance = baseDistance;

                // 4 records per ring (±x, ±y directions)
                double[][] offsets = { { currentDistance, 0 }, // +x
                        { -currentDistance, 0 }, // -x
                        { 0, currentDistance }, // +y
                        { 0, -currentDistance } // -y
                };

                for (double[] offset : offsets) {
                    if (recordCount >= 50)
                        break;

                    // Compute actual vector position
                    double[] vector = { centroid[0] + offset[0], centroid[1] + offset[1] };
                    String primaryKey = "pk_2d_c" + centroidId + "_" + recordCount;

                    ITupleReference tuple = create2DRecordTuple(currentDistance, centroidId, vector, primaryKey);
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
     * Helper method to create a 2D record tuple for optimized search testing.
     * Format: <distance_to_centroid, centroid_id, vector, primary_key>
     */
    private ITupleReference create2DRecordTuple(double distance, int centroidId, double[] vector, String primaryKey)
            throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        // Field 0: distance_to_centroid (raw double - 8 bytes)
        tupleBuilder.getDataOutput().writeDouble(distance);
        tupleBuilder.addFieldEndOffset();

        // Field 1: centroid_id (raw int - 4 bytes)
        tupleBuilder.getDataOutput().writeInt(centroidId);
        tupleBuilder.addFieldEndOffset();

        // Field 2: vector (serialized with DoubleArraySerializerDeserializer)
        DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // Field 3: primary_key (UTF8 string)
        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }

    /**
     * Helper method to create a centroid tuple with format: <centroid_id, vector>
     */
    protected ITupleReference createCentroidTuple(int centroidId, double[] embedding) throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                DoubleArraySerializerDeserializer.INSTANCE };
        Object[] fieldValues = new Object[] { centroidId, embedding };

        TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);

        return tupleRef;
    }

    // ==================== Insert Test Support ====================

    protected static final int INSERT_RECORDS_PER_CLUSTER = 40;

    /**
     * Generate insert records for all leaf centroids.
     * Extracts leaf centroid vectors from the centroids list and generates
     * INSERT_RECORDS_PER_CLUSTER records per leaf centroid in concentric rings.
     *
     * Insert tuple format: <vector, primary_key>
     * PK format: "pk_ins_c{centroidId}_{recordIndex}"
     *
     * @param centroids Full centroid list (all levels)
     * @param centroidSerdes Serializers for centroid tuple format <centroid_id, vector>
     * @param centroidsPerCluster Per-level centroid counts, used to identify leaf centroids
     * @param vectorDimension Dimension of vectors (determines direction offsets)
     * @return List of lists, where each inner list contains insert tuples for one leaf cluster
     */
    protected List<List<ITupleReference>> generateInsertRecords(List<ITupleReference> centroids,
            ISerializerDeserializer[] centroidSerdes, List<List<Integer>> centroidsPerCluster, int vectorDimension)
            throws Exception {

        // Determine leaf centroid count from last level of structure
        List<Integer> lastLevelClusters = centroidsPerCluster.get(centroidsPerCluster.size() - 1);
        int numLeafCentroids = lastLevelClusters.stream().mapToInt(Integer::intValue).sum();
        int firstLeafCentroidIndex = centroids.size() - numLeafCentroids;

        List<List<ITupleReference>> allRecords = new ArrayList<>();

        for (int i = 0; i < numLeafCentroids; i++) {
            // Deserialize centroid tuple to extract ID and vector
            ITupleReference centroidTuple = centroids.get(firstLeafCentroidIndex + i);
            Object[] values = TupleUtils.deserializeTuple(centroidTuple, centroidSerdes);
            int centroidId = (Integer) values[0];
            double[] centroidVector = (double[]) values[1];

            List<ITupleReference> clusterRecords = new ArrayList<>();
            double baseDistance = 0.30;
            int recordCount = 0;

            while (recordCount < INSERT_RECORDS_PER_CLUSTER) {
                double currentDistance = baseDistance;
                double[][] offsets = generateDirectionOffsets(vectorDimension, currentDistance);

                for (double[] offset : offsets) {
                    if (recordCount >= INSERT_RECORDS_PER_CLUSTER)
                        break;

                    double[] vector = new double[vectorDimension];
                    for (int d = 0; d < vectorDimension; d++) {
                        vector[d] = centroidVector[d] + offset[d];
                    }

                    String primaryKey = "pk_ins_c" + centroidId + "_" + recordCount;
                    ITupleReference tuple = createInsertTuple(vector, primaryKey);
                    clusterRecords.add(tuple);
                    recordCount++;
                }

                baseDistance += 0.30;
            }

            allRecords.add(clusterRecords);
        }

        return allRecords;
    }

    /**
     * Generate direction offsets based on vector dimension.
     * 2D: 4 directions (±x, ±y)
     * 3D: 6 directions (±x, ±y, ±z)
     */
    protected double[][] generateDirectionOffsets(int dimension, double distance) {
        double[][] offsets = new double[dimension * 2][dimension];
        for (int d = 0; d < dimension; d++) {
            offsets[d * 2] = new double[dimension];
            offsets[d * 2][d] = distance;
            offsets[d * 2 + 1] = new double[dimension];
            offsets[d * 2 + 1][d] = -distance;
        }
        return offsets;
    }

    /**
     * Helper method to create an insert tuple for vector index.
     * Format: <vector, primary_key>
     *
     * This is the format expected by the LSMVCTree insert operation.
     */
    protected ITupleReference createInsertTuple(double[] vector, String primaryKey) throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        // Field 0: vector (serialized with DoubleArraySerializerDeserializer)
        DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // Field 1: primary_key (UTF8 string)
        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }

    // ==================== Delete Test Support ====================

    /**
     * Generate delete tuples from known vectors and primary keys.
     * Delete tuple format: <vector, primary_key> (same as insert).
     *
     * @param vectors Array of vector values for records to delete
     * @param primaryKeys Array of primary key strings for records to delete
     * @return List of delete tuples
     */
    protected List<ITupleReference> generateDeleteTuples(double[][] vectors, String[] primaryKeys) throws Exception {
        if (vectors.length != primaryKeys.length) {
            throw new IllegalArgumentException("vectors and primaryKeys must have same length");
        }

        List<ITupleReference> deleteTuples = new ArrayList<>();
        for (int i = 0; i < vectors.length; i++) {
            // Delete tuple format is same as insert: <vector, primary_key>
            deleteTuples.add(createInsertTuple(vectors[i], primaryKeys[i]));
        }
        return deleteTuples;
    }

    /**
     * Get centroids for 2D two-layer structure (for reuse by insert tests).
     * Returns c0-c3 (level 0) and c4-c19 (level 1 leaf centroids).
     */
    protected List<ITupleReference> get2DCentroids() throws Exception {
        List<ITupleReference> centroids = new ArrayList<>();

        // Level 0: 4 quadrant centroids (c0-c3)
        centroids.add(createCentroidTuple(0, new double[] { 50.0, 50.0 })); // Q1
        centroids.add(createCentroidTuple(1, new double[] { -50.0, 50.0 })); // Q2
        centroids.add(createCentroidTuple(2, new double[] { -50.0, -50.0 })); // Q3
        centroids.add(createCentroidTuple(3, new double[] { 50.0, -50.0 })); // Q4

        // Level 1: 16 leaf centroids (c4-c19)
        for (int i = 0; i < LEAF_CENTROIDS_2D.length; i++) {
            centroids.add(createCentroidTuple(i + 4, LEAF_CENTROIDS_2D[i]));
        }

        return centroids;
    }

    /**
     * Get structure configuration for 2D two-layer test.
     */
    protected List<Integer> get2DNumClustersPerLevel() {
        return Arrays.asList(1, 4);
    }

    /**
     * Get centroid counts per cluster for 2D two-layer test.
     */
    protected List<List<Integer>> get2DCentroidsPerCluster() {
        return Arrays.asList(Arrays.asList(4), // Level 0: 1 cluster with 4 centroids
                Arrays.asList(4, 4, 4, 4) // Level 1: 4 clusters with 4 centroids each
        );
    }

    /**
     * Generate records for optimized search testing.
     * 20 records at integer distances 1-20 along x-axis from centroid at origin.
     *
     * With query at [5, 0, 0] (D(q,C) = 5.0):
     * - Record at D(x,C) = 1: vector [1,0,0], D(q,x) = 4.0
     * - Record at D(x,C) = 2: vector [2,0,0], D(q,x) = 3.0
     * - Record at D(x,C) = 3: vector [3,0,0], D(q,x) = 2.0
     * - Record at D(x,C) = 4: vector [4,0,0], D(q,x) = 1.0
     * - Record at D(x,C) = 5: vector [5,0,0], D(q,x) = 0.0  ← PIVOT (closest to query)
     * - Record at D(x,C) = 6: vector [6,0,0], D(q,x) = 1.0
     * - Record at D(x,C) = 7: vector [7,0,0], D(q,x) = 2.0
     * - ... etc
     */
    private List<List<ITupleReference>> generateOptimizedSearchRecords() throws Exception {
        List<List<ITupleReference>> allRecords = new ArrayList<>();
        List<ITupleReference> clusterRecords = new ArrayList<>();

        int centroidId = 0; // Single centroid with ID 0

        // Generate 20 records at integer distances 1-20 along x-axis
        for (int i = 1; i <= 20; i++) {
            double distance = i;
            double[] vector = { distance, 0.0, 0.0 }; // Vector at [i, 0, 0]
            String primaryKey = "pk_opt_" + i;

            ITupleReference tuple = createOptimizedSearchRecordTuple(distance, centroidId, vector, primaryKey);
            clusterRecords.add(tuple);
        }

        allRecords.add(clusterRecords);
        return allRecords;
    }

    /**
     * Helper method to create a record tuple for optimized search testing.
     * Format: <distance_to_centroid, centroid_id, vector, primary_key>
     *
     * This format allows LSMVCTreeBlockedCursor to:
     * 1. Extract D(x,C) from field 0 for pivot finding and priority queue ordering
     * 2. Extract centroid_id from field 1 for cluster validation
     * 3. Extract vector from field 2 for computing D(q,x) via IVectorBinaryAccessor
     * 4. Extract primary key from field 3 for result identification
     */
    private ITupleReference createOptimizedSearchRecordTuple(double distance, int centroidId, double[] vector,
            String primaryKey) throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        // Field 0: distance_to_centroid (raw double - 8 bytes)
        tupleBuilder.getDataOutput().writeDouble(distance);
        tupleBuilder.addFieldEndOffset();

        // Field 1: centroid_id (raw int - 4 bytes)
        tupleBuilder.getDataOutput().writeInt(centroidId);
        tupleBuilder.addFieldEndOffset();

        // Field 2: vector (serialized with DoubleArraySerializerDeserializer)
        DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // Field 3: primary_key (UTF8 string)
        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }
}
