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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer;
import org.apache.hyracks.storage.common.IIndexAccessor;
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
        final float[] clusterCentroid;
        final List<float[]> insertedVectors;
        final String clusterId;

        TestClusterData(float[] centroid, String id) {
            this.clusterCentroid = centroid.clone();
            this.insertedVectors = new ArrayList<>();
            this.clusterId = id;
        }
    }

    public List<TestClusterData> insertRecordsIntoMultipleClusters(AbstractVectorTreeTestContext ctx) throws Exception {
        List<TestClusterData> clusterData = new ArrayList<>();

        // Test clusters from different regions of the hierarchical structure
        float[][] testCentroids = { { 22.0f, 22.0f, 15.0f, 10.0f }, // Root region 0, Interior 0, Leaf 0
                { 17.0f, 19.5f, 20.0f, 10.5f }, // Root region 0, Interior 0, Leaf 0 (variation)
                { -22.0f, -22.0f, -22.0f, -10.0f }, // Root region 1, Interior 2, Leaf 4
                { -19.0f, -19.5f, -20.0f, -9.5f }, // Root region 1, Interior 2, Leaf 4 (variation)
                { 25.0f, -17.0f, 22.0f, 14.0f }, // Root region 0, Interior 1, Leaf 2
                { -17.0f, 23.0f, -18.0f, -6.0f } // Root region 1, Interior 3, Leaf 6
        };

        String[] clusterIds =
                { "cluster_0_0", "cluster_0_1", "cluster_4_0", "cluster_4_1", "cluster_2_0", "cluster_6_0" };

        for (int i = 0; i < 1; i++) {
            TestClusterData cluster = new TestClusterData(testCentroids[i], clusterIds[i]);

            // Insert 20 records near each test centroid
            List<float[]> insertedVectors = insertRecordsIntoCluster(ctx, testCentroids[i], 20);
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
    private List<float[]> insertRecordsIntoCluster(AbstractVectorTreeTestContext ctx, float[] centroid, int count)
            throws Exception {
        List<float[]> vectors = new ArrayList<>();
        IIndexAccessor accessor = ctx.getIndexAccessor();
        /* TODO: replace arbitrary random */
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            // Generate vector near the centroid with some noise
            float[] vector = new float[VECTOR_DIMENSIONS];
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
                if (ctx.getFieldSerdes()[j] instanceof FloatArraySerializerDeserializer) {
                    float[] vector = generateRandomVector(vectorDimensions, rnd);
                    fieldValues[j] = vector;
                } else {
                    // String field
                    fieldValues[j] = generateRandomString(5 + rnd.nextInt(10), rnd);
                }
            }

            // Set metadata fields
            for (int j = numKeyFields; j < fieldCount; j++) {
                if (ctx.getFieldSerdes()[j] instanceof FloatArraySerializerDeserializer) {
                    float[] vector = generateRandomVector(vectorDimensions, rnd);
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
                    if (value instanceof float[]) {
                        checkTuple.appendField(new VectorCheckTuple.FloatArrayWrapper((float[]) value));
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
                if (ctx.getFieldSerdes()[j] instanceof FloatArraySerializerDeserializer) {
                    float[] vector;
                    int caseType = i % 4;
                    switch (caseType) {
                        case 0: // Zero vector
                            vector = new float[vectorDimensions];
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
                    if (value instanceof float[]) {
                        checkTuple.appendField(new VectorCheckTuple.FloatArrayWrapper((float[]) value));
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
    private float[] generateRandomVector(int dimensions, Random rnd) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = rnd.nextFloat() * 100.0f - 50.0f; // Range [-50, 50]
        }
        return vector;
    }

    private float[] generateUnitVector(int dimensions, Random rnd) {
        float[] vector = new float[dimensions];
        int nonZeroIndex = rnd.nextInt(dimensions);
        vector[nonZeroIndex] = 1.0f;
        return vector;
    }

    private float[] generateLargeVector(int dimensions, Random rnd) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = rnd.nextFloat() * 10000.0f;
        }
        return vector;
    }

    private float[] generateSmallVector(int dimensions, Random rnd) {
        float[] vector = new float[dimensions];
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
            float[] centroid = generatePredictableVector(4, i); // 4D vectors
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
    private static float[] generatePredictableVector(int dimensions, int index) {
        float[] vector = new float[dimensions];
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
    public static ITupleReference createVectorTuple(float[] vector, String metadata) throws HyracksDataException {
        // Create field serializers for vector and metadata
        ISerializerDeserializer[] fieldSerdes =
                new ISerializerDeserializer[] { FloatArraySerializerDeserializer.INSTANCE,
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
    public static ITupleReference createClusterTuple(int clusterId, float[] centroid, int metadataPointer)
            throws HyracksDataException {
        try {
            // Use ArrayTupleBuilder to create proper cluster tuple
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);

            // Add CID field (field 0) - using IntegerSerializerDeserializer.INSTANCE
            tupleBuilder.addField(
                    org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer.INSTANCE,
                    clusterId);

            // Add centroid field (field 1) - using FloatArraySerializerDeserializer.INSTANCE
            tupleBuilder.addField(FloatArraySerializerDeserializer.INSTANCE, centroid);

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
