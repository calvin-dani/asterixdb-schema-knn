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

public abstract class VectorIndexTestDriver {

    // Define the 24 leaf centroids (c10 ~ c33) - shared between methods
    private static final double[][] LEAF_CENTROIDS = {
            // Cluster 2.1: c10, c11, c12
            { 20.0, 30.0, 20.0 }, { 20.0, 20.0, 30.0 }, { 35.0, 25.0, 25.0 },
            // Cluster 2.2: c13, c14, c15
            { -20.0, 30.0, 20.0 }, { -20.0, 20.0, 30.0 }, { -35.0, 25.0, 25.0 },
            // Cluster 2.3: c16, c17, c18
            { -20.0, -30.0, 20.0 }, { -20.0, -20.0, 30.0 }, { -35.0, -25.0, 25.0 },
            // Cluster 2.4: c19, c20, c21
            { 20.0, -30.0, 20.0 }, { 20.0, -20.0, 30.0 }, { 35.0, -25.0, 25.0 },
            // Cluster 2.5: c22, c23, c24
            { 20.0, 30.0, -20.0 }, { 20.0, 20.0, -30.0 }, { 35.0, 25.0, -25.0 },
            // Cluster 2.6: c25, c26, c27
            { -20.0, 30.0, -20.0 }, { -20.0, 20.0, -30.0 }, { -35.0, 25.0, -25.0 },
            // Cluster 2.7: c28, c29, c30
            { -20.0, -30.0, -20.0 }, { -20.0, -20.0, -30.0 }, { -35.0, -25.0, -25.0 },
            // Cluster 2.8: c31, c32, c33
            { 20.0, -30.0, -20.0 }, { 20.0, -20.0, -30.0 }, { 35.0, -25.0, -25.0 } };

    protected abstract void runTest(ISerializerDeserializer[] centroidSerdes,
            ISerializerDeserializer[] dataRecordSerdes, List<ITupleReference> centroids,
            List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster, int vectorDimension,
            List<List<ITupleReference>> leafRecords) throws Exception;

    @Test
    public void threeDimensionThreeLevels() throws Exception {
        // Create a three level static structure with 3 dimension embedding
        // Level 0: 1 cluster with 2 centroids: c0: {0,0,50}, c1: {0,0,-50} (split on 3rd dimension)
        // Level 1: 2 clusters
        //          cluster 1.1: {c2, c3, c4, c5} - positive z quadrants
        //          cluster 1.2: {c6, c7, c8, c9} - negative z quadrants
        // Level 2: 8 clusters since we have 8 centroids in level 1:
        //          cluster 2.1: {c10, c11, c12} - positive x,y,z quadrant
        //          cluster 2.2: {c13, c14, c15} - negative x, positive y, positive z quadrant
        //          cluster 2.3: {c16, c17, c18} - negative x, negative y, positive z quadrant
        //          cluster 2.4: {c19, c20, c21} - positive x, negative y, positive z quadrant
        //          cluster 2.5: {c22, c23, c24} - positive x, positive y, negative z quadrant
        //          cluster 2.6: {c25, c26, c27} - negative x, positive y, negative z quadrant
        //          cluster 2.7: {c28, c29, c30} - negative x, negative y, negative z quadrant
        //          cluster 2.8: {c31, c32, c33} - positive x, negative y, negative z quadrant

        // Centroid serializers: centroid ID + Double array vector (3D)
        ISerializerDeserializer[] centroidSerdes = { IntegerSerializerDeserializer.INSTANCE, // centroid ID
                DoubleArraySerializerDeserializer.INSTANCE // 3D vector
        };

        // Data record serializers: distance + centroid ID + primary key
        ISerializerDeserializer[] dataRecordSerdes = { DoubleSerializerDeserializer.INSTANCE, // distance
                IntegerSerializerDeserializer.INSTANCE, // centroid ID
                new UTF8StringSerializerDeserializer() // primary key
        };

        // Generate centroids according to the hierarchical structure described in comments
        List<ITupleReference> centroids = new ArrayList<>();

        // Level 0: 2 centroids splitting on 3rd dimension
        centroids.add(createCentroidTuple(0, new double[] { 0.0, 0.0, 50.0 })); // c0: positive z
        centroids.add(createCentroidTuple(1, new double[] { 0.0, 0.0, -50.0 })); // c1: negative z

        // Level 1: 8 centroids in 2 clusters (4 centroids each)
        // Cluster 1.1: positive z quadrants (c2, c3, c4, c5)
        centroids.add(createCentroidTuple(2, new double[] { 25.0, 25.0, 25.0 })); // c2: +x, +y, +z
        centroids.add(createCentroidTuple(3, new double[] { -25.0, 25.0, 25.0 })); // c3: -x, +y, +z
        centroids.add(createCentroidTuple(4, new double[] { -25.0, -25.0, 25.0 })); // c4: -x, -y, +z
        centroids.add(createCentroidTuple(5, new double[] { 25.0, -25.0, 25.0 })); // c5: +x, -y, +z

        // Cluster 1.2: negative z quadrants (c6, c7, c8, c9)
        centroids.add(createCentroidTuple(6, new double[] { 25.0, 25.0, -25.0 })); // c6: +x, +y, -z
        centroids.add(createCentroidTuple(7, new double[] { -25.0, 25.0, -25.0 })); // c7: -x, +y, -z
        centroids.add(createCentroidTuple(8, new double[] { -25.0, -25.0, -25.0 })); // c8: -x, -y, -z
        centroids.add(createCentroidTuple(9, new double[] { 25.0, -25.0, -25.0 })); // c9: +x, -y, -z

        // Level 2: 24 centroids in 8 clusters (3 centroids each)
        // Each cluster's centroids average to their parent centroid from level 1
        // Generate leaf centroids from the shared constant
        for (int i = 0; i < LEAF_CENTROIDS.length; i++) {
            centroids.add(createCentroidTuple(i + 10, LEAF_CENTROIDS[i])); // c10 ~ c33
        }

        // Structure configuration: 3 levels with specified cluster distribution
        // Level 0: 1 cluster with 2 centroids
        // Level 1: 2 clusters with 4 centroids each (total 8 centroids)
        // Level 2: 8 clusters with 3 centroids each (total 24 centroids, leaf level)
        List<Integer> numClustersPerLevel = Arrays.asList(1, 2, 8);
        List<List<Integer>> centroidsPerCluster = Arrays.asList(Arrays.asList(2), // Level 0: 1 cluster with 2 centroids
                Arrays.asList(4, 4), // Level 1: 2 clusters with 4 centroids each
                Arrays.asList(3, 3, 3, 3, 3, 3, 3, 3) // Level 2: 8 clusters with 3 centroids each
        );

        // Generate 100 records for each leaf centroid (c10 ~ c33)
        List<List<ITupleReference>> dataRecords = generateDataRecords();

        runTest(centroidSerdes, dataRecordSerdes, centroids, numClustersPerLevel, centroidsPerCluster, 3,
                dataRecords);
    }

    /**
     * Helper method to create a centroid tuple with format: <centroid_id, vector>
     */
    private ITupleReference createCentroidTuple(int centroidId, double[] embedding) throws Exception {
        // Create tuple builder
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        // Create field serializers and values
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                DoubleArraySerializerDeserializer.INSTANCE };
        Object[] fieldValues = new Object[] { centroidId, embedding };

        // Build the tuple using TupleUtils
        TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);

        return tupleRef;
    }

    /**
     * Generate 100 records for each leaf centroid (c10 ~ c33)
     * Each record format: <distance_to_centroid, centroid_id, primary_key>
     */
    private List<List<ITupleReference>> generateDataRecords() throws Exception {
        List<List<ITupleReference>> allLeafRecords = new ArrayList<>();

        // Generate 100 records for each of the 24 leaf centroids (c10 ~ c33)
        for (int centroidIndex = 0; centroidIndex < LEAF_CENTROIDS.length; centroidIndex++) {
            List<ITupleReference> recordsForCentroid = new ArrayList<>();

            // Centroid ID: c10 ~ c33 (global IDs)
            int centroidId = centroidIndex + 10;

            // Generate records in order of distance from centroid
            double baseDistance = 0.2;
            int recordCount = 0;

            // Generate records in increasing distance rings
            while (recordCount < 100) {
                double currentDistance = baseDistance;

                // Generate 6 records per distance ring (±x, ±y, ±z directions)
                double[][] directions = { { currentDistance, 0, 0 }, // +x
                        { -currentDistance, 0, 0 }, // -x
                        { 0, currentDistance, 0 }, // +y
                        { 0, -currentDistance, 0 }, // -y
                        { 0, 0, currentDistance }, // +z
                        { 0, 0, -currentDistance } // -z
                };

                for (double[] direction : directions) {
                    if (recordCount >= 100)
                        break;

                    // Create primary key
                    String primaryKey = "pk_c_" + centroidId + "_" + recordCount;

                    // Create record tuple for bulk loading
                    ITupleReference recordTuple = createBulkLoadRecordTuple(currentDistance, centroidId, primaryKey);
                    recordsForCentroid.add(recordTuple);
                    recordCount++;
                }

                // Increase distance for next ring
                baseDistance += 0.2;
            }

            allLeafRecords.add(recordsForCentroid);
        }

        return allLeafRecords;
    }

    /**
     * Helper method to create a bulk load record tuple.
     * Format: <distance_to_centroid, centroid_id, primary_key>
     * Note: distance and centroid_id use raw types (no ADM type tags).
     */
    private ITupleReference createBulkLoadRecordTuple(double distance, int centroidId, String primaryKey)
            throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        // Field 0: distance_to_centroid (raw double - 8 bytes, no type tag)
        tupleBuilder.getDataOutput().writeDouble(distance);
        tupleBuilder.addFieldEndOffset();

        // Field 1: centroid_id (raw int - 4 bytes, no type tag)
        tupleBuilder.getDataOutput().writeInt(centroidId);
        tupleBuilder.addFieldEndOffset();

        // Field 2: primary_key (UTF8 string)
        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }

    /**
     * Helper method to create an insert/delete record tuple.
     * Format: <vector_embedding, primary_key>
     * Note: vector_embedding has type tag, primary_key does not.
     */
    private ITupleReference createInsertRecordTuple(double[] vector, String primaryKey) throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        // Field 0: vector_embedding (AORDEREDLIST - type tag + serialized array)
        tupleBuilder.getDataOutput().writeByte(0x38); // AORDEREDLIST type tag
        DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // Field 1: primary_key (no type tag - dynamic field)
        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }
}
