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
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.junit.Test;

public abstract class VectorIndexTestDriver {

    protected abstract void runTest(ISerializerDeserializer[] fieldSerdes, List<ITupleReference> centroids,
            List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster, int vectorDimension)
            throws Exception;

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

        // Field serializers: centroid ID + Double array vector (3D)
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, // centroid ID
                DoubleArraySerializerDeserializer.INSTANCE // 3D vector
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
        // Cluster 2.1: positive x,y,z quadrant (c10, c11, c12)
        centroids.add(createCentroidTuple(10, new double[] { 37.5, 37.5, 37.5 }));
        centroids.add(createCentroidTuple(11, new double[] { 35.0, 35.0, 35.0 }));
        centroids.add(createCentroidTuple(12, new double[] { 40.0, 40.0, 40.0 }));

        // Cluster 2.2: negative x, positive y, positive z quadrant (c13, c14, c15)
        centroids.add(createCentroidTuple(13, new double[] { -37.5, 37.5, 37.5 }));
        centroids.add(createCentroidTuple(14, new double[] { -35.0, 35.0, 35.0 }));
        centroids.add(createCentroidTuple(15, new double[] { -40.0, 40.0, 40.0 }));

        // Cluster 2.3: negative x, negative y, positive z quadrant (c16, c17, c18)
        centroids.add(createCentroidTuple(16, new double[] { -37.5, -37.5, 37.5 }));
        centroids.add(createCentroidTuple(17, new double[] { -35.0, -35.0, 35.0 }));
        centroids.add(createCentroidTuple(18, new double[] { -40.0, -40.0, 40.0 }));

        // Cluster 2.4: positive x, negative y, positive z quadrant (c19, c20, c21)
        centroids.add(createCentroidTuple(19, new double[] { 37.5, -37.5, 37.5 }));
        centroids.add(createCentroidTuple(20, new double[] { 35.0, -35.0, 35.0 }));
        centroids.add(createCentroidTuple(21, new double[] { 40.0, -40.0, 40.0 }));

        // Cluster 2.5: positive x, positive y, negative z quadrant (c22, c23, c24)
        centroids.add(createCentroidTuple(22, new double[] { 37.5, 37.5, -37.5 }));
        centroids.add(createCentroidTuple(23, new double[] { 35.0, 35.0, -35.0 }));
        centroids.add(createCentroidTuple(24, new double[] { 40.0, 40.0, -40.0 }));

        // Cluster 2.6: negative x, positive y, negative z quadrant (c25, c26, c27)
        centroids.add(createCentroidTuple(25, new double[] { -37.5, 37.5, -37.5 }));
        centroids.add(createCentroidTuple(26, new double[] { -35.0, 35.0, -35.0 }));
        centroids.add(createCentroidTuple(27, new double[] { -40.0, 40.0, -40.0 }));

        // Cluster 2.7: negative x, negative y, negative z quadrant (c28, c29, c30)
        centroids.add(createCentroidTuple(28, new double[] { -37.5, -37.5, -37.5 }));
        centroids.add(createCentroidTuple(29, new double[] { -35.0, -35.0, -35.0 }));
        centroids.add(createCentroidTuple(30, new double[] { -40.0, -40.0, -40.0 }));

        // Cluster 2.8: positive x, negative y, negative z quadrant (c31, c32, c33)
        centroids.add(createCentroidTuple(31, new double[] { 37.5, -37.5, -37.5 }));
        centroids.add(createCentroidTuple(32, new double[] { 35.0, -35.0, -35.0 }));
        centroids.add(createCentroidTuple(33, new double[] { 40.0, -40.0, -40.0 }));

        // Structure configuration: 3 levels with specified cluster distribution
        // Level 0: 1 cluster with 2 centroids
        // Level 1: 2 clusters with 4 centroids each (total 8 centroids)
        // Level 2: 8 clusters with 3 centroids each (total 24 centroids, leaf level)
        List<Integer> numClustersPerLevel = Arrays.asList(1, 2, 8);
        List<List<Integer>> centroidsPerCluster = Arrays.asList(Arrays.asList(2), // Level 0: 1 cluster with 2 centroids
                Arrays.asList(4, 4), // Level 1: 2 clusters with 4 centroids each
                Arrays.asList(3, 3, 3, 3, 3, 3, 3, 3) // Level 2: 8 clusters with 3 centroids each
        );

        runTest(fieldSerdes, centroids, numClustersPerLevel, centroidsPerCluster, 3);
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
}
