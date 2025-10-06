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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringMetadataFrameFactory;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringMetadataTupleWriterFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Utility class for creating VectorClusteringTree instances, similar to BTreeUtils.
 * Provides factory methods for creating vector clustering trees with appropriate
 * frame factories and configuration.
 */
public class VectorTreeUtils {

    private VectorTreeUtils() {
        // Utility class - no instantiation
    }

    /**
     * Create a VectorClusteringTree with the specified parameters.
     * 
     * @param bufferCache the buffer cache for page management
     * @param typeTraits type traits for the DATA fields (used for tree field count)
     * @param cmpFactories comparator factories for field comparison
     * @param vectorDimensions number of dimensions in vectors
     * @param file file reference for the tree
     * @param pageManager page manager for allocation
     * @return a new VectorClusteringTree instance
     * @throws HyracksDataException if creation fails
     */
    public static VectorClusteringTree createVectorClusteringTree(IBufferCache bufferCache, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, int vectorDimensions, FileReference file, IPageManager pageManager)
            throws HyracksDataException {

        // We need null-related types for tuple writers - use simple defaults for testing
        ITypeTraits nullTypeTraits = null; // Can be null for basic testing
        INullIntrospector nullIntrospector = null; // Can be null for basic testing

        // Create specific type traits using primitive type traits for better performance
        // Interior/Leaf frames need 3-field cluster tuples: <cid, centroid, pointer>
        ITypeTraits[] clusterTypeTraits = new ITypeTraits[3];
        clusterTypeTraits[0] = IntegerPointable.TYPE_TRAITS; // cluster ID (int) - Fixed 4 bytes
        clusterTypeTraits[1] = VarLengthTypeTrait.INSTANCE; // centroid (float array) - Variable
        clusterTypeTraits[2] = IntegerPointable.TYPE_TRAITS; // pointer (int) - Fixed 4 bytes

        // Metadata frames need 2-field metadata tuples: <max_distance, page_pointer>
        ITypeTraits[] metadataTypeTraits = new ITypeTraits[2];
        metadataTypeTraits[0] = FloatPointable.TYPE_TRAITS; // max distance (double) - Fixed 4 bytes
        metadataTypeTraits[1] = IntegerPointable.TYPE_TRAITS; // page pointer (int) - Fixed 4 bytes

        // Data frames need 4-field data tuples: <distance, cosine_similarity, vector, primary_key>
        ITypeTraits[] dataTypeTraits = new ITypeTraits[4];
        dataTypeTraits[0] = DoublePointable.TYPE_TRAITS; // distance (double) - Fixed 8 bytes
        dataTypeTraits[1] = DoublePointable.TYPE_TRAITS; // cosine similarity (double) - Fixed 8 bytes
        dataTypeTraits[2] = VarLengthTypeTrait.INSTANCE; // vector (float array) - Variable
        dataTypeTraits[3] = VarLengthTypeTrait.INSTANCE; // primary key (string/variable) - Variable

        // Create individual tuple writer factories with correct type traits for each frame type
        VectorClusteringInteriorTupleWriterFactory interiorTupleWriterFactory =
                new VectorClusteringInteriorTupleWriterFactory(clusterTypeTraits, nullTypeTraits, nullIntrospector);
        VectorClusteringLeafTupleWriterFactory leafTupleWriterFactory =
                new VectorClusteringLeafTupleWriterFactory(clusterTypeTraits, nullTypeTraits, nullIntrospector);
        VectorClusteringMetadataTupleWriterFactory metadataTupleWriterFactory =
                new VectorClusteringMetadataTupleWriterFactory(metadataTypeTraits, nullTypeTraits, nullIntrospector);
        VectorClusteringDataTupleWriterFactory dataTupleWriterFactory =
                new VectorClusteringDataTupleWriterFactory(dataTypeTraits, nullTypeTraits, nullIntrospector);

        // Create tuple writers from factories
        ITreeIndexTupleWriter interiorTupleWriter = interiorTupleWriterFactory.createTupleWriter();
        ITreeIndexTupleWriter leafTupleWriter = leafTupleWriterFactory.createTupleWriter();
        ITreeIndexTupleWriter metadataTupleWriter = metadataTupleWriterFactory.createTupleWriter();

        // Create frame factories using individual tuple writers
        ITreeIndexFrameFactory interiorFrameFactory =
                new VectorClusteringInteriorFrameFactory(interiorTupleWriter, vectorDimensions);
        ITreeIndexFrameFactory leafFrameFactory =
                new VectorClusteringLeafFrameFactory(leafTupleWriter, vectorDimensions);
        ITreeIndexFrameFactory metadataFrameFactory =
                new VectorClusteringMetadataFrameFactory(metadataTupleWriter, vectorDimensions);
        ITreeIndexFrameFactory dataFrameFactory =
                new VectorClusteringDataFrameFactory(dataTupleWriterFactory, vectorDimensions);

        return new VectorClusteringTree(bufferCache, pageManager, interiorFrameFactory, leafFrameFactory,
                metadataFrameFactory, dataFrameFactory, cmpFactories, 4, vectorDimensions, file);
    }

    /**
     * Create a VectorClusteringTree with default parameters.
     * 
     * @param bufferCache the buffer cache for page management
     * @param cmpFactories comparator factories for field comparison
     * @param vectorDimensions number of dimensions in vectors
     * @param fieldCount total number of fields in DATA tuples
     * @param file file reference for the tree
     * @param pageManager page manager for allocation
     * @return a new VectorClusteringTree instance
     * @throws HyracksDataException if creation fails
     */
    public static VectorClusteringTree createVectorClusteringTree(IBufferCache bufferCache,
            IBinaryComparatorFactory[] cmpFactories, int vectorDimensions, int fieldCount, FileReference file,
            IPageManager pageManager) throws HyracksDataException {

        // Create default type traits for DATA tuples - assume vector field is variable length
        ITypeTraits[] dataTypeTraits = new ITypeTraits[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            dataTypeTraits[i] = createDefaultTypeTraits();
        }

        return createVectorClusteringTree(bufferCache, dataTypeTraits, cmpFactories, vectorDimensions, file,
                pageManager);
    }

    /**
     * Create default type traits for variable length fields.
     * 
     * @return default type traits instance
     */
    private static ITypeTraits createDefaultTypeTraits() {
        return new ITypeTraits() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean isFixedLength() {
                return false;
            }

            @Override
            public int getFixedLength() {
                return -1;
            }
        };
    }
}
