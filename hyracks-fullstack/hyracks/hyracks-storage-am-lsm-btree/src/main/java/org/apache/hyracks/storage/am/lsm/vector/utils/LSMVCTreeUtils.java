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

package org.apache.hyracks.storage.am.lsm.vector.utils;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTreeDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTreeFileManager;
import org.apache.hyracks.storage.am.lsm.vector.impls.VectorClusteringTreeFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringMetadataFrameFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringMetadataTupleWriterFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Utility class for creating LSM Vector Clustering Tree instances.
 * Provides factory methods to simplify LSMVCTree instantiation with proper configuration.
 */
public class LSMVCTreeUtils {

    private LSMVCTreeUtils() {
        // Utility class, no instantiation
    }

    /**
     * Creates an LSMVCTree with compression support.
     *
     * @param ioManager the I/O manager for file operations
     * @param virtualBufferCaches list of virtual buffer caches for memory components
     * @param file the file reference for the index
     * @param diskBufferCache the buffer cache for disk operations
     * @param typeTraits type traits for all fields in the index
     * @param cmpFactories comparator factories for the vector clustering fields
     * @param bloomFilterFalsePositiveRate false positive rate for bloom filters
     * @param mergePolicy the LSM merge policy
     * @param opTracker operation tracker for LSM operations
     * @param ioScheduler I/O operation scheduler
     * @param ioOpCallbackFactory I/O operation callback factory
     * @param pageWriteCallbackFactory page write callback factory
     * @param needKeyDupCheck whether to check for duplicate keys (primary index flag)
     * @param vectorDimensions number of dimensions in the vectors
     * @param vectorFields array indicating which fields contain vector data
     * @param filterFields array indicating which fields are used for filtering
     * @param filterFrameFactory frame factory for component filters
     * @param filterManager manager for component filters
     * @param filterHelper helper for component filter operations
     * @param durable whether the index should be durable
     * @param metadataPageManagerFactory factory for metadata page managers
     * @param atomic whether operations should be atomic
     * @return configured LSMVCTree instance
     * @throws HyracksDataException if creation fails
     */
    public static LSMVCTree createLSMTree(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, int vectorDimensions, int[] vectorFields, int[] filterFields,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            IComponentFilterHelper filterHelper, boolean durable,
            IMetadataPageManagerFactory metadataPageManagerFactory, boolean atomic) throws HyracksDataException {

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

        VectorClusteringTreeFactory vctreeFactory = new VectorClusteringTreeFactory(ioManager, diskBufferCache,
                metadataPageManagerFactory, interiorFrameFactory, leafFrameFactory, metadataFrameFactory,
                dataFrameFactory, cmpFactories, 4, vectorDimensions);
        // Create file manager for LSM components
        ILSMIndexFileManager fileManager = new LSMVCTreeFileManager(ioManager, file, vctreeFactory);

        // Create disk component factory
        ILSMDiskComponentFactory componentFactory = new LSMVCTreeDiskComponentFactory(vctreeFactory, filterHelper);

        // Create the LSMVCTree instance
        return new LSMVCTree(storageConfig, ioManager, virtualBufferCaches, interiorFrameFactory, leafFrameFactory,
                metadataFrameFactory, dataFrameFactory, diskBufferCache, fileManager, componentFactory,
                componentFactory, filterHelper, filterFrameFactory, filterManager, bloomFilterFalsePositiveRate,
                cmpFactories, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory,
                needKeyDupCheck, vectorDimensions, vectorFields, filterFields, durable, atomic);
    }

    /**
     * Creates an LSMVCTree with simplified parameters (most common use case).
     *
     * @param ioManager the I/O manager for file operations
     * @param virtualBufferCaches list of virtual buffer caches for memory components
     * @param file the file reference for the index
     * @param diskBufferCache the buffer cache for disk operations
     * @param typeTraits type traits for all fields in the index
     * @param cmpFactories comparator factories for the vector clustering fields
     * @param valueProviderFactories value provider factories for primitive types
     * @param bloomFilterFalsePositiveRate false positive rate for bloom filters
     * @param mergePolicy the LSM merge policy
     * @param opTracker operation tracker for LSM operations
     * @param ioScheduler I/O operation scheduler
     * @param ioOpCallbackFactory I/O operation callback factory
     * @param pageWriteCallbackFactory page write callback factory
     * @param needKeyDupCheck whether to check for duplicate keys (primary index flag)
     * @param vectorDimensions number of dimensions in the vectors
     * @param vectorFields array indicating which fields contain vector data
     * @param filterFields array indicating which fields are used for filtering
     * @param durable whether the index should be durable
     * @return configured LSMVCTree instance
     * @throws HyracksDataException if creation fails
     */
    public static LSMVCTree createLSMTree(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, int vectorDimensions, int[] vectorFields, int[] filterFields, boolean durable,
            IMetadataPageManagerFactory metadataPageManagerFactory) throws HyracksDataException {

        // Use default configurations for simplified creation
        ILSMComponentFilterFrameFactory filterFrameFactory = null; // No filtering by default
        LSMComponentFilterManager filterManager = null; // No filter manager by default
        IComponentFilterHelper filterHelper = null; // No filter helper by default
        boolean atomic = true; // Default to atomic operations

        return createLSMTree(storageConfig, ioManager, virtualBufferCaches, file, diskBufferCache, typeTraits,
                cmpFactories, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, needKeyDupCheck, vectorDimensions, vectorFields, filterFields,
                filterFrameFactory, filterManager, filterHelper, durable, metadataPageManagerFactory, atomic);
    }
}
