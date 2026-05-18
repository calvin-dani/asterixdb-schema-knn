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
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.data.std.primitive.DoublePointable;
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
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeFileManager;
import org.apache.hyracks.storage.am.lsm.vector.impls.VTreeFactory;
import org.apache.hyracks.storage.am.lsm.vector.tuples.LSMVTreeDataTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleCreatorFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrameFactory;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleCreatorFactory;
import org.apache.hyracks.storage.am.vector.tuples.VTreeTupleWriterFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Utility class for creating LSM Vector Clustering Tree instances.
 * Provides factory methods to simplify LSMVTree instantiation with proper configuration.
 */
public class LSMVTreeUtils {

    private LSMVTreeUtils() {
        // Utility class, no instantiation
    }

    /**
     * Creates an LSMVTree with compression support.
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
     * @return configured LSMVTree instance
     * @throws HyracksDataException if creation fails
     */
    public static LSMVTree createLSMTree(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, int vectorDimensions, int[] vectorFields, int[] filterFields,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            IComponentFilterHelper filterHelper, boolean durable,
            IMetadataPageManagerFactory metadataPageManagerFactory, boolean atomic, RecordDescriptor inputRecDesc,
            org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory vectorAccessorFactory,
            int numPrimaryKeyFields, int numIncludeFields, IVTreeDataTupleCreatorFactory dataTupleCreatorFactory,
            float[] quantizationParams, String distanceMetric) throws HyracksDataException {

        ITypeTraits nullTypeTraits = null; // Can be null for basic testing
        INullIntrospector nullIntrospector = null; // Can be null for basic testing

        // Create specific type traits using primitive type traits for better performance
        // Interior/Leaf frames need 3-field cluster tuples: <cid, centroid, pointer>
        ITypeTraits[] interiorTypeTraits = new ITypeTraits[3];
        interiorTypeTraits[0] = IntegerPointable.TYPE_TRAITS; // cluster ID (int) - Fixed 4 bytes
        interiorTypeTraits[1] = VarLengthTypeTrait.INSTANCE; // centroid (float array) - Variable
        interiorTypeTraits[2] = IntegerPointable.TYPE_TRAITS; // pointer (int) - Fixed 4 bytes

        // Create specific type traits using primitive type traits for better performance
        // Leaf frames: field count depends on whether quantization is enabled
        // Without quantization: 3-field <cid, centroid, pointer>
        // With quantization: 4-field <cid, centroid, quantized_embedding, pointer>
        ITypeTraits[] leafTypeTraits;
        if (quantizationParams != null) {
            leafTypeTraits = new ITypeTraits[4];
            leafTypeTraits[0] = IntegerPointable.TYPE_TRAITS; // cluster ID (int) - Fixed 4 bytes
            leafTypeTraits[1] = VarLengthTypeTrait.INSTANCE; // centroid (float array) - Variable
            leafTypeTraits[2] = VarLengthTypeTrait.INSTANCE; // quantized embedding (float array) - Variable
            leafTypeTraits[3] = IntegerPointable.TYPE_TRAITS; // pointer (int) - Fixed 4 bytes
        } else {
            leafTypeTraits = new ITypeTraits[3];
            leafTypeTraits[0] = IntegerPointable.TYPE_TRAITS; // cluster ID (int) - Fixed 4 bytes
            leafTypeTraits[1] = VarLengthTypeTrait.INSTANCE; // centroid (float array) - Variable
            leafTypeTraits[2] = IntegerPointable.TYPE_TRAITS; // pointer (int) - Fixed 4 bytes
        }

        // Metadata frames need 2-field metadata tuples: <max_distance, page_pointer>
        ITypeTraits[] metadataTypeTraits = new ITypeTraits[2];
        metadataTypeTraits[0] = DoublePointable.TYPE_TRAITS; // max distance (double) - Fixed 8 bytes
        metadataTypeTraits[1] = IntegerPointable.TYPE_TRAITS; // page pointer (int) - Fixed 4 bytes

        // Data frames: Use the typeTraits parameter (passed from VTreeResourceFactoryProvider)
        // Format: [distance: DOUBLE, centroidId: INT32, primary_keys..., include_fields...]
        // The typeTraits parameter contains the correct ADM-tagged type traits for all fields
        ITypeTraits[] dataTypeTraits = typeTraits;

        // Create individual tuple writer factories with correct type traits for each frame type
        VTreeTupleWriterFactory interiorTupleWriterFactory =
                new VTreeTupleWriterFactory(interiorTypeTraits, nullTypeTraits, nullIntrospector);
        VTreeTupleWriterFactory leafTupleWriterFactory =
                new VTreeTupleWriterFactory(leafTypeTraits, nullTypeTraits, nullIntrospector);
        VTreeTupleWriterFactory metadataTupleWriterFactory =
                new VTreeTupleWriterFactory(metadataTypeTraits, nullTypeTraits, nullIntrospector);
        // Create separate tuple writer factories for INSERT and DELETE operations (LSMBTree pattern)
        // Insert operations use matter tuples (isAntimatter=false)
        LSMVTreeDataTupleWriterFactory insertDataTupleWriterFactory =
                new LSMVTreeDataTupleWriterFactory(dataTypeTraits, false, nullTypeTraits, nullIntrospector);
        // Delete operations use antimatter tuples (isAntimatter=true)
        LSMVTreeDataTupleWriterFactory deleteDataTupleWriterFactory =
                new LSMVTreeDataTupleWriterFactory(dataTypeTraits, true, nullTypeTraits, nullIntrospector);

        ITreeIndexTupleWriter interiorTupleWriter = interiorTupleWriterFactory.createTupleWriter();
        ITreeIndexTupleWriter leafTupleWriter = leafTupleWriterFactory.createTupleWriter();
        ITreeIndexTupleWriter metadataTupleWriter = metadataTupleWriterFactory.createTupleWriter();

        // Create frame factories using individual tuple writers
        ITreeIndexFrameFactory interiorFrameFactory =
                new VTreeInteriorFrameFactory(interiorTupleWriter, vectorDimensions);
        ITreeIndexFrameFactory leafFrameFactory = new VTreeLeafFrameFactory(leafTupleWriter, vectorDimensions);
        ITreeIndexFrameFactory metadataFrameFactory =
                new VTreeMetadataFrameFactory(metadataTupleWriter, vectorDimensions);
        // Create two data frame factories following LSMBTree pattern
        ITreeIndexFrameFactory insertDataFrameFactory =
                new VTreeDataFrameFactory(insertDataTupleWriterFactory, vectorDimensions);
        ITreeIndexFrameFactory deleteDataFrameFactory =
                new VTreeDataFrameFactory(deleteDataTupleWriterFactory, vectorDimensions);

        // VTreeFactory is used for disk components, which are immutable
        // Disk components always use insertDataFrameFactory (no in-place deletes)
        VTreeFactory vtreeFactory = new VTreeFactory(ioManager, diskBufferCache, metadataPageManagerFactory,
                interiorFrameFactory, leafFrameFactory, metadataFrameFactory, insertDataFrameFactory, cmpFactories, 4,
                vectorDimensions, vectorAccessorFactory, dataTupleCreatorFactory, quantizationParams, distanceMetric);
        ILSMIndexFileManager fileManager = new LSMVTreeFileManager(ioManager, file, vtreeFactory);

        // Create disk component factory
        ILSMDiskComponentFactory componentFactory = new LSMVTreeDiskComponentFactory(vtreeFactory, filterHelper);
        LSMVTree result = new LSMVTree(storageConfig, ioManager, virtualBufferCaches, interiorFrameFactory,
                leafFrameFactory, metadataFrameFactory, insertDataFrameFactory, deleteDataFrameFactory, diskBufferCache,
                fileManager, componentFactory, componentFactory, filterHelper, filterFrameFactory, filterManager,
                bloomFilterFalsePositiveRate, cmpFactories, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, needKeyDupCheck, vectorDimensions, vectorFields, filterFields, durable,
                atomic, vectorAccessorFactory, numPrimaryKeyFields, numIncludeFields, dataTupleCreatorFactory,
                quantizationParams, distanceMetric);
        return result;
    }

    /**
     * Creates an LSMVTree with simplified parameters (most common use case).
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
     * @return configured LSMVTree instance
     * @throws HyracksDataException if creation fails
     */
    public static LSMVTree createLSMTree(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, int vectorDimensions, int[] vectorFields, int[] filterFields, boolean durable,
            IMetadataPageManagerFactory metadataPageManagerFactory, RecordDescriptor inputRecDesc,
            org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory vectorAccessorFactory)
            throws HyracksDataException {

        // Use default configurations for simplified creation
        ILSMComponentFilterFrameFactory filterFrameFactory = null; // No filtering by default
        LSMComponentFilterManager filterManager = null; // No filter manager by default
        IComponentFilterHelper filterHelper = null; // No filter helper by default
        boolean atomic = false; // Default to atomic operations

        // Default: single primary key field, no include fields (for legacy/simplified use)
        int numPrimaryKeyFields = 1;
        int numIncludeFields = 0;

        // Default: use standard (non-quantized) data tuple creator factory with no include fields
        IVTreeDataTupleCreatorFactory dataTupleCreatorFactory =
                new VTreeDataTupleCreatorFactory(numIncludeFields, false);

        return createLSMTree(storageConfig, ioManager, virtualBufferCaches, file, diskBufferCache, typeTraits,
                cmpFactories, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, needKeyDupCheck, vectorDimensions, vectorFields, filterFields,
                filterFrameFactory, filterManager, filterHelper, durable, metadataPageManagerFactory, atomic,
                inputRecDesc, vectorAccessorFactory, numPrimaryKeyFields, numIncludeFields, dataTupleCreatorFactory,
                null, "euclidean");
    }
}
