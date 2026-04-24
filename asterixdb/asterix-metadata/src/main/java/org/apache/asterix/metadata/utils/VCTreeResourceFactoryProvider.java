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
package org.apache.asterix.metadata.utils;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.common.AOrderedListVectorBinaryAccessorFactory;
import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.RawBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.FixedLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.LSMVTreeLocalResourceFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleCreatorFactory;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleCreatorFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.util.LogRedactionUtil;

public class VCTreeResourceFactoryProvider implements IResourceFactoryProvider {

    public static final VCTreeResourceFactoryProvider INSTANCE = new VCTreeResourceFactoryProvider();

    private VCTreeResourceFactoryProvider() {
    }

    @Override
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {

        Index.VectorIndexDetails vectorIndexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        if (vectorIndexDetails.getKeyFieldNames().size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD,
                    vectorIndexDetails.getKeyFieldNames().size(), index.getIndexType(), 1);
        }

        // Get vector field information from keyFieldNames (the actual vector field)
        List<String> vectorFieldNames = vectorIndexDetails.getKeyFieldNames().get(0);

        // Try to get the actual field type from the schema first (for closed fields)
        IAType vectorFieldType = null;
        try {
            vectorFieldType = recordType.getSubFieldType(vectorFieldNames);
        } catch (AlgebricksException e) {
            // Field not found in schema, will use default for open fields
            vectorFieldType = null;
        }

        // If field type is not found in schema (open field), provide default type
        if (vectorFieldType == null) {
            vectorFieldType = DefaultOpenFieldType.getDefaultOpenFieldType(ATypeTag.ARRAY);
        }

        Pair<IAType, Boolean> vectorTypePair =
                Index.getNonNullableOpenFieldType(index, vectorFieldType, vectorFieldNames, recordType);
        IAType vectorType = vectorTypePair.first;
        if (vectorType == null) {
            throw new CompilationException(ErrorCode.COMPILATION_FIELD_NOT_FOUND,
                    LogRedactionUtil.userData(vectorFieldNames.toString()));
        }

        // Extract vector dimensions from WITH clause
        AdmObjectNode withObjectNode = vectorIndexDetails.getWithObjectNode();
        int vectorDimensions = (withObjectNode != null) ? withObjectNode.getOptionalInt("dimension", 384) : 384;

        // Get INCLUDE fields count from index details (needed by factory)
        List<List<String>> includeFieldNames = vectorIndexDetails.getIncludeFieldNames();
        int numIncludeFields = (includeFieldNames != null) ? includeFieldNames.size() : 0;

        // Determine data tuple creator factory based on description (quantization indicator)
        String description = (withObjectNode != null) ? withObjectNode.getOptionalString("quantization", null) : null;
        boolean isQuantized = (description != null);
        IVTreeDataTupleCreatorFactory dataTupleCreatorFactory;
        if (isQuantized) {
            dataTupleCreatorFactory = new VTreeDataTupleCreatorFactory(numIncludeFields, true);
        } else {
            dataTupleCreatorFactory = new VTreeDataTupleCreatorFactory(numIncludeFields, false);
        }

        List<List<String>> primaryKeyFields = dataset.getPrimaryKeys();
        int numPrimaryKeys = primaryKeyFields.size();

        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        ITypeTraitProvider typeTraitProvider = mdProvider.getDataFormat().getTypeTraitProvider();

        // Get type traits and comparator factories (conditional on quantization)
        ITypeTraits[] typeTraits = getTypeTraits(mdProvider, dataset, index, recordType, metaType, isQuantized);
        IBinaryComparatorFactory[] cmpFactories =
                getCmpFactories(mdProvider, dataset, index, recordType, metaType, isQuantized);

        // Set up vector fields array
        int[] vectorFields = new int[1]; // Only one vector field
        vectorFields[0] = 0;

        // Set up filter fields
        int[] filterFields = null;
        if (filterTypeTraits != null && filterTypeTraits.length > 0) {
            filterFields = new int[vectorFields.length + numPrimaryKeys];
            System.arraycopy(vectorFields, 0, filterFields, 0, vectorFields.length);
            for (int i = 0; i < numPrimaryKeys; i++) {
                filterFields[vectorFields.length + i] = vectorFields.length + i;
            }
        }

        IStorageManager storageManager = storageComponentProvider.getStorageManager();
        ILSMOperationTrackerFactory opTrackerFactory = dataset.getIndexOperationTrackerFactory(index);
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = dataset.getIoOperationCallbackFactory(index);
        ILSMPageWriteCallbackFactory pageWriteCallbackFactory = dataset.getPageWriteCallbackFactory();
        IMetadataPageManagerFactory metadataPageManagerFactory =
                storageComponentProvider.getMetadataPageManagerFactory();
        ILSMIOOperationSchedulerProvider ioSchedulerProvider =
                storageComponentProvider.getIoOperationSchedulerProvider();

        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            AsterixVirtualBufferCacheProvider vbcProvider =
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId());

            // Quantization parameters are computed in the index creation job (QuantizationConstantsAggregate)
            // and stored on LSMVTreeLocalResource via QuantizedIndexCreateOperatorDescriptor.
            String indexName = index.getIndexName();

            // Create vector accessor factory for extracting vectors from ADM ordered lists
            AOrderedListVectorBinaryAccessorFactory vectorAccessorFactory =
                    new AOrderedListVectorBinaryAccessorFactory();
            return new LSMVTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                    filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory, pageWriteCallbackFactory,
                    metadataPageManagerFactory, vbcProvider, ioSchedulerProvider, mergePolicyFactory,
                    mergePolicyProperties, true, vectorDimensions, vectorFields,
                    typeTraitProvider.getTypeTrait(BuiltinType.ANULL), NullIntrospector.INSTANCE, false,
                    vectorAccessorFactory, numPrimaryKeys, numIncludeFields, dataTupleCreatorFactory, indexName);
        } else {
            return null;
        }
    }

    private static ITypeTraits[] getTypeTraits(MetadataProvider metadataProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, boolean isQuantized) throws AlgebricksException {
        ITypeTraitProvider ttProvider = metadataProvider.getStorageComponentProvider().getTypeTraitProvider();
        Index.VectorIndexDetails vectorIndexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(metadataProvider, recordType, metaType);

        // Get INCLUDE fields (optional)
        List<List<String>> includeFieldNames = vectorIndexDetails.getIncludeFieldNames();
        int numIncludeFields = (includeFieldNames != null) ? includeFieldNames.size() : 0;

        // Data frame tuple format depends on quantization:
        // Non-quantized: [distance, centroidId, primary_keys..., include_fields...]
        // Quantized:     [distance, centroidId, quantized_distance, quantized_embedding, primary_keys..., include_fields...]
        int numSecondaryFields = isQuantized ? 4 : 2;
        int totalFields = numSecondaryFields + numPrimaryKeys + numIncludeFields;
        ITypeTraits[] typeTraits = new ITypeTraits[totalFields];

        // Field 0: distance (raw double - 8 bytes, no ADM type tag)
        typeTraits[0] = new FixedLengthTypeTrait(8);

        if (isQuantized) {
            // Field 1: centroidId (raw int - 4 bytes, no ADM type tag)
            typeTraits[1] = new FixedLengthTypeTrait(4);
            // Field 2: quantized_distance (variable length byte[])
            typeTraits[2] = VarLengthTypeTrait.INSTANCE;
            // Field 3: quantized_embedding (variable length byte[])
            typeTraits[3] = VarLengthTypeTrait.INSTANCE;
        } else {
            // Field 1: centroidId (raw int - 4 bytes, no ADM type tag)
            typeTraits[1] = new FixedLengthTypeTrait(4);
        }

        int pkStart = numSecondaryFields;

        // Primary keys
        for (int i = 0; i < numPrimaryKeys; i++) {
            typeTraits[pkStart + i] = primaryTypeTraits[i];
        }

        // INCLUDE fields (if any)
        if (numIncludeFields > 0) {
            List<IAType> includeFieldTypes = vectorIndexDetails.getIncludeFieldTypes();
            List<Integer> includeSourceIndicators = vectorIndexDetails.getIncludeFieldSourceIndicators();

            for (int i = 0; i < numIncludeFields; i++) {
                IAType includeFieldType = null;

                // Get field type from index details
                if (includeFieldTypes != null && i < includeFieldTypes.size()) {
                    includeFieldType = includeFieldTypes.get(i);
                } else {
                    // Fallback: try to get from record type
                    List<String> includeFieldName = includeFieldNames.get(i);
                    ARecordType sourceType = (includeSourceIndicators == null || includeSourceIndicators.get(i) == 0)
                            ? recordType : metaType;
                    try {
                        includeFieldType = sourceType.getSubFieldType(includeFieldName);
                    } catch (AlgebricksException e) {
                        // Use default for open fields
                        includeFieldType = DefaultOpenFieldType.getDefaultOpenFieldType(ATypeTag.ANY);
                    }
                }

                if (includeFieldType == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_FIELD_NOT_FOUND,
                            includeFieldNames.get(i).toString());
                }

                typeTraits[pkStart + numPrimaryKeys + i] = ttProvider.getTypeTrait(includeFieldType);
            }
        }

        return typeTraits;
    }

    private static IBinaryComparatorFactory[] getCmpFactories(MetadataProvider metadataProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType, boolean isQuantized) throws AlgebricksException {
        IBinaryComparatorFactoryProvider cmpFactoryProvider =
                metadataProvider.getStorageComponentProvider().getComparatorFactoryProvider();
        Index.VectorIndexDetails vectorIndexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();

        // Get INCLUDE fields (optional)
        List<List<String>> includeFieldNames = vectorIndexDetails.getIncludeFieldNames();
        int numIncludeFields = (includeFieldNames != null) ? includeFieldNames.size() : 0;

        // Data frame comparators depend on quantization:
        // Non-quantized: [distance, centroidId, primary_keys..., include_fields...]
        // Quantized:     [distance, centroidId, quantized_distance, quantized_embedding, primary_keys..., include_fields...]
        int numSecondaryFields = isQuantized ? 4 : 2;
        int totalFields = numSecondaryFields + numPrimaryKeys + numIncludeFields;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[totalFields];

        // Comparator 0: distance (raw double - no ADM type tag)
        cmpFactories[0] = DoubleBinaryComparatorFactory.INSTANCE;

        if (isQuantized) {
            // Comparator 1: centroidId (raw int - no ADM type tag)
            cmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
            // Comparator 2: quantized_distance (variable length byte[])
            cmpFactories[2] = RawBinaryComparatorFactory.INSTANCE;
            // Comparator 3: quantized_embedding (variable length byte[])
            cmpFactories[3] = RawBinaryComparatorFactory.INSTANCE;
        } else {
            // Comparator 1: centroidId (raw int - no ADM type tag)
            cmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        }

        int pkStart = numSecondaryFields;

        // Primary key comparators
        IBinaryComparatorFactory[] primaryComparatorFactories =
                dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        for (int i = 0; i < numPrimaryKeys; i++) {
            cmpFactories[pkStart + i] = primaryComparatorFactories[i];
        }

        // Comparators after primary keys: INCLUDE fields (if any)
        if (numIncludeFields > 0) {
            List<IAType> includeFieldTypes = vectorIndexDetails.getIncludeFieldTypes();
            List<Integer> includeSourceIndicators = vectorIndexDetails.getIncludeFieldSourceIndicators();

            for (int i = 0; i < numIncludeFields; i++) {
                IAType includeFieldType = null;

                // Get field type from index details
                if (includeFieldTypes != null && i < includeFieldTypes.size()) {
                    includeFieldType = includeFieldTypes.get(i);
                } else {
                    // Fallback: try to get from record type
                    List<String> includeFieldName = includeFieldNames.get(i);
                    ARecordType sourceType = (includeSourceIndicators == null || includeSourceIndicators.get(i) == 0)
                            ? recordType : metaType;
                    try {
                        includeFieldType = sourceType.getSubFieldType(includeFieldName);
                    } catch (AlgebricksException e) {
                        // Use default for open fields
                        includeFieldType = DefaultOpenFieldType.getDefaultOpenFieldType(ATypeTag.ANY);
                    }
                }

                if (includeFieldType == null) {
                    throw new CompilationException(ErrorCode.COMPILATION_FIELD_NOT_FOUND,
                            includeFieldNames.get(i).toString());
                }

                cmpFactories[pkStart + numPrimaryKeys + i] =
                        cmpFactoryProvider.getBinaryComparatorFactory(includeFieldType, true);
            }
        }

        return cmpFactories;
    }
}
