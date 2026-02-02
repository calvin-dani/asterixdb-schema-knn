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
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.LSMVCTreeLocalResourceFactory;
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

        List<List<String>> primaryKeyFields = dataset.getPrimaryKeys();
        int numPrimaryKeys = primaryKeyFields.size();

        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        ITypeTraitProvider typeTraitProvider = mdProvider.getDataFormat().getTypeTraitProvider();

        // Get type traits and comparator factories
        ITypeTraits[] typeTraits = getTypeTraits(mdProvider, dataset, index, recordType, metaType);
        IBinaryComparatorFactory[] cmpFactories = getCmpFactories(mdProvider, dataset, index, recordType, metaType);

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

            // Pass index name to factory so LSMVCTreeLocalResource can read quantization sidecar file
            // The sidecar file is written by Job 0.5 (quantization computation) and is located at:
            // dataset_dir/.quantization_<indexName>
            String indexName = index.getIndexName();
            System.err.println("[VCTreeResourceFactoryProvider] Creating factory with indexName=" + indexName + 
                    " for sidecar file lookup");

            return new LSMVCTreeLocalResourceFactory(storageManager, typeTraits, cmpFactories, filterTypeTraits,
                    filterCmpFactories, filterFields, opTrackerFactory, ioOpCallbackFactory, pageWriteCallbackFactory,
                    metadataPageManagerFactory, vbcProvider, ioSchedulerProvider, mergePolicyFactory,
                    mergePolicyProperties, true, vectorDimensions, vectorFields,
                    typeTraitProvider.getTypeTrait(BuiltinType.ANULL), NullIntrospector.INSTANCE, dataset.isAtomic(),
                    indexName);
        } else {
            return null;
        }
    }

    private static ITypeTraits[] getTypeTraits(MetadataProvider metadataProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        ITypeTraitProvider ttProvider = metadataProvider.getStorageComponentProvider().getTypeTraitProvider();
        Index.VectorIndexDetails vectorIndexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        List<List<String>> keyFieldNames = vectorIndexDetails.getKeyFieldNames();
        int numKeyFields = keyFieldNames.size();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(metadataProvider, recordType, metaType);

        if (numKeyFields != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD, numKeyFields,
                    index.getIndexType(), 1);
        }

        // Get vector field type
        List<String> vectorFieldNames = keyFieldNames.get(0);

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
            throw new CompilationException(ErrorCode.COMPILATION_FIELD_NOT_FOUND, vectorFieldNames.toString());
        }

        // Create type traits for vector field + primary keys
        ITypeTraits[] typeTraits = new ITypeTraits[numKeyFields + numPrimaryKeys];
        typeTraits[0] = ttProvider.getTypeTrait(vectorType); // Vector field
        for (int i = 0; i < numPrimaryKeys; i++) {
            typeTraits[numKeyFields + i] = primaryTypeTraits[i];
        }

        return typeTraits;
    }

    private static IBinaryComparatorFactory[] getCmpFactories(MetadataProvider metadataProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        IBinaryComparatorFactoryProvider cmpFactoryProvider =
                metadataProvider.getStorageComponentProvider().getComparatorFactoryProvider();
        Index.VectorIndexDetails vectorIndexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        List<List<String>> keyFieldNames = vectorIndexDetails.getKeyFieldNames();
        int numKeyFields = keyFieldNames.size();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();

        if (numKeyFields != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD, numKeyFields,
                    index.getIndexType(), 1);
        }

        // Get vector field type
        List<String> vectorFieldNames = keyFieldNames.get(0);

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
            throw new CompilationException(ErrorCode.COMPILATION_FIELD_NOT_FOUND, vectorFieldNames.toString());
        }

        // Create comparator factories for vector field + primary keys
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[numKeyFields + numPrimaryKeys];
        cmpFactories[0] = cmpFactoryProvider.getBinaryComparatorFactory(vectorType, true); // Vector field

        // Add primary key comparator factories
        IBinaryComparatorFactory[] primaryComparatorFactories =
                dataset.getPrimaryComparatorFactories(metadataProvider, recordType, metaType);
        for (int i = 0; i < numPrimaryKeys; i++) {
            cmpFactories[numKeyFields + i] = primaryComparatorFactories[i];
        }

        return cmpFactories;
    }
}
