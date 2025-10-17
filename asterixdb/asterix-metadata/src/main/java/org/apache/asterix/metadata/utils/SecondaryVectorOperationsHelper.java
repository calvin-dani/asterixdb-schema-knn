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

import static org.apache.asterix.om.types.BuiltinType.ADOUBLE;
import static org.apache.asterix.om.types.BuiltinType.AINT32;

import java.util.List;
import java.util.UUID;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor;
import org.apache.asterix.runtime.operators.VCTreeBulkLoaderAndGroupingOperatorDescriptor;
import org.apache.asterix.runtime.operators.VCTreeStaticStructureCreatorOperatorDescriptor;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class SecondaryVectorOperationsHelper extends SecondaryTreeIndexOperationsHelper {

    private RecordDescriptor recordDesc;

    protected SecondaryVectorOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    @Override
    public void init() throws AlgebricksException {
        super.init();
        recordDesc = dataset.getPrimaryRecordDescriptor(metadataProvider);

    }

    @Override
    public JobSpecification buildStaticStructureJobSpec() throws AlgebricksException {
        // Force output to both System.out and System.err to ensure visibility
        System.out.println(
                "*** VECTOR INDEX DEBUG: SecondaryVectorOperationsHelper.buildStaticStructureJobSpec() CALLED ***");
        System.err.println("==========================================");
        System.err.println("*** SecondaryVectorOperationsHelper.buildStaticStructureJobSpec() CALLED ***");
        System.err.println("==========================================");
        System.err.println("Dataset: " + dataset.getDatasetName());
        System.err.println("Index: " + index.getIndexName());
        System.err.println("Index type: " + index.getIndexType());
        System.err.println("Index details: " + index.getIndexDetails());
        System.err.println("Creating Static Structure Job for K-means + Structure Creation");
        System.err.println("==========================================");

        IDataFormat format = metadataProvider.getDataFormat();
        int nFields = recordDesc.getFieldCount();
        int[] columns = new int[nFields];
        for (int i = 0; i < nFields; i++) {
            columns[i] = i;
        }
        ISerializerDeserializerProvider serdeProvider = format.getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = format.getTypeTraitProvider();

        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);
        // job spec:
        // key provider -> primary idx scan -> cast assign -> k-means -> static structure creator -> sink
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        System.err.println("=== STATIC STRUCTURE JOB SETUP ===");
        System.err.println("Dataset: " + dataset.getDatasetName());
        System.err.println("Index: " + index.getIndexName());
        System.err.println("Creating static structure job for K-means + structure creation");

        // if format == column, then project only the indexed fields
        ITupleProjectorFactory projectorFactory =
                IndexUtil.createPrimaryIndexScanTupleProjectorFactory(dataset.getDatasetFormatInfo(),
                        indexDetails.getIndexExpectedType(), itemType, metaType, numPrimaryKeys);
        // dummy key provider -> primary index scan
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp =
                DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        System.err.println("Connected: DummyKeyProvider → PrimaryIndexScan");

        sourceOp = targetOp;
        // primary index -> cast assign op (produces the secondary index entry)
        targetOp = createAssignOp(spec, numSecondaryKeys, recordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        System.err.println("Connected: PrimaryIndexScan → CastAssign");
        System.err.println("CastAssign operator: " + targetOp);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        UUID sampleUUID = UUID.randomUUID();
        UUID tupleCountUUID = UUID.randomUUID();
        UUID permitUUID = UUID.randomUUID();

        // Register permit state for structure creation coordination
        System.err.println("=== REGISTERING PERMIT STATE ===");
        System.err.println("Permit UUID: " + permitUUID);
        System.err.println("Permit state will be used by StaticStructureCreator operator");

        int K = 10;
        int maxScalableKmeansIter = 2;

        // Create record descriptor for hierarchical k-means output (level, clusterId, centroidId, embedding)
        ISerializerDeserializer[] hierarchicalSerde = new ISerializerDeserializer[4];
        ITypeTraits[] hierarchicalTraits = new ITypeTraits[4];

        // Level (int)
        hierarchicalSerde[0] = serdeProvider.getSerializerDeserializer(AINT32);
        hierarchicalTraits[0] = typeTraitProvider.getTypeTrait(AINT32);

        // ClusterId (int)
        hierarchicalSerde[1] = serdeProvider.getSerializerDeserializer(AINT32);
        hierarchicalTraits[1] = typeTraitProvider.getTypeTrait(AINT32);

        // CentroidId (int)
        hierarchicalSerde[2] = serdeProvider.getSerializerDeserializer(AINT32);
        hierarchicalTraits[2] = typeTraitProvider.getTypeTrait(AINT32);

        // Embedding (float array)
        hierarchicalSerde[3] = serdeProvider.getSerializerDeserializer(new AOrderedListType(ADOUBLE, "embedding"));
        hierarchicalTraits[3] = typeTraitProvider.getTypeTrait(new AOrderedListType(ADOUBLE, "embedding"));

        RecordDescriptor hierarchicalRecDesc = new RecordDescriptor(hierarchicalSerde, hierarchicalTraits);

        System.err.println("=== RECORD DESCRIPTOR COMPARISON ===");
        System.err.println("Input record descriptor (from dataset): " + recordDesc);
        System.err.println("Secondary record descriptor (from assign op): " + secondaryRecDesc);
        System.err.println("Hierarchical record descriptor (to k-means): " + hierarchicalRecDesc);
        System.err.println("Number of fields in input: " + recordDesc.getFieldCount());
        System.err.println("Number of fields in secondary: " + secondaryRecDesc.getFieldCount());
        System.err.println("Number of fields in hierarchical: " + hierarchicalRecDesc.getFieldCount());

        // ====== STATIC STRUCTURE JOB: K-MEANS → STATIC STRUCTURE CREATION → SINK ======
        System.out.println("*** VECTOR INDEX DEBUG: CREATING STATIC STRUCTURE PIPELINE ***");
        System.err.println("==========================================");
        System.err.println("*** CREATING STATIC STRUCTURE PIPELINE ***");
        System.err.println("==========================================");
        System.err.println("Pipeline: CastAssign → K-means → StaticStructureCreator → Sink");

        // 1. K-means operator
        System.err.println("🔧 CREATING HIERARCHICAL K-MEANS OPERATOR");
        UUID materializedDataUUID = UUID.randomUUID();
        HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor candidates =
                new HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(spec, hierarchicalRecDesc, secondaryRecDesc,
                        sampleUUID, tupleCountUUID, materializedDataUUID, new ColumnAccessEvalFactory(0), K,
                        maxScalableKmeansIter);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, candidates,
                primaryPartitionConstraint);
        targetOp = candidates;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        System.err.println("Connected: CastAssign → K-means");
        System.err.println("K-means operator: " + candidates);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        // 2. VCTree static structure creator
        System.err.println("🔧 CREATING VCTreeStaticStructureCreator");
        VCTreeStaticStructureCreatorOperatorDescriptor vcTreeCreator =
                new VCTreeStaticStructureCreatorOperatorDescriptor(spec, dataflowHelperFactory, 100, 0.7f,
                        hierarchicalRecDesc, permitUUID, materializedDataUUID);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, vcTreeCreator,
                primaryPartitionConstraint);
        targetOp = vcTreeCreator;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        System.err.println("Connected: K-means → StaticStructureCreator");
        System.err.println("StaticStructureCreator operator: " + vcTreeCreator);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        // 3. Final sink
        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0, new IPushRuntimeFactory[] { sinkRuntimeFactory },
                new RecordDescriptor[] { hierarchicalRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, primaryPartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        System.err.println("Connected: StaticStructureCreator → Sink");
        System.err.println("Sink operator: " + targetOp);
        System.err.println("=== STATIC STRUCTURE PIPELINE COMPLETE ===");

        // Add single branch as root
        spec.addRoot(targetOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        System.err.println("=== STATIC STRUCTURE JOB SPECIFICATION COMPLETE ===");
        System.err.println("Root operators added:");
        System.err.println("  Root: " + targetOp + " (Static Structure - K-means → StaticStructureCreator → Sink)");
        System.err.println("=== STATIC STRUCTURE JOB CREATED ===");
        System.err.println("=== PIPELINE: DataSource → CastAssign → K-means → StaticStructureCreator → Sink ===");
        System.err.println(
                "=== STRUCTURE CREATION: K-means creates centroids, StaticStructureCreator stores structure ===");
        System.err.println("==========================================");
        System.err.println("*** SecondaryVectorOperationsHelper.buildStaticStructureJobSpec() COMPLETED ***");
        System.err.println("==========================================");
        System.out.println(
                "*** VECTOR INDEX DEBUG: SecondaryVectorOperationsHelper.buildStaticStructureJobSpec() COMPLETED ***");
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        // Force output to both System.out and System.err to ensure visibility
        System.out.println("*** VECTOR INDEX DEBUG: SecondaryVectorOperationsHelper.buildLoadingJobSpec() CALLED ***");
        System.err.println("==========================================");
        System.err.println("*** SecondaryVectorOperationsHelper.buildLoadingJobSpec() CALLED ***");
        System.err.println("==========================================");
        System.err.println("Dataset: " + dataset.getDatasetName());
        System.err.println("Index: " + index.getIndexName());
        System.err.println("Index type: " + index.getIndexType());
        System.err.println("Index details: " + index.getIndexDetails());
        System.err.println("Creating Data Loading Job with Bulk Loader and Grouping (Job 3)");
        System.err.println("==========================================");

        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);

        // Job spec: key provider -> primary idx scan -> cast assign -> bulk loader and grouping -> sink
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        System.err.println("=== DATA LOADING JOB SETUP ===");
        System.err.println("Dataset: " + dataset.getDatasetName());
        System.err.println("Index: " + index.getIndexName());
        System.err.println("Creating data loading job with bulk loader and grouping functionality");

        // if format == column, then project only the indexed fields
        ITupleProjectorFactory projectorFactory =
                IndexUtil.createPrimaryIndexScanTupleProjectorFactory(dataset.getDatasetFormatInfo(),
                        indexDetails.getIndexExpectedType(), itemType, metaType, numPrimaryKeys);

        // dummy key provider -> primary index scan
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp =
                DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        System.err.println("Connected: DummyKeyProvider → PrimaryIndexScan");

        sourceOp = targetOp;
        // primary index -> cast assign op (produces the secondary index entry)
        targetOp = createAssignOp(spec, numSecondaryKeys, recordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        System.err.println("Connected: PrimaryIndexScan → CastAssign");
        System.err.println("CastAssign operator: " + targetOp);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        // ====== DATA LOADING JOB: CAST ASSIGN → BULK LOADER AND GROUPING → SINK ======
        System.out.println("*** VECTOR INDEX DEBUG: CREATING DATA LOADING PIPELINE ***");
        System.err.println("==========================================");
        System.err.println("*** CREATING DATA LOADING PIPELINE ***");
        System.err.println("==========================================");
        System.err.println("Pipeline: CastAssign → BulkLoaderAndGrouping → Sink");

        // Create permit UUIDs for coordination
        UUID permitUUID = UUID.randomUUID();
        UUID materializedDataUUID = UUID.randomUUID();

        // Create VCTreeBulkLoaderAndGroupingOperatorDescriptor
        // Use ColumnAccessEvalFactory(0) to access the first field (vector field) from processed tuple
        IScalarEvaluatorFactory vectorFieldAccessor = new ColumnAccessEvalFactory(0);
        VCTreeBulkLoaderAndGroupingOperatorDescriptor bulkLoaderAndGroupingOp =
                new VCTreeBulkLoaderAndGroupingOperatorDescriptor(spec, dataflowHelperFactory, 128, 0.7f,
                        secondaryRecDesc, permitUUID, materializedDataUUID, vectorFieldAccessor);
        bulkLoaderAndGroupingOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, bulkLoaderAndGroupingOp,
                primaryPartitionConstraint);

        // Connect CastAssign → BulkLoaderAndGrouping (which is a sink operator)
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, bulkLoaderAndGroupingOp, 0);
        System.err.println("Connected: CastAssign → BulkLoaderAndGrouping");
        System.err.println("BulkLoaderAndGrouping operator: " + bulkLoaderAndGroupingOp);
        System.err.println("=== DATA LOADING PIPELINE COMPLETE ===");

        // Add single branch as root (BulkLoaderAndGrouping is the sink)
        spec.addRoot(bulkLoaderAndGroupingOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        System.err.println("=== DATA LOADING JOB SPECIFICATION COMPLETE ===");
        System.err.println("Root operators added:");
        System.err
                .println("  Root: " + bulkLoaderAndGroupingOp + " (Data Loading - CastAssign → BulkLoaderAndGrouping)");
        System.err.println("=== DATA LOADING JOB CREATED ===");
        System.err.println("=== PIPELINE: DataSource → CastAssign → BulkLoaderAndGrouping ===");
        System.err.println("=== DATA LOADING: Initializes bulk loader and groups data into run files ===");
        System.err.println("==========================================");
        System.err.println("*** SecondaryVectorOperationsHelper.buildLoadingJobSpec() COMPLETED ***");
        System.err.println("==========================================");
        System.out
                .println("*** VECTOR INDEX DEBUG: SecondaryVectorOperationsHelper.buildLoadingJobSpec() COMPLETED ***");
        return spec;
    }

    @Override
    protected int getNumSecondaryKeys() {
        Index.VectorIndexDetails vectorIndexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        // For vector indexes, we always have at least 1 secondary key (the vector field itself)
        // Include fields are additional fields beyond the vector field
        List<List<String>> includeFieldNames = vectorIndexDetails.getIncludeFieldNames();
        int includeFieldsCount = (includeFieldNames == null) ? 0 : includeFieldNames.size();
        return 1 + includeFieldsCount; // 1 for vector field + include fields
    }

    /**
     * ======
     * |  SK  |             Bloom filter
     * ======
     * ====== ======
     * |  SK  |  PK  |      comparators, type traits
     * ====== ======
     * ====== ........
     * |  SK  | Filter |    field access evaluators
     * ====== ........
     * ====== ====== ........
     * |  SK  |  PK  | Filter |   record fields
     * ====== ====== ........
     * ====== ========= ........ ........
     * |  PK  | Payload |  Meta  | Filter | enforced record
     * ====== ========= ........ ........
     */
    @Override
    protected void setSecondaryRecDescAndComparators() throws AlgebricksException {
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        secondaryFieldAccessEvalFactories = new IScalarEvaluatorFactory[numSecondaryKeys + numFilterFields];
        secondaryComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        secondaryBloomFilterKeyFields = new int[numSecondaryKeys];
        ISerializerDeserializer[] secondaryRecFields =
                new ISerializerDeserializer[numSecondaryKeys + numPrimaryKeys + numFilterFields];
        ISerializerDeserializer[] enforcedRecFields =
                new ISerializerDeserializer[1 + numPrimaryKeys + (dataset.hasMetaPart() ? 1 : 0) + numFilterFields];
        ITypeTraits[] enforcedTypeTraits =
                new ITypeTraits[1 + numPrimaryKeys + (dataset.hasMetaPart() ? 1 : 0) + numFilterFields];
        secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadataProvider.getDataFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = metadataProvider.getDataFormat().getTypeTraitProvider();
        IBinaryComparatorFactoryProvider comparatorFactoryProvider =
                metadataProvider.getDataFormat().getBinaryComparatorFactoryProvider();
        // Record column is 0 for external datasets, numPrimaryKeys for internal ones
        int recordColumn = dataset.getDatasetType() == DatasetType.INTERNAL ? numPrimaryKeys : 0;
        boolean isOverridingKeyFieldTypes = indexDetails.isOverridingKeyFieldTypes();

        // For VECTOR indexes, we process the vector field first, then include fields
        List<List<String>> keyFieldNames = indexDetails.getKeyFieldNames();
        List<List<String>> includeFieldNames = indexDetails.getIncludeFieldNames();
        List<IAType> includeFieldTypes = indexDetails.getIncludeFieldTypes();
        List<Integer> includeSourceIndicators = indexDetails.getIncludeFieldSourceIndicators();

        // Process the vector field as the first secondary key
        if (keyFieldNames != null && !keyFieldNames.isEmpty()) {
            // Vector field is always in the record part (source indicator 0)
            ARecordType sourceType = itemType;
            ARecordType enforcedType = enforcedItemType;
            int sourceColumn = recordColumn;

            System.err.println("=== VECTOR FIELD PROCESSING ===");
            System.err.println("Key field names: " + keyFieldNames);
            System.err.println("Include field names: " + includeFieldNames);
            System.err.println("Include field types: " + includeFieldTypes);
            System.err.println("Source type: " + sourceType);
            System.err.println("Record column: " + sourceColumn);
            System.err.println("Number of secondary keys: " + numSecondaryKeys);

            List<String> vectorFieldName = keyFieldNames.get(0);
            IAType vectorFieldType = new AOrderedListType(ADOUBLE, "embedding"); // Default vector type

            System.err.println("Vector field name: " + vectorFieldName);
            System.err.println("Vector field type: " + vectorFieldType);

            Pair<IAType, Boolean> keyTypePair =
                    Index.getNonNullableOpenFieldType(index, vectorFieldType, vectorFieldName, sourceType);
            IAType keyType = keyTypePair.first;
            System.err.println("Resolved key type: " + keyType);

            IScalarEvaluatorFactory vectorFieldAccessor =
                    createFieldAccessor(sourceType, sourceColumn, vectorFieldName);
            System.err.println("Created vector field accessor: " + vectorFieldAccessor);

            secondaryFieldAccessEvalFactories[0] =
                    createFieldCast(vectorFieldAccessor, isOverridingKeyFieldTypes, enforcedType, sourceType, keyType);
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            secondaryRecFields[0] = serdeProvider.getSerializerDeserializer(keyType);
            secondaryComparatorFactories[0] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
            secondaryTypeTraits[0] = typeTraitProvider.getTypeTrait(keyType);
            secondaryBloomFilterKeyFields[0] = 0;

            System.err.println("Vector field configured as secondary key 0");
            System.err.println("=== END VECTOR FIELD PROCESSING ===");
        } else {
            System.err.println("=== NO VECTOR FIELD FOUND ===");
            System.err.println("Key field names: " + keyFieldNames);
        }

        // Process include fields (if any)
        if (includeFieldNames != null && !includeFieldNames.isEmpty() && includeFieldTypes != null
                && !includeFieldTypes.isEmpty() && includeFieldNames.size() == includeFieldTypes.size()) {
            for (int i = 0; i < includeFieldNames.size(); i++) {
                ARecordType sourceType;
                ARecordType enforcedType;
                int sourceColumn;
                if (includeSourceIndicators == null || includeSourceIndicators.get(i) == 0) {
                    sourceType = itemType;
                    sourceColumn = recordColumn;
                    enforcedType = enforcedItemType;
                } else {
                    sourceType = metaType;
                    sourceColumn = recordColumn + 1;
                    enforcedType = enforcedMetaType;
                }
                List<String> secFieldName = includeFieldNames.get(i);
                IAType secFieldType = null;

                // Safely get the field type, handling potential index out of bounds
                if (i < includeFieldTypes.size()) {
                    secFieldType = includeFieldTypes.get(i);
                }

                // Skip if the field type is null or if we couldn't get it
                if (secFieldType == null) {
                    continue;
                }

                // Include fields start at index 1 (index 0 is the vector field)
                int fieldIndex = 1 + i;
                Pair<IAType, Boolean> keyTypePair =
                        Index.getNonNullableOpenFieldType(index, secFieldType, secFieldName, sourceType);
                IAType keyType = keyTypePair.first;
                IScalarEvaluatorFactory secFieldAccessor = createFieldAccessor(sourceType, sourceColumn, secFieldName);
                secondaryFieldAccessEvalFactories[fieldIndex] =
                        createFieldCast(secFieldAccessor, isOverridingKeyFieldTypes, enforcedType, sourceType, keyType);
                anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
                secondaryRecFields[fieldIndex] = serdeProvider.getSerializerDeserializer(keyType);
                secondaryComparatorFactories[fieldIndex] =
                        comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
                secondaryTypeTraits[fieldIndex] = typeTraitProvider.getTypeTrait(keyType);
                secondaryBloomFilterKeyFields[fieldIndex] = fieldIndex;
            }
        }
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            // Add serializers and comparators for primary index fields.
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFields[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
                enforcedRecFields[i] = primaryRecDesc.getFields()[i];
                secondaryTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
                enforcedTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
                secondaryComparatorFactories[numSecondaryKeys + i] = primaryComparatorFactories[i];
            }
        } else {
            // Add serializers and comparators for RID fields.
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFields[numSecondaryKeys + i] = IndexingConstants.getSerializerDeserializer(i);
                enforcedRecFields[i] = IndexingConstants.getSerializerDeserializer(i);
                secondaryTypeTraits[numSecondaryKeys + i] = IndexingConstants.getTypeTraits(i);
                enforcedTypeTraits[i] = IndexingConstants.getTypeTraits(i);
                secondaryComparatorFactories[numSecondaryKeys + i] = IndexingConstants.getComparatorFactory(i);
            }
        }
        enforcedRecFields[numPrimaryKeys] = serdeProvider.getSerializerDeserializer(itemType);
        enforcedTypeTraits[numPrimaryKeys] = typeTraitProvider.getTypeTrait(itemType);
        if (dataset.hasMetaPart()) {
            enforcedRecFields[numPrimaryKeys + 1] = serdeProvider.getSerializerDeserializer(metaType);
            enforcedTypeTraits[numPrimaryKeys + 1] = typeTraitProvider.getTypeTrait(metaType);
        }

        if (numFilterFields > 0) {
            Integer filterSourceIndicator =
                    ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterSourceIndicator();
            ARecordType sourceType;
            ARecordType enforcedType;
            int sourceColumn;
            if (filterSourceIndicator == null || filterSourceIndicator == 0) {
                sourceType = itemType;
                sourceColumn = recordColumn;
                enforcedType = enforcedItemType;
            } else {
                sourceType = metaType;
                sourceColumn = recordColumn + 1;
                enforcedType = enforcedMetaType;
            }
            IAType filterType = Index.getNonNullableKeyFieldType(filterFieldName, sourceType).first;
            IScalarEvaluatorFactory filterAccessor = createFieldAccessor(sourceType, sourceColumn, filterFieldName);
            secondaryFieldAccessEvalFactories[numSecondaryKeys] =
                    createFieldCast(filterAccessor, isOverridingKeyFieldTypes, enforcedType, sourceType, filterType);
            ISerializerDeserializer serde = serdeProvider.getSerializerDeserializer(filterType);
            secondaryRecFields[numPrimaryKeys + numSecondaryKeys] = serde;
            enforcedRecFields[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)] = serde;
            enforcedTypeTraits[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)] =
                    typeTraitProvider.getTypeTrait(filterType);
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields, secondaryTypeTraits);
        enforcedRecDesc = new RecordDescriptor(enforcedRecFields, enforcedTypeTraits);

    }

    private int[] createFieldPermutationForBulkLoadOp(int numSecondaryKeyFields) {
        int[] fieldPermutation = new int[numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = i;
        }
        return fieldPermutation;
    }
}
