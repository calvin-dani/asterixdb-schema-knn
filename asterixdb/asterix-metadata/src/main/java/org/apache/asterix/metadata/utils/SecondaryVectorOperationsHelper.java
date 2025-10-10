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
import static org.apache.asterix.om.types.BuiltinType.AFLOAT;
import static org.apache.asterix.om.types.BuiltinType.AINT32;

import java.util.List;
import java.util.UUID;

import org.apache.asterix.common.cluster.PartitioningProperties;
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
import org.apache.asterix.runtime.operators.VCTreeStaticStructureBulkLoaderOperatorDescriptor;
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
import org.apache.hyracks.dataflow.std.misc.ReplicateOperatorDescriptor;
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
        System.err.println("Creating 2-Phase Job with Permit Mechanism");
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
        PartitioningProperties partitioningProperties =
                metadataProvider.getPartitioningProperties(dataset, index.getIndexName());
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        int[] fieldPermutation = createFieldPermutationForBulkLoadOp(numSecondaryKeys);
        int[] pkFields = createPkFieldPermutationForBulkLoadOp(fieldPermutation, numSecondaryKeys);
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);
        // job spec:
        // key provider -> primary idx scan -> cast assign -> replicate -> [branch1: k-means -> bulk load -> sink] [branch2: placeholder -> sink]
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        System.err.println("=== INITIAL JOB SETUP ===");
        System.err.println("Dataset: " + dataset.getDatasetName());
        System.err.println("Index: " + index.getIndexName());
        System.err.println("Creating 2-branch job with replication");

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

        UUID sampleUUID = UUID.randomUUID();
        UUID KCentroidsUUID = UUID.randomUUID();
        UUID centroidsUUID = UUID.randomUUID();
        UUID permitUUID = UUID.randomUUID();
        UUID kCentroidsUUID = UUID.randomUUID();

        int K = 10;
        int maxScalableKmeansIter = 2;

        // _ -> init centroids (materialize sample)

        ISerializerDeserializer[] newCentSerde = new ISerializerDeserializer[1];
        ITypeTraits[] newCentTraits = new ITypeTraits[1];

        newCentSerde[0] = serdeProvider.getSerializerDeserializer(new AOrderedListType(AFLOAT, "embedding"));

        newCentTraits[0] = typeTraitProvider.getTypeTrait(new AOrderedListType(AFLOAT, "embedding"));
        // Construct the new RecordDescriptor

        RecordDescriptor centroidRecDesc = new RecordDescriptor(newCentSerde, newCentTraits);

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

        // ====== REPLICATION OPERATOR: Split data stream into two branches ======
        System.out.println("*** VECTOR INDEX DEBUG: CREATING REPLICATION OPERATOR ***");
        System.err.println("==========================================");
        System.err.println("*** CREATING REPLICATION OPERATOR ***");
        System.err.println("==========================================");
        sourceOp = targetOp;
        ReplicateOperatorDescriptor replicateOp = new ReplicateOperatorDescriptor(spec, secondaryRecDesc, 2);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, replicateOp,
                primaryPartitionConstraint);
        targetOp = replicateOp;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        System.out.println("*** VECTOR INDEX DEBUG: REPLICATION OPERATOR CREATED ***");
        System.err.println("=== REPLICATION OPERATOR CREATED ===");
        System.err.println("Replicate operator: " + replicateOp);
        System.err.println("Input record descriptor: " + secondaryRecDesc);
        System.err.println("Number of outputs: 2");
        System.err.println("Output 0 → Branch 1 (Structure Creation)");
        System.err.println("Output 1 → Branch 2 (Data Loading Placeholder)");
        System.err.println("==========================================");

        // ====== BRANCH 1: STRUCTURE CREATION (Phase 1) ======
        IOperatorDescriptor branch1Source = targetOp;

        System.err.println("=== BRANCH 1 SETUP: STRUCTURE CREATION ===");
        System.err.println("Branch 1 source: " + branch1Source);

        // init centroids -(broadcast)> candidate centroids
        HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor candidates =
                new HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(spec, hierarchicalRecDesc, secondaryRecDesc,
                        sampleUUID, centroidsUUID, new ColumnAccessEvalFactory(0), K, maxScalableKmeansIter);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, candidates,
                primaryPartitionConstraint);
        IOperatorDescriptor branch1Target = candidates;
        spec.connect(new OneToOneConnectorDescriptor(spec), branch1Source, 0, branch1Target, 0);

        System.err.println("Branch 1 - HierarchicalKMeans: " + candidates);
        System.err.println("Connected: " + branch1Source + " output 0 → " + branch1Target + " input 0");

        // Connect hierarchical k-means output to VCTree bulk loader
        branch1Source = branch1Target;
        VCTreeStaticStructureBulkLoaderOperatorDescriptor vcTreeLoader =
                new VCTreeStaticStructureBulkLoaderOperatorDescriptor(spec, dataflowHelperFactory, 100, 0.7f,
                        hierarchicalRecDesc, permitUUID);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, vcTreeLoader,
                primaryPartitionConstraint);
        branch1Target = vcTreeLoader;
        spec.connect(new OneToOneConnectorDescriptor(spec), branch1Source, 0, branch1Target, 0);

        System.err.println("Branch 1 - VCTreeBulkLoader: " + vcTreeLoader);
        System.err.println("Connected: " + branch1Source + " output 0 → " + branch1Target + " input 0");

        // Add sink for Branch 1 final output
        branch1Source = branch1Target;
        SinkRuntimeFactory branch1SinkRuntimeFactory = new SinkRuntimeFactory();
        branch1SinkRuntimeFactory.setSourceLocation(sourceLoc);
        IOperatorDescriptor branch1Sink = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { branch1SinkRuntimeFactory },
                new RecordDescriptor[] { hierarchicalRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, branch1Sink,
                primaryPartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), branch1Source, 0, branch1Sink, 0);

        System.err.println("Branch 1 - Sink: " + branch1Sink);
        System.err.println("Connected: " + branch1Source + " output 0 → " + branch1Sink + " input 0");
        System.err.println("=== BRANCH 1 COMPLETE ===");

        // ====== BRANCH 2: DATA LOADING PLACEHOLDER (Phase 2) ======
        IOperatorDescriptor branch2Source = targetOp; // Same replicate operator, output 1

        System.err.println("=== BRANCH 2 SETUP: DATA LOADING PLACEHOLDER ===");
        System.err.println("Branch 2 source: " + branch2Source);
        System.err.println("Using replicate operator output 1");

        // Placeholder operator for data loading - does nothing for now
        SinkRuntimeFactory branch2PlaceholderRuntimeFactory = new SinkRuntimeFactory();
        branch2PlaceholderRuntimeFactory.setSourceLocation(sourceLoc);
        IOperatorDescriptor branch2Placeholder = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { branch2PlaceholderRuntimeFactory },
                new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, branch2Placeholder,
                primaryPartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), branch2Source, 1, branch2Placeholder, 0);

        System.err.println("Branch 2 - Placeholder Sink: " + branch2Placeholder);
        System.err.println("Connected: " + branch2Source + " output 1 → " + branch2Placeholder + " input 0");
        System.err.println("=== BRANCH 2 COMPLETE ===");

        // ====== PERMIT MECHANISM ======
        System.err.println("=== PERMIT MECHANISM INITIALIZED ===");
        System.err.println("Permit UUID: " + permitUUID);
        System.err.println("Branch 1 (Structure Creation) will complete first");
        System.err.println("Branch 2 (Data Loading) is placeholder - ready for future implementation");

        // Add both branches as roots
        spec.addRoot(branch1Sink);
        spec.addRoot(branch2Placeholder);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        System.err.println("=== JOB SPECIFICATION COMPLETE ===");
        System.err.println("Root operators added:");
        System.err.println("  Root 1: " + branch1Sink + " (Branch 1 - Structure Creation)");
        System.err.println("  Root 2: " + branch2Placeholder + " (Branch 2 - Data Loading Placeholder)");
        System.err.println("=== 2-PHASE JOB CREATED WITH REPLICATION ===");
        System.err.println("=== BRANCH 1: DataSource → Replicate → K-means → StructureBuilder → Sink ===");
        System.err.println("=== BRANCH 2: DataSource → Replicate → Placeholder (Data Loading) ===");
        System.err.println("=== PERMIT MECHANISM: Ready for Phase 2 coordination ===");
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
