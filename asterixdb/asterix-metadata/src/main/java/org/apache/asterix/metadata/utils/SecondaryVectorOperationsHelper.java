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

import static org.apache.asterix.om.types.BuiltinType.AFLOAT;

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
import org.apache.asterix.runtime.operators.CandidateCentroidsOperatorDescriptor;
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
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {

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
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        int[] fieldPermutation = createFieldPermutationForBulkLoadOp(numSecondaryKeys);
        int[] pkFields = createPkFieldPermutationForBulkLoadOp(fieldPermutation, numSecondaryKeys);
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);
        // job spec:
        // key provider -> primary idx scan -> cast assign -> (select)? -> (sort)? -> bulk load -> sink
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // if format == column, then project only the indexed fields
        ITupleProjectorFactory projectorFactory =
                IndexUtil.createPrimaryIndexScanTupleProjectorFactory(dataset.getDatasetFormatInfo(),
                        indexDetails.getIndexExpectedType(), itemType, metaType, numPrimaryKeys);
        // dummy key provider -> primary index scan
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp =
                DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        sourceOp = targetOp;
        // primary index -> cast assign op (produces the secondary index entry)
        targetOp = createAssignOp(spec, numSecondaryKeys, recordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        UUID sampleUUID = UUID.randomUUID();
        UUID KCentroidsUUID = UUID.randomUUID();
        UUID centroidsUUID = UUID.randomUUID();
        UUID permitUUID = UUID.randomUUID();
        UUID kCentroidsUUID = UUID.randomUUID();

        int K = 200;
        int maxScalableKmeansIter = 2;

        // _ -> init centroids (materialize sample)

        ISerializerDeserializer[] newCentSerde = new ISerializerDeserializer[1];
        ITypeTraits[] newCentTraits = new ITypeTraits[1];

        newCentSerde[0] = serdeProvider.getSerializerDeserializer(new AOrderedListType(AFLOAT, "embedding"));

        newCentTraits[0] = typeTraitProvider.getTypeTrait(new AOrderedListType(AFLOAT, "embedding"));
        // Construct the new RecordDescriptor

        RecordDescriptor centroidRecDesc = new RecordDescriptor(newCentSerde, newCentTraits);

        // init centroids -(broadcast)> candidate centroids
        sourceOp = targetOp;
        CandidateCentroidsOperatorDescriptor candidates = new CandidateCentroidsOperatorDescriptor(spec,
                secondaryRecDesc, sampleUUID, centroidsUUID, permitUUID, new ColumnAccessEvalFactory(0), K,maxScalableKmeansIter);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, candidates,
                primaryPartitionConstraint);
        targetOp = candidates;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        //        sourceOp = targetOp;
        //        KCandidateCentroidsOperatorDescriptor Kcandidates =
        //                new KCandidateCentroidsOperatorDescriptor(spec, secondaryRecDesc, sampleUUID, kCentroidsUUID,
        //                        permitUUID, new ColumnAccessEvalFactory(0), centroidRecDesc, K, 0);
        //        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, Kcandidates,
        //                primaryPartitionConstraint);
        //        targetOp = Kcandidates;
        //        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        //
        // ADD ITERATIVE STEPS TO CREATE THE UPPER LEVEL OF THE BTREE CLUSTER BASED ON THE OUTPUT
        // TODO CALVIN DANI : CONTINUE FROM THE CENTROIDS OR REDO THE KMEANS ++ (CHECK DATASET SIZE)
        //        int roundedK = (int) Math.round((double) K / (2 * 1));
        //        for (int depth = 1; depth < 3 && roundedK > 1; depth++) {
        //
        //            roundedK = (int) Math.round((double) K / (2 * depth));
        //
        //            sourceOp = targetOp;
        //            CandidateCentroidsOperatorDescriptor2 candidates2 =
        //                    new CandidateCentroidsOperatorDescriptor2(spec, onlyCentroidRecDesc, sampleUUID, centroidsUUID,
        //                            permitUUID, new ColumnAccessEvalFactory(0), roundedK);
        //            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, candidates2,
        //                    primaryPartitionConstraint);
        //            targetOp = candidates2;
        //            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        //
        //            sourceOp = targetOp;
        //            KCandidateCentroidsOperatorDescriptor2 Kcandidates2 =
        //                    new KCandidateCentroidsOperatorDescriptor2(spec, secondaryRecDesc, KCentroidsUUID, kCentroidsUUID,
        //                            permitUUID, new ColumnAccessEvalFactory(0), onlyCentroidRecDesc, roundedK, depth);
        //            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, Kcandidates2,
        //                    primaryPartitionConstraint);
        //            targetOp = Kcandidates2;
        //            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        //        }

        sourceOp = targetOp;
        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0, new IPushRuntimeFactory[] { sinkRuntimeFactory },
                new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, primaryPartitionConstraint);
        //        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, targetOp, metadataProvider.getClusterLocations().getLocations()[0]);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        spec.addRoot(targetOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    protected int getNumSecondaryKeys() {
        return ((Index.ValueIndexDetails) index.getIndexDetails()).getKeyFieldNames().size();
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
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
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
        for (int i = 0; i < numSecondaryKeys; i++) {
            ARecordType sourceType;
            ARecordType enforcedType;
            int sourceColumn;
            List<Integer> keySourceIndicators = indexDetails.getKeyFieldSourceIndicators();
            if (keySourceIndicators == null || keySourceIndicators.get(i) == 0) {
                sourceType = itemType;
                sourceColumn = recordColumn;
                enforcedType = enforcedItemType;
            } else {
                sourceType = metaType;
                sourceColumn = recordColumn + 1;
                enforcedType = enforcedMetaType;
            }
            List<String> secFieldName = indexDetails.getKeyFieldNames().get(i);
            IAType secFieldType = indexDetails.getKeyFieldTypes().get(i);
            Pair<IAType, Boolean> keyTypePair =
                    Index.getNonNullableOpenFieldType(index, secFieldType, secFieldName, sourceType);
            IAType keyType = keyTypePair.first;
            IScalarEvaluatorFactory secFieldAccessor = createFieldAccessor(sourceType, sourceColumn, secFieldName);
            secondaryFieldAccessEvalFactories[i] =
                    createFieldCast(secFieldAccessor, isOverridingKeyFieldTypes, enforcedType, sourceType, keyType);
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            secondaryRecFields[i] = serdeProvider.getSerializerDeserializer(keyType);
            secondaryComparatorFactories[i] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(keyType);
            secondaryBloomFilterKeyFields[i] = i;
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
