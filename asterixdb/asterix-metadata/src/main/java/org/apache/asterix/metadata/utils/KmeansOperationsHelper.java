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

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionManager;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.cluster.KmeansClusterEvalFactory;
import org.apache.asterix.runtime.evaluators.comparisons.GreaterThanDescriptor;
import org.apache.asterix.runtime.operators.DatasetStreamStatsOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.runningaggregates.std.SampleSlotRunningAggregateFunctionFactory;
import org.apache.asterix.runtime.runningaggregates.std.TidRunningAggregateDescriptor;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.AggregateRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggrun.RunningAggregateRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionerFactory;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;

/**
 * Utility class for sampling operations.
 * <p>
 * The sampling method described in:
 * "A Convenient Algorithm for Drawing a Simple Random Sample",
 * by A. I. McLeod and D. R. Bellhouse
 */
public class KmeansOperationsHelper implements ISecondaryIndexOperationsHelper {

    public static final String DATASET_STATS_OPERATOR_NAME = "Sample.DatasetStats";

    private final MetadataProvider metadataProvider;
    private final Dataset dataset;
    private final Index sampleIdx;
    private final SourceLocation sourceLoc;

    private ARecordType itemType;
    private ARecordType metaType;
    private RecordDescriptor recordDesc;
    private int secondayKeys;
    private IBinaryComparatorFactory[] comparatorFactories;
    private IFileSplitProvider fileSplitProvider;
    private AlgebricksPartitionConstraint partitionConstraint;
    private ILSMMergePolicyFactory mergePolicyFactory;
    private Map<String, String> mergePolicyProperties;
    private int groupbyNumFrames;
    private int[][] computeStorageMap;
    private int numPartitions;
    protected ITypeTraits[] secondaryTypeTraits;
    protected List<String> filterFieldName;
    protected RecordDescriptor primaryRecDesc;
    private int numPrimaryKeys;
    private int numFilterFields;
    protected IScalarEvaluatorFactory[] secondaryFieldAccessEvalFactories;
    protected boolean anySecondaryKeyIsNullable = false;
    protected IBinaryComparatorFactory[] secondaryComparatorFactories;
    protected int[] secondaryBloomFilterKeyFields;
    protected final ARecordType enforcedItemType;
    protected final ARecordType enforcedMetaType;
    protected RecordDescriptor secondaryRecDesc;
    protected RecordDescriptor enforcedRecDesc;
    protected IBinaryComparatorFactory[] primaryComparatorFactories;


    protected KmeansOperationsHelper(Dataset dataset, Index sampleIdx, MetadataProvider metadataProvider,
                                     SourceLocation sourceLoc) throws AlgebricksException {
        this.dataset = dataset;
        this.sampleIdx = sampleIdx;
        this.metadataProvider = metadataProvider;
        Pair<ARecordType, ARecordType> enforcedTypes = getEnforcedType(sampleIdx, itemType, metaType);
        this.enforcedItemType = enforcedTypes.first;
        this.enforcedMetaType = enforcedTypes.second;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void init() throws AlgebricksException {
        itemType = (ARecordType) metadataProvider.findType(dataset.getItemTypeDatabaseName(),
                dataset.getItemTypeDataverseName(), dataset.getItemTypeName());
        metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        itemType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(itemType, metaType, dataset);

        numPrimaryKeys = dataset.getPrimaryKeys().size();
        if (dataset.getDatasetType() == DatasetConfig.DatasetType.INTERNAL) {
            filterFieldName = DatasetUtil.getFilterField(dataset);
            if (filterFieldName != null) {
                numFilterFields = 1;
            } else {
                numFilterFields = 0;
            }

        }

        recordDesc = dataset.getPrimaryRecordDescriptor(metadataProvider);
        comparatorFactories = dataset.getPrimaryComparatorFactories(metadataProvider, itemType, metaType);
        groupbyNumFrames = getGroupByNumFrames(metadataProvider, sourceLoc);

        secondayKeys = ((ArrayList) ((Index.SampleIndexDetails) sampleIdx.getIndexDetails()).getKeyFieldNames()).size();

        // make sure to always use the dataset + index to get the partitioning properties
        // this is because in some situations the nodegroup of the passed dataset is different from the index
        // this can happen during a rebalance for example where the dataset represents the new target dataset while
        // the index object information is fetched from the old source dataset
        PartitioningProperties samplePartitioningProperties =
                metadataProvider.getPartitioningProperties(dataset, sampleIdx.getIndexName());
        fileSplitProvider = samplePartitioningProperties.getSplitsProvider();
        partitionConstraint = samplePartitioningProperties.getConstraints();
        computeStorageMap = samplePartitioningProperties.getComputeStorageMap();
        numPartitions = samplePartitioningProperties.getNumberOfPartitions();
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        mergePolicyFactory = compactionInfo.first;
        mergePolicyProperties = compactionInfo.second;

//        setSecondaryRecDescAndComparators();
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IndexBuilderFactory[][] indexBuilderFactories =
                DatasetUtil.getIndexBuilderFactories(dataset, metadataProvider, sampleIdx, itemType, metaType,
                        fileSplitProvider, mergePolicyFactory, mergePolicyProperties, computeStorageMap);
        IndexCreateOperatorDescriptor indexCreateOp =
                new IndexCreateOperatorDescriptor(spec, indexBuilderFactories, computeStorageMap);
        indexCreateOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexCreateOp, partitionConstraint);
        spec.addRoot(indexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        Index.SampleIndexDetails indexDetails = (Index.SampleIndexDetails) sampleIdx.getIndexDetails();
        int sampleCardinalityTarget = indexDetails.getSampleCardinalityTarget();
        long sampleSeed = indexDetails.getSampleSeed();
        IDataFormat format = metadataProvider.getDataFormat();
        int nFields = recordDesc.getFieldCount();
        int[] columns = new int[nFields];
        for (int i = 0; i < nFields; i++) {
            columns[i] = i;
        }
        IStorageManager storageMgr = metadataProvider.getStorageComponentProvider().getStorageManager();
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IIndexDataflowHelperFactory dataflowHelperFactory =
                new IndexDataflowHelperFactory(storageMgr, fileSplitProvider);

        // job spec:
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // if format == column. Bring the entire record as we are sampling
        ITupleProjectorFactory projectorFactory = IndexUtil.createPrimaryIndexScanTupleProjectorFactory(
                dataset.getDatasetFormatInfo(), ALL_FIELDS_TYPE, itemType, metaType, dataset.getPrimaryKeys().size());

        // dummy key provider ----> primary index scan
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp =
                DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // primary index scan ----> stream stats op
        List<Index> dsIndexes = metadataProvider.getSecondaryIndexes(dataset);
        IndexDataflowHelperFactory[] indexes = new IndexDataflowHelperFactory[dsIndexes.size()];
        String[] names = new String[dsIndexes.size()];
        for (int i = 0; i < indexes.length; i++) {
            Index idx = dsIndexes.get(i);
            PartitioningProperties idxPartitioningProps =
                    metadataProvider.getPartitioningProperties(dataset, idx.getIndexName());
            indexes[i] = new IndexDataflowHelperFactory(storageMgr, idxPartitioningProps.getSplitsProvider());
            names[i] = idx.getIndexName();
        }
        targetOp = new DatasetStreamStatsOperatorDescriptor(spec, recordDesc, DATASET_STATS_OPERATOR_NAME, indexes,
                names, computeStorageMap);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // stream stats op ----> (running agg + select)
        // ragg produces a slot number and a tuple counter for each tuple
        // If the slot number is 0 then the tuple is not in the sample and is removed by subsequent select op.
        // If the slot number is greater than 0 then the tuple is in the sample.
        // There could be several tuples with the same slot number, the latest one wins
        // (with the greatest tuple counter). This is accomplished by the group by below
        BuiltinType raggSlotType = BuiltinType.AINT32;
        BuiltinType raggCounterType = BuiltinType.AINT64;
        int[] raggProjectColumns = new int[nFields + 2];
        raggProjectColumns[0] = nFields;
        raggProjectColumns[1] = nFields + 1;
        System.arraycopy(columns, 0, raggProjectColumns, 2, nFields);
        int[] raggAggColumns = { nFields, nFields + 1 };

        ISerializerDeserializerProvider serdeProvider = format.getSerdeProvider();
        ISerializerDeserializer[] raggSerdes = new ISerializerDeserializer[nFields + 2];
        raggSerdes[0] = serdeProvider.getSerializerDeserializer(raggSlotType);
        raggSerdes[1] = serdeProvider.getSerializerDeserializer(raggCounterType);
        System.arraycopy(recordDesc.getFields(), 0, raggSerdes, 2, nFields);
        // Create a manual descriptor
        ITypeTraitProvider typeTraitProvider = format.getTypeTraitProvider();
        ITypeTraits[] raggTraits = new ITypeTraits[nFields + 2];
        raggTraits[0] = typeTraitProvider.getTypeTrait(raggSlotType);
        raggTraits[1] = typeTraitProvider.getTypeTrait(raggCounterType);
        System.arraycopy(recordDesc.getTypeTraits(), 0, raggTraits, 2, nFields);

        RecordDescriptor raggRecordDesc = new RecordDescriptor(raggSerdes, raggTraits);


        IRunningAggregateEvaluatorFactory raggSlotEvalFactory =
                new SampleSlotRunningAggregateFunctionFactory(sampleCardinalityTarget, sampleSeed);
        IRunningAggregateEvaluatorFactory raggCounterEvalFactory = TidRunningAggregateDescriptor.FACTORY
                .createFunctionDescriptor().createRunningAggregateEvaluatorFactory(new IScalarEvaluatorFactory[0]);
        RunningAggregateRuntimeFactory raggRuntimeFactory =
                new RunningAggregateRuntimeFactory(raggProjectColumns, raggAggColumns,
                        new IRunningAggregateEvaluatorFactory[] { raggSlotEvalFactory, raggCounterEvalFactory });

        IFunctionDescriptor gtDescriptor = GreaterThanDescriptor.FACTORY.createFunctionDescriptor();
        gtDescriptor.setImmutableStates(raggSlotType, raggSlotType);
        IScalarEvaluatorFactory gtFactory =
                gtDescriptor.createEvaluatorFactory(new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0),
                        format.getConstantEvalFactory(new AsterixConstantValue(new AInt32(0))) });
        StreamSelectRuntimeFactory selectRuntimeFactory = new StreamSelectRuntimeFactory(gtFactory, null,
                format.getBinaryBooleanInspectorFactory(), false, -1, null);

        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { raggRuntimeFactory, selectRuntimeFactory },
                new RecordDescriptor[] { raggRecordDesc, raggRecordDesc });
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

//        int[] projectColumns = new int[nFields];
//                projectColumns[0] = 1; // [slot]
//        for (int i = 0; i < nFields; i++) {
//            projectColumns[i] = 2 + i;
//        }


        BuiltinType embeddingType = BuiltinType.ANY;
        ISerializerDeserializer[] rembeddingSerde = new ISerializerDeserializer[1];
        rembeddingSerde[0] = serdeProvider.getSerializerDeserializer(embeddingType);
//        System.arraycopy(recordDesc.getFields(), 0, rembeddingSerde, 4, 1);
        ITypeTraits[] rembeddingTraits = new ITypeTraits[1];
        rembeddingTraits[0] = typeTraitProvider.getTypeTrait(embeddingType);
//        System.arraycopy(recordDesc.getTypeTraits(), 0, rembeddingTraits, 4, 1);

        RecordDescriptor rembeddingRecordDesc = new RecordDescriptor(rembeddingSerde, rembeddingTraits);

        BuiltinType aggType = BuiltinType.AINT64;
        ISerializerDeserializer[] aggSerde = new ISerializerDeserializer[1];
        aggSerde[0] = serdeProvider.getSerializerDeserializer(aggType);
//        System.arraycopy(recordDesc.getFields(), 0, rembeddingSerde, 4, 1);
        ITypeTraits[] aggTraits = new ITypeTraits[1];
        aggTraits[0] = typeTraitProvider.getTypeTrait(aggType);
//        System.arraycopy(recordDesc.getTypeTraits(), 0, rembeddingTraits, 4, 1);

        RecordDescriptor aggRecordDesc = new RecordDescriptor(aggSerde, aggTraits);

        secondaryFieldAccessEvalFactories = new IScalarEvaluatorFactory[1];
        List<String> embedddingListName = Arrays.asList("embedding");
        // TODO CALVIN DANI : AFTER PIPELINE fix the recordcolumns
        IScalarEvaluatorFactory secFieldAccessor = createFieldAccessor(itemType, 3, embedddingListName);
        secondaryFieldAccessEvalFactories[0] =
                createFieldCast(secFieldAccessor, false, null, itemType, new AOrderedListType(BuiltinType.AINT64,"embedding"));
        // primary index ----> cast assign op (produces the secondary index entry)
        targetOp = createAssignOp(spec, secondayKeys, raggRecordDesc,rembeddingRecordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;


//        int[] projectColumns = new int[nFields];
//        projectColumns[0] = 0; // [slot]
//        StreamProjectRuntimeFactory projectRuntimeFactory = new StreamProjectRuntimeFactory(projectColumns);
//        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1, new IPushRuntimeFactory[] { projectRuntimeFactory },
//                new RecordDescriptor[] { recordDesc });
//        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
//        sourceOp = targetOp;
//
//        List<String> embedddingListName = Arrays.asList("embedding");
//        IScalarEvaluatorFactory[] secFieldAccessor = new IScalarEvaluatorFactory[1];
//        secFieldAccessor[0] = createFieldAccessor(itemType, 1, embedddingListName);
//
//        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[1];
//        System.arraycopy(secondaryFieldAccessEvalFactories, 0, sefs, 0, secondaryFieldAccessEvalFactories.length);
//        int[] outColumns = new int[] { 1 }; // [slot]
//        int[] projectionList = new int[] { 1 }; // [slot]
//        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
//        assign.setSourceLocation(sourceLoc);
//        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1, new IPushRuntimeFactory[] { assign },
//                new RecordDescriptor[] { recordDesc });
//        targetOp.setSourceLocation(sourceLoc);
//        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
//        sourceOp = targetOp;

        //        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
        //                primaryPartitionConstraint);

        //
        //        AssignRuntimeFactory assign =
        //                new AssignRuntimeFactory([1], sefs.toArray(new IScalarEvaluatorFactory[0]), projectionList);
        //        assign.setSourceLocation(sourceLoc);
        //        AlgebricksMetaOperatorDescriptor algebricksMetaOperatorDescriptor = new AlgebricksMetaOperatorDescriptor(spec,
        //                1, 1, new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { assignRecDesc });
        //

        //        test = metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(
        //                metadataProvider.getFunctionManager(), recordDesc.getFields()[1],
        //                [1], 1, sourceLoc);
//        IAggregateEvaluatorFactory kmeansClusterFactory = new KmeansClusterEvalFactory( new IScalarEvaluatorFactory[] { new FieldAccessByNameEvalFactory(new ColumnAccessEvalFactory(1), new ) }, false, sourceLoc);
        // To confirm
        secFieldAccessor = createFieldAccessor(itemType, 0, embedddingListName);
        secondaryFieldAccessEvalFactories[0] =
                createFieldCast(secFieldAccessor, false, null, itemType, new ARecordType("embedding",
                        new String[] { "embedding" }, new IAType[] { new AOrderedListType(BuiltinType.AINT64, "embedding") },
                        false));

        IAggregateEvaluatorFactory kmeansClusterFactory = new KmeansClusterEvalFactory( new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(0) }, false, sourceLoc);
                AggregateRuntimeFactory aggRuntimeFactory =
                        new AggregateRuntimeFactory(new IAggregateEvaluatorFactory[]{kmeansClusterFactory});
                targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                        new IPushRuntimeFactory[] { aggRuntimeFactory },
                        new RecordDescriptor[] { rembeddingRecordDesc, aggRecordDesc });
                spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
                sourceOp = targetOp;

        // (running agg + select) ---> group-by
        //        int[] groupFields = new int[] { 0 }; // [slot]
        //        int[] sortFields = new int[] { 0, 1 }; // [slot, counter]
        //        OrderOperator.IOrder sortSlotOrder = OrderOperator.ASC_ORDER;
        //        OrderOperator.IOrder sortCounterOrder = OrderOperator.DESC_ORDER;
        //        IBinaryComparatorFactoryProvider comparatorFactoryProvider = format.getBinaryComparatorFactoryProvider();
        //        IBinaryComparatorFactory[] raggCmpFactories = {
        //                comparatorFactoryProvider.getBinaryComparatorFactory(raggSlotType,
        //                        sortSlotOrder.getKind() == OrderOperator.IOrder.OrderKind.ASC),
        //                comparatorFactoryProvider.getBinaryComparatorFactory(raggCounterType,
        //                        sortCounterOrder.getKind() == OrderOperator.IOrder.OrderKind.ASC) };
        //
        //        INormalizedKeyComputerFactoryProvider normKeyProvider = format.getNormalizedKeyComputerFactoryProvider();
        //        INormalizedKeyComputerFactory[] normKeyFactories = {
        //                normKeyProvider.getNormalizedKeyComputerFactory(raggSlotType,
        //                        sortSlotOrder.getKind() == OrderOperator.IOrder.OrderKind.ASC),
        //                normKeyProvider.getNormalizedKeyComputerFactory(raggCounterType,
        //                        sortCounterOrder.getKind() == OrderOperator.IOrder.OrderKind.ASC) };
        //
        //        // agg = [counter, .. original columns ..]
        //        IAggregateEvaluatorFactory[] aggFactories = new IAggregateEvaluatorFactory[nFields + 1];
        //        for (int i = 0; i < aggFactories.length; i++) {
        //            aggFactories[i] = new FirstElementEvalFactory(
        //                    new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(1 + i) }, false, sourceLoc);
        //        }
        //        AbstractAggregatorDescriptorFactory aggregatorFactory =
        //                new SimpleAlgebricksAccumulatingAggregatorFactory(aggFactories, groupFields);
        //
        //        targetOp = new SortGroupByOperatorDescriptor(spec, groupbyNumFrames, sortFields, groupFields, normKeyFactories,
        //                raggCmpFactories, aggregatorFactory, aggregatorFactory, raggRecordDesc, raggRecordDesc, false);
        //        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        //        sourceOp = targetOp;

        // group by --> project (remove ragg fields)

        // project ---> bulk load op
        //        targetOp = createTreeIndexBulkLoadOp(spec, columns, dataflowHelperFactory,
        //                StorageConstants.DEFAULT_TREE_FILL_FACTOR, sampleCardinalityTarget);
        //        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        //        sourceOp = targetOp;

        //        // bulk load op ----> sink op
        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0, new IPushRuntimeFactory[] { sinkRuntimeFactory },
                new RecordDescriptor[] { aggRecordDesc });
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        spec.addRoot(targetOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        return spec;
    }

    protected LSMIndexBulkLoadOperatorDescriptor createTreeIndexBulkLoadOp(JobSpecification spec,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor,
            long numElementHint) throws AlgebricksException {
        int[] pkFields = new int[dataset.getPrimaryKeys().size()];
        System.arraycopy(fieldPermutation, 0, pkFields, 0, pkFields.length);
        IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(metadataProvider);
        ITuplePartitionerFactory partitionerFactory =
                new FieldHashPartitionerFactory(pkFields, pkHashFunFactories, numPartitions);
        LSMIndexBulkLoadOperatorDescriptor treeIndexBulkLoadOp = new LSMIndexBulkLoadOperatorDescriptor(spec,
                recordDesc, fieldPermutation, fillFactor, false, numElementHint, true, dataflowHelperFactory, null,
                LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage.LOAD, dataset.getDatasetId(), null, partitionerFactory,
                computeStorageMap);
        treeIndexBulkLoadOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                partitionConstraint);
        return treeIndexBulkLoadOp;
    }

    @Override
    public JobSpecification buildDropJobSpec(Set<IndexDropOperatorDescriptor.DropOption> options)
            throws AlgebricksException {
        return SecondaryTreeIndexOperationsHelper.buildDropJobSpecImpl(dataset, sampleIdx, options, metadataProvider,
                sourceLoc);
    }

    protected IScalarEvaluatorFactory createFieldAccessor(ARecordType recordType, int recordColumn,
            List<String> fieldName) throws AlgebricksException {
        IFunctionManager funManger = metadataProvider.getFunctionManager();
        IDataFormat dataFormat = metadataProvider.getDataFormat();
        return dataFormat.getFieldAccessEvaluatorFactory(funManger, recordType, fieldName, recordColumn, sourceLoc);
    }

    protected AlgebricksMetaOperatorDescriptor createAssignOp(JobSpecification spec, int numSecondaryKeyFields,
                                                              RecordDescriptor prevOpRecDesc, RecordDescriptor nextOpRecDesc) throws AlgebricksException {
        int numFilterFields = 0;
//        int[] outColumns = new int[numSecondaryKeyFields + numFilterFields];
//        int[] projectionList = new int[numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
//        for (int i = 0; i < numSecondaryKeyFields + numFilterFields; i++) {
//            outColumns[i] = numPrimaryKeys + i;
//        }
//        int projCount = 0;
//        for (int i = 0; i < numSecondaryKeyFields; i++) {
//            projectionList[projCount++] = numPrimaryKeys + i;
//        }
//        for (int i = 0; i < numPrimaryKeys; i++) {
//            projectionList[projCount++] = i;
//        }
//        if (numFilterFields > 0) {
//            projectionList[projCount] = numPrimaryKeys + numSecondaryKeyFields;
//        }
//
        int[] outColumns = new int[1];
        int[] projectionList = new int[1];
        outColumns[0] = 1; // [slot]
        projectionList[0] = 1; // [slot]
        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[secondaryFieldAccessEvalFactories.length];
        System.arraycopy(secondaryFieldAccessEvalFactories, 0, sefs, 0, secondaryFieldAccessEvalFactories.length);
//        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        assign.setSourceLocation(sourceLoc);
        // TDOO CALVIN Change the record descroptor.
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { prevOpRecDesc, nextOpRecDesc });
        asterixAssignOp.setSourceLocation(sourceLoc);
        // not needed.
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                getSecondaryPartitionConstraint());
        return asterixAssignOp;
    }

    private static Pair<ARecordType, ARecordType> getEnforcedType(Index index, ARecordType aRecordType,
                                                                  ARecordType metaRecordType) throws AlgebricksException {
        return index.getIndexDetails().isOverridingKeyFieldTypes()
                ? TypeUtil.createEnforcedType(aRecordType, metaRecordType, Collections.singletonList(index))
                : new Pair<>(null, null);
    }

    protected void setSecondaryRecDescAndComparators() throws AlgebricksException {
        Index.SampleIndexDetails indexDetails = (Index.SampleIndexDetails) sampleIdx.getIndexDetails();
        int numSecondaryKeys = secondayKeys;
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
        int recordColumn = dataset.getDatasetType() == DatasetConfig.DatasetType.INTERNAL ? numPrimaryKeys : 0;
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
                    Index.getNonNullableOpenFieldType(sampleIdx, secFieldType, secFieldName, sourceType);
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
        if (dataset.getDatasetType() == DatasetConfig.DatasetType.INTERNAL) {
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

    protected IScalarEvaluatorFactory createFieldCast(IScalarEvaluatorFactory fieldEvalFactory,
                                                      boolean isOverridingKeyFieldTypes, IAType enforcedRecordType, ARecordType recordType, IAType targetType)
            throws AlgebricksException {


        IFunctionManager funManger = metadataProvider.getFunctionManager();
        IDataFormat dataFormat = metadataProvider.getDataFormat();
        if (ATypeTag.ANY.equals(targetType.getTypeTag())) {
            // this is to ensure records and lists values are in the open format
            IScalarEvaluatorFactory[] castArg = new IScalarEvaluatorFactory[] { fieldEvalFactory };
            return createCastFunction(targetType, BuiltinType.ANY, true, sourceLoc).createEvaluatorFactory(castArg);
        }

        // check IndexUtil.castDefaultNull(index), too, because we always want to cast even if the overriding type is
        // the same as the overridden type (this is for the case where overriding the type of closed field is allowed)
        // e.g. field "a" is a string in the dataset ds; CREATE INDEX .. ON ds(a:string) CAST (DEFAULT NULL)
        boolean castIndexedField = isOverridingKeyFieldTypes
                && (!enforcedRecordType.equals(recordType) || IndexUtil.castDefaultNull(sampleIdx));
        if (!castIndexedField) {
            return fieldEvalFactory;
        }

        IScalarEvaluatorFactory castFieldEvalFactory;
        if (IndexUtil.castDefaultNull(sampleIdx)) {
            castFieldEvalFactory = createConstructorFunction(funManger, dataFormat, fieldEvalFactory, targetType);
        } else if (sampleIdx.isEnforced()) {
            IScalarEvaluatorFactory[] castArg = new IScalarEvaluatorFactory[] { fieldEvalFactory };
            castFieldEvalFactory =
                    createCastFunction(targetType, BuiltinType.ANY, true, sourceLoc).createEvaluatorFactory(castArg);
        } else {
            IScalarEvaluatorFactory[] castArg = new IScalarEvaluatorFactory[] { fieldEvalFactory };
            castFieldEvalFactory =
                    createCastFunction(targetType, BuiltinType.ANY, false, sourceLoc).createEvaluatorFactory(castArg);
        }
        return castFieldEvalFactory;
    }

    protected IFunctionDescriptor createCastFunction(IAType targetType, IAType inputType, boolean strictCast,
                                                     SourceLocation sourceLoc) throws AlgebricksException {
        IFunctionDescriptor castFuncDesc = metadataProvider.getFunctionManager()
                .lookupFunction(strictCast ? BuiltinFunctions.CAST_TYPE : BuiltinFunctions.CAST_TYPE_LAX, sourceLoc);
        castFuncDesc.setSourceLocation(sourceLoc);
        castFuncDesc.setImmutableStates(targetType, inputType);
        return castFuncDesc;
    }

    protected IScalarEvaluatorFactory createConstructorFunction(IFunctionManager funManager, IDataFormat dataFormat,
                                                                IScalarEvaluatorFactory fieldEvalFactory, IAType fieldType) throws AlgebricksException {
        IAType targetType = TypeComputeUtils.getActualType(fieldType);
        Pair<FunctionIdentifier, IAObject> constructorWithFmt =
                IndexUtil.getTypeConstructorDefaultNull(sampleIdx, targetType, sourceLoc);
        FunctionIdentifier typeConstructorFun = constructorWithFmt.first;
        IFunctionDescriptor typeConstructor = funManager.lookupFunction(typeConstructorFun, sourceLoc);
        IScalarEvaluatorFactory[] args;
        // add the format argument if specified
        if (constructorWithFmt.second != null) {
            IScalarEvaluatorFactory fmtEvalFactory =
                    dataFormat.getConstantEvalFactory(new AsterixConstantValue(constructorWithFmt.second));
            args = new IScalarEvaluatorFactory[] { fieldEvalFactory, fmtEvalFactory };
        } else {
            args = new IScalarEvaluatorFactory[] { fieldEvalFactory };
        }
        typeConstructor.setSourceLocation(sourceLoc);
        return typeConstructor.createEvaluatorFactory(args);
    }

    @Override
    public JobSpecification buildCompactJobSpec() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IFileSplitProvider getSecondaryFileSplitProvider() {
        return fileSplitProvider;
    }

    @Override
    public RecordDescriptor getSecondaryRecDesc() {
        return recordDesc;
    }

    @Override
    public IBinaryComparatorFactory[] getSecondaryComparatorFactories() {
        return comparatorFactories;
    }

    @Override
    public AlgebricksPartitionConstraint getSecondaryPartitionConstraint() {
        return partitionConstraint;
    }

    private static int getGroupByNumFrames(MetadataProvider metadataProvider, SourceLocation sourceLoc)
            throws AlgebricksException {
        return OptimizationConfUtil.getGroupByNumFrames(
                metadataProvider.getApplicationContext().getCompilerProperties(), metadataProvider.getConfig(),
                sourceLoc);
    }
}
