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

import static org.apache.asterix.om.types.ATypeTag.ARRAY;
import static org.apache.asterix.om.types.BuiltinType.*;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;

import java.util.List;
import java.util.UUID;

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.OptimizationConfUtil;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.base.IDataFormat;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.std.QuantizationConstantsAggregateDescriptor;
import org.apache.asterix.runtime.operators.*;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionerFactory;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.sort.SortGroupByOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class SecondaryVectorOperationsHelper extends SecondaryTreeIndexOperationsHelper {

    private RecordDescriptor recordDesc;
    private static final float DEFAULT_CONFIDENCE_INTERVAL = 0.99f;
    private static final int DEFAULT_BITS = 4;

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
        //        System.out.println(
        //                "*** VECTOR INDEX DEBUG: SecondaryVectorOperationsHelper.buildStaticStructureJobSpec() CALLED ***");

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

        //        System.err.println("=== STATIC STRUCTURE JOB SETUP ===");
        //        System.err.println("Dataset: " + dataset.getDatasetName());
        //        System.err.println("Index: " + index.getIndexName());
        //        System.err.println("Creating static structure job for K-means + structure creation");

        // if format == column, then project only the indexed fields
        ITupleProjectorFactory projectorFactory =
                IndexUtil.createPrimaryIndexScanTupleProjectorFactory(dataset.getDatasetFormatInfo(),
                        indexDetails.getIndexExpectedType(), itemType, metaType, numPrimaryKeys);

        // ============ SAMPLING INSTEAD OF FULL TABLE SCAN ============
        // Extract sampling parameters from WITH clause
        AdmObjectNode withObjectNodeForSampling = indexDetails.getWithObjectNode();
        int sampleSize = (withObjectNodeForSampling != null)
                ? withObjectNodeForSampling.getOptionalInt("sample_size", 10000) : 10000;
        long sampleSeed = (withObjectNodeForSampling != null)
                ? (long) withObjectNodeForSampling.getOptionalDouble("sample_seed", System.currentTimeMillis())
                : System.currentTimeMillis();

        // Calculate per-partition sample size
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        int numPartitions = partitioningProperties.getNumberOfPartitions();
        int sampleCardinalityPerPartition = Math.max(1, sampleSize / numPartitions);

        // dummy key provider -> sample scan (replacing full primary index scan)
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp = DatasetUtil.createSampleScanOp(spec, metadataProvider, dataset,
                sampleCardinalityPerPartition, sampleSeed, projectorFactory);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        // ============ ORIGINAL FULL TABLE SCAN (COMMENTED OUT) ============
        // // dummy key provider -> primary index scan
        // IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        // IOperatorDescriptor targetOp =
        //         DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        // spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        //        System.err.println("Connected: DummyKeyProvider → PrimaryIndexScan");

        sourceOp = targetOp;
        // primary index -> cast assign op (produces the secondary index entry)
        targetOp = createAssignOp(spec, numSecondaryKeys, recordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        //        System.err.println("Connected: PrimaryIndexScan → CastAssign");
        //        System.err.println("CastAssign operator: " + targetOp);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        UUID sampleUUID = UUID.randomUUID();
        UUID tupleCountUUID = UUID.randomUUID();
        UUID permitUUID = UUID.randomUUID();
        UUID scalarValuesUUID = UUID.randomUUID();

        // Register permit state for structure creation coordination
        //        System.err.println("=== REGISTERING PERMIT STATE ===");
        //        System.err.println("Permit UUID: " + permitUUID);
        //        System.err.println("Permit state will be used by StaticStructureCreator operator");

        // Extract K value from WITH clause
        AdmObjectNode withObjectNode = indexDetails.getWithObjectNode();
        int K = 20; // default value
        if (withObjectNode != null) {
            K = withObjectNode.getOptionalInt("num_clusters", 20);
        }
        // Validate K value
        if (K <= 0) {
            //            System.err.println("WARNING: Invalid num_k value: " + K + ", using default: 20");
            K = 20;
        }
        //        System.err.println("Using K value: " + K + " for K-means clustering");

        // Extract distance metric from WITH clause
        // Extract vector dimension from WITH clause
        int vectorDimension = (withObjectNode != null) ? withObjectNode.getOptionalInt("dimension", 384) : 384;
        //        System.err.println("Vector dimension from CREATE INDEX: " + vectorDimension);

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

        // Create RecordDescriptor for scalar values (single double field)
        // Use the same serializer as ScalarValueRunFileWriter to ensure compatibility
        ISerializerDeserializer[] scalarFieldSerdes = new ISerializerDeserializer[1];
        // Use DoubleSerializerDeserializer.INSTANCE to match ScalarValueRunFileWriter
        scalarFieldSerdes[0] =
                org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer.INSTANCE;
        RecordDescriptor scalarRecDesc = new RecordDescriptor(scalarFieldSerdes);

        //        System.err.println("=== RECORD DESCRIPTOR COMPARISON ===");

        // ====== STATIC STRUCTURE JOB: K-MEANS → STATIC STRUCTURE CREATION → SINK ======

        // 1. K-means operator
        //        System.err.println("🔧 CREATING HIERARCHICAL K-MEANS OPERATOR");
        UUID materializedDataUUID = UUID.randomUUID();
        HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor candidates =
                new HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(spec, hierarchicalRecDesc, secondaryRecDesc,
                        sampleUUID, tupleCountUUID, materializedDataUUID, scalarValuesUUID,
                        new ColumnAccessEvalFactory(0), K, maxScalableKmeansIter, dataflowHelperFactory);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, candidates,
                primaryPartitionConstraint);
        targetOp = candidates;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        //        System.err.println("Connected: CastAssign → K-means");
        //        System.err.println("K-means operator: " + candidates);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        // 2. VCTree static structure creator
        //        System.err.println("🔧 CREATING VCTreeStaticStructureCreator");
        VCTreeStaticStructureCreatorOperatorDescriptor vcTreeCreator =
                new VCTreeStaticStructureCreatorOperatorDescriptor(spec, dataflowHelperFactory, 100, 0.7f,
                        hierarchicalRecDesc, permitUUID, materializedDataUUID, scalarValuesUUID, scalarRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, vcTreeCreator,
                primaryPartitionConstraint);
        targetOp = vcTreeCreator;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        //        System.err.println("Connected: K-means → StaticStructureCreator");
        //        System.err.println("StaticStructureCreator operator: " + vcTreeCreator);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        // 3. ExternalSortOperatorDescriptor - Sort scalar values (ascending)
        // Use same framesLimit as loading job
        int sortFrames = Math.max(sortNumFrames, 2);
        int[] scalarSortFields = { 0 }; // Sort by first (and only) field
        // Use DoubleBinaryComparatorFactory.INSTANCE to match DoubleSerializerDeserializer.INSTANCE
        // (raw doubles without type tags)
        IBinaryComparatorFactory[] scalarSortComparatorFactories =
                { org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory.INSTANCE };
        ExternalSortOperatorDescriptor scalarSortOp = new ExternalSortOperatorDescriptor(spec, sortFrames,
                scalarSortFields, scalarSortComparatorFactories, scalarRecDesc);
        scalarSortOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, scalarSortOp,
                primaryPartitionConstraint);
        // Connect: PassThroughActivity output → ExternalSortOperatorDescriptor
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, scalarSortOp, 0);

        // Update sourceOp to continue the chain
        sourceOp = scalarSortOp;

        // 4. QuantileCalculatorOperatorDescriptor - Compute quantiles from sorted values
        QuantileCalculatorOperatorDescriptor quantileOp =
                new QuantileCalculatorOperatorDescriptor(spec, scalarRecDesc, 0.99f, // confidenceInterval
                        7, // bits
                        dataflowHelperFactory);
        quantileOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, quantileOp,
                primaryPartitionConstraint);
        // Connect: ExternalSortOperatorDescriptor → QuantileCalculatorOperatorDescriptor
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, quantileOp, 0);

        // Update sourceOp to continue the chain
        sourceOp = quantileOp;

        // 5. Final sink
        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0, new IPushRuntimeFactory[] { sinkRuntimeFactory },
                new RecordDescriptor[] { scalarRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, primaryPartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        //        System.err.println("Connected: StaticStructureCreator → Sink");
        //        System.err.println("Sink operator: " + targetOp);
        //        System.err.println("=== STATIC STRUCTURE PIPELINE COMPLETE ===");

        // Add single branch as root
        spec.addRoot(targetOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        //        System.err.println("=== STATIC STRUCTURE JOB SPECIFICATION COMPLETE ===");
        //        System.err.println("Root operators added:");
        //        System.err.println("  Root: " + targetOp + " (Static Structure - K-means → StaticStructureCreator → Sink)");
        //        System.err.println("=== STATIC STRUCTURE JOB CREATED ===");
        //        System.err.println("=== PIPELINE: DataSource → CastAssign → K-means → StaticStructureCreator → Sink ===");
        //        System.err.println(
        //                "=== STRUCTURE CREATION: K-means creates centroids, StaticStructureCreator stores structure ===");
        //        System.err.println("==========================================");
        //        System.err.println("*** SecondaryVectorOperationsHelper.buildStaticStructureJobSpec() COMPLETED ***");
        //        System.err.println("==========================================");
        //        System.out.println(
        //                "*** VECTOR INDEX DEBUG: SecondaryVectorOperationsHelper.buildStaticStructureJobSpec() COMPLETED ***");
        return spec;
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        // Force output to both System.out and System.err to ensure visibility
        //        System.out.println("*** VECTOR INDEX DEBUG: SecondaryVectorOperationsHelper.buildLoadingJobSpec() CALLED ***");
        //        System.err.println("==========================================");
        //        System.err.println("*** SecondaryVectorOperationsHelper.buildLoadingJobSpec() CALLED ***");
        //        System.err.println("==========================================");
        //        System.err.println("Dataset: " + dataset.getDatasetName());
        //        System.err.println("Index: " + index.getIndexName());
        //        System.err.println("Index type: " + index.getIndexType());
        //        System.err.println("Index details: " + index.getIndexDetails());
        //        System.err.println("Creating Data Loading Job with Bulk Loader and Grouping (Job 3)");
        //        System.err.println("==========================================");

        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = getNumSecondaryKeys();
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);

        // Job spec: key provider -> primary idx scan -> cast assign -> bulk loader and grouping -> sink
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        //        System.err.println("=== DATA LOADING JOB SETUP ===");
        //        System.err.println("Dataset: " + dataset.getDatasetName());
        //        System.err.println("Index: " + index.getIndexName());
        //        System.err.println("Creating data loading job with bulk loader and grouping functionality");

        // if format == column, then project only the indexed fields
        ITupleProjectorFactory projectorFactory =
                IndexUtil.createPrimaryIndexScanTupleProjectorFactory(dataset.getDatasetFormatInfo(),
                        indexDetails.getIndexExpectedType(), itemType, metaType, numPrimaryKeys);

        // dummy key provider -> primary index scan
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IOperatorDescriptor targetOp =
                DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        //        System.err.println("Connected: DummyKeyProvider → PrimaryIndexScan");

        sourceOp = targetOp;
        // primary index -> cast assign op (produces the secondary index entry)
        targetOp = createAssignOp(spec, numSecondaryKeys, recordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        //        System.err.println("Connected: PrimaryIndexScan → CastAssign");
        //        System.err.println("CastAssign operator: " + targetOp);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        // ====== DATA LOADING JOB: CAST ASSIGN → BULK LOADER AND GROUPING → SINK ======
        //        System.out.println("*** VECTOR INDEX DEBUG: CREATING DATA LOADING PIPELINE ***");
        //        System.err.println("==========================================");
        //        System.err.println("*** CREATING DATA LOADING PIPELINE ***");
        //        System.err.println("==========================================");
        //        System.err.println("Pipeline: CastAssign → BulkLoaderAndGrouping → Sink");

        // Create permit UUIDs for coordination
        UUID permitUUID = UUID.randomUUID();
        UUID materializedDataUUID = UUID.randomUUID();

        // Create output record descriptor for VCTreeBulkLoaderAndGroupingOperatorDescriptor
        // Format: [centroidId, distance, ...original fields from secondaryRecDesc...]
        IDataFormat format = metadataProvider.getDataFormat();
        ISerializerDeserializerProvider serdeProvider = format.getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = format.getTypeTraitProvider();

        ISerializerDeserializer[] outputRecFields =
                new ISerializerDeserializer[2 + secondaryRecDesc.getFieldCount() - 1];
        ITypeTraits[] outputTypeTraits = new ITypeTraits[2 + secondaryRecDesc.getFieldCount() - 1];

        outputRecFields[0] = serdeProvider.getSerializerDeserializer(ADOUBLE);
        outputTypeTraits[0] = typeTraitProvider.getTypeTrait(ADOUBLE);

        // Add centroidId field (int)
        outputRecFields[1] = serdeProvider.getSerializerDeserializer(AINT32);
        outputTypeTraits[1] = typeTraitProvider.getTypeTrait(AINT32);

        // Copy all original fields from secondaryRecDesc
        for (int i = 1; i < secondaryRecDesc.getFieldCount(); i++) {
            outputRecFields[2 + i - 1] = secondaryRecDesc.getFields()[i];
            outputTypeTraits[2 + i - 1] = secondaryRecDesc.getTypeTraits()[i];
        }

        RecordDescriptor outputRecDesc = new RecordDescriptor(outputRecFields, outputTypeTraits);

        // Extract distance metric from WITH clause
        AdmObjectNode withObjectNode = indexDetails.getWithObjectNode();
        String distanceMetric =
                (withObjectNode != null) ? withObjectNode.getOptionalString("similarity", "euclidean") : "euclidean";
        //        System.err.println("Distance metric from CREATE INDEX: " + distanceMetric);

        // Extract vector dimension from WITH clause
        int vectorDimension = (withObjectNode != null) ? withObjectNode.getOptionalInt("dimension", 384) : 384;
        //        System.err.println("Vector dimension from CREATE INDEX: " + vectorDimension);

        // Create VCTreeBulkLoaderAndGroupingOperatorDescriptor
        // Use ColumnAccessEvalFactory(0) to access the first field (vector field) from processed tuple
        IScalarEvaluatorFactory vectorFieldAccessor = new ColumnAccessEvalFactory(0);
        VCTreeBulkLoaderAndGroupingOperatorDescriptor bulkLoaderAndGroupingOp =
                new VCTreeBulkLoaderAndGroupingOperatorDescriptor(spec, dataflowHelperFactory, 128, 0.7f,
                        secondaryRecDesc, outputRecDesc, permitUUID, materializedDataUUID, vectorFieldAccessor,
                        distanceMetric, vectorDimension);
        bulkLoaderAndGroupingOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, bulkLoaderAndGroupingOp,
                primaryPartitionConstraint);

        // Connect CastAssign → BulkLoaderAndGrouping (which now outputs transformed tuples)
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, bulkLoaderAndGroupingOp, 0);
        //        System.err.println("Connected: CastAssign → BulkLoaderAndGrouping");
        //        System.err.println("BulkLoaderAndGrouping operator: " + bulkLoaderAndGroupingOp);

        // Update sourceOp to continue the chain
        sourceOp = bulkLoaderAndGroupingOp;

        // 4. ExternalSortOperatorDescriptor - Sort by [centroidId, distance]
        //        System.err.println("🔧 CREATING ExternalSortOperatorDescriptor");
        //        System.err.println("SortNumFrames from config: " + sortNumFrames);
        int[] sortFields = { 1, 0, 2 }; // Sort by centroidId (0) first, then distance (1)
        IBinaryComparatorFactory[] sortComparatorFactories =
                { BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(AINT32, true), // centroidId
                        BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(ADOUBLE, true), // distance
                        BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(AINT64, true) // distance
                };
        // Ensure minimum frames for sort operator (must be > 1)
        int sortFrames = Math.max(sortNumFrames, 2);
        //        System.err.println("Using sortFrames: " + sortFrames);
        //        System.err.println("OutputRecDesc field count: " + outputRecDesc.getFieldCount());
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec, sortFrames, sortFields,
                sortComparatorFactories, outputRecDesc);
        sortOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        targetOp = sortOp;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        //        System.err.println("Connected: BulkLoaderAndGrouping → Sort");
        //        System.err.println("Sort operator: " + sortOp);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        // 5. LSMIndexBulkLoadOperatorDescriptor - Load sorted tuples into VCTree index
        //        System.err.println("🔧 CREATING LSMIndexBulkLoadOperatorDescriptor for data loading");

        // Create field permutation: include all fields in same order (identity permutation)
        int[] fieldPermutation = createFieldPermutationForSortedDataBulkLoad(outputRecDesc);

        // Create primary key fields array for partitioner (numSecondaryKeys already defined above)
        int[] pkFields = createPkFieldsForBulkLoadOp(fieldPermutation, numSecondaryKeys);

        // Get partitioning properties
        PartitioningProperties partitioningProperties = metadataProvider.getPartitioningProperties(dataset);

        // Create primary index helper factory (for secondary indexes, this may not be needed but required by interface)
        IndexDataflowHelperFactory primaryIndexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), primaryFileSplitProvider);

        // Create partitioner factory using primary key hash functions
        IBinaryHashFunctionFactory[] pkHashFunFactories = dataset.getPrimaryHashFunctionFactories(metadataProvider);
        ITuplePartitionerFactory partitionerFactory = new FieldHashPartitionerFactory(pkFields, pkHashFunFactories,
                partitioningProperties.getNumberOfPartitions());

        // Create LSMIndexBulkLoadOperatorDescriptor for data loading
        LSMIndexBulkLoadOperatorDescriptor sortedBulkLoaderOp = new LSMIndexBulkLoadOperatorDescriptor(spec,
                outputRecDesc, // Use secondaryRecDesc, not outputRecDesc
                fieldPermutation, 0.7f, // fillFactor
                false, // verifyInput
                numElementsHint, // numElementsHint
                false, // checkIfEmptyIndex
                dataflowHelperFactory, primaryIndexHelperFactory, BulkLoadUsage.LOAD, dataset.getDatasetId(), null,
                // tupleFilterFactory
                partitionerFactory, partitioningProperties.getComputeStorageMap());
        sortedBulkLoaderOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortedBulkLoaderOp,
                primaryPartitionConstraint);
        targetOp = sortedBulkLoaderOp;
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        //        System.err.println("Connected: Sort → LSMIndexBulkLoad");
        //        System.err.println("LSMIndexBulkLoad operator: " + sortedBulkLoaderOp);

        // Update sourceOp to continue the chain
        sourceOp = targetOp;

        // 6. Final sink operator
        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor sinkOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { sinkRuntimeFactory }, new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sinkOp, primaryPartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, sinkOp, 0);
        //        System.err.println("Connected: LSMIndexBulkLoad → Sink");
        //        System.err.println("Sink operator: " + sinkOp);
        //        System.err.println("=== DATA LOADING PIPELINE COMPLETE ===");

        // Add sink as root
        spec.addRoot(sinkOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        //        System.err.println("=== DATA LOADING JOB SPECIFICATION COMPLETE ===");
        //        System.err.println("Root operators added:");
        //        System.err.println("  Root: " + sinkOp
        //                + " (Data Loading - CastAssign → BulkLoaderAndGrouping → Sort → LSMIndexBulkLoad → Sink)");
        //        System.err.println("=== DATA LOADING JOB CREATED ===");
        //        System.err.println(
        //                "=== PIPELINE: DataSource → CastAssign → BulkLoaderAndGrouping → Sort → LSMIndexBulkLoad → Sink ===");
        //        System.err.println(
        //                "=== DATA LOADING: Groups data, sorts by centroidId/distance, loads into VCTree using LSM bulk loading infrastructure ===");
        //        System.err.println("==========================================");
        //        System.err.println("*** SecondaryVectorOperationsHelper.buildLoadingJobSpec() COMPLETED ***");
        //        System.err.println("==========================================");
        //        System.out
        //                .println("*** VECTOR INDEX DEBUG: SecondaryVectorOperationsHelper.buildLoadingJobSpec() COMPLETED ***");
        return spec;
    }




    /**
     * Builds job spec to calculate quantization constants from ANALYZE sample index.
     *
     * @return Job specification for quantization constants calculation
     * @throws AlgebricksException if ANALYZE not run or other errors
     * Dataset dataset, Index vectorIndex, MetadataProvider metadataProvider,
     *                                                 SourceLocation sourceLoc
     */

    @Override
    public JobSpecification buildQuantizationMetadataJobSpec() throws AlgebricksException {

        // Check if ANALYZE sample index exists
        Index sampleIndex = metadataProvider.findSampleIndex(dataset.getDatabaseName(), dataset.getDataverseName(),
                dataset.getDatasetName());
        if (sampleIndex == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "ANALYZE DATASET must be run before creating vector index with quantization");
        }

        // Get vector index details
        Index.VectorIndexDetails indexDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        List<List<String>> vectorFieldPath = indexDetails.getKeyFieldNames();
        if (vectorFieldPath == null || vectorFieldPath.isEmpty()) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Vector index must specify vector field");
        }

        // Extract quantization parameters from WITH clause
        AdmObjectNode withObjectNode = indexDetails.getWithObjectNode();
        int bits = (withObjectNode != null) ? withObjectNode.getOptionalInt("bits", DEFAULT_BITS) : DEFAULT_BITS;
        float confidenceInterval = (withObjectNode != null)
                ? (float) withObjectNode.getOptionalDouble("confidence_interval", DEFAULT_CONFIDENCE_INTERVAL)
                : DEFAULT_CONFIDENCE_INTERVAL;

        // Get dataset types
        ARecordType itemType = (ARecordType) metadataProvider.findType(dataset);
        ARecordType metaType = DatasetUtil.getMetaType(metadataProvider, dataset);
        itemType = (ARecordType) metadataProvider.findTypeForDatasetWithoutType(itemType, dataset);

        // Get vector field type
        List<String> vectorFieldName = vectorFieldPath.get(0);
        IAType vectorFieldType = itemType.getSubFieldType(vectorFieldName);

        if (vectorFieldType == null) {
            vectorFieldType = DefaultOpenFieldType.getDefaultOpenFieldType(ARRAY);
        }

        ARecordType sourceType = itemType;
//        ARecordType enforcedType = enforcedItemType;

        Pair<IAType, Boolean> keyTypePair =
                Index.getNonNullableOpenFieldType(index, vectorFieldType, vectorFieldName, sourceType);

        if (vectorFieldType == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Vector field not found: " + vectorFieldName);
        }

        // Determine vector dimensions from WITH clause or type
        int vectorDimensions = -1;
        if (withObjectNode != null) {
            vectorDimensions = (withObjectNode != null) ? withObjectNode.getOptionalInt("dimension", 384) : 384;
        }
        if (vectorDimensions <= 0) {
            // Try to get from type (check if it's a fixed-length ordered list)
            if (vectorFieldType.getTypeTag() == ARRAY) {
                org.apache.asterix.om.types.AOrderedListType listType =
                        (org.apache.asterix.om.types.AOrderedListType) vectorFieldType;
                // AOrderedListType doesn't store dimension directly, so we need to get it from WITH clause
                // For now, require dimensions in WITH clause
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Vector dimensions must be specified in WITH clause");
            } else {
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Vector field must be an array type");
            }
        }

        // Create job specification
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // Get partitioning properties for sample index
        PartitioningProperties samplePartitioningProperties =
                metadataProvider.getPartitioningProperties(dataset, sampleIndex.getIndexName());
        IFileSplitProvider sampleFileSplitProvider = samplePartitioningProperties.getSplitsProvider();
        AlgebricksPartitionConstraint samplePartitionConstraint = samplePartitioningProperties.getConstraints();

        // Create index dataflow helper factory for sample index scan
        IStorageManager storageMgr = metadataProvider.getStorageComponentProvider().getStorageManager();
        IIndexDataflowHelperFactory sampleIndexHelperFactory =
                new IndexDataflowHelperFactory(storageMgr, sampleFileSplitProvider);

        // Get record descriptor for sample index
        RecordDescriptor sampleRecordDesc = dataset.getPrimaryRecordDescriptor(metadataProvider);

        // Create tuple projector to extract vector field
        ITupleProjectorFactory projectorFactory = IndexUtil.createPrimaryIndexScanTupleProjectorFactory(
                dataset.getDatasetFormatInfo(), ALL_FIELDS_TYPE, itemType, metaType, dataset.getPrimaryKeys().size());

        // Step 1: Dummy key provider -> Sample index scan
        // Use DatasetUtil.createSampleScanOp() to scan the ANALYZE sample index
        // This is the same method used by SampleOperationsHelper and SecondaryVectorOperationsHelper
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);

        // For quantization constants, we want all samples (no limit), so use a large sample size
        // The sample index already contains the ANALYZE samples, so we just need to scan all of them
        int sampleCardinalityPerPartition = Integer.MAX_VALUE; // Scan all samples
        long sampleSeed = 0; // Not used when scanning existing sample index

        IOperatorDescriptor targetOp = DatasetUtil.createSampleScanOp(spec, metadataProvider, dataset,
                sampleCardinalityPerPartition, sampleSeed, projectorFactory);

        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // Step 2: Extract and flatten vector components
        // Create evaluator factory to access vector field
        // For sample index scan with ALL_FIELDS_TYPE projection, the record descriptor has:
        // [primary key fields..., payload field (which contains all other fields)]
        // The vector field is inside the payload, so we need to access it via field path
        // Since VectorComponentExtractorOperatorDescriptor expects an IScalarEvaluatorFactory,
        // and we're scanning the full record, the vector field should be accessible via its path
        // For now, assume the vector field is in the payload at position numPrimaryKeys
        // (This is a simplification - in reality, we'd need a nested field accessor)
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        // The payload field is typically at numPrimaryKeys, and contains the full record
        // For a proper implementation, we'd need a FieldAccessEvalFactory that can handle nested paths
        // For now, use ColumnAccessEvalFactory assuming the vector field is directly accessible
        // TODO: Implement proper nested field access for vector field path
        int vectorFieldIndex = numPrimaryKeys; // This assumes vector field is directly in payload
        IScalarEvaluatorFactory vectorFieldEvalFactory = new ColumnAccessEvalFactory(vectorFieldIndex);

        // Record descriptor for flattened components (single double value per tuple)
        IDataFormat format = metadataProvider.getDataFormat();
        ISerializerDeserializerProvider serdeProvider = format.getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = format.getTypeTraitProvider();
        ISerializerDeserializer<?>[] flattenedSerDes =
                new ISerializerDeserializer[] { serdeProvider.getSerializerDeserializer(BuiltinType.ADOUBLE) };
        ITypeTraits[] flattenedTypeTraits = new ITypeTraits[] { typeTraitProvider.getTypeTrait(BuiltinType.ADOUBLE) };
        RecordDescriptor flattenedRecordDesc = new RecordDescriptor(flattenedSerDes, flattenedTypeTraits);

        targetOp = new VectorComponentExtractorOperatorDescriptor(spec, vectorFieldEvalFactory, sampleRecordDesc,
                flattenedRecordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // Step 3: Local aggregate - collect all component values per partition
        // Record descriptor for aggregate output (binary containing serialized values)
        ISerializerDeserializer<?>[] aggOutputSerDes =
                new ISerializerDeserializer[] { serdeProvider.getSerializerDeserializer(BuiltinType.ABINARY) };
        ITypeTraits[] aggOutputTypeTraits = new ITypeTraits[] { typeTraitProvider.getTypeTrait(BuiltinType.ABINARY) };
        RecordDescriptor aggOutputRecordDesc = new RecordDescriptor(aggOutputSerDes, aggOutputTypeTraits);

        // Create aggregate function (same descriptor for local and global)
        IScalarEvaluatorFactory[] aggArgs = new IScalarEvaluatorFactory[1];
        aggArgs[0] = new ColumnAccessEvalFactory(0); // Access the flattened component value

        QuantizationConstantsAggregateDescriptor aggDescriptor =
                (QuantizationConstantsAggregateDescriptor) QuantizationConstantsAggregateDescriptor.FACTORY
                        .createFunctionDescriptor();
        aggDescriptor.setImmutableStates(confidenceInterval, bits);

        IAggregateEvaluatorFactory localAggFactory = aggDescriptor.createAggregateEvaluatorFactory(aggArgs);

        // No grouping fields - collect all values into single aggregate
        int[] groupFields = new int[0];
        int[] sortFields = new int[0]; // No sorting needed when no grouping
        IBinaryComparatorFactoryProvider bcfp = format.getBinaryComparatorFactoryProvider();
        IBinaryComparatorFactory[] groupComparators = new IBinaryComparatorFactory[0];
        INormalizedKeyComputerFactoryProvider normKeyProvider = format.getNormalizedKeyComputerFactoryProvider();
        INormalizedKeyComputerFactory[] normKeyFactories = null; // No normalized keys when no grouping

        // Get frames limit for group by operator
        int groupbyNumFrames = OptimizationConfUtil.getGroupByNumFrames(
                metadataProvider.getApplicationContext().getCompilerProperties(), metadataProvider.getConfig(),
                sourceLoc);

        // Local aggregate operator (collects values per partition)
        AbstractAggregatorDescriptorFactory localAggFactoryDesc = new SimpleAlgebricksAccumulatingAggregatorFactory(
                new IAggregateEvaluatorFactory[] { localAggFactory }, groupFields);

        targetOp = new SortGroupByOperatorDescriptor(spec, groupbyNumFrames, sortFields, groupFields, normKeyFactories,
                groupComparators, localAggFactoryDesc, localAggFactoryDesc, flattenedRecordDesc, aggOutputRecordDesc,
                false);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // Step 4: Exchange -> Global aggregate
        // Global aggregate - compute quantiles and alpha (same descriptor, receives serialized values)
        // The aggregate operator will handle collecting from all partitions
        IAggregateEvaluatorFactory globalAggFactory = aggDescriptor.createAggregateEvaluatorFactory(aggArgs);

        AbstractAggregatorDescriptorFactory globalAggFactoryDesc = new SimpleAlgebricksAccumulatingAggregatorFactory(
                new IAggregateEvaluatorFactory[] { globalAggFactory }, groupFields);

        targetOp = new SortGroupByOperatorDescriptor(spec, groupbyNumFrames, sortFields, groupFields, normKeyFactories,
                groupComparators, globalAggFactoryDesc, globalAggFactoryDesc, aggOutputRecordDesc, aggOutputRecordDesc,
                false);
        // Connect with OneToOne - the aggregate will collect from all partitions
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;

        // Step 5: Sink operator - extract quantization constants and store in task context
        String quantizationKey = java.util.UUID.randomUUID().toString();
        targetOp = new QuantizationConstantsSinkOperatorDescriptor(spec, quantizationKey, aggOutputRecordDesc);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

        spec.addRoot(targetOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

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

            List<String> vectorFieldName = keyFieldNames.get(0);

            // Try to get the actual field type from the schema first (for closed fields)
            IAType vectorFieldType = null;
            try {
                vectorFieldType = sourceType.getSubFieldType(vectorFieldName);
            } catch (AlgebricksException e) {
                // Field not found in schema, will use default for open fields
                vectorFieldType = null;
            }

            // If field type is not found in schema (open field), provide default type
            if (vectorFieldType == null) {
                vectorFieldType = DefaultOpenFieldType.getDefaultOpenFieldType(ARRAY);
            }

            Pair<IAType, Boolean> keyTypePair =
                    Index.getNonNullableOpenFieldType(index, vectorFieldType, vectorFieldName, sourceType);
            IAType keyType = keyTypePair.first;

            IScalarEvaluatorFactory vectorFieldAccessor =
                    createFieldAccessor(sourceType, sourceColumn, vectorFieldName);

            secondaryFieldAccessEvalFactories[0] =
                    createFieldCast(vectorFieldAccessor, isOverridingKeyFieldTypes, enforcedType, sourceType, keyType);
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            secondaryRecFields[0] = serdeProvider.getSerializerDeserializer(keyType);
            secondaryComparatorFactories[0] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
            secondaryTypeTraits[0] = typeTraitProvider.getTypeTrait(keyType);
            secondaryBloomFilterKeyFields[0] = 0;

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

    /**
     * Create field permutation for sorted data bulk load operation.
     * Creates identity permutation that includes all fields from outputRecDesc in the same order.
     * outputRecDesc format: [distance(0), centroidId(1), ...secondaryRecDesc fields(2+)...]
     * 
     * @param outputRecDesc the output record descriptor containing all fields
     * @return fieldPermutation array mapping input fields to output fields in same order (identity)
     */
    private int[] createFieldPermutationForSortedDataBulkLoad(RecordDescriptor outputRecDesc) {
        int numFields = outputRecDesc.getFieldCount();
        int[] fieldPermutation = new int[numFields];
        // Identity permutation: include all fields in same order
        for (int i = 0; i < numFields; i++) {
            fieldPermutation[i] = i;
        }
        return fieldPermutation;
    }

    /**
     * Create primary key fields array for bulk load operation partitioner.
     * Primary keys are located in outputRecDesc after distance(0), centroidId(1), and secondary keys.
     * outputRecDesc format: [distance(0), centroidId(1), ...secondaryKeys(2 to 2+numSecondaryKeys-1), ...primaryKeys(2+numSecondaryKeys to 2+numSecondaryKeys+numPrimaryKeys-1), ...filterFields...]
     * 
     * @param fieldPermutation the field permutation array
     * @param numSecondaryKeys number of secondary key fields
     * @return array of field indices where primary keys are located
     */
    private int[] createPkFieldsForBulkLoadOp(int[] fieldPermutation, int numSecondaryKeys) {
        int[] pkFields = new int[numPrimaryKeys];
        // Primary keys start at index 2 + numSecondaryKeys in outputRecDesc
        // (accounting for distance at 0 and centroidId at 1)
        // In identity permutation, they map to fieldPermutation[2 + numSecondaryKeys + i]
        for (int i = 0; i < numPrimaryKeys; i++) {
            pkFields[i] = fieldPermutation[1 + numSecondaryKeys + i];
        }
        return pkFields;
    }
}
