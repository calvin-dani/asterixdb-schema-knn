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
import org.apache.asterix.dataflow.data.common.AOrderedListVectorBinaryAccessorFactory;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.base.IDataFormat;
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
import org.apache.asterix.runtime.operators.HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMIndexBulkLoadOperatorDescriptor.BulkLoadUsage;
import org.apache.asterix.runtime.operators.QuantileCalculatorOperatorDescriptor;
import org.apache.asterix.runtime.operators.QuantizationConstantsSinkOperatorDescriptor;
import org.apache.asterix.runtime.operators.VCTreeBulkLoaderAndGroupingOperatorDescriptor;
import org.apache.asterix.runtime.operators.VCTreeStaticStructureCreatorOperatorDescriptor;
import org.apache.asterix.runtime.operators.VectorComponentExtractorOperatorDescriptor;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
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
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.FixedLengthTypeTrait;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionerFactory;
import org.apache.hyracks.dataflow.common.data.partition.OnePartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class SecondaryVectorOperationsHelper extends SecondaryTreeIndexOperationsHelper {

    private RecordDescriptor recordDesc;
    private static final float DEFAULT_CONFIDENCE_INTERVAL = 0.99f;
    private static final int DEFAULT_BITS = 4;
    private IVectorBinaryAccessorFactory vectorAccessorFactory;

    protected SecondaryVectorOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    @Override
    public void init() throws AlgebricksException {
        super.init();
        recordDesc = dataset.getPrimaryRecordDescriptor(metadataProvider);

        // Initialize vector accessor factory for extracting vectors from ADM ordered lists
        vectorAccessorFactory = new AOrderedListVectorBinaryAccessorFactory();
    }

    /**
     * Get the vector accessor factory for extracting vectors from tuples.
     * This factory is passed to the Hyracks layer to handle ADM-specific vector deserialization.
     */
    public IVectorBinaryAccessorFactory getVectorAccessorFactory() {
        return vectorAccessorFactory;
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
        // Output tuple format: [distance, centroidId, pk..., include_fields...]
        // secondaryRecDesc format: [embedding, include_fields..., pk...]
        // numSecondaryKeys = 1 (embedding) + numIncludeFields
        // So in secondaryRecDesc:
        //   - Field 0: embedding (skipped in output)
        //   - Fields 1 to numSecondaryKeys-1: include fields
        //   - Fields numSecondaryKeys to numSecondaryKeys+numPrimaryKeys-1: primary keys
        int numIncludeFieldsForOutput = numSecondaryKeys - 1; // exclude embedding

        ISerializerDeserializer[] outputRecFields =
                new ISerializerDeserializer[2 + numPrimaryKeys + numIncludeFieldsForOutput];
        ITypeTraits[] outputTypeTraits = new ITypeTraits[2 + numPrimaryKeys + numIncludeFieldsForOutput];

        // Use raw serializers (without ADM type tags) to match VCTreeBulkLoaderAndGroupingOperatorDescriptor
        // Distance field (raw double - 8 bytes, no type tag)
        outputRecFields[0] = DoubleSerializerDeserializer.INSTANCE;
        outputTypeTraits[0] = new FixedLengthTypeTrait(8);

        // CentroidId field (raw int - 4 bytes, no type tag)
        outputRecFields[1] = IntegerSerializerDeserializer.INSTANCE;
        outputTypeTraits[1] = new FixedLengthTypeTrait(4);

        // Add primary key fields first (they are at positions numSecondaryKeys to numSecondaryKeys+numPrimaryKeys-1 in secondaryRecDesc)
        for (int i = 0; i < numPrimaryKeys; i++) {
            int secondaryRecIdx = numSecondaryKeys + i;
            outputRecFields[2 + i] = secondaryRecDesc.getFields()[secondaryRecIdx];
            outputTypeTraits[2 + i] = secondaryRecDesc.getTypeTraits()[secondaryRecIdx];
        }

        // Add include fields after PKs (they are at positions 1 to numSecondaryKeys-1 in secondaryRecDesc)
        for (int i = 0; i < numIncludeFieldsForOutput; i++) {
            int secondaryRecIdx = 1 + i; // Skip embedding at index 0
            outputRecFields[2 + numPrimaryKeys + i] = secondaryRecDesc.getFields()[secondaryRecIdx];
            outputTypeTraits[2 + numPrimaryKeys + i] = secondaryRecDesc.getTypeTraits()[secondaryRecIdx];
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

        // Calculate number of include fields for field reordering in createTransformedTuple
        int numIncludeFieldsForBulkLoader = (indexDetails.getIncludeFieldNames() != null)
                ? indexDetails.getIncludeFieldNames().size()
                : 0;

        VCTreeBulkLoaderAndGroupingOperatorDescriptor bulkLoaderAndGroupingOp =
                new VCTreeBulkLoaderAndGroupingOperatorDescriptor(spec, dataflowHelperFactory, 128, 0.7f,
                        secondaryRecDesc, outputRecDesc, permitUUID, materializedDataUUID, vectorFieldAccessor,
                        distanceMetric, vectorDimension, numPrimaryKeys, numIncludeFieldsForBulkLoader);
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
        // Sort by centroidId (field 1) first, then distance (field 0)
        // Use raw type comparators to match the raw serializers (no ADM type tags)
        int[] sortFields = { 1, 0 };
        IBinaryComparatorFactory[] sortComparatorFactories = { IntegerBinaryComparatorFactory.INSTANCE, // centroidId (raw int)
                DoubleBinaryComparatorFactory.INSTANCE // distance (raw double)
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
        System.err.println("==========================================");
        System.err.println("*** QUANTIZATION METADATA JOB: START ***");
        System.err.println("==========================================");
        System.err.println("Dataset: " + dataset.getDatasetName());
        System.err.println("Index: " + index.getIndexName());

        // Check if ANALYZE sample index exists
        Index sampleIndex = metadataProvider.findSampleIndex(dataset.getDatabaseName(), dataset.getDataverseName(),
                dataset.getDatasetName());
        if (sampleIndex == null) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "ANALYZE DATASET must be run before creating vector index with quantization");
        }
        System.err.println("✓ Sample index found: " + sampleIndex.getIndexName());

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
        System.err.println("Quantization parameters: bits=" + bits + ", confidenceInterval=" + confidenceInterval);

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
        System.err.println("--- STEP 1: Creating sample index scan ---");
        // Use DatasetUtil.createSampleScanOp() to scan the ANALYZE sample index
        // This is the same method used by SampleOperationsHelper and SecondaryVectorOperationsHelper
        IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);

        // For quantization constants, we want all samples (no limit), so use a large sample size
        // The sample index already contains the ANALYZE samples, so we just need to scan all of them
        int sampleCardinalityPerPartition = Integer.MAX_VALUE; // Scan all samples
        long sampleSeed = 0; // Not used when scanning existing sample index
        System.err.println(
                "Sample scan: cardinalityPerPartition=" + sampleCardinalityPerPartition + ", seed=" + sampleSeed);

        IOperatorDescriptor targetOp = DatasetUtil.createSampleScanOp(spec, metadataProvider, dataset,
                sampleCardinalityPerPartition, sampleSeed, projectorFactory);

        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;
        System.err.println("✓ Connected: DummyKeyProvider → SampleIndexScan");

        // Step 2: Extract and flatten vector components
        System.err.println("--- STEP 2: Creating vector component extractor ---");
        // Create evaluator factory to access vector field using proper nested field accessor
        // For sample index scan with ALL_FIELDS_TYPE projection, the record descriptor has:
        // [primary key fields..., payload field (which contains all other fields)]
        // The vector field is inside the payload, so we need to use createFieldAccessor
        // which creates a proper evaluator that navigates into the record to extract the field
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        int recordColumn = dataset.getDatasetType() == DatasetType.INTERNAL ? numPrimaryKeys : 0;
        IScalarEvaluatorFactory vectorFieldEvalFactory = createFieldAccessor(itemType, recordColumn, vectorFieldName);
        System.err.println(
                "Vector field accessor: recordColumn=" + recordColumn + ", vectorFieldName=" + vectorFieldName);

        // Record descriptor for flattened components (single double value per tuple)
        IDataFormat format = metadataProvider.getDataFormat();
        ISerializerDeserializerProvider serdeProvider = format.getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = format.getTypeTraitProvider();
        ISerializerDeserializer<?>[] flattenedSerDes =
                new ISerializerDeserializer[] { serdeProvider.getSerializerDeserializer(BuiltinType.ADOUBLE) };
        ITypeTraits[] flattenedTypeTraits = new ITypeTraits[] { typeTraitProvider.getTypeTrait(BuiltinType.ADOUBLE) };
        RecordDescriptor flattenedRecordDesc = new RecordDescriptor(flattenedSerDes, flattenedTypeTraits);
        System.err.println("Flattened record descriptor: single DOUBLE field");

        targetOp = new VectorComponentExtractorOperatorDescriptor(spec, vectorFieldEvalFactory, sampleRecordDesc,
                flattenedRecordDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, samplePartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;
        System.err.println("✓ Connected: SampleIndexScan → VectorComponentExtractor");

        // Step 3: Local aggregate - collect all component values per partition
        System.err.println("--- STEP 3: Creating local aggregate ---");
        // Record descriptor for aggregate output (binary containing serialized values)
        ISerializerDeserializer<?>[] aggOutputSerDes =
                new ISerializerDeserializer[] { serdeProvider.getSerializerDeserializer(BuiltinType.ABINARY) };
        ITypeTraits[] aggOutputTypeTraits = new ITypeTraits[] { typeTraitProvider.getTypeTrait(BuiltinType.ABINARY) };
        RecordDescriptor aggOutputRecordDesc = new RecordDescriptor(aggOutputSerDes, aggOutputTypeTraits);
        System.err.println("Aggregate output: BINARY (serialized double values)");

        // Create aggregate function (same descriptor for local and global)
        IScalarEvaluatorFactory[] aggArgs = new IScalarEvaluatorFactory[1];
        aggArgs[0] = new ColumnAccessEvalFactory(0); // Access the flattened component value
        System.err.println("Aggregate argument: ColumnAccessEvalFactory(0) - flattened component value");

        QuantizationConstantsAggregateDescriptor aggDescriptor =
                (QuantizationConstantsAggregateDescriptor) QuantizationConstantsAggregateDescriptor.FACTORY
                        .createFunctionDescriptor();
        aggDescriptor.setImmutableStates(confidenceInterval, bits);
        System.err.println("Aggregate descriptor created with states: confidenceInterval=" + confidenceInterval
                + ", bits=" + bits);

        IAggregateEvaluatorFactory localAggFactory = aggDescriptor.createAggregateEvaluatorFactory(aggArgs);
        System.err.println("Local aggregate factory created");

        // No grouping fields - collect all values into single aggregate (groupAll)
        int[] groupFields = new int[0];

        // Get frames limit for group by operator
        int framesLimit = OptimizationConfUtil.getGroupByNumFrames(
                metadataProvider.getApplicationContext().getCompilerProperties(), metadataProvider.getConfig(),
                sourceLoc);

        // Local aggregate operator (collects values per partition)
        AbstractAggregatorDescriptorFactory localAggFactoryDesc = new SimpleAlgebricksAccumulatingAggregatorFactory(
                new IAggregateEvaluatorFactory[] { localAggFactory }, groupFields);

        // Use PreclusteredGroupOperatorDescriptor for in-memory aggregation (no external sort, no merge phase)
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[0]; // Empty for groupAll
        targetOp = new PreclusteredGroupOperatorDescriptor(spec, groupFields, comparatorFactories, localAggFactoryDesc,
                aggOutputRecordDesc, true, framesLimit); // groupAll=true
        // Local aggregate runs on all partitions (same as sample partition constraint)
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, samplePartitionConstraint);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
        sourceOp = targetOp;
        System.err.println("✓ Connected: VectorComponentExtractor → LocalAggregate (PreclusteredGroup)");
        System.err.println(
                "Local aggregate: collecting all component values per partition (in-memory, no external sort)");

        // Step 4: Exchange -> Global aggregate
        System.err.println("--- STEP 4: Creating global aggregate ---");
        // Global aggregate - compute quantiles and alpha (same descriptor, receives serialized values)
        // The global aggregate runs on a SINGLE partition to collect results from all local aggregates
        IAggregateEvaluatorFactory globalAggFactory = aggDescriptor.createAggregateEvaluatorFactory(aggArgs);
        System.err.println("Global aggregate factory created");

        AbstractAggregatorDescriptorFactory globalAggFactoryDesc = new SimpleAlgebricksAccumulatingAggregatorFactory(
                new IAggregateEvaluatorFactory[] { globalAggFactory }, groupFields);

        // Use PreclusteredGroupOperatorDescriptor for in-memory aggregation (no external sort, no merge phase)
        targetOp = new PreclusteredGroupOperatorDescriptor(spec, groupFields, comparatorFactories, globalAggFactoryDesc,
                aggOutputRecordDesc, true, framesLimit); // groupAll=true

        // Get single partition constraint - use first location from cluster
        AlgebricksAbsolutePartitionConstraint clusterLocations =
                metadataProvider.getApplicationContext().getClusterStateManager().getNodeSortedClusterLocations();
        AlgebricksAbsolutePartitionConstraint singlePartitionConstraint =
                new AlgebricksAbsolutePartitionConstraint(new String[] { clusterLocations.getLocations()[0] });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, singlePartitionConstraint);

        // Use MToNPartitioningConnectorDescriptor with OnePartitionComputerFactory to route all tuples to partition 0
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, new OnePartitionComputerFactory()), sourceOp, 0,
                targetOp, 0);
        sourceOp = targetOp;
        System.err.println("✓ Connected: LocalAggregate → GlobalAggregate (via MToN with OnePartitionComputer)");
        System.err.println(
                "Global aggregate: computing quantiles and alpha from all partitions on single node (in-memory, no external sort)");

        // Step 5: Broadcast to all partitions -> Sink operator - write quantization constants to sidecar files
        // The sidecar file must be written to ALL partitions because Job 1's IndexBuilder runs on all partitions
        // and each partition's LSMVCTreeLocalResource reads from its own partition directory
        System.err.println("--- STEP 5: Creating broadcast and sink operators ---");
        String quantizationKey = java.util.UUID.randomUUID().toString();

        // Get dataset partitioning properties for writing sidecar file
        // The sink will write to the dataset directory (not the index directory, which doesn't exist yet)
        PartitioningProperties datasetPartitioningProperties = metadataProvider.getPartitioningProperties(dataset);
        org.apache.hyracks.api.io.FileSplit[] datasetFileSplits =
                datasetPartitioningProperties.getSplitsProvider().getFileSplits();
        AlgebricksPartitionConstraint datasetPartitionConstraint = datasetPartitioningProperties.getConstraints();
        System.err.println("Dataset file splits for sidecar file: " + datasetFileSplits.length);

        targetOp = new QuantizationConstantsSinkOperatorDescriptor(spec, quantizationKey, aggOutputRecordDesc,
                datasetFileSplits, index.getIndexName());
        // Sink runs on ALL partitions to write sidecar file to each partition's dataset directory
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, targetOp, datasetPartitionConstraint);
        // Use broadcast connector to replicate the quantization constants from single partition to all partitions
        spec.connect(new org.apache.hyracks.dataflow.std.connectors.MToNBroadcastConnectorDescriptor(spec), sourceOp, 0,
                targetOp, 0);
        System.err.println("✓ Connected: GlobalAggregate → (broadcast) → QuantizationConstantsSink");
        System.err.println("Sink: writing quantization constants to sidecar files on all " + datasetFileSplits.length
                + " partitions");

        spec.addRoot(targetOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        System.err.println("==========================================");
        System.err.println("*** QUANTIZATION METADATA JOB: COMPLETE ***");
        System.err.println("==========================================");
        System.err.println("Pipeline: SampleScan → VectorExtract → LocalAgg → GlobalAgg → Sink");
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
     * Primary keys are located in outputRecDesc at positions 2 to 2+numPrimaryKeys-1.
     * outputRecDesc format: [distance(0), centroidId(1), pk...(2 to 2+numPrimaryKeys-1), include_fields...]
     *
     * @param fieldPermutation the field permutation array
     * @param numSecondaryKeys number of secondary key fields (not used after reordering, kept for API compatibility)
     * @return array of field indices where primary keys are located
     */
    private int[] createPkFieldsForBulkLoadOp(int[] fieldPermutation, int numSecondaryKeys) {
        int[] pkFields = new int[numPrimaryKeys];
        // Primary keys start at index 2 in outputRecDesc (after distance and centroidId)
        for (int i = 0; i < numPrimaryKeys; i++) {
            pkFields[i] = fieldPermutation[2 + i];
        }
        return pkFields;
    }
}
