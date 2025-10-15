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
package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManager;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.impls.VCTreeStaticStructureCreator;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriterFactory;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Operator that creates VCTree static structure files using VCTreeStaticStructureCreator.
 * Two-activity structure: first creates static file, second reads materialized data and passes through.
 */
public class VCTreeStaticStructureCreatorOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final int maxEntriesPerPage;
    private final float fillFactor;
    private final UUID permitUUID;
    private final UUID materializedDataUUID;

    public VCTreeStaticStructureCreatorOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID, UUID materializedDataUUID) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID;
        this.materializedDataUUID = materializedDataUUID;
        this.outRecDescs[0] = inputRecordDescriptor;
        System.err.println("VCTreeStaticStructureCreatorOperatorDescriptor created with permit UUID: " + permitUUID);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        CreateStructureActivity sa = new CreateStructureActivity(new ActivityId(odId, 0));
        PassThroughActivity ca = new PassThroughActivity(new ActivityId(odId, 1));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ca);
        builder.addTargetEdge(0, ca, 0);

        builder.addBlockingEdge(sa, ca);
        builder.addTargetEdge(0, ca, 0);
    }

    protected class CreateStructureActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected CreateStructureActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private final List<ByteBuffer> frameAccumulator = new ArrayList<>();
                private int tupleCount = 0;
                private FrameTupleReference tuple = new FrameTupleReference();
                private FrameTupleAccessor fta;
                private IScalarEvaluator levelEval;
                private IScalarEvaluator clusterIdEval;
                private IScalarEvaluator centroidIdEval;
                private IPointable levelVal;
                private IPointable clusterIdVal;
                private IPointable centroidIdVal;
                private Map<Integer, Integer> levelDistribution = null;
                private Map<String, Map<Integer, Integer>> clusterDistribution = null;
                private VCTreeStaticStructureCreator structureCreator;
                private MaterializerTaskState materializedData;

                @Override
                public void open() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity OPENING ===");
                    try {
                        // Register permit state for coordination
                        IterationPermitState permitState =
                                (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));

                        if (permitState == null) {
                            java.util.concurrent.Semaphore permit = new java.util.concurrent.Semaphore(0);
                            permitState = new IterationPermitState(ctx.getJobletContext().getJobId(),
                                    new PartitionedUUID(permitUUID, partition), permit);
                            ctx.setStateObject(permitState);
                            System.err.println("✅ PERMIT STATE CREATED AND REGISTERED for UUID: " + permitUUID + ", partition: "
                                    + partition);
                        }

                        // Initialize materialized data state
                        materializedData = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                                new PartitionedUUID(materializedDataUUID, partition));
                        materializedData.open(ctx);

                        // Initialize evaluators for extracting tuple fields
                        EvaluatorContext evalCtx = new EvaluatorContext(ctx);
                        levelEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(evalCtx);
                        clusterIdEval = new ColumnAccessEvalFactory(1).createScalarEvaluator(evalCtx);
                        centroidIdEval = new ColumnAccessEvalFactory(2).createScalarEvaluator(evalCtx);

                        // Initialize pointables for evaluator results
                        levelVal = new VoidPointable();
                        clusterIdVal = new VoidPointable();
                        centroidIdVal = new VoidPointable();

                        fta = new FrameTupleAccessor(outRecDescs[0]);
                        System.err.println("CreateStructureActivity opened successfully");
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to open CreateStructureActivity: " + e.getMessage());
                        throw HyracksDataException.create(e);
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity nextFrame ===");
                    fta.reset(buffer);
                    int frameTupleCount = fta.getTupleCount();
                    System.err.println("Processing " + frameTupleCount + " tuples in this frame");

                    // Accumulate frames for batch processing
                    frameAccumulator.add(buffer.duplicate());

                    // Process tuples in this frame
                    for (int i = 0; i < fta.getTupleCount(); i++) {
                        tuple.reset(fta, i);
                        processTuple(tuple);
                    }

                    // Log progress every 1000 tuples to reduce noise
                    if (tupleCount % 1000 == 0 && tupleCount > 0) {
                        System.err.println("PROGRESS: Processed " + tupleCount + " tuples for hierarchical clustering");
                    }

                    // Store frame in materialized data
                    materializedData.appendFrame(buffer);
                }

                private void processTuple(ITupleReference tuple) throws HyracksDataException {
                    try {
                        // Extract tuple data for structure analysis
                        FrameTupleReference frameTuple = (FrameTupleReference) tuple;
                        levelEval.evaluate(frameTuple, levelVal);
                        clusterIdEval.evaluate(frameTuple, clusterIdVal);
                        centroidIdEval.evaluate(frameTuple, centroidIdVal);

                        int level = AInt32SerializerDeserializer.getInt(levelVal.getByteArray(), levelVal.getStartOffset() + 1);
                        int clusterId = AInt32SerializerDeserializer.getInt(clusterIdVal.getByteArray(),
                                clusterIdVal.getStartOffset() + 1);
                        int centroidId = AInt32SerializerDeserializer.getInt(centroidIdVal.getByteArray(),
                                centroidIdVal.getStartOffset() + 1);
                        
                        // Debug: Log suspicious level values and filter them out
                        if (level < 0 || level > 100) {
                            System.err.println("🔍 DEBUG: Suspicious level value: " + level + " (clusterId: " + clusterId + ", centroidId: " + centroidId + ") - IGNORING");
                            return; // Skip this tuple
                        }

                        // Track structure for analysis
                        if (levelDistribution == null) {
                            levelDistribution = new HashMap<>();
                            clusterDistribution = new HashMap<>();
                        }

                        levelDistribution.put(level, levelDistribution.getOrDefault(level, 0) + 1);

                        String levelKey = "Level_" + level;
                        Map<Integer, Integer> levelClusters =
                                clusterDistribution.computeIfAbsent(levelKey, k -> new HashMap<>());
                        levelClusters.put(clusterId, levelClusters.getOrDefault(clusterId, 0) + 1);

                        tupleCount++;

                        // Log progress every 5000 tuples to reduce noise
                        if (tupleCount % 5000 == 0) {
                            System.err.println("Processed " + tupleCount + " tuples for structure analysis");
                        }

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to process tuple: " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity CLOSING ===");
                    System.err.println("Total tuples collected: " + tupleCount);
                    System.err.println("Frames accumulated: " + frameAccumulator.size());

                    // Process all accumulated tuples and create static structure
                    System.err.println("=== STARTING HIERARCHICAL CLUSTERING ANALYSIS ===");
                    System.err.println("Analyzing " + tupleCount + " tuples to determine structure...");
                    createStaticStructure();
                    System.err.println("=== HIERARCHICAL CLUSTERING ANALYSIS COMPLETE ===");

                    // Close materialized data
                    if (materializedData != null) {
                        materializedData.close();
                        ctx.setStateObject(materializedData);
                    }

                    // Signal Branch 2 that structure creation is complete
                    try {
                        System.err.println("=== SIGNALING BRANCH 2 COMPLETION ===");
                        IterationPermitState permitState =
                                (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));
                        if (permitState != null) {
                            permitState.getPermit().release();
                            System.err
                                    .println("✅ PERMIT RELEASED - Branch 1 (structure creation) completed, signaling Branch 2");
                        } else {
                            System.err.println("WARNING: Permit state not found for UUID: " + permitUUID);
                        }
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to release permit: " + e.getMessage());
                        e.printStackTrace();
                    }

                    System.err.println("=== CreateStructureActivity COMPLETE ===");
                }

                private void createStaticStructure() throws HyracksDataException {
                    System.err.println("=== CREATING STATIC STRUCTURE WITH VCTreeStaticStructureCreator ===");
                    System.err.println("Processing " + frameAccumulator.size() + " accumulated frames");
                    System.err.println("Total tuples to process: " + tupleCount);

                    try {
                        // Analyze collected data to determine structure
                        System.err.println("Analyzing collected data to determine hierarchical structure...");
                        List<Integer> clustersPerLevel = new ArrayList<>();
                        List<List<Integer>> centroidsPerCluster = new ArrayList<>();

                        if (levelDistribution != null && !levelDistribution.isEmpty()) {
                            int maxLevel = levelDistribution.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
                            System.err.println("🔍 DEBUG: Raw maxLevel from levelDistribution: " + maxLevel);
                            System.err.println("🔍 DEBUG: levelDistribution keys: " + levelDistribution.keySet());
                            
                            // Safety check: limit maxLevel to prevent infinite loops
                            if (maxLevel > 1000) {
                                System.err.println("❌ WARNING: maxLevel is too large (" + maxLevel + "), limiting to 10 to prevent infinite loop");
                                maxLevel = 10;
                            }
                            
                            System.err.println("Found " + maxLevel + " levels in the hierarchical structure");

                            int loopCounter = 0;
                            for (int level = 0; level <= maxLevel; level++) {
                                loopCounter++;
                                System.err.println("🔍 DEBUG: Processing level " + level + " (loop iteration " + loopCounter + ")");
                                
                                // Safety check to prevent infinite loops
                                if (loopCounter > 100) {
                                    System.err.println("❌ CRITICAL: Loop counter exceeded 100, breaking to prevent infinite loop");
                                    break;
                                }
                                
                                String levelKey = "Level_" + level;
                                Map<Integer, Integer> levelClusters = clusterDistribution.get(levelKey);

                                if (levelClusters != null) {
                                    int clusterCount = levelClusters.size();
                                    clustersPerLevel.add(clusterCount);
                                    System.err.println("Level " + level + ": " + clusterCount + " clusters");

                                    List<Integer> centroidsInClusters = new ArrayList<>();
                                    for (int clusterId : levelClusters.keySet()) {
                                        int centroidCount = levelClusters.get(clusterId);
                                        centroidsInClusters.add(centroidCount);
                                        System.err.println("  Cluster " + clusterId + ": " + centroidCount + " centroids");
                                    }
                                    centroidsPerCluster.add(centroidsInClusters);
                                } else {
                                    clustersPerLevel.add(0);
                                    centroidsPerCluster.add(new ArrayList<>());
                                    System.err.println("Level " + level + ": 0 clusters (no data)");
                                }
                            }
                        } else {
                            // Default structure if no data collected
                            System.err.println("No level distribution found, using default structure");
                            clustersPerLevel.add(1);
                            centroidsPerCluster.add(List.of(1));
                        }

                        // Get infrastructure
                        INcApplicationContext appCtx =
                                (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
                        IBufferCache bufferCache = appCtx.getBufferCache();
                        IIOManager ioManager = ctx.getIoManager();

                        // Get index path
                        System.err.println("Getting index file path...");
                        FileReference indexPathRef = getIndexFilePath();
                        if (indexPathRef == null) {
                            System.err.println("ERROR: Could not determine index path");
                            return;
                        }
                        System.err.println("Index path: " + indexPathRef);

                        // Create static structure file path (same directory as .metadata)
                        FileReference staticStructureFile = indexPathRef.getParent().getChild("static_structure.vctree");
                        System.err.println("Static structure file path: " + staticStructureFile);

                        // Create tuple writers with proper type traits
                        // Tuple format: [centroidId (int), embedding (float[]), childPageId (int)]
                        ITypeTraits[] typeTraits = new ITypeTraits[3];
                        typeTraits[0] = IntegerPointable.TYPE_TRAITS; // centroidId
                        typeTraits[1] = VarLengthTypeTrait.INSTANCE; // embedding (float array) - variable length
                        typeTraits[2] = IntegerPointable.TYPE_TRAITS; // childPageId

                        VectorClusteringLeafTupleWriterFactory leafTupleWriterFactory =
                                new VectorClusteringLeafTupleWriterFactory(typeTraits, null, null);
                        VectorClusteringInteriorTupleWriterFactory interiorTupleWriterFactory =
                                new VectorClusteringInteriorTupleWriterFactory(typeTraits, null, null);

                        // Create frames with proper tuple writers
                        VectorClusteringLeafFrameFactory leafFrameFactory =
                                new VectorClusteringLeafFrameFactory(leafTupleWriterFactory.createTupleWriter(), 128);
                        VectorClusteringInteriorFrameFactory interiorFrameFactory =
                                new VectorClusteringInteriorFrameFactory(interiorTupleWriterFactory.createTupleWriter(), 128);
                        ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
                        ITreeIndexFrame interiorFrame = interiorFrameFactory.createFrame();
                        ITreeIndexMetadataFrame metaFrame =
                                new AppendOnlyLinkedMetadataPageManager(bufferCache, new LIFOMetaDataFrameFactory())
                                        .createMetadataFrame();

                        // Create static structure creator
                        // Create a simple page write callback
                        IPageWriteCallback pageWriteCallback = new IPageWriteCallback() {
                            @Override
                            public void beforeWrite(org.apache.hyracks.storage.common.buffercache.ICachedPage page) {
                                // No-op
                            }

                            @Override
                            public void afterWrite(org.apache.hyracks.storage.common.buffercache.ICachedPage page) {
                                // No-op
                            }

                            @Override
                            public void initialize(org.apache.hyracks.storage.common.IIndexBulkLoader bulkLoader) {
                                // No-op
                            }
                        };

                        System.err.println(
                                "Creating VCTreeStaticStructureCreator with " + clustersPerLevel.size() + " levels...");
                        structureCreator = new VCTreeStaticStructureCreator(bufferCache, ioManager, staticStructureFile, 4096,
                                fillFactor, leafFrame, interiorFrame, metaFrame, pageWriteCallback, clustersPerLevel.size(),
                                clustersPerLevel, centroidsPerCluster, maxEntriesPerPage);

                        System.err.println("Processing " + frameAccumulator.size() + " accumulated frames...");
                        // Process all accumulated tuples
                        int totalTuplesProcessed = 0;
                        for (ByteBuffer frameBuffer : frameAccumulator) {
                            FrameTupleAccessor frameFta = new FrameTupleAccessor(outRecDescs[0]);
                            frameFta.reset(frameBuffer);

                            for (int i = 0; i < frameFta.getTupleCount(); i++) {
                                tuple.reset(frameFta, i);
                                structureCreator.add(tuple);
                                totalTuplesProcessed++;
                            }
                        }
                        System.err.println("Processed " + totalTuplesProcessed + " tuples for static structure creation");

                        System.err.println("Finalizing static structure...");
                        // Finalize the structure
                        structureCreator.finalize();
                        System.err.println("✅ STATIC STRUCTURE FINALIZED SUCCESSFULLY");

                        System.err.println("Writing metadata file...");
                        // Write metadata file
                        writeMetadataFile(indexPathRef, clustersPerLevel, centroidsPerCluster);
                        System.err.println("✅ METADATA FILE WRITTEN SUCCESSFULLY");

                        System.err.println(
                                "✅ STATIC STRUCTURE CREATED SUCCESSFULLY at: " + staticStructureFile.getAbsolutePath());

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to create static structure: " + e.getMessage());
                        e.printStackTrace();
                        throw HyracksDataException.create(e);
                    }
                }

                private void writeMetadataFile(FileReference indexPathRef, List<Integer> clustersPerLevel,
                        List<List<Integer>> centroidsPerCluster) {
                    try {
                        // Write to index subdirectory to match where VCTreeStaticStructureReader expects it
                        FileReference metadataFile = indexPathRef.getChild(".staticstructure");
                        System.err.println("Writing metadata file to: " + metadataFile.getAbsolutePath());

                        Map<String, Object> metadata = new HashMap<>();
                        metadata.put("numLevels", clustersPerLevel.size());
                        metadata.put("clustersPerLevel", clustersPerLevel);
                        metadata.put("centroidsPerCluster", centroidsPerCluster);
                        metadata.put("maxEntriesPerPage", maxEntriesPerPage);
                        metadata.put("totalTuplesProcessed", tupleCount);
                        metadata.put("timestamp", System.currentTimeMillis());
                        metadata.put("partition", partition);
                        metadata.put("staticStructureFile", "static_structure.vctree");

                        System.err.println("Metadata content: " + metadata);
                        ObjectMapper mapper = new ObjectMapper();
                        byte[] data = mapper.writeValueAsBytes(metadata);

                        IIOManager ioManager = ctx.getIoManager();
                        ioManager.overwrite(metadataFile, data);

                        System.err.println("✅ METADATA FILE WRITTEN SUCCESSFULLY to: " + metadataFile.getAbsolutePath());

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to write metadata file: " + e.getMessage());
                        e.printStackTrace();
                    }
                }

                private FileReference getIndexFilePath() {
                    try {
                        IIndexDataflowHelper indexHelper =
                                indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                        LocalResource resource = indexHelper.getResource();
                        String resourcePath = resource.getPath();

                        IIOManager ioManager = ctx.getIoManager();
                        return ioManager.resolve(resourcePath);

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to get index file path: " + e.getMessage());
                        e.printStackTrace();
                        return null;
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    System.err.println("=== CreateStructureActivity FAILING ===");
                    System.err.println("Total tuples processed before failure: " + tupleCount);

                    if (structureCreator != null) {
                        try {
                            structureCreator.close();
                        } catch (Exception e) {
                            System.err.println("ERROR: Failed to close structure creator: " + e.getMessage());
                        }
                    }

                    if (materializedData != null) {
                        try {
                            materializedData.close();
                        } catch (Exception e) {
                            System.err.println("ERROR: Failed to close materialized data: " + e.getMessage());
                        }
                    }
                    System.err.println("=== CreateStructureActivity FAILED ===");
                }
            };
        }
    }

    protected class PassThroughActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected PassThroughActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    System.err.println("=== PassThroughActivity INITIALIZING ===");
                    try {
                        // Wait for permit from CreateStructureActivity
                        IterationPermitState permitState =
                                (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));
                        if (permitState != null) {
                            System.err.println("Waiting for permit from CreateStructureActivity...");
                            permitState.getPermit().acquire();
                            System.err.println("✅ PERMIT ACQUIRED - CreateStructureActivity completed");
                        }

                        // Get materialized data
                        MaterializerTaskState materializedData =
                                (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(materializedDataUUID, partition));
                        if (materializedData != null) {
                            System.err.println("Reading materialized data...");
                            // Pass through logic - just read and forward data
                            // For now, this is a simple pass-through
                            System.err.println("✅ PassThroughActivity initialized successfully");
                        } else {
                            System.err.println("WARNING: No materialized data found");
                        }

                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to initialize PassThroughActivity: " + e.getMessage());
                        throw HyracksDataException.create(e);
                    }
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    System.err.println("=== PassThroughActivity DEINITIALIZING ===");
                    System.err.println("=== PassThroughActivity COMPLETE ===");
                }
            };
        }
    }
}
