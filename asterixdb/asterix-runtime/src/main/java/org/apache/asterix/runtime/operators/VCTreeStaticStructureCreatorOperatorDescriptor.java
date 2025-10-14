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
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManager;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringInteriorTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringLeafTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.impls.VCTreeStaticStructureCreator;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Operator that creates VCTree static structure files using VCTreeStaticStructureCreator.
 * Replaces VCTreeStaticStructureBinaryWriter with proper page-based structure creation.
 */
public class VCTreeStaticStructureCreatorOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final int maxEntriesPerPage;
    private final float fillFactor;
    private final UUID permitUUID;

    public VCTreeStaticStructureCreatorOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID) {
        super(spec, 1, 1);
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID;
        this.outRecDescs[0] = inputRecordDescriptor;
        System.err.println("VCTreeStaticStructureCreatorOperatorDescriptor created with permit UUID: " + permitUUID);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeStaticStructureCreatorNodePushable(ctx, partition, nPartitions, inputRecDesc, permitUUID);
    }

    private class VCTreeStaticStructureCreatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;
        private final RecordDescriptor inputRecDesc;
        private final UUID permitUUID;

        // Data collection
        private final List<ByteBuffer> frameAccumulator = new ArrayList<>();
        private boolean dataCollectionComplete = false;
        private int tupleCount = 0;
        private FrameTupleReference tuple = new FrameTupleReference();
        private FrameTupleAccessor fta;

        // Evaluators for extracting tuple fields
        private IScalarEvaluator levelEval;
        private IScalarEvaluator clusterIdEval;
        private IScalarEvaluator centroidIdEval;
        private IScalarEvaluator embeddingEval;
        private IPointable levelVal;
        private IPointable clusterIdVal;
        private IPointable centroidIdVal;
        private IPointable embeddingVal;

        // Structure analysis
        private Map<Integer, Integer> levelDistribution = null;
        private Map<String, Map<Integer, Integer>> clusterDistribution = null;

        // Static structure creator
        private VCTreeStaticStructureCreator structureCreator;

        public VCTreeStaticStructureCreatorNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc, UUID permitUUID) throws HyracksDataException {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.inputRecDesc = inputRecDesc;
            this.permitUUID = permitUUID;
            this.fta = new FrameTupleAccessor(inputRecDesc);
        }

        @Override
        public void open() throws HyracksDataException {
            System.err.println("=== VCTreeStaticStructureCreator OPENING ===");
            try {
                // Initialize evaluators for extracting tuple fields
                EvaluatorContext evalCtx = new EvaluatorContext(ctx);
                levelEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(evalCtx);
                clusterIdEval = new ColumnAccessEvalFactory(1).createScalarEvaluator(evalCtx);
                centroidIdEval = new ColumnAccessEvalFactory(2).createScalarEvaluator(evalCtx);
                embeddingEval = new ColumnAccessEvalFactory(3).createScalarEvaluator(evalCtx);

                // Initialize pointables for evaluator results
                levelVal = new VoidPointable();
                clusterIdVal = new VoidPointable();
                centroidIdVal = new VoidPointable();
                embeddingVal = new VoidPointable();

                System.err.println("Evaluators initialized for field extraction");
                System.err.println("Expected 4 fields: level(0), clusterId(1), centroidId(2), embedding(3)");

                if (writer != null) {
                    writer.open();
                }

                System.err.println("VCTreeStaticStructureCreator opened successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeStaticStructureCreator: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            fta.reset(buffer);

            // Accumulate frames for batch processing
            frameAccumulator.add(buffer.duplicate());

            // Process tuples in this frame
            for (int i = 0; i < fta.getTupleCount(); i++) {
                tuple.reset(fta, i);
                processTuple(tuple);
            }

            // Pass through the frame to output
            if (writer != null) {
                writer.nextFrame(buffer);
            }
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

                // Log progress every 1000 tuples
                if (tupleCount % 1000 == 0) {
                    System.err.println("Processed " + tupleCount + " tuples for structure analysis");
                }

            } catch (Exception e) {
                System.err.println("ERROR: Failed to process tuple: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws HyracksDataException {
            // Process all accumulated tuples and create static structure
            if (!dataCollectionComplete) {
                createStaticStructure();
                dataCollectionComplete = true;
            }

            // Close the writer if available
            if (writer != null) {
                writer.close();
            }

            System.err.println("=== VCTreeStaticStructureCreator COMPLETE ===");
        }

        private void createStaticStructure() throws HyracksDataException {
            System.err.println("=== CREATING STATIC STRUCTURE WITH VCTreeStaticStructureCreator ===");

            try {
                // Analyze collected data to determine structure
                List<Integer> clustersPerLevel = new ArrayList<>();
                List<List<Integer>> centroidsPerCluster = new ArrayList<>();

                if (levelDistribution != null && !levelDistribution.isEmpty()) {
                    int maxLevel = levelDistribution.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);

                    for (int level = 0; level <= maxLevel; level++) {
                        String levelKey = "Level_" + level;
                        Map<Integer, Integer> levelClusters = clusterDistribution.get(levelKey);

                        if (levelClusters != null) {
                            clustersPerLevel.add(levelClusters.size());

                            List<Integer> centroidsInClusters = new ArrayList<>();
                            for (int clusterId : levelClusters.keySet()) {
                                centroidsInClusters.add(levelClusters.get(clusterId));
                            }
                            centroidsPerCluster.add(centroidsInClusters);
                        } else {
                            clustersPerLevel.add(0);
                            centroidsPerCluster.add(new ArrayList<>());
                        }
                    }
                } else {
                    // Default structure if no data collected
                    clustersPerLevel.add(1);
                    centroidsPerCluster.add(List.of(1));
                }

                // Get infrastructure
                INcApplicationContext appCtx =
                        (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
                IBufferCache bufferCache = appCtx.getBufferCache();
                IIOManager ioManager = ctx.getIoManager();

                // Get index path
                FileReference indexPathRef = getIndexFilePath();
                if (indexPathRef == null) {
                    System.err.println("ERROR: Could not determine index path");
                    return;
                }

                // Create static structure file path (same directory as .metadata)
                FileReference staticStructureFile = indexPathRef.getParent().getChild("static_structure.vctree");

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

                structureCreator = new VCTreeStaticStructureCreator(bufferCache, ioManager, staticStructureFile, 4096,
                        fillFactor, leafFrame, interiorFrame, metaFrame, pageWriteCallback, clustersPerLevel.size(),
                        clustersPerLevel, centroidsPerCluster, maxEntriesPerPage);

                // Process all accumulated tuples
                for (ByteBuffer frameBuffer : frameAccumulator) {
                    FrameTupleAccessor frameFta = new FrameTupleAccessor(inputRecDesc);
                    frameFta.reset(frameBuffer);

                    for (int i = 0; i < frameFta.getTupleCount(); i++) {
                        tuple.reset(frameFta, i);
                        structureCreator.add(tuple);
                    }
                }

                // Finalize the structure
                structureCreator.finalize();

                // Write metadata file
                writeMetadataFile(indexPathRef, clustersPerLevel, centroidsPerCluster);

                System.err
                        .println("Static structure created successfully at: " + staticStructureFile.getAbsolutePath());

            } catch (Exception e) {
                System.err.println("ERROR: Failed to create static structure: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        private void writeMetadataFile(FileReference indexPathRef, List<Integer> clustersPerLevel,
                List<List<Integer>> centroidsPerCluster) {
            try {
                FileReference metadataFile = indexPathRef.getParent().getChild(".staticstructure");

                Map<String, Object> metadata = new HashMap<>();
                metadata.put("numLevels", clustersPerLevel.size());
                metadata.put("clustersPerLevel", clustersPerLevel);
                metadata.put("centroidsPerCluster", centroidsPerCluster);
                metadata.put("maxEntriesPerPage", maxEntriesPerPage);
                metadata.put("totalTuplesProcessed", tupleCount);
                metadata.put("timestamp", System.currentTimeMillis());
                metadata.put("partition", partition);
                metadata.put("staticStructureFile", "static_structure.vctree");

                ObjectMapper mapper = new ObjectMapper();
                byte[] data = mapper.writeValueAsBytes(metadata);

                IIOManager ioManager = ctx.getIoManager();
                ioManager.overwrite(metadataFile, data);

                System.err.println("Metadata written to: " + metadataFile.getAbsolutePath());

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
            System.err.println("=== VCTreeStaticStructureCreator FAILING ===");
            System.err.println("Total tuples processed before failure: " + tupleCount);

            if (structureCreator != null) {
                try {
                    structureCreator.close();
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to close structure creator: " + e.getMessage());
                }
            }

            if (writer != null) {
                writer.fail();
            }
            System.err.println("=== VCTreeStaticStructureCreator FAILED ===");
        }
    }
}
