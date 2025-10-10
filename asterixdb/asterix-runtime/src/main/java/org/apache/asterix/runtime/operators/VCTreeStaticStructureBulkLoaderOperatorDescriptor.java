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

import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.utils.StorageConstants;
// import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
// import org.apache.hyracks.storage.am.vector.api.IVectorClusteringFrame;
// import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
// import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
// import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
// import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContextProvider;
// import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
// import org.apache.asterix.om.base.AInt32;
// import org.apache.asterix.om.base.AMutableInt32;
// import org.apache.asterix.om.base.IAObject;
// import org.apache.asterix.om.types.ATypeTag;
// import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
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
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.vector.impls.VCTreeStaticStructureLoader;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.LocalResource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class VCTreeStaticStructureBulkLoaderOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final int maxEntriesPerPage;
    private final float fillFactor;
    private final UUID permitUUID;

    public VCTreeStaticStructureBulkLoaderOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID) {
        super(spec, 1, 1); // Input arity 1, Output arity 1 (regular operator)
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID; // New field
        this.outRecDescs[0] = inputRecordDescriptor; // Pass through the same record descriptor
        System.err.println("VCTreeStaticStructureBulkLoaderOperatorDescriptor created with permit UUID: " + permitUUID);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeStaticStructureBulkLoaderNodePushable(ctx, partition, nPartitions, inputRecDesc, permitUUID);
    }

    private class VCTreeStaticStructureBulkLoaderNodePushable
            extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;
        private final RecordDescriptor inputRecDesc;
        private final UUID permitUUID;

        private VCTreeStaticStructureLoader staticStructureLoader;
        private boolean bulkLoadingStarted = false;
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

        // Debug tracking fields
        private Map<Integer, Integer> levelDistribution = null;
        private Map<String, Map<Integer, Integer>> clusterDistribution = null;

        // Accumulator to collect all frame data before processing
        private List<ByteBuffer> frameAccumulator = new ArrayList<>();
        private boolean dataCollectionComplete = false;

        public VCTreeStaticStructureBulkLoaderNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
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
            System.err.println("=== VCTreeStaticStructureBulkLoader OPENING ===");
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

                System.err.println("VCTreeStaticStructureBulkLoader opened successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeStaticStructureBulkLoader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            fta.reset(buffer);

            System.err.println("=== VCTreeStaticStructureBulkLoader: Processing frame with " + fta.getTupleCount()
                    + " tuples ===");
            System.err.println("=== THREAD: " + Thread.currentThread().getName() + " ===");
            System.err.println("=== TIMESTAMP: " + System.currentTimeMillis() + " ===");

            if (!dataCollectionComplete) {
                // Accumulate the entire frame for later processing
                ByteBuffer frameCopy = ByteBuffer.allocate(buffer.remaining());
                frameCopy.put(buffer.duplicate());
                frameCopy.flip();
                frameAccumulator.add(frameCopy);

                tupleCount += fta.getTupleCount();
                System.err.println("=== ACCUMULATED FRAME #" + frameAccumulator.size() + " with " + fta.getTupleCount()
                        + " tuples ===");
                System.err.println("=== TOTAL TUPLES ACCUMULATED: " + tupleCount + " ===");
            } else {
                // Process frame immediately (shouldn't happen in this pattern)
                for (int i = 0; i < fta.getTupleCount(); i++) {
                    tuple.reset(fta, i);
                    processTuple(tuple);
                }
            }

            // Pass through the input frame to output
            if (writer != null) {
                writer.nextFrame(buffer);
            }
        }

        private void processTuple(ITupleReference tuple) throws HyracksDataException {
            // This method is no longer used in the accumulation pattern
            // All processing happens in processAllAccumulatedTuples()
            System.err.println("WARNING: processTuple called but should use accumulation pattern");
        }

        private void processAllAccumulatedTuples() throws HyracksDataException {
            System.err.println("=== PROCESSING " + frameAccumulator.size() + " ACCUMULATED FRAMES ===");

            // Initialize data collection structures
            levelDistribution = new HashMap<>();
            clusterDistribution = new HashMap<>();

            // Process each accumulated frame
            for (int frameIndex = 0; frameIndex < frameAccumulator.size(); frameIndex++) {
                ByteBuffer frameBuffer = frameAccumulator.get(frameIndex);
                FrameTupleAccessor frameFta = new FrameTupleAccessor(inputRecDesc);
                frameFta.reset(frameBuffer);

                System.err.println("=== PROCESSING FRAME #" + (frameIndex + 1) + " with " + frameFta.getTupleCount()
                        + " tuples ===");

                // Process each tuple in the frame
                for (int i = 0; i < frameFta.getTupleCount(); i++) {
                    tuple.reset(frameFta, i);

                    // Debug: Extract and log tuple data before processing
                    try {
                        FrameTupleReference frameTuple = (FrameTupleReference) tuple;
                        levelEval.evaluate(frameTuple, levelVal);
                        clusterIdEval.evaluate(frameTuple, clusterIdVal);
                        centroidIdEval.evaluate(frameTuple, centroidIdVal);

                        int level = AInt32SerializerDeserializer.getInt(levelVal.getByteArray(),
                                levelVal.getStartOffset() + 1);
                        int clusterId = AInt32SerializerDeserializer.getInt(clusterIdVal.getByteArray(),
                                clusterIdVal.getStartOffset() + 1);
                        int centroidId = AInt32SerializerDeserializer.getInt(centroidIdVal.getByteArray(),
                                centroidIdVal.getStartOffset() + 1);

                        System.err.println("=== FRAME #" + (frameIndex + 1) + " TUPLE #" + (i + 1) + " DATA: Level="
                                + level + ", Cluster=" + clusterId + ", Centroid=" + centroidId + " ===");
                    } catch (Exception e) {
                        System.err.println("=== FRAME #" + (frameIndex + 1) + " TUPLE #" + (i + 1)
                                + " DATA: ERROR extracting data: " + e.getMessage() + " ===");
                    }

                    processRealTuple(tuple);
                }
            }

            System.err.println("=== COMPLETED PROCESSING ALL " + frameAccumulator.size() + " FRAMES ===");
        }

        private void processRealTuple(ITupleReference tuple) throws HyracksDataException {

            try {
                // Cast to FrameTupleReference (it's already created in nextFrame)
                FrameTupleReference frameTuple = (FrameTupleReference) tuple;

                // Extract data using evaluators (like hierarchical k-means does)
                levelEval.evaluate(frameTuple, levelVal);
                clusterIdEval.evaluate(frameTuple, clusterIdVal);
                centroidIdEval.evaluate(frameTuple, centroidIdVal);
                embeddingEval.evaluate(frameTuple, embeddingVal);

                // Deserialize integer fields using AsterixDB deserializers
                int level = AInt32SerializerDeserializer.getInt(levelVal.getByteArray(), levelVal.getStartOffset() + 1);
                int clusterId = AInt32SerializerDeserializer.getInt(clusterIdVal.getByteArray(),
                        clusterIdVal.getStartOffset() + 1);
                int centroidId = AInt32SerializerDeserializer.getInt(centroidIdVal.getByteArray(),
                        centroidIdVal.getStartOffset() + 1);

                // Track level distribution
                levelDistribution.put(level, levelDistribution.getOrDefault(level, 0) + 1);

                // Track cluster distribution per level
                String levelKey = "Level_" + level;
                Map<Integer, Integer> levelClusters =
                        clusterDistribution.computeIfAbsent(levelKey, k -> new HashMap<>());
                levelClusters.put(clusterId, levelClusters.getOrDefault(clusterId, 0) + 1);

                // Process the real centroid data
                processRealCentroid(level, clusterId, centroidId, embeddingVal, tuple);

                // Log progress every 100 tuples
                if (tupleCount % 100 == 0) {
                    System.err.println("Processed " + tupleCount + " real centroids from hierarchical k-means");
                    logCurrentStructure();
                }

            } catch (Exception e) {
                System.err.println("ERROR: Failed to process real tuple: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void processRealCentroid(int level, int clusterId, int centroidId, IPointable embeddingVal,
                ITupleReference tuple) throws HyracksDataException {
            try {
                // Extract embedding data from the tuple
                // The embedding is stored as a double array in the 4th field
                int embeddingLength = embeddingVal.getLength();

                // Skip the type tag (first byte) for the embedding
                if (embeddingLength > 1) {
                    // The embedding is serialized as a double array
                    // We need to deserialize it properly
                    System.err.println("Processing real centroid: Level=" + level + ", Cluster=" + clusterId
                            + ", Centroid=" + centroidId + ", EmbeddingSize=" + (embeddingLength - 1) + " bytes");

                    // TODO: Deserialize the actual double array from embeddingBytes
                    // For now, we'll create a placeholder double array
                    int numDoubles = (embeddingLength - 1) / 8; // Assuming 8 bytes per double
                    double[] embedding = new double[Math.max(1, numDoubles)];

                    // Fill with placeholder values for now
                    for (int i = 0; i < embedding.length; i++) {
                        embedding[i] = (Math.random() - 0.5) * 10; // Random values between -5 and 5
                    }

                    // Add this centroid to the VCTreeStaticStructureLoader
                    if (staticStructureLoader != null) {
                        // staticStructureLoader.add(tuple);
                        System.err.println("  -> Added to VCTreeStaticStructureLoader");
                    } else {
                        System.err.println("  -> VCTreeStaticStructureLoader not available, storing for later");
                    }

                } else {
                    System.err.println("WARNING: Empty or invalid embedding data for centroid " + centroidId);
                }

            } catch (Exception e) {
                System.err.println("ERROR: Failed to process real centroid: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void createVCTreeStaticStructureLoaderWithRealData() throws HyracksDataException {
            System.err.println("=== CREATING VCTreeStaticStructureLoader WITH REAL DATA ===");

            try {
                // Analyze the collected data to determine the real structure
                int numLevels = levelDistribution.size();
                List<Integer> clustersPerLevel = new ArrayList<>();
                List<List<Integer>> centroidsPerCluster = new ArrayList<>();

                // Sort levels to ensure consistent ordering
                List<Integer> sortedLevels = new ArrayList<>(levelDistribution.keySet());
                sortedLevels.sort(Integer::compareTo);

                System.err.println("Real structure analysis:");
                System.err.println("  Number of levels: " + numLevels);

                for (int level : sortedLevels) {
                    String levelKey = "Level_" + level;
                    Map<Integer, Integer> levelClusters = clusterDistribution.get(levelKey);

                    if (levelClusters != null) {
                        int clustersInLevel = levelClusters.size();
                        clustersPerLevel.add(clustersInLevel);

                        List<Integer> centroidsInClusters = new ArrayList<>();
                        for (int clusterId : levelClusters.keySet()) {
                            int centroidsInCluster = levelClusters.get(clusterId);
                            centroidsInClusters.add(centroidsInCluster);
                        }
                        centroidsPerCluster.add(centroidsInClusters);

                        System.err.println("  Level " + level + ": " + clustersInLevel + " clusters");
                        System.err.println("    Centroids per cluster: " + centroidsInClusters);
                    } else {
                        System.err.println("  Level " + level + ": No cluster data found");
                        clustersPerLevel.add(0);
                        centroidsPerCluster.add(new ArrayList<>());
                    }
                }

                System.err.println("Final structure:");
                System.err.println("  - numLevels: " + numLevels);
                System.err.println("  - clustersPerLevel: " + clustersPerLevel);
                System.err.println("  - centroidsPerCluster: " + centroidsPerCluster);
                System.err.println("  - maxEntriesPerPage: " + maxEntriesPerPage);
                System.err.println("  - fillFactor: " + fillFactor);

                // Create LSM index with static structure
                createLSMIndexWithStaticStructure(numLevels, clustersPerLevel, centroidsPerCluster);

            } catch (Exception e) {
                System.err.println(
                        "ERROR: Failed to create VCTreeStaticStructureLoader with real data: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Creates the LSM index with static structure parameters.
         * 
         * This method actually creates the LSM index and calls the bulk loader
         * to build the static structure pages, following the LSMBTree pattern.
         */
        private void createLSMIndexWithStaticStructure(int numLevels, List<Integer> clustersPerLevel,
                List<List<Integer>> centroidsPerCluster) {
            System.err.println("=== CREATING LSM INDEX WITH STATIC STRUCTURE ===");

            try {
                // Create the LSM index using the indexHelperFactory
                IIndexDataflowHelper indexHelper =
                        indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);

                System.err.println("Opening LSM index...");
                indexHelper.open();
                ILSMIndex lsmIndex = (ILSMIndex) indexHelper.getIndexInstance();
                System.err.println("LSM index opened successfully");

                // Create parameters map for the LSM index creation
                Map<String, Object> structureParameters = new HashMap<>();
                structureParameters.put("numLevels", numLevels);
                structureParameters.put("clustersPerLevel", clustersPerLevel);
                structureParameters.put("centroidsPerCluster", centroidsPerCluster);
                structureParameters.put("maxEntriesPerPage", maxEntriesPerPage);
                structureParameters.put("fillFactor", fillFactor);
                structureParameters.put("isStaticStructureLoad", true);
                structureParameters.put("totalTuplesProcessed", tupleCount);

                // Add required LSM component ID parameter (following LSMIndexBulkLoadOperatorNodePushable pattern)
                structureParameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID,
                        LSMComponentId.DEFAULT_COMPONENT_ID);

                System.err.println("Structure parameters for LSM index creation:");
                System.err.println("  - numLevels: " + structureParameters.get("numLevels"));
                System.err.println("  - clustersPerLevel: " + structureParameters.get("clustersPerLevel"));
                System.err.println("  - centroidsPerCluster: " + structureParameters.get("centroidsPerCluster"));
                System.err.println("  - maxEntriesPerPage: " + structureParameters.get("maxEntriesPerPage"));
                System.err.println("  - fillFactor: " + structureParameters.get("fillFactor"));
                System.err.println("  - isStaticStructureLoad: " + structureParameters.get("isStaticStructureLoad"));
                System.err.println("  - totalTuplesProcessed: " + structureParameters.get("totalTuplesProcessed"));
                System.err.println("  - flushedComponentId: "
                        + structureParameters.get(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID));

                // Create the bulk loader with static structure parameters
                System.err.println("Creating LSM bulk loader with static structure...");
                IIndexBulkLoader bulkLoader =
                        lsmIndex.createBulkLoader(fillFactor, false, tupleCount, false, structureParameters);

                System.err.println("LSM bulk loader created successfully");

                // Process all accumulated tuples and transform them for the LSM bulk loader
                System.err.println("Adding " + frameAccumulator.size() + " accumulated frames to bulk loader...");
                int totalTuplesAdded = 0;
                
                for (int frameIndex = 0; frameIndex < frameAccumulator.size(); frameIndex++) {
                    ByteBuffer frameBuffer = frameAccumulator.get(frameIndex);
                    FrameTupleAccessor frameFta = new FrameTupleAccessor(inputRecDesc);
                    frameFta.reset(frameBuffer);
                    
                    System.err.println("Processing frame #" + (frameIndex + 1) + " with " + frameFta.getTupleCount() + " tuples");
                    
                    // Transform each tuple in the frame for the LSM bulk loader
                    for (int i = 0; i < frameFta.getTupleCount(); i++) {
                        tuple.reset(frameFta, i);
                        try {
                            // Transform the hierarchical k-means tuple to the expected LSM format
                            ITupleReference transformedTuple = transformTupleForLSM(tuple);
                            if (transformedTuple != null) {
                                bulkLoader.add(transformedTuple);
                                totalTuplesAdded++;
                            }
                        } catch (Exception e) {
                            System.err.println("ERROR: Failed to transform and add tuple " + (i + 1) + " from frame " + (frameIndex + 1) + " to bulk loader: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
                
                System.err.println("Successfully added " + totalTuplesAdded + " transformed tuples to bulk loader");

                // Finalize the bulk loader to create the static structure
                System.err.println("Finalizing bulk loader to create static structure pages...");
                bulkLoader.end();

                System.err.println("Static structure pages created successfully in LSM index");

                // Close the index
                indexHelper.close();
                System.err.println("LSM index closed successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to create LSM index with static structure: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Transforms a hierarchical k-means tuple to the format expected by the LSM bulk loader.
         * Based on the error, the LSM index might expect only 1 field (centroidId).
         * Let's try providing just the centroidId first to see if that resolves the issue.
         */
        private ITupleReference transformTupleForLSM(ITupleReference originalTuple) throws HyracksDataException {
            try {
                // Debug: Check original tuple structure
                System.err.println("DEBUG: Original tuple has " + originalTuple.getFieldCount() + " fields");
                for (int i = 0; i < originalTuple.getFieldCount(); i++) {
                    System.err.println("  Field " + i + ": length=" + originalTuple.getFieldLength(i));
                }
                
                // Try providing only the centroidId (field 2) to see if that resolves the field slots issue
                // The error suggests the LSM index expects only 1 field, not 2
                int[] fieldPermutation = {2}; // Map output field 0 to input field 2 (centroidId)
                
                PermutingFrameTupleReference permutingTuple = new PermutingFrameTupleReference(fieldPermutation);
                
                // Get the frame accessor and tuple index from the original tuple
                FrameTupleReference frameTuple = (FrameTupleReference) originalTuple;
                permutingTuple.reset(frameTuple.getFrameTupleAccessor(), frameTuple.getTupleIndex());
                
                // Debug: Check transformed tuple structure
                System.err.println("DEBUG: Transformed tuple has " + permutingTuple.getFieldCount() + " fields");
                for (int i = 0; i < permutingTuple.getFieldCount(); i++) {
                    System.err.println("  Transformed field " + i + ": length=" + permutingTuple.getFieldLength(i));
                }
                
                return permutingTuple;

            } catch (Exception e) {
                System.err.println("ERROR: Failed to transform tuple for LSM: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }

        private void logCurrentStructure() {
            System.err.println("=== CURRENT HIERARCHICAL STRUCTURE ===");
            System.err.println("Level distribution: " + levelDistribution);

            for (String levelKey : clusterDistribution.keySet()) {
                Map<Integer, Integer> levelClusters = clusterDistribution.get(levelKey);
                System.err.println(levelKey + " clusters: " + levelClusters);
            }

            // Calculate total centroids
            int totalCentroids = levelDistribution.values().stream().mapToInt(Integer::intValue).sum();
            System.err.println("Total centroids processed: " + totalCentroids);
            System.err.println("=====================================");
        }

        /**
         * Writes the static structure information to .staticstructure file in the same directory as the index's .metadata file
         */
        private void writeStaticStructureToFile() {
            try {
                // Get the LSM index file manager to determine the correct component directory
                IIndexDataflowHelper indexHelper =
                        indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);

                // Get the LSM index file manager
                // For now, we'll use a simple approach to get the base directory
                // The LSM index should be created in the target directory
                IIOManager ioManager = ctx.getIoManager();

                // Get the index file path from the index helper factory
                // The .staticstructure file should be in the same directory as the .metadata file
                FileReference indexPath = getIndexFilePath();
                if (indexPath == null) {
                    System.err.println(
                            "WARNING: Could not determine index file path, skipping .staticstructure file creation");
                    return;
                }

                // Create the .staticstructure file in the same directory as the index
                String staticStructureFileName = StorageConstants.STATIC_STRUCTURE_FILE_NAME;
                FileReference staticStructureFile = indexPath.getParent().getChild(staticStructureFileName);

                System.err.println("Writing .staticstructure file to index directory:");
                System.err.println("  - Index directory: " + indexPath.getParent().getAbsolutePath());
                System.err.println("  - Static structure file: " + staticStructureFile.getAbsolutePath());

                // Prepare structure data
                Map<String, Object> structureData = new HashMap<>();
                structureData.put("numLevels", levelDistribution.size());
                structureData.put("levelDistribution", levelDistribution);
                structureData.put("clusterDistribution", clusterDistribution);
                structureData.put("totalTuplesProcessed", tupleCount);
                structureData.put("timestamp", System.currentTimeMillis());
                structureData.put("partition", partition);

                // Write to index directory using atomic write pattern
                writeStaticStructureAtomically(staticStructureFile, structureData);

                System.err.println("Static structure written to index directory");
                System.err.println("  - Levels: " + levelDistribution.size());
                System.err.println("  - Total centroids: "
                        + levelDistribution.values().stream().mapToInt(Integer::intValue).sum());

            } catch (Exception e) {
                System.err
                        .println("ERROR: Failed to write static structure file to index directory: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Gets the next component sequence for the LSM component.
         * This should match the sequence used by the LSM file manager.
         */
        private String getNextComponentSequence() {
            // For now, use a simple timestamp-based sequence
            // In a real implementation, this should coordinate with the LSM file manager
            return String.format("%020d", System.currentTimeMillis());
        }

        /**
         * Gets the file path for the LSM index.
         * This is used to determine where to place the .staticstructure file.
         */
        private FileReference getIndexFilePath() {
            try {
                // Use the index helper factory to create an index helper for this partition
                // This follows the same pattern as other LSM operators
                IIndexDataflowHelper indexHelper =
                        indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);

                // Get the LocalResource from the index helper
                LocalResource resource = indexHelper.getResource();

                // Get the file path from the resource
                String resourcePath = resource.getPath();

                // Convert to FileReference using the IOManager
                IIOManager ioManager = ctx.getIoManager();
                return ioManager.resolve(resourcePath);

            } catch (Exception e) {
                System.err.println("ERROR: Failed to get index file path: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }

        /**
         * Writes the static structure file atomically using the mask file pattern.
         */
        private void writeStaticStructureAtomically(FileReference staticStructureFile,
                Map<String, Object> structureData) throws Exception {
            IIOManager ioManager = ctx.getIoManager();

            // Ensure parent directory exists
            FileReference parentDir = staticStructureFile.getParent();
            if (!ioManager.exists(parentDir)) {
                ioManager.makeDirectories(parentDir);
            }

            // Create mask file for atomic write
            FileReference maskFile = getMaskFile(staticStructureFile);
            if (ioManager.exists(maskFile)) {
                ioManager.delete(maskFile);
            }
            ioManager.create(maskFile);

            try {
                // Write structure data as JSON
                ObjectMapper mapper = new ObjectMapper();
                byte[] data = mapper.writeValueAsBytes(structureData);
                ioManager.overwrite(staticStructureFile, data);

                // Remove mask file to indicate successful write
                ioManager.delete(maskFile);

                System.err
                        .println("Static structure file written atomically: " + staticStructureFile.getAbsolutePath());

            } catch (Exception e) {
                // Clean up on failure
                if (ioManager.exists(staticStructureFile)) {
                    ioManager.delete(staticStructureFile);
                }
                if (ioManager.exists(maskFile)) {
                    ioManager.delete(maskFile);
                }
                throw new RuntimeException("Failed to write static structure file atomically", e);
            }
        }

        /**
         * Gets the mask file for a .staticstructure file.
         */
        private FileReference getMaskFile(FileReference staticStructureFile) {
            String maskFileName = "." + staticStructureFile.getName();
            return staticStructureFile.getParent().getChild(maskFileName);
        }

        @Override
        public void close() throws HyracksDataException {
            System.err.println("=== VCTreeStaticStructureBulkLoader FINALIZING ===");
            System.err.println("Total frames accumulated: " + frameAccumulator.size());
            System.err.println("Total tuples accumulated: " + tupleCount);

            // Process all accumulated tuples at once
            if (!dataCollectionComplete) {
                System.err.println("=== PROCESSING ALL ACCUMULATED TUPLES ===");
                processAllAccumulatedTuples();
                dataCollectionComplete = true;
            }

            // Log final structure
            logCurrentStructure();

            // Create VCTreeStaticStructureLoader with real structure
            createVCTreeStaticStructureLoaderWithRealData();

            if (staticStructureLoader != null) {
                try {
                    // staticStructureLoader.end();
                    System.err.println("VCTreeStaticStructureLoader finalized with real data");
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to finalize VCTreeStaticStructureLoader: " + e.getMessage());
                }
            }

            // Write static structure to .staticstructure file
            writeStaticStructureToFile();

            // Close the writer if available
            if (writer != null) {
                writer.close();
            }

            System.err.println("=== VCTreeStaticStructureBulkLoader COMPLETE ===");
        }

        @Override
        public void fail() throws HyracksDataException {
            System.err.println("=== VCTreeStaticStructureBulkLoader FAILING ===");
            System.err.println("Total tuples processed before failure: " + tupleCount);

            if (staticStructureLoader != null) {
                System.err.println("Aborting VCTreeStaticStructureLoader...");
                try {
                    // staticStructureLoader.abort();
                    System.err.println("VCTreeStaticStructureLoader aborted");
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to abort VCTreeStaticStructureLoader: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            // Fail the writer if available
            if (writer != null) {
                writer.fail();
            }
            System.err.println("=== VCTreeStaticStructureBulkLoader FAILED ===");
        }
    }

}
