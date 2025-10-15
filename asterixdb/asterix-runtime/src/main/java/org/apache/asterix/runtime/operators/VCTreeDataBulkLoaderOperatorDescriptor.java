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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.LocalResource;

/**
 * Bulk loader that loads sorted data directly to leaf pages using centroid ID mapping.
 * 
 * This operator:
 * 1. Reads static structure to map centroid IDs to leaf page IDs
 * 2. Loads sorted tuples from run files directly to corresponding leaf pages
 * 3. Skips static structure traversal using direct mapping
 * 4. Finalizes LSM component with bulkLoader.end()
 */
public class VCTreeDataBulkLoaderOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final int maxEntriesPerPage;
    private final float fillFactor;
    private final UUID permitUUID;
    private final Map<Integer, String> runFilePaths; // centroidId -> run file path

    public VCTreeDataBulkLoaderOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory indexHelperFactory, int maxEntriesPerPage, float fillFactor,
            RecordDescriptor inputRecordDescriptor, UUID permitUUID, Map<Integer, String> runFilePaths) {
        super(spec, 1, 1); // Input arity 1, Output arity 1
        this.indexHelperFactory = indexHelperFactory;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.fillFactor = fillFactor;
        this.permitUUID = permitUUID;
        this.runFilePaths = runFilePaths;
        this.outRecDescs[0] = inputRecordDescriptor;
        System.err.println("VCTreeDataBulkLoaderOperatorDescriptor created with " + runFilePaths.size() + " run files");
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeDataBulkLoaderNodePushable(ctx, partition, nPartitions, inputRecDesc, permitUUID);
    }

    private class VCTreeDataBulkLoaderNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;
        private final RecordDescriptor inputRecDesc;
        private final UUID permitUUID;
        private boolean permitAcquired = false;
        private boolean staticStructureInitialized = false;
        private boolean writerOpen = false;

        // Static structure reader for centroid-to-page mapping
        private VCTreeStaticStructureReader staticStructureReader;
        private Map<Integer, Integer> centroidToPageIdMap; // centroidId -> leaf page ID
        private Map<Integer, Integer> centroidToMetadataPageIdMap; // centroidId -> metadata page ID

        // LSM index components
        private ILSMIndex lsmIndex;
        private ILSMIndexOperationContext opCtx;
        private ILSMIndexAccessor accessor;
        private LSMIndexDiskComponentBulkLoader bulkLoader;

        // Data loading state
        private boolean dataLoadingStarted = false;
        private int totalTuplesLoaded = 0;
        private FrameTupleReference tuple = new FrameTupleReference();
        private FrameTupleAccessor fta;

        // Evaluators for extracting tuple fields
        private IScalarEvaluator centroidIdEval;
        private IScalarEvaluator dataTupleEval;
        private IPointable centroidIdVal;
        private IPointable dataTupleVal;

        public VCTreeDataBulkLoaderNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
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
            System.err.println("=== VCTreeDataBulkLoader OPENING ===");
            try {
                // Initialize evaluators for tuple processing (safe to do in open)
                initializeEvaluators();
                writer.open();
                writerOpen = true;
                System.err.println("VCTreeDataBulkLoader opened successfully (static structure reading deferred)");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeDataBulkLoader: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initializes the static structure reader.
         */
        private void initializeStaticStructureReader() throws HyracksDataException {
            try {
                IIOManager ioManager = ctx.getIoManager();
                String indexPath = getIndexPath();

                staticStructureReader = new VCTreeStaticStructureReader(ioManager, indexPath);
                System.err.println("Static structure reader initialized with indexPath: " + indexPath);

            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Reads static structure and creates centroid-to-page mapping.
         */
        private void readStaticStructureAndCreateMapping() throws HyracksDataException {
            System.err.println("=== READING STATIC STRUCTURE AND CREATING MAPPING ===");

            try {
                // Read static structure from binary file
                staticStructureReader.readStaticStructure();

                // Get structure parameters
                Map<String, Object> structureParams = staticStructureReader.getStructureParameters();
                int numLevels = (Integer) structureParams.get("numLevels");
                @SuppressWarnings("unchecked")
                List<Integer> clustersPerLevel = (List<Integer>) structureParams.get("clustersPerLevel");
                @SuppressWarnings("unchecked")
                List<List<Integer>> centroidsPerCluster =
                        (List<List<Integer>>) structureParams.get("centroidsPerCluster");

                // Create centroid-to-page mapping
                createCentroidToPageMapping(numLevels, clustersPerLevel, centroidsPerCluster);

                System.err.println("Created centroid-to-page mapping with " + centroidToPageIdMap.size() + " entries");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to read static structure and create mapping: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Creates mapping from centroid ID to leaf page ID.
         */
        private void createCentroidToPageMapping(int numLevels, List<Integer> clustersPerLevel,
                List<List<Integer>> centroidsPerCluster) {
            centroidToPageIdMap = new HashMap<>();
            centroidToMetadataPageIdMap = new HashMap<>();

            // Calculate page offsets for each level
            int[] totalPagesUpToLevel = new int[numLevels + 1];
            totalPagesUpToLevel[0] = 0;
            for (int level = 0; level < numLevels; level++) {
                totalPagesUpToLevel[level + 1] = totalPagesUpToLevel[level] + clustersPerLevel.get(level);
            }

            // Find leaf level (highest level)
            int leafLevel = numLevels - 1;
            int leafPageStart = totalPagesUpToLevel[leafLevel];

            // Create mapping for leaf centroids
            int centroidId = 0;
            for (int level = 0; level < numLevels; level++) {
                for (int cluster = 0; cluster < clustersPerLevel.get(level); cluster++) {
                    int centroidsInCluster = centroidsPerCluster.get(level).get(cluster);

                    for (int centroidInCluster = 0; centroidInCluster < centroidsInCluster; centroidInCluster++) {
                        if (level == leafLevel) {
                            // This is a leaf centroid
                            int leafPageId = leafPageStart + cluster;
                            centroidToPageIdMap.put(centroidId, leafPageId);

                            // For now, use a placeholder metadata page ID
                            // In real implementation, this would be the actual metadata page ID
                            int metadataPageId = leafPageId + 1000; // Placeholder offset
                            centroidToMetadataPageIdMap.put(centroidId, metadataPageId);

                            System.err.println("Mapped centroid " + centroidId + " to leaf page " + leafPageId
                                    + " (metadata page " + metadataPageId + ")");
                        }
                        centroidId++;
                    }
                }
            }

            System.err.println("Created mapping for " + centroidToPageIdMap.size() + " leaf centroids");
        }

        /**
         * Initializes LSM index and bulk loader.
         */
        private void initializeLSMIndexAndBulkLoader() throws HyracksDataException {
            try {
                // Get LSM index
                IIndexDataflowHelper indexHelper =
                        indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                indexHelper.open();
                lsmIndex = (ILSMIndex) indexHelper.getIndexInstance();

                // Create accessor with proper parameters
                IIndexAccessParameters iap = new IndexAccessParameters(null, NoOpOperationCallback.INSTANCE);
                accessor = lsmIndex.createAccessor(iap);

                // Create bulk loader
                Map<String, Object> parameters = new HashMap<>();
                parameters.put("numLevels", staticStructureReader.getNumLevels());
                parameters.put("clustersPerLevel", staticStructureReader.getClustersPerLevel());
                parameters.put("centroidsPerCluster", staticStructureReader.getCentroidsPerCluster());
                parameters.put("maxEntriesPerPage", maxEntriesPerPage);

                bulkLoader = (LSMIndexDiskComponentBulkLoader) lsmIndex.createBulkLoader(fillFactor, false, 0, false,
                        parameters);

                System.err.println("LSM index and bulk loader initialized successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize LSM index and bulk loader: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initializes evaluators for tuple processing.
         */
        private void initializeEvaluators() throws HyracksDataException {
            try {
                EvaluatorContext evalCtx = new EvaluatorContext(ctx);
                centroidIdEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(evalCtx);
                dataTupleEval = new ColumnAccessEvalFactory(1).createScalarEvaluator(evalCtx);

                centroidIdVal = new VoidPointable();
                dataTupleVal = new VoidPointable();

                System.err.println("Evaluators initialized for tuple processing");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize evaluators: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            // Wait for permit on the first frame
            if (!permitAcquired) {
                System.err.println("=== VCTreeDataBulkLoader: First frame received, waiting for permit ===");
                try {
                    // Wait for permit state to be created by Branch 1
                    IterationPermitState permitState = null;
                    int maxRetries = 100; // 10 seconds timeout
                    int retryCount = 0;

                    while (permitState == null && retryCount < maxRetries) {
                        permitState =
                                (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));

                        if (permitState == null) {
                            // Log only every 10 attempts to reduce noise
                            if (retryCount % 10 == 0) {
                                System.err.println("Waiting for permit state... (attempt " + (retryCount + 1) + "/"
                                        + maxRetries + ")");
                            }
                            Thread.sleep(100); // Wait 100ms before retrying
                            retryCount++;
                        }
                    }

                    if (permitState == null) {
                        throw new HyracksDataException("Permit state not created by Branch 1 after timeout for UUID: "
                                + permitUUID + ", partition: " + partition);
                    }

                    System.err.println("Found permit state, waiting for Branch 1 completion...");
                    permitState.getPermit().acquire(); // Block until Branch 1 completes
                    System.err.println("✅ Permit acquired - Branch 1 completed, starting Branch 2 data loading");
                    permitAcquired = true;

                } catch (InterruptedException e) {
                    System.err.println("ERROR: Interrupted while waiting for permit: " + e.getMessage());
                    throw HyracksDataException.create(e);
                } catch (Exception e) {
                    System.err.println("ERROR: Failed to acquire permit: " + e.getMessage());
                    throw HyracksDataException.create(e);
                }
            }

            // Simple pass-through operation - just forward data to output
            System.err.println("=== VCTreeDataBulkLoader: Pass-through mode - forwarding data to sink ===");

            // Pass through the frame to output without any processing
            if (writerOpen) {
                writer.nextFrame(buffer);
            }
        }

        /**
         * Process sorted tuples and load them directly to leaf pages.
         */
        private void processSortedTuple(ITupleReference tuple) throws HyracksDataException {
            try {
                // Extract centroid ID and data tuple
                FrameTupleReference frameTuple = (FrameTupleReference) tuple;
                centroidIdEval.evaluate(frameTuple, centroidIdVal);
                dataTupleEval.evaluate(frameTuple, dataTupleVal);

                int centroidId = AInt32SerializerDeserializer.getInt(centroidIdVal.getByteArray(),
                        centroidIdVal.getStartOffset() + 1);

                // Get leaf page ID for this centroid
                Integer leafPageId = centroidToPageIdMap.get(centroidId);
                if (leafPageId == null) {
                    System.err.println("WARNING: No leaf page mapping found for centroid " + centroidId);
                    return;
                }

                // Load data tuple directly to leaf page
                loadDataTupleToLeafPage(dataTupleVal, leafPageId, centroidId);

                totalTuplesLoaded++;

                // Log progress every 5000 tuples to reduce noise
                if (totalTuplesLoaded % 5000 == 0) {
                    System.err.println("Loaded " + totalTuplesLoaded + " tuples to leaf pages");
                }

            } catch (Exception e) {
                System.err.println("ERROR: Failed to process sorted tuple: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Loads a data tuple using the bulk loader.
         */
        private void loadDataTupleToLeafPage(IPointable dataTupleVal, int leafPageId, int centroidId)
                throws HyracksDataException {
            try {
                // Create tuple reference from data
                ITupleReference dataTuple = createDataTupleReference(dataTupleVal);

                // Use bulk loader to add the tuple
                bulkLoader.add(dataTuple);

                System.err.println("Loaded tuple for centroid " + centroidId + " using bulk loader");

            } catch (Exception e) {
                System.err
                        .println("ERROR: Failed to load data tuple for centroid " + centroidId + ": " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Creates a tuple reference from the data pointable.
         */
        private ITupleReference createDataTupleReference(IPointable dataTupleVal) throws HyracksDataException {
            // For now, create a simple tuple reference
            // In real implementation, this would properly deserialize the data tuple
            return new FrameTupleReference();
        }

        /**
         * Gets the index path.
         */
        private String getIndexPath() throws HyracksDataException {
            try {
                IIndexDataflowHelper indexHelper =
                        indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                LocalResource resource = indexHelper.getResource();
                return resource.getPath();
            } catch (Exception e) {
                System.err.println("ERROR: Failed to get index path: " + e.getMessage());
                return null;
            }
        }

        @Override
        public void close() throws HyracksDataException {
            System.err.println("=== VCTreeDataBulkLoader CLOSING ===");
            if (writerOpen) {
                writer.close();
                writerOpen = false;
            }
            System.err.println("VCTreeDataBulkLoader completed successfully (pass-through mode)");
            System.err.println("Total tuples processed: " + totalTuplesLoaded);
        }

        @Override
        public void fail() throws HyracksDataException {
            System.err.println("=== VCTreeDataBulkLoader FAILING ===");
            if (writerOpen) {
                writer.fail();
                writerOpen = false;
            }
            System.err.println("Total tuples processed before failure: " + totalTuplesLoaded);
            System.err.println("=== VCTreeDataBulkLoader FAILED ===");
        }
    }
}
