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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTreeDiskComponent;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.am.vector.impls.VCTreeBulkLoder;
import org.apache.hyracks.storage.am.vector.impls.VCTreeStaticStructureNavigator;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;

/**
 * Bulk loader operator that processes sorted tuples from ExternalSortOperatorDescriptor.
 * 
 * This operator:
 * 1. Receives sorted tuples with format [centroidId, distance, ...original fields...]
 * 2. Sorts by centroidId first, then distance (handled by ExternalSortOperatorDescriptor)
 * 3. Prints the first 5 values for each centroid ID
 * 4. Passes through all data to the sink operator
 * 
 * The sorting is configured in ExternalSortOperatorDescriptor with sortFields = {0, 1}
 * where field 0 is centroidId and field 1 is distance.
 */
public class VCTreeSortedDataBulkLoaderOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    @SuppressWarnings("unused")
    private final RecordDescriptor inputRecordDescriptor;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final float fillFactor;

    public VCTreeSortedDataBulkLoaderOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor inputRecordDescriptor, IIndexDataflowHelperFactory indexHelperFactory, float fillFactor) {
        super(spec, 1, 1); // Input arity 1, Output arity 1
        this.inputRecordDescriptor = inputRecordDescriptor;
        this.indexHelperFactory = indexHelperFactory;
        this.fillFactor = fillFactor;
        this.outRecDescs[0] = inputRecordDescriptor; // Pass through same record descriptor
        System.err
                .println("VCTreeSortedDataBulkLoaderOperatorDescriptor created with indexHelperFactory and fillFactor: "
                        + fillFactor);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeSortedDataBulkLoaderNodePushable(ctx, partition, nPartitions, inputRecDesc, indexHelperFactory,
                fillFactor);
    }

    private class VCTreeSortedDataBulkLoaderNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        @SuppressWarnings("unused")
        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;
        @SuppressWarnings("unused")
        private final RecordDescriptor inputRecDesc;
        private final FrameTupleAccessor fta;
        private final FrameTupleReference tuple;
        private IFrameWriter writer;
        @SuppressWarnings("unused")
        private boolean writerOpen = false;

        // LSM Index bulk loader components
        private final IIndexDataflowHelperFactory indexHelperFactory;
        private final float fillFactor;
        private IIndexDataflowHelper indexHelper;
        private ILSMIndex lsmIndex;
        private IIndexBulkLoader bulkLoader;

        // Centroid tracking state
        private int currentCentroidId = -1;
        private int tupleCountForCurrentCentroid = 0;
        private final Map<Integer, Integer> centroidTotalCounts = new HashMap<>();
        private int totalTuplesProcessed = 0;

        // Serializers for tuple field extraction
        @SuppressWarnings("rawtypes")
        private final ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, // Field 0: centroidId
                DoubleSerializerDeserializer.INSTANCE // Field 1: distance
        };

        // Navigation components for level-order page copying
        private IBufferCache bufferCache;
        private int staticStructureFileId;
        private ITreeIndexFrameFactory interiorFrameFactory;
        private ITreeIndexFrameFactory leafFrameFactory;

        public VCTreeSortedDataBulkLoaderNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc, IIndexDataflowHelperFactory indexHelperFactory, float fillFactor)
                throws HyracksDataException {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.inputRecDesc = inputRecDesc;
            this.indexHelperFactory = indexHelperFactory;
            this.fillFactor = fillFactor;
            this.fta = new FrameTupleAccessor(inputRecDesc);
            this.tuple = new FrameTupleReference();
        }

        @Override
        public void open() throws HyracksDataException {
            System.err.println("=== VCTreeSortedDataBulkLoader OPENING ===");
            System.err.println("Partition: " + partition + "/" + nPartitions);

            try {
                // Initialize LSM index bulk loader
                initializeLSMIndexBulkLoader();

                // Initialize navigation components for level-order page copying
                initializeNavigationComponents();

                // Copy pages in level order using navigator
//                copyPagesInLevelOrder();

                System.err.println("VCTreeSortedDataBulkLoader opened successfully with level-order page copying");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeSortedDataBulkLoader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initialize LSM index bulk loader.
         * 
         * @throws HyracksDataException if initialization fails
         */
        private void initializeLSMIndexBulkLoader() throws HyracksDataException {
            try {
                System.err.println("=== INITIALIZING LSM INDEX BULK LOADER ===");

                // Create index helper
                indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                indexHelper.open();

                // Get LSM index instance
                lsmIndex = (ILSMIndex) indexHelper.getIndexInstance();
                System.err.println("LSM Index instance obtained: " + lsmIndex);

                // Create bulk loader with LSM parameters
                Map<String, Object> parameters = new HashMap<>();
                parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID, LSMComponentId.DEFAULT_COMPONENT_ID);

                // Add hierarchical structure parameters for VCTree
                parameters.put("numLevels", 3);
                parameters.put("clustersPerLevel", Arrays.asList(1, 2, 5));
                parameters.put("centroidsPerCluster",
                        Arrays.asList(Arrays.asList(2), Arrays.asList(3, 2), Arrays.asList(2, 1, 2, 3, 2)));
                parameters.put("maxEntriesPerPage", 100);

                bulkLoader = (LSMIndexDiskComponentBulkLoader) lsmIndex.createBulkLoader(fillFactor, false, 0, false,
                        parameters);
                System.err.println("LSM Bulk Loader created with fillFactor: " + fillFactor);

                System.err.println("✅ LSM Index bulk loader initialized successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize LSM Index bulk loader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initialize navigation components for static structure access.
         * 
         * @throws HyracksDataException if navigation initialization fails
         */
        private void initializeNavigationComponents() throws HyracksDataException {
            try {
                System.err.println("=== INITIALIZING NAVIGATION COMPONENTS ===");

                // Set up buffer cache access
                this.bufferCache = ((org.apache.asterix.common.api.INcApplicationContext) ctx.getJobletContext()
                        .getServiceContext().getApplicationContext()).getBufferCache();

                // Open static structure file
                this.staticStructureFileId = openStaticStructureFile();

                // Configure VCTree frame factories manually (not dependent on LSMVCTree)
                configureVCTreeFrameFactories();

                System.err.println("✅ Navigation components initialized successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize navigation components: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Configure VCTree frame factories manually (not dependent on LSMVCTree).
         * 
         * @throws HyracksDataException if frame factory configuration fails
         */
        private void configureVCTreeFrameFactories() throws HyracksDataException {
            try {
                System.err.println("=== CONFIGURING VCTREE FRAME FACTORIES MANUALLY ===");

                // Create tuple writers with proper type traits for VCTree
                // Tuple format: [centroidId (int), embedding (float[]), childPageId (int)]
                ITypeTraits[] typeTraits = new ITypeTraits[3];
                typeTraits[0] = IntegerPointable.TYPE_TRAITS; // centroidId
                typeTraits[1] = VarLengthTypeTrait.INSTANCE; // embedding (variable length)
                typeTraits[2] = IntegerPointable.TYPE_TRAITS; // childPageId

                // Create frame factories with vector dimensions (using default 256)
                this.interiorFrameFactory = new VectorClusteringInteriorFrameFactory(256);
                this.leafFrameFactory = new VectorClusteringLeafFrameFactory(null, 256);

                System.err.println("✅ VCTree frame factories configured successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to configure VCTree frame factories: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Open static structure file and return file ID.
         * This follows the same approach as VCTreeStaticStructureCreatorOperatorDescriptor.
         * 
         * @return file ID for the static structure file
         * @throws HyracksDataException if file opening fails
         */
        private int openStaticStructureFile() throws HyracksDataException {
            try {
                System.err.println("=== OPENING STATIC STRUCTURE FILE ===");

                // Get index path (same approach as VCTreeStaticStructureCreatorOperatorDescriptor)
                FileReference indexPathRef = getIndexFilePath();
                if (indexPathRef == null) {
                    throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                            "Could not determine index path");
                }
                System.err.println("Index path: " + indexPathRef);

                // Create static structure file path
                FileReference staticStructureFile = indexPathRef.getChild(".static_structure_vctree");
                System.err.println("Static structure file path: " + staticStructureFile);

                // Open the static structure file
                int fileId;
                try {
                    // Check if file exists in the file system
                    IIOManager ioManager = ctx.getJobletContext().getServiceContext().getIoManager();
                    if (ioManager.exists(staticStructureFile)) {
                        System.err.println("Static structure file exists, opening it...");
                        fileId = bufferCache.openFile(staticStructureFile);
                    } else {
                        throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                                "Static structure file does not exist: " + staticStructureFile);
                    }
                } catch (Exception e) {
                    throw HyracksDataException.create(e);
                }

                System.err.println("Static structure file opened with ID: " + fileId);
                return fileId;

            } catch (Exception e) {
                System.err.println("ERROR: Failed to open static structure file: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Get the index file path for accessing the static structure file.
         * This follows the same approach as VCTreeStaticStructureCreatorOperatorDescriptor.
         * 
         * @return FileReference to the index directory
         */
        private FileReference getIndexFilePath() {
            try {
                IIndexDataflowHelper indexHelper =
                        indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                LocalResource resource = indexHelper.getResource();
                String resourcePath = resource.getPath();

                IIOManager ioManager = ctx.getJobletContext().getServiceContext().getIoManager();
                return ioManager.resolve(resourcePath);

            } catch (Exception e) {
                System.err.println("ERROR: Failed to get index file path: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }

        /**
         * Get VCTreeBulkLoder for page copying operations.
         * 
         * @return VCTreeBulkLoder instance for page copying
         * @throws HyracksDataException if VCTreeBulkLoder retrieval fails
         */
        private VCTreeBulkLoder getVCTreeBulkLoaderForPageCopying() throws HyracksDataException {
            try {
                System.err.println("=== GETTING VCTREEBULKLODER FOR PAGE COPYING ===");

                if (lsmIndex instanceof LSMVCTree) {
                    LSMVCTree lsmVCTree = (LSMVCTree) lsmIndex;
                    List<ILSMDiskComponent> components = lsmVCTree.getDiskComponents();

                    if (components.size() > 0 && components.get(0) instanceof LSMVCTreeDiskComponent) {
                        LSMVCTreeDiskComponent vcDiskComponent = (LSMVCTreeDiskComponent) components.get(0);
                        VectorClusteringTree vectorTree = vcDiskComponent.getIndex();

                        // Create VCTreeBulkLoder directly from the VectorClusteringTree
                        VCTreeBulkLoder vcBulkLoader = (VCTreeBulkLoder) vectorTree.createBulkLoader(fillFactor, false, // verifyInput
                                0L, // numElementsHint
                                false, // checkIfEmptyIndex
                                NoOpPageWriteCallback.INSTANCE // callback
                        );

                        System.err.println("✅ VCTreeBulkLoder obtained successfully for page copying");
                        return vcBulkLoader;
                    }
                }

                throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                        "Failed to get VCTreeBulkLoder - LSM index is not LSMVCTree or component is not LSMVCTreeDiskComponent");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to get VCTreeBulkLoder for page copying: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Copy pages in level order using VCTreeStaticStructureNavigator.
         * 
         * @throws HyracksDataException if page copying fails
         */
        private void copyPagesInLevelOrder() throws HyracksDataException {
            try {
                System.err.println("=== COPYING PAGES IN LEVEL ORDER ===");

                // Get VCTreeBulkLoder for page copying
                VCTreeBulkLoder vcBulkLoader = getVCTreeBulkLoaderForPageCopying();

                // Create navigator and copy pages in level order
                VCTreeStaticStructureNavigator navigator = new VCTreeStaticStructureNavigator(bufferCache,
                        staticStructureFileId, interiorFrameFactory, leafFrameFactory);

                // Copy pages in level order
                navigator.copyPagesInLevelOrder(vcBulkLoader);

                System.err.println("✅ Level-order page copying completed successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to copy pages in level order: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            fta.reset(buffer);

            // Process each tuple in the frame
            for (int i = 0; i < fta.getTupleCount(); i++) {
                tuple.reset(fta, i);
//                processSortedTuple(tuple);
            }

            // Pass through the sorted frame to output (sink operator)
//            if (writer != null) {
//                writer.nextFrame(buffer);
//            }
        }

        /**
         * Process sorted tuples from ExternalSortOperatorDescriptor.
         * These tuples are in format: [centroidId, distance, ...original fields...]
         * and are sorted by centroidId first, then distance.
         */
        private void processSortedTuple(ITupleReference tuple) throws HyracksDataException {
            try {
                // Extract centroidId and distance from tuple
                int centroidId = extractCentroidId(tuple);
                double distance = extractDistance(tuple);

                // Check if we've moved to a new centroid
                if (currentCentroidId != centroidId) {
                    // New centroid - log previous centroid summary if any
                    if (currentCentroidId != -1) {
                        logCentroidSummary(currentCentroidId, tupleCountForCurrentCentroid);
                    }
                    // TODO CALVIN DANI: to call bulkload.nextcentroid()
                    // Reset for new centroid
                    currentCentroidId = centroidId;
                    tupleCountForCurrentCentroid = 0;

                    System.err.println("=== Processing Centroid ID: " + centroidId + " ===");
                }

                // Increment counters
                tupleCountForCurrentCentroid++;
                totalTuplesProcessed++;
                centroidTotalCounts.put(centroidId, centroidTotalCounts.getOrDefault(centroidId, 0) + 1);

                // Print first 5 values for this centroid
                if (tupleCountForCurrentCentroid <= 5) {
                    printCentroidData(centroidId, distance, tuple, tupleCountForCurrentCentroid);
                }

                // Add tuple to LSM Index bulk loader
                if (bulkLoader != null) {
                    try {
                        bulkLoader.add(tuple);
                    } catch (Exception e) {
                        System.err.println(
                                "ERROR: Failed to add tuple to LSM Index bulk loader: " + e.getMessage());
                        e.printStackTrace();
                        // Continue processing other tuples
                    }
                }

            } catch (Exception e) {
                System.err.println("ERROR: Failed to process sorted tuple: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Extract centroidId from field 0 of the tuple.
         */
        private int extractCentroidId(ITupleReference tuple) throws HyracksDataException {
            try {
                // Use TupleUtils to deserialize the integer field
                Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);
                return (Integer) fieldValues[0];
            } catch (Exception e) {
                System.err.println("ERROR: Failed to extract centroidId from tuple: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Extract distance from field 1 of the tuple.
         */
        private double extractDistance(ITupleReference tuple) throws HyracksDataException {
            try {
                // Use TupleUtils to deserialize the double field
                Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);
                return (Double) fieldValues[1];
            } catch (Exception e) {
                System.err.println("ERROR: Failed to extract distance from tuple: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Print the first 5 values for each centroid ID.
         */
        private void printCentroidData(int centroidId, double distance, ITupleReference tuple, int tupleIndex) {
            System.err.println("Centroid " + centroidId + ", Tuple " + tupleIndex + ": distance="
                    + String.format("%.6f", distance) + ", totalFields=" + tuple.getFieldCount());

            // Print additional tuple information for debugging
            if (tupleIndex <= 3) { // Only for first 3 tuples to avoid spam
                System.err.println("  Tuple details: " + tuple.toString());
            }
        }

        /**
         * Log summary for a completed centroid.
         */
        private void logCentroidSummary(int centroidId, int tupleCount) {
            System.err.println("=== Centroid " + centroidId + " Summary ===");
            System.err.println("Total tuples processed: " + tupleCount);
            System.err.println("First 5 values printed above");
            System.err.println("================================");
        }

        @Override
        public void fail() throws HyracksDataException {
            if (writer != null) {
                writer.fail();
            }
        }

        @Override
        public void flush() throws HyracksDataException {
            if (writer != null) {
                writer.flush();
            }
        }

        @Override
        public void close() throws HyracksDataException {
            try {
                // Finalize LSM Index bulk loader
                if (bulkLoader != null) {
                    System.err.println("=== FINALIZING LSM INDEX BULK LOADER ===");
                    bulkLoader.end();
                    System.err.println("✅ LSM Index bulk loader finalized successfully");
                }

                // Close index helper
                if (indexHelper != null) {
                    indexHelper.close();
                    System.err.println("✅ Index helper closed successfully");
                }

                // Log final summary for current centroid if any
                if (currentCentroidId != -1) {
                    logCentroidSummary(currentCentroidId, tupleCountForCurrentCentroid);
                }

                // Log overall processing summary
                System.err.println("=== VCTreeSortedDataBulkLoader FINAL SUMMARY ===");
                System.err.println("Total tuples processed: " + totalTuplesProcessed);
                System.err.println("Total centroids processed: " + centroidTotalCounts.size());
                System.err.println("Tuples per centroid:");
                for (Map.Entry<Integer, Integer> entry : centroidTotalCounts.entrySet()) {
                    System.err.println("  Centroid " + entry.getKey() + ": " + entry.getValue() + " tuples");
                }
                System.err.println("===============================================");

                if (writer != null) {
                    writer.close();
                }

            } catch (Exception e) {
                System.err.println("ERROR: Failed to close VCTreeSortedDataBulkLoader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }
    }
}
