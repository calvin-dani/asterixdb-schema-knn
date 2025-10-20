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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt32;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.NoMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.ThreadCountingTracker;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTreeBulkLoader;
import org.apache.hyracks.storage.am.lsm.vector.utils.LSMVCTreeUtils;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrameFactory;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrameFactory;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

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
    private final int vectorDimensions;
    private final int[] vectorFields;
    private final int[] filterFields;
    private final boolean durable;

    public VCTreeSortedDataBulkLoaderOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor inputRecordDescriptor, IIndexDataflowHelperFactory indexHelperFactory, float fillFactor,
            int vectorDimensions, int[] vectorFields, int[] filterFields, boolean durable) {
        super(spec, 1, 1); // Input arity 1, Output arity 1
        this.inputRecordDescriptor = inputRecordDescriptor;
        this.indexHelperFactory = indexHelperFactory;
        this.fillFactor = fillFactor;
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.filterFields = filterFields;
        this.durable = durable;
        this.outRecDescs[0] = inputRecordDescriptor; // Pass through same record descriptor
        System.err.println("VCTreeSortedDataBulkLoaderOperatorDescriptor created with LSMVCTree-specific parameters");
        System.err.println("  - fillFactor: " + fillFactor);
        System.err.println("  - vectorDimensions: " + vectorDimensions);
        System.err.println("  - vectorFields: " + Arrays.toString(vectorFields));
        System.err.println("  - filterFields: " + Arrays.toString(filterFields));
        System.err.println("  - durable: " + durable);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeSortedDataBulkLoaderNodePushable(ctx, partition, nPartitions, inputRecDesc, indexHelperFactory,
                fillFactor, vectorDimensions, vectorFields, filterFields, durable);
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

        // LSMVCTree components
        private final IIndexDataflowHelperFactory indexHelperFactory;
        private final float fillFactor;
        private final int vectorDimensions;
        private final int[] vectorFields;
        private final int[] filterFields;
        private final boolean durable;
        private LSMVCTree lsmvcTree;
        private LSMVCTreeBulkLoader bulkLoader;

        // Static structure navigation components
        private IBufferCache bufferCache;
        private int staticStructureFileId;
        private VectorClusteringInteriorFrameFactory interiorFrameFactory;
        private VectorClusteringLeafFrameFactory leafFrameFactory;

        // Centroid tracking state
        private int currentCentroidId = -1;
        private int tupleCountForCurrentCentroid = 0;
        private final Map<Integer, Integer> centroidTotalCounts = new HashMap<>();
        private int totalTuplesProcessed = 0;
        private boolean first;
        // Serializers for tuple field extraction
        //        @SuppressWarnings("rawtypes")
        //        private final ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, // Field 0: centroidId
        //                DoubleSerializerDeserializer.INSTANCE // Field 1: distance
        //        };

        public VCTreeSortedDataBulkLoaderNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc, IIndexDataflowHelperFactory indexHelperFactory, float fillFactor,
                int vectorDimensions, int[] vectorFields, int[] filterFields, boolean durable)
                throws HyracksDataException {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.inputRecDesc = inputRecDesc;
            this.indexHelperFactory = indexHelperFactory;
            this.fillFactor = fillFactor;
            this.vectorDimensions = vectorDimensions;
            this.vectorFields = vectorFields;
            this.filterFields = filterFields;
            this.durable = durable;
            this.fta = new FrameTupleAccessor(inputRecDesc);
            this.tuple = new FrameTupleReference();
            this.first = true;
        }

        @Override
        public void open() throws HyracksDataException {
            System.err.println("=== VCTreeSortedDataBulkLoader OPENING ===");
            System.err.println("Partition: " + partition + "/" + nPartitions);

            try {
                // Initialize LSMVCTree
                initializeLSMVCTree();

                // Initialize bulk loader first (needed for page copying)
                initializeBulkLoader();

                // Navigate existing static structure and copy pages
                navigateExistingStaticStructure();

                System.err.println("VCTreeSortedDataBulkLoader opened successfully");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeSortedDataBulkLoader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initialize LSMVCTree with proper configuration.
         * 
         * @throws HyracksDataException if initialization fails
         */
        private void initializeLSMVCTree() throws HyracksDataException {
            try {
                System.err.println("=== INITIALIZING LSMVCTree ===");

                // Create NCConfig
                //TODO : CALVIN DANI CHANGE
                NCConfig ncConfig = new NCConfig(null);

                // Get IOManager
                IIOManager ioManager = ctx.getJobletContext().getServiceContext().getIoManager();

                // Create virtual buffer caches
                List<IVirtualBufferCache> virtualBufferCaches = new ArrayList<>();
                IVirtualBufferCache virtualBufferCache = new VirtualBufferCache(new HeapBufferAllocator(), 512, 1000);
                virtualBufferCaches.add(virtualBufferCache);

                // Create file reference using proper IO device path
                // Use the same approach as other operators - get the index path from the dataflow helper
                FileReference file = getIndexFilePath();
                if (file == null) {
                    throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                            "Could not determine index file path for LSMVCTree");
                }

                // Get disk buffer cache
                IBufferCache diskBufferCache = ((org.apache.asterix.common.api.INcApplicationContext) ctx
                        .getJobletContext().getServiceContext().getApplicationContext()).getBufferCache();

                // Create field serializers for vector fields
                @SuppressWarnings("rawtypes")
                ISerializerDeserializer[] fieldSerdes =
                        new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, // centroidId
                                DoubleSerializerDeserializer.INSTANCE, // distance
                                DoubleArraySerializerDeserializer.INSTANCE, // vector
                                DoubleArraySerializerDeserializer.INSTANCE // primary key
                        };

                // Create type traits for vector fields
                ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
                IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[fieldSerdes.length];
                for (int i = 0; i < fieldSerdes.length; i++) {
                    if (fieldSerdes[i] instanceof UTF8StringSerializerDeserializer) {
                        cmpFactories[i] = UTF8StringBinaryComparatorFactory.INSTANCE;
                    } else {
                        // For vector field and other numeric fields, use integer comparator for now
                        cmpFactories[i] = IntegerBinaryComparatorFactory.INSTANCE;
                    }
                }

                // Create LSMVCTree using LSMVCTreeUtils (full version like in tests)
                lsmvcTree = LSMVCTreeUtils.createLSMTree(ncConfig, ioManager, virtualBufferCaches, file,
                        diskBufferCache, typeTraits, cmpFactories, -1, new NoMergePolicy(), new ThreadCountingTracker(),
                        SynchronousSchedulerProvider.INSTANCE.getIoScheduler(null),
                        NoOpIOOperationCallbackFactory.INSTANCE, NoOpPageWriteCallbackFactory.INSTANCE, false,
                        vectorDimensions, vectorFields, filterFields, null, null, null, durable,
                        AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE, false, inputRecDesc);

                System.err.println("✅ LSMVCTree initialized successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize LSMVCTree: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Navigate existing static structure file using level order traversal and copy pages.
         * The static structure was already created by VCTreeStaticStructureCreatorOperatorDescriptor.
         * 
         * @throws HyracksDataException if navigation fails
         */
        private void navigateExistingStaticStructure() throws HyracksDataException {
            try {
                System.err.println("=== NAVIGATING EXISTING STATIC STRUCTURE WITH LEVEL ORDER TRAVERSAL ===");

                // Get the static structure file path
                FileReference indexPathRef = getIndexFilePath();
                if (indexPathRef == null) {
                    throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                            "Could not determine index path for static structure navigation");
                }

                // Create static structure file path
                FileReference staticStructureFile = indexPathRef.getChild(".static_structure_vctree");
                System.err.println("Static structure file path: " + staticStructureFile);

                // Check if static structure file exists
                IIOManager ioManager = ctx.getJobletContext().getServiceContext().getIoManager();
                if (!ioManager.exists(staticStructureFile)) {
                    throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.FILE_DOES_NOT_EXIST,
                            "Static structure file does not exist: " + staticStructureFile);
                }

                // Initialize navigation components
                initializeNavigationComponents(staticStructureFile);

                // Perform level order traversal and page copying
                copyPagesInLevelOrder();

                System.err.println("✅ Static structure navigation and page copying completed successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to navigate existing static structure: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Copy all pages in level-order using BFS traversal.
         * This follows the same pattern as VCTreeStaticStructureNavigator.copyPagesInLevelOrder().
         * 
         * @throws HyracksDataException if any error occurs during traversal
         */
        private void copyPagesInLevelOrder() throws HyracksDataException {
            try {
                System.err.println("=== STARTING LEVEL ORDER PAGE COPYING ===");
                Queue<Integer> pageQueue = new LinkedList<>();
                pageQueue.offer(1); // Start with root page (page ID 0)

                int pagesProcessed = 0;

                for (int pageId = 1; pageId <= 8; pageId++) {
                    ICachedPage sourcePage =
                            bufferCache.pin(BufferedFileHandle.getDiskPageId(staticStructureFileId, pageId));
                    bulkLoader.copyPage(sourcePage);
                    bufferCache.unpin(sourcePage);
                }

                //                while (!pageQueue.isEmpty()) {
                //                    int currentPageId = pageQueue.poll();
                //
                //                    // Pin and copy the current page
                //                    ICachedPage sourcePage =
                //                    try {
                //                        sourcePage.acquireReadLatch();
                //
                //                        // Copy the page using bulk loader
                //                        if (bulkLoader != null) {
                //                            bulkLoader.copyPage(sourcePage);
                //                        }
                //                        pagesProcessed++;
                //
                //                        System.err.println("Copied page " + currentPageId + " (total processed: " + pagesProcessed + ")");
                //
                //                        // Check if this is an interior page (has children)
                //                        org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrame interiorFrame =
                //                                (org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrame) interiorFrameFactory.createFrame();
                //                        interiorFrame.setPage(sourcePage);
                //
                //                        if (!interiorFrame.isLeaf()) {
                //                            // Add all child pages to queue
                //                            int tupleCount = interiorFrame.getTupleCount();
                //                            for (int i = 0; i < tupleCount; i++) {
                //                                int childPageId = interiorFrame.getChildPageId(i);
                //                                if (childPageId > 0) {
                //                                    pageQueue.offer(childPageId);
                //                                    System.err.println("Added child page " + childPageId + " to queue");
                //                                }
                //                            }
                //                        }
                //
                //                    } finally {
                //                        sourcePage.releaseReadLatch();
                //                        bufferCache.unpin(sourcePage);
                //                    }
                //                }

                System.err.println("✅ Level-order page copying completed. Total pages processed: " + pagesProcessed);

            } catch (Exception e) {
                System.err.println("ERROR: Failed to copy pages in level order: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initialize navigation components for static structure traversal.
         * 
         * @param staticStructureFile File reference to the static structure file
         * @throws HyracksDataException if initialization fails
         */
        private void initializeNavigationComponents(FileReference staticStructureFile) throws HyracksDataException {
            try {
                System.err.println("=== INITIALIZING NAVIGATION COMPONENTS ===");

                // Get buffer cache
                bufferCache = ((org.apache.asterix.common.api.INcApplicationContext) ctx.getJobletContext()
                        .getServiceContext().getApplicationContext()).getBufferCache();

                // Open static structure file
                staticStructureFileId = bufferCache.openFile(staticStructureFile);

                // Configure VCTree frame factories
                configureVCTreeFrameFactories();

                System.err.println("✅ Navigation components initialized successfully");
                System.err.println("  - Static structure file ID: " + staticStructureFileId);
                System.err.println("  - Buffer cache: " + (bufferCache != null ? "OK" : "NULL"));

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize navigation components: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Configure VCTree frame factories for static structure navigation.
         * 
         * @throws HyracksDataException if configuration fails
         */
        private void configureVCTreeFrameFactories() throws HyracksDataException {
            try {
                System.err.println("=== CONFIGURING VCTREE FRAME FACTORIES ===");

                // Initialize frame factories
                interiorFrameFactory = new VectorClusteringInteriorFrameFactory(vectorDimensions);
                leafFrameFactory = new VectorClusteringLeafFrameFactory(null, vectorDimensions);

                System.err.println("✅ VCTree frame factories configured successfully");
                System.err.println("  - Interior frame factory: " + (interiorFrameFactory != null ? "OK" : "NULL"));
                System.err.println("  - Leaf frame factory: " + (leafFrameFactory != null ? "OK" : "NULL"));

            } catch (Exception e) {
                System.err.println("ERROR: Failed to configure VCTree frame factories: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initialize bulk loader for data loading.
         * 
         * @throws HyracksDataException if bulk loader initialization fails
         */
        private void initializeBulkLoader() throws HyracksDataException {
            try {
                System.err.println("=== INITIALIZING BULK LOADER ===");

                // Get metadata from static structure
                int numLeafCentroids = 10; // Default value
                int firstLeafCentroidId = 7; // Default value

                // Create field serializers for data loading
                @SuppressWarnings("rawtypes")
                ISerializerDeserializer[] dataFrameSerdes =
                        new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE, // distance
                                DoubleSerializerDeserializer.INSTANCE, // cosine similarity
                                DoubleArraySerializerDeserializer.INSTANCE, // vector
                                DoubleArraySerializerDeserializer.INSTANCE // primary key
                        };

                // Create LSMVCTreeBulkLoader
                bulkLoader = lsmvcTree.createBulkLoader(numLeafCentroids, firstLeafCentroidId, dataFrameSerdes);

                System.err.println("✅ Bulk loader initialized successfully");

            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize bulk loader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Get the index file path for LSMVCTree creation.
         * This follows the same approach as other operators in the system.
         * 
         * @return FileReference to the index directory
         */
        private FileReference getIndexFilePath() {
            try {
                // Use the same approach as VCTreeStaticStructureCreatorOperatorDescriptor
                IIndexDataflowHelper indexHelper =
                        indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                //                indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                //                indexHelper.open();
                //                lsmIndex = (ILSMIndex) indexHelper.getIndexInstance();
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

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            fta.reset(buffer);

            // Process each tuple in the frame
            for (int i = 0; i < fta.getTupleCount(); i++) {
                tuple.reset(fta, i);
                processSortedTuple(tuple);
            }

            // Pass through the sorted frame to output (sink operator)
            if (writer != null) {
                writer.nextFrame(buffer);
            }
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
                    if (!first) {
                        bulkLoader.next();
                    }
                    first = false;
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
                        System.err.println("ERROR: Failed to add tuple to LSM Index bulk loader: " + e.getMessage());
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
                Object[] fieldValues = TupleUtils.deserializeTuple(tuple, inputRecDesc.getFields());
                return ((AInt32) fieldValues[1]).getIntegerValue();
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
                Object[] fieldValues = TupleUtils.deserializeTuple(tuple, inputRecDesc.getFields());
                return ((ADouble) fieldValues[0]).getDoubleValue();
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
                // Finalize LSMVCTreeBulkLoader
                if (bulkLoader != null) {
                    System.err.println("=== FINALIZING LSMVCTreeBulkLoader ===");
                    bulkLoader.end();
                    System.err.println("✅ LSMVCTreeBulkLoader finalized successfully");
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
