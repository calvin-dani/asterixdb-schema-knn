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

package org.apache.hyracks.storage.am.vector.impls;

import static org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider.NEW;

import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringMetadataFrame;
import org.apache.hyracks.storage.am.vector.tuples.VectorClusteringTupleUtils;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.am.vector.utils.VCTreeNavigationUtils;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Vector Clustering Tree implementation for multi-level k-means vector index.
 *
 * This tree supports hierarchical vector clustering with specialized frame types:
 * - Interior frames: Store cluster
 * centroids and child page pointers
 * - Leaf frames: Store cluster centroids and metadata page pointers
 * - Metadata frames: Store max distances and data page pointers
 * - Data frames: Store distances, primary keys
 */
public class VectorClusteringTree extends AbstractTreeIndex {

    private static final Logger LOGGER = LogManager.getLogger();
    private final int vectorDimensions;
    private final ITreeIndexFrameFactory metadataFrameFactory;
    private final ITreeIndexFrameFactory dataFrameFactory;
    private final IVectorBinaryAccessorFactory vectorAccessorFactory;

    // Add missing frameTuple declaration
    private ITreeIndexTupleReference frameTuple;
    // TODO: leave only one flag
    private boolean isStaticStructureInitialized = true;

    private boolean initialized = false;

    public VectorClusteringTree(IBufferCache bufferCache, IPageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            ITreeIndexFrameFactory metadataFrameFactory, ITreeIndexFrameFactory dataFrameFactory,
            IBinaryComparatorFactory[] cmpFactories, int fieldCount, int vectorDimensions, FileReference file,
            org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory vectorAccessorFactory) {
        super(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
        this.vectorDimensions = vectorDimensions;
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
        this.vectorAccessorFactory = vectorAccessorFactory;
    }

    /**
     * Get the data frame factory for creating data frames.
     *
     * @return the data frame factory
     */
    public ITreeIndexFrameFactory getDataFrameFactory() {
        return dataFrameFactory;
    }

    /**
     * Get the metadata frame factory for creating metadata frames.
     *
     * @return the metadata frame factory
     */
    public ITreeIndexFrameFactory getMetadataFrameFactory() {
        return metadataFrameFactory;
    }

    /**
     * Create a tuple reference for this tree.
     *
     * @return a new SimpleTupleReference instance
     */
    public ITreeIndexTupleReference createTupleReference() {
        return new SimpleTupleReference();
    }

    @Override
    public ITreeIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return new VectorClusteringTreeAccessor(this, iap);
    }

    @Override
    public int getNumOfFilterFields() {
        return 0;
    }

    public IIndexBulkLoader createComponentBulkLoader(NoOpPageWriteCallback instance,
            ITreeIndexAccessor static_accessor, ISerializerDeserializer[] dataFrameSerdes) throws HyracksDataException {
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] dataFrameSerds;
        if (dataFrameSerdes != null && dataFrameSerdes.length > 0) {
            // Use provided serializers from RecordDescriptor
            dataFrameSerds = dataFrameSerdes;
        } else {
            // Fallback to hardcoded serializers for backward compatibility
            dataFrameSerds = new ISerializerDeserializer[4];
            dataFrameSerds[0] = DoubleSerializerDeserializer.INSTANCE; // distance
            dataFrameSerds[1] = IntegerSerializerDeserializer.INSTANCE; // cosine similarity
            dataFrameSerds[2] = DoubleArraySerializerDeserializer.INSTANCE; // vector
            dataFrameSerds[3] = IntegerSerializerDeserializer.INSTANCE; // primary key
        }
        return new VCTreeBulkLoader(0, instance, this, leafFrameFactory.createFrame(), dataFrameFactory.createFrame(),
                DefaultBufferCacheWriteContext.INSTANCE, dataFrameSerds, static_accessor);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
        //        return new VCTreeBulkLoader(fillFactor, verifyInput, callback, this);
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a bulk loader for building the static hierarchical clustering structure.
     * This is called during index creation to build the multi-level centroid tree.
     *
     * @param numLevels Number of levels in the hierarchy
     * @param clustersPerLevel List of cluster counts per level
     * @param centroidsPerCluster List of centroids per cluster per level
     * @param maxEntriesPerPage Maximum entries per page
     * @param callback Page write callback
     * @return IIndexBulkLoader for building static structure
     */
    public IIndexBulkLoader createStaticStructureBulkLoader(int numLevels, List<Integer> clustersPerLevel,
            List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage, IPageWriteCallback callback)
            throws HyracksDataException {
        return new VCTreeStaticStructureBuilder(callback, this, leafFrameFactory.createFrame(),
                dataFrameFactory.createFrame(), numLevels, clustersPerLevel, centroidsPerCluster, maxEntriesPerPage);
    }

    public IIndexBulkLoader createFlushLoader(float fillFactor, IPageWriteCallback callback)
            throws HyracksDataException {
        return new VectorClusteringTreeFlushLoader(fillFactor, this, callback);
    }

    /**
     * Insert a vector into the clustering tree. The vector tuple contains: <vector_embedding, primary_key,
     * [additional_fields]>
     */
    private void insertVector(ITupleReference tuple, VectorClusteringOpContext ctx) throws HyracksDataException {
        ClusterAccessResult accessResult = findClusterAndPrepareAccess(tuple, ctx, true);

        try {
            // Extract vector for distance calculations
            double[] vector = extractVectorFromTuple(tuple);

            // Calculate distance to cluster centroid and get centroid ID
            double[] centroidDouble = accessResult.clusterResult.centroid;
            double distance = VectorUtils.calculateEuclideanDistance(vector, centroidDouble);
            int centroidId = accessResult.clusterResult.centroidId;

            // Insert into appropriate data page based on distance
            insertIntoDataPages(accessResult.metadataPageId, vector, distance, centroidId, tuple, ctx);

        } finally {
            accessResult.leafPage.releaseWriteLatch(true);
            bufferCache.unpin(accessResult.leafPage);
        }
    }

    /**
     * Insert vector data into data pages via metadata pages. This method traverses through all linked metadata pages to
     * find the appropriate data page.
     */
    private void insertIntoDataPages(long metadataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VectorClusteringOpContext ctx) throws HyracksDataException {

        // Traverse through all linked metadata pages to find the appropriate data page
        long currentMetadataPageId = metadataPageId;

        while (currentMetadataPageId != -1) {
            ICachedPage metadataPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) currentMetadataPageId));
            ctx.setMetadataPageId(currentMetadataPageId);
            try {
                metadataPage.acquireWriteLatch();
                ctx.getMetadataFrame().setPage(metadataPage);

                LOGGER.log(Level.DEBUG, "Searching metadata page {} for distance {}", currentMetadataPageId,
                        metadataPage);

                // Try to find appropriate data page in current metadata page based on distance
                long targetDataPageId = findDataPageInMetadataPage(ctx.getMetadataFrame(), distance);

                if (targetDataPageId != -1) {
                    // Found appropriate data page - try to insert
                    LOGGER.log(Level.DEBUG, "Found target data page {} in metadata page {}",
                            targetDataPageId,  currentMetadataPageId);

                    boolean inserted =
                            tryInsertIntoDataPage(targetDataPageId, vector, distance, centroidId, originalTuple, ctx);

                    if (inserted) {
                        System.err.println("DEBUG: Successfully inserted into data page " + targetDataPageId);
                        return; // Successfully inserted
                    }

                    // If insert failed due to space, we need to handle overflow
                    System.err.println("DEBUG: Data page " + targetDataPageId + " is full, handling overflow");
                    handleDataPageOverflow(currentMetadataPageId, vector, distance, centroidId, originalTuple, ctx);
                    return;
                }

                // Check if there's a next metadata page to examine
                int nextMetadataPageId = ctx.getMetadataFrame().getNextPage();
                System.err.println("DEBUG: No suitable data page found in metadata page " + currentMetadataPageId
                        + ", next metadata page: " + nextMetadataPageId);

                // TODO: properly handle empty directory page
                if (nextMetadataPageId == 0 || nextMetadataPageId == -1) {
                    // Reached end of metadata chain - need to create new data page
                    System.err.println("DEBUG: Reached end of metadata chain, creating new data page");
                    handleDataPageOverflow(currentMetadataPageId, vector, distance, centroidId, originalTuple, ctx);
                    return;
                }

                currentMetadataPageId = nextMetadataPageId;

            } finally {
                metadataPage.releaseWriteLatch(true);
                bufferCache.unpin(metadataPage);
            }
        }
    }

    /**
     * Find the appropriate data page in a specific metadata page based on distance. This searches for a data page that
     * can accommodate the given distance.
     *
     * CRITICAL: Returns last data page as catch-all when distance > all max_distance values.
     * This is needed for BOTH insertion and deletion:
     * - Matter insertion: Vectors with distance > all max values go into last page (catch-all)
     * - Antimatter insertion: Same as matter insertion - uses last page as catch-all
     * - Physical deletion: To find those vectors, we must check the last page (same catch-all)
     *
     * The last page dynamically expands and metadata max_distance is updated automatically
     * via updateMetadataMaxDistanceIfNeeded() in the insertion path.
     *
     * @param metadataFrame The metadata frame to search
     * @param distance The distance to search for
     * @return Data page ID, or -1 if metadata is empty
     */
    private long findDataPageInMetadataPage(IVectorClusteringMetadataFrame metadataFrame, double distance)
            throws HyracksDataException {

        int tupleCount = metadataFrame.getTupleCount();

        // Search through metadata entries to find appropriate data page
        for (int i = 0; i < tupleCount; i++) {
            double maxDistance = metadataFrame.getMaxDistance(i);
            long dataPageId = metadataFrame.getDataPagePointer(i);

            // If our distance is within this data page's range, return it
            if (distance <= maxDistance) {
                return dataPageId;
            }
        }

        // No exact match: use last data page as catch-all (for ALL operations)
        if (tupleCount > 0) {
            return metadataFrame.getDataPagePointer(tupleCount - 1);
        }

        return -1; // Metadata is empty
    }

    /**
     * Try to insert into a specific data page. Returns true if successful, false if page is full.
     */
    private boolean tryInsertIntoDataPage(long dataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VectorClusteringOpContext ctx) throws HyracksDataException {

        ICachedPage dataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) dataPageId));

        try {
            dataPage.acquireWriteLatch();
            ctx.getDataFrame().setPage(dataPage);

            // Create data tuple: <distance, centroidId, vector, PK>
            // Pass context so createDataTuple can check operation type and set antimatter bit if DELETE
            ITupleReference dataTuple = ctx.getDataFrame().createDataTuple(vector, distance, centroidId, originalTuple, ctx);

            // Check if there's space for the tuple
            FrameOpSpaceStatus spaceStatus = ctx.getDataFrame().hasSpaceInsert(dataTuple);

            // Handle null case from mock frames in tests
            if (spaceStatus == null) {
                spaceStatus = FrameOpSpaceStatus.SUFFICIENT_SPACE;
            }

            switch (spaceStatus) {
                case SUFFICIENT_CONTIGUOUS_SPACE:
                case SUFFICIENT_SPACE:
                    // Find correct insertion position to maintain sorted order by distance
                    int insertIndex =
                            ((VectorClusteringDataFrame) ctx.getDataFrame()).findInsertPosition(distance);

                    // Insert the tuple
                    ctx.getDataFrame().insert(dataTuple, insertIndex);
                    ctx.getModificationCallback().found(null, originalTuple);

                    // Update page LSN
                    ctx.getDataFrame().setPageLsn(ctx.getDataFrame().getPageLsn() + 1);

                    // Update metadata maxDistance if this is the new maximum for this data page
                    updateMetadataMaxDistanceIfNeeded(ctx.getMetadataPageId(), dataPageId, distance, ctx);

                    System.err.println("DEBUG: Successfully inserted tuple at index " + insertIndex + " in data page "
                            + dataPageId);
                    return true;

                case INSUFFICIENT_SPACE:
                    // Handle overflow by splitting the data page
                    System.err.println(
                            "DEBUG: Insufficient space in data page " + dataPageId + ", triggering data page split");

                    // Find correct insertion position to maintain sorted order by distance
                    int splitInsertIndex =
                            ((VectorClusteringDataFrame) ctx.getDataFrame()).findInsertPosition(distance);

                    // Split the data page while maintaining distance-based ordering
                    splitDataPageMaintainOrder(dataPageId, dataTuple, splitInsertIndex, ctx);

                    System.err.println("DEBUG: Successfully split data page " + dataPageId + " and inserted tuple");
                    return true;

                default:
                    System.err.println("DEBUG: Unexpected space status in data page " + dataPageId + ", spaceStatus="
                            + spaceStatus);
                    return false; // Unexpected space status
            }

        } finally {
            dataPage.releaseWriteLatch(true);
            bufferCache.unpin(dataPage);
        }
    }

    /**
     * Split data page while maintaining distance-based ordering.
     */
    private void splitDataPageMaintainOrder(long dataPageId, ITupleReference newTuple, int insertIndex,
            VectorClusteringOpContext ctx) throws HyracksDataException {

        // Create new data page for split
        int newDataPageId = freePageManager.takePage(ctx.getMetaFrame());
        ICachedPage newDataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newDataPageId), NEW);

        try {
            newDataPage.acquireWriteLatch();
            VectorClusteringDataFrame newFrame = (VectorClusteringDataFrame) ctx.getDataFrameFactory().createFrame();
            newFrame.setPage(newDataPage);
            newFrame.initBuffer((byte) 0);

            // Use the frame's split method (following BTree pattern)
            ctx.getDataFrame().split(newFrame, newTuple, insertIndex);

            // Update page links (maintain linked list structure)
            int originalNextPage = ctx.getDataFrame().getNextPage();
            ctx.getDataFrame().setNextPage(newDataPageId);
            newFrame.setNextPage(originalNextPage);

            // Update page LSNs
            long currentLsn = System.currentTimeMillis(); // Or use proper LSN management
            ctx.getDataFrame().setPageLsn(currentLsn);
            newFrame.setPageLsn(currentLsn);

            // Update metadata to reflect the split
            updateMetadataAfterDataSplit(dataPageId, newDataPageId, ctx);

        } finally {
            newDataPage.releaseWriteLatch(true);
            bufferCache.unpin(newDataPage);
        }
    }

    /**
     * Update metadata page after data page split.
     * Updates BOTH original page's maxDistance and adds new page's entry.
     */
    private void updateMetadataAfterDataSplit(long originalDataPageId, int newDataPageId, VectorClusteringOpContext ctx)
            throws HyracksDataException {

        // Find the metadata page that contains the reference to the original data page
        /* TODO: metadataPageId should be passed down from caller to avoid this search */
        long targetMetadataPageId = ctx.getMetadataPageId();

        if (targetMetadataPageId == -1) {
            System.out
                    .println("DEBUG: Could not find metadata page containing originalDataPageId=" + originalDataPageId);
            return; // Could not find the metadata page
        }

        // Get max distance from ORIGINAL data page after split
        ICachedPage originalDataPage =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) originalDataPageId));
        double originalPageMaxDistance = 0.0;

        try {
            originalDataPage.acquireReadLatch();
            IVectorClusteringDataFrame originalDataFrame = (IVectorClusteringDataFrame) ctx.getDataFrameFactory().createFrame();
            originalDataFrame.setPage(originalDataPage);

            int originalTupleCount = originalDataFrame.getTupleCount();
            if (originalTupleCount > 0) {
                originalPageMaxDistance = originalDataFrame.getDistanceToCentroid(originalTupleCount - 1);
            }

            System.err.println("DEBUG: After split, original data page " + originalDataPageId + " has maxDistance="
                    + originalPageMaxDistance + ", tupleCount=" + originalTupleCount);

        } finally {
            originalDataPage.releaseReadLatch();
            bufferCache.unpin(originalDataPage);
        }

        // Get max distance from new data page
        ICachedPage newDataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newDataPageId));
        double newPageMaxDistance = 0.0;

        try {
            newDataPage.acquireReadLatch();
            IVectorClusteringDataFrame newDataFrame = (IVectorClusteringDataFrame) ctx.getDataFrameFactory().createFrame();
            newDataFrame.setPage(newDataPage);

            int newTupleCount = newDataFrame.getTupleCount();
            if (newTupleCount > 0) {
                newPageMaxDistance = newDataFrame.getDistanceToCentroid(newTupleCount - 1);
            }

            System.err.println("DEBUG: New data page " + newDataPageId + " has maxDistance=" + newPageMaxDistance
                    + ", tupleCount=" + newTupleCount);

        } finally {
            newDataPage.releaseReadLatch();
            bufferCache.unpin(newDataPage);
        }

        // Update ORIGINAL page's maxDistance in metadata (decreased after split)
        forceUpdateMetadataMaxDistance(targetMetadataPageId, originalDataPageId, originalPageMaxDistance, ctx);

        // Add NEW page's metadata entry
        updateMetadataWithNewDataPage(targetMetadataPageId, newDataPageId, newPageMaxDistance, ctx);

        System.err.println("DEBUG: Successfully updated metadata page " + targetMetadataPageId
                + " - updated original page " + originalDataPageId + " maxDistance=" + originalPageMaxDistance
                + ", added new page " + newDataPageId + " maxDistance=" + newPageMaxDistance);
    }


    /**
     * In-place deletion with three-scenario handling (follows LSMBTree pattern):
     * 1. Tuple not found → Insert antimatter (delegates to insertIntoDataPages)
     * 2. Tuple found (matter only) → Physical DELETE
     * 3. Tuple found (after antimatter was replaced by matter during reinsertion) → Physical DELETE
     */
    private boolean deleteVector(ITupleReference tuple, VectorClusteringOpContext ctx)
            throws HyracksDataException {

        // Extract vector and primary key
        double[] vector = extractVectorFromTuple(tuple);
        byte[] primaryKey = extractPrimaryKeyFromTuple(tuple);
        long pkValue = extractLongFromPrimaryKey(primaryKey);

        // Find cluster and access data pages
        ClusterAccessResult accessResult = findClusterAndPrepareAccess(tuple, ctx, true);

        try {
            // Calculate distance to centroid
            double[] centroid = accessResult.clusterResult.centroid;
            double distance = VectorUtils.calculateEuclideanDistance(vector, centroid);
            int centroidId = accessResult.clusterResult.centroidId;

            // Try to find and physically delete tuple from data pages (Scenarios 2 & 3)
            boolean foundAndDeleted = tryPhysicalDelete(
                accessResult.metadataPageId,
                distance,
                primaryKey,
                centroidId,
                tuple,
                ctx,
                pkValue
            );

            if (foundAndDeleted) {
                // Scenarios 2 & 3: Successfully deleted from memory
                return true;
            }

            // Scenario 1: Tuple not found in memory → Insert antimatter
            // CRITICAL FIX: Use insertIntoDataPages() which handles:
            // - Empty metadata pages (creates new data page)
            // - Page splits (if data page is full)
            // - Metadata max distance updates
            System.err.println("DELETE PK=" + pkValue + " → Tuple not found in memory, inserting antimatter");
            insertIntoDataPages(accessResult.metadataPageId, vector, distance, centroidId, tuple, ctx);
            System.err.println("DELETE PK=" + pkValue + " → ANTIMATTER inserted (disk tuple) in cluster=" + centroidId + ", distance=" + distance);
            return true;

        } finally {
            // Release leaf page
            accessResult.leafPage.releaseWriteLatch(true);
            bufferCache.unpin(accessResult.leafPage);
        }
    }

    /**
     * Try to physically delete a tuple from data pages. Searches through metadata
     * pages to find the tuple and delete it. Returns true if found and deleted,
     * false if not found (caller should insert antimatter).
     */
    private boolean tryPhysicalDelete(
            long metadataPageId,
            double distance,
            byte[] primaryKey,
            int centroidId,
            ITupleReference originalTuple,
            VectorClusteringOpContext ctx,
            long pkValue) throws HyracksDataException {

        // Traverse through all linked metadata pages
        long currentMetadataPageId = metadataPageId;

        while (currentMetadataPageId != -1) {
            ICachedPage metadataPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) currentMetadataPageId));

            try {
                metadataPage.acquireReadLatch();
                ctx.getMetadataFrame().setPage(metadataPage);

                // Find appropriate data page based on distance
                long targetDataPageId = findDataPageInMetadataPage(ctx.getMetadataFrame(), distance);

                if (targetDataPageId != -1) {
                    // Try physical deletion in this data page
                    ICachedPage dataPage = bufferCache.pin(
                        BufferedFileHandle.getDiskPageId(getFileId(), (int) targetDataPageId)
                    );

                    try {
                        dataPage.acquireWriteLatch();
                        ctx.getDataFrame().setPage(dataPage);

                        // Search for tuple by distance + PK
                        int tupleIndex = ((VectorClusteringDataFrame) ctx.getDataFrame())
                            .findTupleByDistanceAndPrimaryKey(distance, primaryKey, pkValue);

                        if (tupleIndex >= 0) {
                            // Found! Physically delete it
                            VectorClusteringDataFrame dataFrame = (VectorClusteringDataFrame) ctx.getDataFrame();

                            int tupleCountBefore = dataFrame.getTupleCount();
                            System.err.println("BEFORE DELETE PK=" + pkValue + " | Page has " + tupleCountBefore +
                                " tuples, deleting at index=" + tupleIndex);

                            byte[] pkAtIndex = dataFrame.getPrimaryKey(tupleIndex);
                            long pkAtIndexValue = extractLongFromPrimaryKey(pkAtIndex);

                            // Safety check
                            if (pkAtIndexValue != pkValue) {
                                System.err.println("ERROR: DELETE PK=" + pkValue + " → Found WRONG tuple at index " +
                                    tupleIndex + " with PK=" + pkAtIndexValue + " (BUG!)");
                                return false;
                            }

                            // Physical delete
                            ctx.getDataFrame().delete(originalTuple, tupleIndex);

                            int tupleCountAfter = dataFrame.getTupleCount();
                            System.err.println("AFTER DELETE PK=" + pkValue + " | Page now has " + tupleCountAfter +
                                " tuples (deleted 1)");
                            System.err.println("DELETE PK=" + pkValue + " → PHYSICAL delete at index=" + tupleIndex +
                                " in cluster=" + centroidId + ", distance=" + distance + " (verified PK match)");

                            return true; // Successfully deleted
                        }

                        // Not found in this data page
                    } finally {
                        dataPage.releaseWriteLatch(true);
                        bufferCache.unpin(dataPage);
                    }
                }

                // Check next metadata page
                int nextMetadataPageId = ctx.getMetadataFrame().getNextPage();
                if (nextMetadataPageId == 0 || nextMetadataPageId == -1) {
                    break; // End of chain
                }
                currentMetadataPageId = nextMetadataPageId;

            } finally {
                metadataPage.releaseReadLatch();
                bufferCache.unpin(metadataPage);
            }
        }

        return false; // Not found in any data page
    }

    /**
     * Extract primary key from tuple (assumes last field).
     */
    private byte[] extractPrimaryKeyFromTuple(ITupleReference tuple) {
        return VectorClusteringTupleUtils.extractPrimaryKeyFromTuple(tuple);
    }

    /**
     * Extract long value from primary key bytes (format: [type_tag][8-byte long]).
     */
    private long extractLongFromPrimaryKey(byte[] primaryKey) {
        // Skip type tag (first byte at offset 0), read 8-byte long value starting at offset 1
        return LongPointable.getLong(primaryKey, 1);
    }

    /**
     * Extract vector from tuple.
     */
    private double[] extractVectorFromTuple(ITupleReference tuple) throws HyracksDataException {
        if (tuple == null || tuple.getFieldCount() == 0 || vectorAccessorFactory == null) {
            return null;
        }

        // Vector is always at field 0 in the permuted tuple
        byte[] data = tuple.getFieldData(0);
        int offset = tuple.getFieldStart(0);
        int length = tuple.getFieldLength(0);

        // Use the vector accessor factory to extract the vector
        IVectorBinaryAccessor accessor = vectorAccessorFactory.createAccessor();
        accessor.reset(data, offset, length);
        return accessor.getVector();
    }


    private void upsertVector(ITupleReference tuple, VectorClusteringOpContext ctx) throws HyracksDataException {
        // Implement vector upsert using accessor and frame factories
        try {
            // Try update first
            ctx.getAccessor().update(tuple);
        } catch (HyracksDataException e) {
            // If update fails, try insert
            ctx.getAccessor().insert(tuple);
        }
    }

    private void handleDataPageOverflow(long metadataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VectorClusteringOpContext ctx) throws HyracksDataException {
        // Use the frame factories and page manager to handle overflow
        IVectorClusteringDataFrame dataFrame = (IVectorClusteringDataFrame) ctx.getDataFrameFactory().createFrame();
        IPageManager pageManager = ctx.getFreePageManager();

        // Create a new data page for overflow
        int newDataPageId = pageManager.takePage(ctx.getMetaFrame());
        ICachedPage newPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newDataPageId), NEW);

        try {
            newPage.acquireWriteLatch();
            // Initialize the new data frame
            dataFrame.setPage(newPage);
            dataFrame.initBuffer((byte) 0);

            // Create data tuple for the new vector
            ITupleReference dataTuple = dataFrame.createDataTuple(vector, distance, centroidId, originalTuple, ctx);

            // Insert the tuple into the new page
            dataFrame.insert(dataTuple, 0);

            // Update metadata page to include the new data page
            updateMetadataWithNewDataPage(metadataPageId, newDataPageId, distance, ctx);

        } finally {
            newPage.releaseWriteLatch(true);
            bufferCache.unpin(newPage);
        }
    }

    /**
     * Update metadata maxDistance if the new distance exceeds the current maxDistance.
     * This is needed when inserting into the last data page with a distance greater than
     * the current maxDistance - the last page acts as a catch-all that dynamically expands.
     */
    private void updateMetadataMaxDistanceIfNeeded(long metadataPageId, long dataPageId, double newDistance,
            VectorClusteringOpContext ctx) throws HyracksDataException {

        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) metadataPageId));

        try {
            metadataPage.acquireWriteLatch();
            VectorClusteringMetadataFrame metadataFrame = (VectorClusteringMetadataFrame) ctx.getMetadataFrame();
            metadataFrame.setPage(metadataPage);

            // Find the metadata entry for this data page
            int tupleCount = metadataFrame.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                long pagePtr = metadataFrame.getDataPagePointer(i);

                if (pagePtr == dataPageId) {
                    double currentMaxDistance = metadataFrame.getMaxDistance(i);

                    // Only update if new distance is larger
                    if (newDistance > currentMaxDistance) {
                        metadataFrame.updateMaxDistance(i, newDistance);

                        System.err.println("DEBUG: Updated metadata entry " + i + " for data page " + dataPageId
                                + ": maxDistance " + currentMaxDistance + " → " + newDistance);
                    }
                    break;
                }
            }

        } finally {
            metadataPage.releaseWriteLatch(true);
            bufferCache.unpin(metadataPage);
        }
    }

    /**
     * Force update metadata maxDistance to a specific value (regardless of increase/decrease).
     * This is needed after data page splits where the original page's maxDistance decreases.
     */
    private void forceUpdateMetadataMaxDistance(long metadataPageId, long dataPageId, double newMaxDistance,
            VectorClusteringOpContext ctx) throws HyracksDataException {

        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) metadataPageId));

        try {
            metadataPage.acquireWriteLatch();
            VectorClusteringMetadataFrame metadataFrame = (VectorClusteringMetadataFrame) ctx.getMetadataFrame();
            metadataFrame.setPage(metadataPage);

            // Find the metadata entry for this data page
            int tupleCount = metadataFrame.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                long pagePtr = metadataFrame.getDataPagePointer(i);

                if (pagePtr == dataPageId) {
                    double currentMaxDistance = metadataFrame.getMaxDistance(i);

                    // ALWAYS update (even if decreasing)
                    metadataFrame.updateMaxDistance(i, newMaxDistance);

                    System.err.println("DEBUG: Force updated metadata entry " + i + " for data page " + dataPageId
                            + ": maxDistance " + currentMaxDistance + " → " + newMaxDistance);
                    break;
                }
            }

        } finally {
            metadataPage.releaseWriteLatch(true);
            bufferCache.unpin(metadataPage);
        }
    }

    /**
     * Update metadata page to include a new data page.
     */
    /**
     * Update metadata page to include a new data page. Handles metadata page overflow by splitting when necessary.
     */
    private void updateMetadataWithNewDataPage(long metadataPageId, int newDataPageId, double maxDistance,
            VectorClusteringOpContext ctx) throws HyracksDataException {

        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) metadataPageId));

        try {
            metadataPage.acquireWriteLatch();
            ctx.getMetadataFrame().setPage(metadataPage);

            // Create metadata tuple for new data page
            ITupleReference metadataTuple = ctx.getMetadataFrame().createMetadataTuple(maxDistance, newDataPageId);
            ITreeIndexTupleWriter metadataFrameTupleWriter = ctx.getMetadataFrame().getTupleWriter();
            int slotSize = ctx.getMetadataFrame().getSlotSize();

            // Check if there's space for the new metadata entry
            // Check if directory page has space
            int spaceNeeded = metadataFrameTupleWriter.bytesRequired(metadataTuple) + slotSize;
            int spaceAvailable = ctx.getMetadataFrame().getTotalFreeSpace();

            if (spaceNeeded > spaceAvailable) {
                // Insufficient space - need to split metadata page
                handleMetadataPageOverflow(metadataPageId, metadataTuple, ctx);
            } else {
                ctx.getMetadataFrame().insert(metadataTuple, ctx.getMetadataFrame().getTupleCount());
            }

        } finally {
            metadataPage.releaseWriteLatch(true);
            bufferCache.unpin(metadataPage);
        }
    }

    /**
     * Handle metadata page overflow by splitting the page and distributing tuples.
     */
    private void handleMetadataPageOverflow(long metadataPageId, ITupleReference newTuple,
            VectorClusteringOpContext ctx) throws HyracksDataException {

        // Allocate a new metadata page
        int newMetadataPageId = freePageManager.takePage(ctx.getMetaFrame());
        ICachedPage newMetadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newMetadataPageId));

        try {
            newMetadataPage.acquireWriteLatch();

            // Create new metadata frame for the split page
            IVectorClusteringMetadataFrame rightFrame =
                    (IVectorClusteringMetadataFrame) metadataFrameFactory.createFrame();
            rightFrame.setPage(newMetadataPage);
            rightFrame.initBuffer((byte) 0);

            // Split the current metadata page using the correct method from VectorClusteringMetadataFrame
            ((VectorClusteringMetadataFrame) ctx.getMetadataFrame()).split(rightFrame, newTuple);

            // Update the next page pointer in the original metadata page
            ctx.getMetadataFrame().setNextPage(newMetadataPageId);

            // Initialize the next page pointer in the new metadata page to -1
            rightFrame.setNextPage(-1);

            System.out
                    .println("DEBUG: Split metadata page " + metadataPageId + " created new page " + newMetadataPageId);

        } finally {
            newMetadataPage.releaseWriteLatch(true);
            bufferCache.unpin(newMetadataPage);
        }
    }

    public IIndexCursor createSearchCursor(boolean exclusive) {
        return new VectorClusteringSearchCursor();
    }

    @Override
    public void validate() throws HyracksDataException {
        // Validation logic specific to vector clustering tree
    }

    /**
     * Convert distance metric string to IVectorDistanceFunction implementation.
     * 
     * @param distanceMetric Distance metric string (e.g., "euclidean", "cosine similarity", etc.)
     * @return IVectorDistanceFunction implementation, or Euclidean as default
     */
    private static IVectorDistanceFunction convertDistanceMetricToFunction(String distanceMetric) {
        if (distanceMetric == null || distanceMetric.trim().isEmpty()) {
            return VectorUtils::calculateEuclideanDistance;
        }

        String metricLower = distanceMetric.toLowerCase().trim();

        // Map distance metric strings to IVectorDistanceFunction implementations
        switch (metricLower) {
            case "euclidean":
            case "l2":
                return VectorUtils::calculateEuclideanDistance;
            case "euclidean_squared":
            case "l2_squared":
                return (a, b) -> {
                    double sum = 0.0;
                    for (int i = 0; i < a.length; i++) {
                        double diff = a[i] - b[i];
                        sum += diff * diff;
                    }
                    return sum; // Return squared distance (no sqrt)
                };
            case "manhattan distance":
            case "manhattan":
            case "l1":
                return (a, b) -> {
                    double sum = 0.0;
                    for (int i = 0; i < a.length; i++) {
                        sum += Math.abs(a[i] - b[i]);
                    }
                    return sum;
                };
            case "cosine similarity":
            case "cosine":
                return (a, b) -> {
                    // Cosine similarity returns 1 - similarity as distance (for minimization)
                    double dotProduct = 0.0;
                    double normA = 0.0;
                    double normB = 0.0;
                    for (int i = 0; i < a.length; i++) {
                        dotProduct += a[i] * b[i];
                        normA += a[i] * a[i];
                        normB += b[i] * b[i];
                    }
                    if (normA == 0.0 || normB == 0.0) {
                        return 1.0; // Maximum distance for zero vectors
                    }
                    double similarity = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
                    return 1.0 - similarity; // Convert similarity to distance
                };
            case "dot":
            case "dot product":
                return (a, b) -> {
                    // Dot product as distance (negated for minimization - higher dot product = closer)
                    double dotProduct = 0.0;
                    for (int i = 0; i < a.length; i++) {
                        dotProduct += a[i] * b[i];
                    }
                    return -dotProduct; // Negate for minimization
                };
            default:
                System.err.println(
                        "WARNING: Unsupported distance function: " + distanceMetric + ", defaulting to euclidean");
                return VectorUtils::calculateEuclideanDistance;
        }
    }

    /**
     * Find the closest cluster starting from root and traversing down to leaf level. Handles overflow pages for both
     * interior and leaf frames.
     */
    public ClusterSearchResult findClosestClusterFromRoot(double[] queryVector, VectorClusteringOpContext ctx,
            IVectorDistanceFunction distanceFunction) throws HyracksDataException {

        LOGGER.debug("Starting findClosestClusterFromRoot with rootPage={}", rootPage);

        // Use the common navigation logic from VCTreeNavigationUtils
        return VCTreeNavigationUtils.findClosestCentroid(bufferCache, getFileId(), rootPage, getInteriorFrameFactory(),
                getLeafFrameFactory(), queryVector, distanceFunction);
    }


    public int getVectorDimensions() {
        return vectorDimensions;
    }

    public boolean isStaticStructureInitialized() {
        return isStaticStructureInitialized;
    }

    public void setStaticStructureInitialized() {
        isStaticStructureInitialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized() {
        initialized = true;
    }


    public void setStaticStructure(VectorClusteringTreeAccessor staticAccessor) throws HyracksDataException {
        VectorClusteringTree staticStructure =  staticAccessor.getIndex();
        ITreeIndexMetadataFrame metaFrame = staticAccessor.getOpContext().getMetaFrame();
        int maxPageId = staticStructure.getPageManager().getMaxPageId(metaFrame);

        // copy all pages in static structure

        for (int pageId = 1; pageId <= maxPageId; pageId++) {
            ICachedPage sourcePage = staticAccessor.getCachedPage(pageId);
            copyPage(sourcePage);
            staticAccessor.releasePage(sourcePage);
        }

        MutableArrayValueReference key = new MutableArrayValueReference("num_leaf_centroids".getBytes());
        LongPointable value = LongPointable.FACTORY.createPointable();
        metaFrame.get(key, value);
        int numLeafCentroid = value.intValue();
        ITreeIndexFrame directoryFrame = metadataFrameFactory.createFrame();

        for (int leafCentroidId = 0; leafCentroidId < numLeafCentroid; leafCentroidId++) {
            // For metadata pages, use freePageManager.takePage() normally
            int metadataPageId = freePageManager.takePage(metaFrame) - 1;

            ICachedPage targetPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), metadataPageId), NEW);

            directoryFrame.setPage(targetPage);
            directoryFrame.initBuffer((byte) 0);

            bufferCache.unpin(targetPage);

            LOGGER.debug("Created directory page {} for leaf centroid {}", metadataPageId, leafCentroidId);
        }

        // TODO: a very hacky way
        ICachedPage targetPage =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), maxPageId + numLeafCentroid + 1), NEW);
        bufferCache.unpin(targetPage);

        initialized = true;
    }

    private void copyPage(ICachedPage sourcePage) throws HyracksDataException {
        // Copy page from source to target
        ITreeIndexMetadataFrame metaFrame = freePageManager.createMetadataFrame();
        int targetPageId = freePageManager.takePage(metaFrame) - 1;
        ICachedPage targetPage =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), targetPageId), NEW);
        // Copy entire page content
        System.arraycopy(sourcePage.getBuffer().array(), 0, targetPage.getBuffer().array(), 0,
                sourcePage.getBuffer().capacity());
        bufferCache.unpin(targetPage);

        LOGGER.debug("Copied page {} ", targetPage);
    }

    public void setRootPageId(int rootPageId) {
        rootPage = rootPageId;
    }

    /**
         * Unified cluster search result that includes metadata page access.
         * This encapsulates the common pattern used by insert, delete, and update operations.
         */
    public static class ClusterAccessResult {
        final ClusterSearchResult clusterResult;
        final ICachedPage leafPage;
        final long metadataPageId;
        final boolean isWriteOperation;

        ClusterAccessResult(ClusterSearchResult clusterResult, ICachedPage leafPage, long metadataPageId,
                boolean isWriteOperation) {
            this.clusterResult = clusterResult;
            this.leafPage = leafPage;
            this.metadataPageId = metadataPageId;
            this.isWriteOperation = isWriteOperation;
        }

        public void release() throws HyracksDataException {
            try {
                if (isWriteOperation) {
                    leafPage.releaseWriteLatch(true);
                } else {
                    leafPage.releaseReadLatch();
                }
            } finally {
                // Get buffer cache instance from the tree
                // Note: This assumes we have access to bufferCache from the result
                // We'll handle this in the calling method instead
            }
        }
    }

    /**
     * Unified method to find the closest cluster and prepare for data page operations.
     * This method encapsulates the common pattern used by insert, delete, and update operations:
     * 1. Extract vector from tuple
     * 2. Find closest cluster using tree traversal
     * 3. Pin leaf page and acquire appropriate latch
     * 4. Get metadata page pointer for cluster
     *
     * @param tuple The input tuple containing the vector
     * @param ctx The operation context
     * @param isWriteOperation True for insert operations that need write latches, false for read operations
     * @return ClusterAccessResult containing all necessary information for data page operations
     * @throws HyracksDataException if any error occurs during cluster search or page access
     */
    private ClusterAccessResult findClusterAndPrepareAccess(ITupleReference tuple, VectorClusteringOpContext ctx,
            boolean isWriteOperation) throws HyracksDataException {
        // Extract vector from tuple
        double[] vector = extractVectorFromTuple(tuple);
        if (vector == null) {
            throw HyracksDataException.create(ErrorCode.INDEX_NOT_UPDATABLE, "Failed to extract vector from tuple");
        }

        // Find the closest cluster by traversing from root to leaf
        // Use default Euclidean distance for internal operations (backward compatibility)
        IVectorDistanceFunction defaultDistanceFunction = VectorUtils::calculateEuclideanDistance;
        ClusterSearchResult clusterResult = findClosestClusterFromRoot(vector, ctx, defaultDistanceFunction);
        if (clusterResult == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No cluster found for vector");
        }

        // Pin the leaf page containing the cluster
        ICachedPage leafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), clusterResult.leafPageId));

        try {
            // Acquire appropriate latch based on operation type
            if (isWriteOperation) {
                leafPage.acquireWriteLatch();
            } else {
                leafPage.acquireReadLatch();
            }

            // Set the leaf frame for accessing cluster metadata
            ctx.getLeafFrame().setPage(leafPage);

            // Get metadata page pointer for this cluster
            long metadataPageId = ctx.getLeafFrame().getMetadataPagePointer(clusterResult.clusterIndex);

            return new ClusterAccessResult(clusterResult, leafPage, metadataPageId, isWriteOperation);

        } catch (Exception e) {
            // If anything goes wrong, make sure to release the page
            try {
                if (isWriteOperation) {
                    leafPage.releaseWriteLatch(false);
                } else {
                    leafPage.releaseReadLatch();
                }
            } finally {
                bufferCache.unpin(leafPage);
            }
            throw e;
        }
    }

    public class VectorClusteringTreeAccessor implements ITreeIndexAccessor {

        private VectorClusteringTree tree;
        private VectorClusteringOpContext ctx;
        private IIndexAccessParameters iap;
        private boolean destroyed = false;

        public VectorClusteringTreeAccessor(VectorClusteringTree tree, IIndexAccessParameters iap) {
            this.tree = tree;
            this.iap = iap;
            this.ctx = new VectorClusteringOpContext(this, tree.interiorFrameFactory, tree.leafFrameFactory,
                    tree.metadataFrameFactory, tree.dataFrameFactory, tree.freePageManager, tree.cmpFactories,
                    tree.vectorDimensions, iap.getModificationCallback(), iap.getSearchOperationCallback());
        }

        public void reset(VectorClusteringTree vctree, IIndexAccessParameters iap) {
            this.tree = vctree;
            this.iap = iap;
            ctx.setCallbacks(iap.getModificationCallback(), iap.getSearchOperationCallback());
            ctx.reset();
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.INSERT);
            insertVector(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.UPDATE);
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DELETE);
            // Use in-place deletion instead of always inserting antimatter
            tree.deleteVector(tuple, ctx);
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.UPSERT);
            upsertVector(tuple, ctx);
        }

        @Override
        public IIndexCursor createSearchCursor(boolean exclusive) throws HyracksDataException {
            return createSearchCursor(exclusive, false);
        }

        /**
         * Create a search cursor with explicit full-scan mode control.
         *
         * @param exclusive Whether to create an exclusive cursor
         * @param fullScanMode true for full-scan (merge) mode, false for query mode
         * @return Configured search cursor
         */
        public IIndexCursor createSearchCursor(boolean exclusive, boolean fullScanMode) throws HyracksDataException {
            VectorClusteringSearchCursor cursor = new VectorClusteringSearchCursor();

            // Configure cursor with tree navigation capabilities
            cursor.setBufferCache(tree.bufferCache);
            cursor.setFileId(tree.getFileId());
            cursor.setRootPageId(tree.rootPage);
            cursor.setFrameFactories(tree.interiorFrameFactory, tree.leafFrameFactory, tree.metadataFrameFactory,
                    tree.dataFrameFactory);

            // Set full-scan mode if requested (for merge operations)
            cursor.setFullScanMode(fullScanMode);

            return cursor;
        }

        public VectorClusteringTree getIndex() {
            return tree;
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            ctx.setOperation(IndexOperation.SEARCH);

            // No need to call tree.search() anymore since the cursor does everything
            VectorClusteringSearchCursor vectorCursor = (VectorClusteringSearchCursor) cursor;

            // Configure cursor with tree navigation capabilities
            vectorCursor.setBufferCache(tree.bufferCache);
            vectorCursor.setFileId(tree.getFileId());
            vectorCursor.setRootPageId(tree.rootPage);
            vectorCursor.setFrameFactories(tree.interiorFrameFactory, tree.leafFrameFactory, tree.metadataFrameFactory,
                    tree.dataFrameFactory);

            // Extract query vector and distance metric from predicate using the accessor factory
            // The predicate holds the tuple reference (updated per-tuple in resetSearchPredicate)
            double[] queryVector = null;
            String distanceMetric = null;
            if (searchPred instanceof VectorPointPredicate) {
                VectorPointPredicate vectorPred = (VectorPointPredicate) searchPred;
                ITupleReference queryTuple = vectorPred.getQueryTuple();
                int queryFieldIndex = vectorPred.getQueryFieldIndex();

                if (queryTuple != null) {
                    // Get accessor factory from parameters (stored during index accessor creation)
                    IVectorBinaryAccessorFactory factory =
                            (IVectorBinaryAccessorFactory) iap.getParameters().get(HyracksConstants.VECTOR_QUERY);

                    if (factory != null) {
                        // Create accessor and extract vector from tuple
                        IVectorBinaryAccessor accessor = factory.createAccessor();
                        accessor.reset(queryTuple.getFieldData(queryFieldIndex),
                                queryTuple.getFieldStart(queryFieldIndex), queryTuple.getFieldLength(queryFieldIndex));
                        queryVector = accessor.getVector();
                    }
                }

                // Extract distance metric from predicate
                distanceMetric = vectorPred.getDistanceMetric();
            }

            // Convert distance metric string to IVectorDistanceFunction
            // Try to use factory from AsterixDB (wraps VectorDistanceArrCalculation) if available
            // Otherwise fall back to local implementation for backward compatibility
            IVectorDistanceFunction distanceFunction = null;
            java.io.Serializable factoryObj =
                    (java.io.Serializable) iap.getParameters().get(HyracksConstants.VECTOR_DISTANCE_FUNCTION_FACTORY);

            if (factoryObj != null) {
                // Use factory from AsterixDB (wraps VectorDistanceArrCalculation)
                try {
                    // Use reflection to call createDistanceFunction() method
                    // This avoids importing AsterixDB classes in Hyracks
                    java.lang.reflect.Method createMethod =
                            factoryObj.getClass().getMethod("createDistanceFunction", String.class);
                    distanceFunction = (IVectorDistanceFunction) createMethod.invoke(factoryObj, distanceMetric);
                } catch (Exception e) {
                    System.err.println(
                            "WARNING: Failed to use distance function factory, falling back to local implementation: "
                                    + e.getMessage());
                    distanceFunction = convertDistanceMetricToFunction(distanceMetric);
                }
            } else {
                // Fallback to local implementation (for backward compatibility or when factory not provided)
                distanceFunction = convertDistanceMetricToFunction(distanceMetric);
            }

            // Create initial state with query vector and distance function
            VectorCursorInitialState initialState = new VectorCursorInitialState(ctx.getAccessor());
            initialState.setRootPageId(tree.rootPage);
            if (queryVector != null) {
                initialState.setQueryVector(queryVector);
            }
            initialState.setDistanceFunction(distanceFunction);

            // Open the cursor - it will perform centroid finding and position on data pages
            vectorCursor.open(initialState, searchPred);
        }

        /**
         * Find the closest leaf centroid for a given query vector.
         * This method delegates to the tree's findClosestClusterFromRoot implementation.
         *
         * @param queryVector The query vector to find the closest centroid for
         * @param distanceFunction The distance function to use for centroid finding
         * @return ClusterSearchResult containing information about the closest leaf centroid
         * @throws HyracksDataException if any error occurs during the search
         */
        public ClusterSearchResult findClosestLeafCentroid(double[] queryVector,
                IVectorDistanceFunction distanceFunction) throws HyracksDataException {
            if (destroyed) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Accessor has been destroyed");
            }

            if (queryVector == null) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Query vector cannot be null");
            }

            if (queryVector.length != tree.vectorDimensions) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Query vector dimension ("
                        + queryVector.length + ") does not match tree dimension (" + tree.vectorDimensions + ")");
            }

            // Ensure the tree is initialized
            if (!tree.isStaticStructureInitialized()) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Tree static structure is not initialized");
            }

            // Delegate to the tree's implementation
            return tree.findClosestClusterFromRoot(queryVector, ctx, distanceFunction);
        }

        @Override
        public void destroy() throws HyracksDataException {
            if (destroyed) {
                return;
            }
            destroyed = true;
            ctx.destroy();
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            return new TreeIndexDiskOrderScanCursor(leafFrameFactory.createFrame());
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DISKORDERSCAN);
            // TODO: Implement disk order scan
            throw new UnsupportedOperationException("Disk order scan not yet implemented");
        }

        public VectorClusteringOpContext getOpContext() {
            return ctx;
        }

        public ICachedPage getCachedPage(int pageId) throws HyracksDataException {
            return bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId));
        }

        public void releasePage(ICachedPage page) {
            bufferCache.unpin(page);
        }
    }
}
