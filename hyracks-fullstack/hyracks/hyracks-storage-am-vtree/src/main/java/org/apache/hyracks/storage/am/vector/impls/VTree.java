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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleCreatorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrame;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.tuples.VTreeTupleUtils;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleConstants;
import org.apache.hyracks.storage.am.vector.utils.VTreeNavigationUtils;
import org.apache.hyracks.storage.am.vector.utils.VectorUtils;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISampler;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
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
public class VTree extends AbstractTreeIndex {

    private static final Logger LOGGER = LogManager.getLogger();
    private final int vectorDimensions;
    private final ITreeIndexFrameFactory metadataFrameFactory;
    private final ITreeIndexFrameFactory dataFrameFactory;
    private final IVTreeBinaryAccessorFactory vectorAccessorFactory;
    private final IVTreeDataTupleCreatorFactory dataTupleCreatorFactory;
    // Raw quantization params: {minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}
    // null = non-quantized index. Quantizer is created lazily at query time using distanceMetric.
    private final float[] quantizationParams;
    private final String distanceMetric;
    private final IVTreeDistanceFunction distanceFunction;

    private boolean initialized = false;

    // For memory components: reference to static structure for navigation
    private IBufferCache staticBufferCache;
    private int staticFileId;
    private int staticRootPage;

    // Centroid-to-directory-page mapping (memory components only)
    private int[] centroidDirPageMap; // centroidIndex -> VBC directory page ID
    private int firstLeafCentroidIdMem;
    private int numLeafCentroidMem;

    // Track directory page IDs for flush pointer adjustment
    private Set<Integer> directoryPageIds;

    public VTree(IBufferCache bufferCache, IPageManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, ITreeIndexFrameFactory metadataFrameFactory,
            ITreeIndexFrameFactory dataFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            int vectorDimensions, FileReference file, IVTreeBinaryAccessorFactory vectorAccessorFactory,
            IVTreeDataTupleCreatorFactory dataTupleCreatorFactory, float[] quantizationParams, String distanceMetric) {
        super(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
        this.vectorDimensions = vectorDimensions;
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.dataTupleCreatorFactory = dataTupleCreatorFactory;
        this.quantizationParams = quantizationParams;
        this.distanceMetric = distanceMetric != null ? distanceMetric : "euclidean";
        this.distanceFunction = VectorUtils.forMetric(this.distanceMetric);
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

    @Override
    public ITreeIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return new VTreeAccessor(this, iap);
    }

    @Override
    public int getNumOfFilterFields() {
        return 0;
    }

    public IIndexBulkLoader createComponentBulkLoader(IPageWriteCallback callback, ITreeIndexAccessor staticAccessor,
            ISampler sampler) throws HyracksDataException {
        return new VTreeBulkLoader(callback, this, staticAccessor, sampler);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, ISampler sampler, IPageWriteCallback callback) throws HyracksDataException {
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
        return new VTreeStaticStructureBuilder(callback, this, numLevels, clustersPerLevel, centroidsPerCluster,
                maxEntriesPerPage);
    }

    /**
     * Insert a vector into the clustering tree. The vector tuple contains: <vector_embedding, primary_key,
     * [additional_fields]>
     */
    private void insertVector(ITupleReference tuple, VTreeOpContext ctx) throws HyracksDataException {
        try (ClusterAccessResult accessResult = findClusterAndPrepareAccess(tuple, ctx)) {
            double[] vector = VTreeTupleUtils.extractVectorFromTuple(tuple, 0, vectorAccessorFactory);

            // Calculate distance to cluster centroid and get centroid ID
            double[] centroidDouble = accessResult.clusterResult().centroid;
            double distance = distanceFunction.apply(vector, centroidDouble);
            int centroidId = accessResult.clusterResult().centroidId;

            // Insert into appropriate data page based on distance
            insertIntoDataPages(accessResult.metadataPageId(), vector, distance, centroidId, tuple, ctx);
        }
    }

    /**
     * Insert vector data into data pages via metadata pages. This method traverses through all linked metadata pages to
     * find the appropriate data page.
     */
    private void insertIntoDataPages(long metadataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {

        // Traverse through all linked directory (metadata) pages to find the appropriate data page
        long currentMetadataPageId = metadataPageId;
        int loopCount = 0;

        while (currentMetadataPageId != -1) {
            loopCount++;
            if (loopCount > 100) {
                throw new HyracksDataException(
                        "Infinite loop in directory page chain starting at page " + metadataPageId);
            }
            ICachedPage metadataPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) currentMetadataPageId));
            ctx.setMetadataPageId(currentMetadataPageId);
            try {
                metadataPage.acquireWriteLatch();
                ctx.getMetadataFrame().setPage(metadataPage);

                // Determine if this is the last directory page in the chain
                int nextMetadataPageId = ctx.getMetadataFrame().getNextPage();
                boolean isLastInChain = (nextMetadataPageId <= 0);

                // Try to find appropriate data page based on distance.
                // Only uses catch-all (last data page) on the last directory page;
                // otherwise returns -1 so we traverse to the next directory page.
                long targetDataPageId = findDataPageInMetadataPage(ctx.getMetadataFrame(), distance, isLastInChain);

                if (targetDataPageId != -1) {
                    // Found appropriate data page - try to insert
                    boolean inserted =
                            tryInsertIntoDataPage(targetDataPageId, vector, distance, centroidId, originalTuple, ctx);

                    if (inserted) {
                        return; // Successfully inserted
                    }

                    // If insert failed due to space, we need to handle overflow
                    handleDataPageOverflow(currentMetadataPageId, vector, distance, centroidId, originalTuple, ctx);
                    return;
                }

                // No match on this directory page
                if (isLastInChain) {
                    // Last page in chain - create new data page
                    handleDataPageOverflow(currentMetadataPageId, vector, distance, centroidId, originalTuple, ctx);
                    return;
                }

                // Traverse to next directory page
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
     * Returns last data page as catch-all when distance > all max_distance values.
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
    private long findDataPageInMetadataPage(IVTreeMetadataFrame metadataFrame, double distance, boolean isLastInChain)
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

        // Only use catch-all (last data page) on the last directory page in the chain.
        // For non-last pages, return -1 so the caller traverses to the next directory page.
        if (isLastInChain && tupleCount > 0) {
            return metadataFrame.getDataPagePointer(tupleCount - 1);
        }

        return -1; // No match on this page (or empty)
    }

    /**
     * Try to insert into a specific data page. Returns true if successful, false if page is full.
     */
    private boolean tryInsertIntoDataPage(long dataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {

        ICachedPage dataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) dataPageId));

        try {
            dataPage.acquireWriteLatch();
            ctx.getDataFrame().setPage(dataPage);

            // Create data tuple: <distance, centroidId, vector, PK>
            // Pass context so createDataTuple can check operation type and set antimatter bit if DELETE
            ITupleReference dataTuple =
                    ctx.getDataTupleCreator().createDataTuple(vector, distance, centroidId, originalTuple);

            // Check if there's space for the tuple
            FrameOpSpaceStatus spaceStatus = ctx.getDataFrame().hasSpaceInsert(dataTuple);

            // Handle null case from mock frames in tests
            if (spaceStatus == null) {
                spaceStatus = FrameOpSpaceStatus.SUFFICIENT_SPACE;
            }

            switch (spaceStatus) {
                case SUFFICIENT_CONTIGUOUS_SPACE: {
                    // Find correct insertion position to maintain sorted order by distance
                    int insertIndex = ((VTreeDataFrame) ctx.getDataFrame()).findInsertPosition(distance);

                    // Insert the tuple
                    ctx.getDataFrame().insert(dataTuple, insertIndex);
                    ctx.getModificationCallback().found(null, originalTuple);

                    // Update page LSN
                    ctx.getDataFrame().setPageLsn(ctx.getDataFrame().getPageLsn() + 1);

                    // Update metadata maxDistance if this is the new maximum for this data page
                    updateMetadataMaxDistanceIfNeeded(ctx.getMetadataPageId(), dataPageId, distance, ctx);

                    return true;
                }
                case SUFFICIENT_SPACE: {
                    // Fix bug-vtree-delete-frame-corruption: reclaimable space exists but is fragmented
                    // (FREE_SPACE_OFFSET has been pushed past the slot region by prior inserts whose
                    // deletes only updated TOTAL_FREE_SPACE_OFFSET). Compact first to reset
                    // FREE_SPACE_OFFSET to a safe high-water mark, then insert. Matches the canonical
                    // BTreeNSMLeafFrame pattern (BTree.java:309-315).
                    ctx.getDataFrame().compact();
                    int insertIndex = ((VTreeDataFrame) ctx.getDataFrame()).findInsertPosition(distance);

                    ctx.getDataFrame().insert(dataTuple, insertIndex);
                    ctx.getModificationCallback().found(null, originalTuple);

                    ctx.getDataFrame().setPageLsn(ctx.getDataFrame().getPageLsn() + 1);
                    updateMetadataMaxDistanceIfNeeded(ctx.getMetadataPageId(), dataPageId, distance, ctx);

                    return true;
                }
                case INSUFFICIENT_SPACE:
                    // Handle overflow by splitting the data page

                    // Find correct insertion position to maintain sorted order by distance
                    int splitInsertIndex = ((VTreeDataFrame) ctx.getDataFrame()).findInsertPosition(distance);

                    // Split the data page while maintaining distance-based ordering
                    splitDataPageMaintainOrder(ctx.getMetadataPageId(), dataPageId, dataTuple, splitInsertIndex, ctx);
                    return true;

                default:
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
    private void splitDataPageMaintainOrder(long metadataPageId, long dataPageId, ITupleReference newTuple,
            int insertIndex, VTreeOpContext ctx) throws HyracksDataException {

        // Create new data page for split
        int newDataPageId = freePageManager.takePage(ctx.getMetaFrame());
        ICachedPage newDataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newDataPageId), NEW);

        try {
            newDataPage.acquireWriteLatch();
            VTreeDataFrame newFrame = (VTreeDataFrame) ctx.getDataFrameFactory().createFrame();
            newFrame.setPage(newDataPage);
            newFrame.initBuffer((byte) 0);

            // Use the frame's split method (following BTree pattern)
            ctx.getDataFrame().split(newFrame, newTuple, insertIndex);

            // Update page links (maintain linked list structure)
            int originalNextPage = ctx.getDataFrame().getNextPage();
            ctx.getDataFrame().setNextPage(newDataPageId);
            newFrame.setNextPage(originalNextPage);

            // Bump both pages past the source page's prior LSN. Page LSNs are not currently
            // consulted for recovery on these LSM-component pages (no readers in this codebase),
            // but keeping the value monotonic mirrors the increment pattern used elsewhere in
            // this class and avoids the non-monotonicity of System.currentTimeMillis().
            long currentLsn = ctx.getDataFrame().getPageLsn() + 1;
            ctx.getDataFrame().setPageLsn(currentLsn);
            newFrame.setPageLsn(currentLsn);

            // Update metadata to reflect the split
            updateMetadataAfterDataSplit(metadataPageId, dataPageId, newDataPageId, ctx);

        } finally {
            newDataPage.releaseWriteLatch(true);
            bufferCache.unpin(newDataPage);
        }
    }

    /**
     * Update metadata page after data page split.
     * Updates BOTH original page's maxDistance and adds new page's entry.
     */
    private void updateMetadataAfterDataSplit(long targetMetadataPageId, long originalDataPageId, int newDataPageId,
            VTreeOpContext ctx) throws HyracksDataException {
        if (targetMetadataPageId == -1) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "updateMetadataAfterDataSplit called without a metadata page id (originalDataPageId="
                            + originalDataPageId + ")");
        }

        // Get max distance from ORIGINAL data page after split
        ICachedPage originalDataPage =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) originalDataPageId));
        double originalPageMaxDistance = 0.0;

        try {
            originalDataPage.acquireReadLatch();
            IVTreeDataFrame originalDataFrame = (IVTreeDataFrame) ctx.getDataFrameFactory().createFrame();
            originalDataFrame.setPage(originalDataPage);

            int originalTupleCount = originalDataFrame.getTupleCount();
            if (originalTupleCount > 0) {
                originalPageMaxDistance = originalDataFrame.getDistanceToCentroid(originalTupleCount - 1);
            }

        } finally {
            originalDataPage.releaseReadLatch();
            bufferCache.unpin(originalDataPage);
        }

        // Get max distance from new data page
        ICachedPage newDataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newDataPageId));
        double newPageMaxDistance = 0.0;

        try {
            newDataPage.acquireReadLatch();
            IVTreeDataFrame newDataFrame = (IVTreeDataFrame) ctx.getDataFrameFactory().createFrame();
            newDataFrame.setPage(newDataPage);

            int newTupleCount = newDataFrame.getTupleCount();
            if (newTupleCount > 0) {
                newPageMaxDistance = newDataFrame.getDistanceToCentroid(newTupleCount - 1);
            }

        } finally {
            newDataPage.releaseReadLatch();
            bufferCache.unpin(newDataPage);
        }

        // Update ORIGINAL page's maxDistance in metadata (decreased after split)
        forceUpdateMetadataMaxDistance(targetMetadataPageId, originalDataPageId, originalPageMaxDistance, ctx);

        // Add NEW page's metadata entry
        updateMetadataWithNewDataPage(targetMetadataPageId, newDataPageId, newPageMaxDistance, ctx);
    }

    /**
     * In-place deletion with three-scenario handling (follows LSMBTree pattern):
     * 1. Tuple not found → Insert antimatter (delegates to insertIntoDataPages)
     * 2. Tuple found (matter only) → Physical DELETE
     * 3. Tuple found (after antimatter was replaced by matter during reinsertion) → Physical DELETE
     */
    private void deleteVector(ITupleReference tuple, VTreeOpContext ctx) throws HyracksDataException {

        // Extract vector and primary key (binary format - no type assumption)
        double[] vector = VTreeTupleUtils.extractVectorFromTuple(tuple, 0, vectorAccessorFactory);
        byte[] primaryKey = VTreeTupleUtils.extractPrimaryKeyFromTuple(tuple);

        // Find cluster and access data pages
        try (ClusterAccessResult accessResult = findClusterAndPrepareAccess(tuple, ctx)) {
            double[] centroid = accessResult.clusterResult().centroid;
            double distance = distanceFunction.apply(vector, centroid);
            int centroidId = accessResult.clusterResult().centroidId;

            // Try to find and physically delete tuple from data pages (Scenarios 2 & 3)
            boolean foundAndDeleted =
                    tryPhysicalDelete(accessResult.metadataPageId(), distance, primaryKey, tuple, ctx);

            if (foundAndDeleted) {
                // Scenarios 2 & 3: Successfully deleted from memory
                return;
            }

            // Scenario 1: Tuple not found in memory → Insert antimatter via insertIntoDataPages
            // which handles empty metadata pages, page splits, and metadata max-distance updates.
            insertIntoDataPages(accessResult.metadataPageId(), vector, distance, centroidId, tuple, ctx);
        }
    }

    /**
     * Try to physically delete a tuple from data pages. Searches through metadata
     * pages to find the tuple and delete it. Returns true if found and deleted,
     * false if not found (caller should insert antimatter).
     *
     * Uses binary comparison for primary key matching - no type assumption.
     */
    private boolean tryPhysicalDelete(long metadataPageId, double distance, byte[] primaryKey,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {

        // Traverse through all linked directory (metadata) pages
        long currentMetadataPageId = metadataPageId;

        while (currentMetadataPageId != -1) {
            ICachedPage metadataPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) currentMetadataPageId));

            try {
                metadataPage.acquireReadLatch();
                ctx.getMetadataFrame().setPage(metadataPage);

                // Determine if this is the last directory page in the chain
                int nextMetadataPageId = ctx.getMetadataFrame().getNextPage();
                boolean isLastInChain = (nextMetadataPageId <= 0);

                // Find appropriate data page based on distance
                long targetDataPageId = findDataPageInMetadataPage(ctx.getMetadataFrame(), distance, isLastInChain);

                if (targetDataPageId != -1) {
                    // Try physical deletion in this data page
                    ICachedPage dataPage =
                            bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) targetDataPageId));

                    try {
                        dataPage.acquireWriteLatch();
                        ctx.getDataFrame().setPage(dataPage);

                        // Search for tuple by distance + PK (uses binary comparison internally)
                        int pkFieldIndex = VTreeDataTupleConstants.getPkStartField(quantizationParams != null);
                        int tupleIndex = ((VTreeDataFrame) ctx.getDataFrame())
                                .findTupleByDistanceAndPrimaryKey(distance, primaryKey, pkFieldIndex);

                        if (tupleIndex >= 0) {
                            // Found! Physically delete it
                            VTreeDataFrame dataFrame = (VTreeDataFrame) ctx.getDataFrame();

                            byte[] pkAtIndex = dataFrame.getPrimaryKey(tupleIndex, pkFieldIndex);

                            // Safety check using binary comparison (no type assumption)
                            if (!Arrays.equals(pkAtIndex, primaryKey)) {
                                return false;
                            }

                            // Physical delete
                            ctx.getDataFrame().delete(originalTuple, tupleIndex);

                            return true; // Successfully deleted
                        }

                        // Not found in this data page
                    } finally {
                        dataPage.releaseWriteLatch(true);
                        bufferCache.unpin(dataPage);
                    }
                }

                // No match or not found - check next directory page
                if (isLastInChain) {
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

    private void handleDataPageOverflow(long metadataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {
        // Use the frame factories and page manager to handle overflow
        IVTreeDataFrame dataFrame = (IVTreeDataFrame) ctx.getDataFrameFactory().createFrame();
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
            ITupleReference dataTuple =
                    ctx.getDataTupleCreator().createDataTuple(vector, distance, centroidId, originalTuple);

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
            VTreeOpContext ctx) throws HyracksDataException {

        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) metadataPageId));

        try {
            metadataPage.acquireWriteLatch();
            VTreeMetadataFrame metadataFrame = (VTreeMetadataFrame) ctx.getMetadataFrame();
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
            VTreeOpContext ctx) throws HyracksDataException {

        ICachedPage metadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), (int) metadataPageId));

        try {
            metadataPage.acquireWriteLatch();
            VTreeMetadataFrame metadataFrame = (VTreeMetadataFrame) ctx.getMetadataFrame();
            metadataFrame.setPage(metadataPage);

            // Find the metadata entry for this data page
            int tupleCount = metadataFrame.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                long pagePtr = metadataFrame.getDataPagePointer(i);

                if (pagePtr == dataPageId) {
                    metadataFrame.updateMaxDistance(i, newMaxDistance);
                    break;
                }
            }

        } finally {
            metadataPage.releaseWriteLatch(true);
            bufferCache.unpin(metadataPage);
        }
    }

    /**
     * Update metadata page to include a new data page. Handles metadata page overflow by splitting when necessary.
     */
    private void updateMetadataWithNewDataPage(long metadataPageId, int newDataPageId, double maxDistance,
            VTreeOpContext ctx) throws HyracksDataException {

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
    private void handleMetadataPageOverflow(long metadataPageId, ITupleReference newTuple, VTreeOpContext ctx)
            throws HyracksDataException {

        // Allocate a new metadata page
        int newMetadataPageId = freePageManager.takePage(ctx.getMetaFrame());
        if (directoryPageIds != null) {
            directoryPageIds.add(newMetadataPageId);
        }
        ICachedPage newMetadataPage =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newMetadataPageId), NEW);

        try {
            newMetadataPage.acquireWriteLatch();

            // Create new metadata frame for the split page
            IVTreeMetadataFrame rightFrame = (IVTreeMetadataFrame) metadataFrameFactory.createFrame();
            rightFrame.setPage(newMetadataPage);
            rightFrame.initBuffer((byte) 0);

            // Split the current metadata page using the correct method from VTreeMetadataFrame
            ((VTreeMetadataFrame) ctx.getMetadataFrame()).split(rightFrame, newTuple);

            // Update the next page pointer in the original metadata page
            ctx.getMetadataFrame().setNextPage(newMetadataPageId);

            // Initialize the next page pointer in the new metadata page to -1
            rightFrame.setNextPage(-1);

        } finally {
            newMetadataPage.releaseWriteLatch(true);
            bufferCache.unpin(newMetadataPage);
        }
    }

    @Override
    public void validate() throws HyracksDataException {
        // Validation logic specific to vector clustering tree
    }

    /**
     * Find the closest cluster starting from root and traversing down to leaf level. Handles overflow pages for both
     * interior and leaf frames.
     */
    public ClusterSearchResult findClosestClusterFromRoot(double[] queryVector, IVTreeDistanceFunction distanceFunction)
            throws HyracksDataException {
        return findClosestClusterFromRoot(queryVector, distanceFunction, null, null);
    }

    /**
     * Find the closest cluster starting from root and traversing down to leaf level,
     * optionally computing a quantized distance for the best result.
     *
     * @param queryVector Query vector for navigation (full precision)
     * @param distanceFunction Distance function for centroid comparison
     * @param quantizedQueryVector Quantized form of queryVector (nullable — pass null to skip quantized distance)
     * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable — pass null to skip)
     * @return ClusterSearchResult with optional quantizedDistance
     */
    public ClusterSearchResult findClosestClusterFromRoot(double[] queryVector, IVTreeDistanceFunction distanceFunction,
            double[] quantizedQueryVector, IVTreeQuantizer quantizer) throws HyracksDataException {

        // For memory components: navigate via static structure reference
        IBufferCache navBC = (staticBufferCache != null) ? staticBufferCache : bufferCache;
        int navFileId = (staticBufferCache != null) ? staticFileId : getFileId();
        int navRoot = (staticBufferCache != null) ? staticRootPage : rootPage;

        LOGGER.log(Level.TRACE, "Starting findClosestClusterFromRoot with navRoot={}, isMemoryComponent={}", navRoot,
                staticBufferCache != null);

        ClusterSearchResult result =
                VTreeNavigationUtils.findClosestCentroid(navBC, navFileId, navRoot, getInteriorFrameFactory(),
                        getLeafFrameFactory(), queryVector, distanceFunction, quantizedQueryVector, quantizer);

        // For memory components: replace directoryPageId with VBC mapping
        if (centroidDirPageMap != null) {
            int centroidIndex = result.centroidId - firstLeafCentroidIdMem;
            if (centroidIndex >= 0 && centroidIndex < centroidDirPageMap.length) {
                result = ClusterSearchResult.create(result.leafPageId, result.clusterIndex, result.centroid,
                        result.distance, result.centroidId, centroidDirPageMap[centroidIndex],
                        result.quantizedDistance);
            }
        }

        return result;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    /**
     * Get the buffer cache to use for tree navigation.
     * For memory components, this returns the static structure's buffer cache.
     * For disk components, this returns the component's own buffer cache.
     */
    public IBufferCache getNavigationBufferCache() {
        return (staticBufferCache != null) ? staticBufferCache : bufferCache;
    }

    /**
     * Get the file ID to use for tree navigation.
     * For memory components, this returns the static structure's file ID.
     * For disk components, this returns the component's own file ID.
     */
    public int getNavigationFileId() {
        return (staticBufferCache != null) ? staticFileId : getFileId();
    }

    /**
     * Get the root page ID to use for tree navigation.
     * For memory components, this returns the static structure's root page.
     * For disk components, this returns the component's own root page.
     */
    public int getNavigationRootPageId() {
        return (staticBufferCache != null) ? staticRootPage : rootPage;
    }

    /**
     * Find close centroids via level-wise traversal with global sort (delegates to
     * VTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort). Handles overflow pages for both
     * interior and leaf frames.
     */
    public List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSortFromRoot(double[] queryVector,
            IVTreeDistanceFunction distanceFunction, double ep) throws HyracksDataException {

        // For memory components: navigate via static structure reference
        IBufferCache navBC = (staticBufferCache != null) ? staticBufferCache : bufferCache;
        int navFileId = (staticBufferCache != null) ? staticFileId : getFileId();
        int navRoot = (staticBufferCache != null) ? staticRootPage : rootPage;

        LOGGER.log(Level.TRACE, "Starting findCloseCentroidsLevelWiseFromRoot with navRoot={}", navRoot);

        List<ClusterSearchResult> results = VTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort(navBC, navFileId,
                navRoot, getInteriorFrameFactory(), getLeafFrameFactory(), queryVector, distanceFunction, ep);

        // For memory components: replace directoryPageId with VBC mapping
        if (centroidDirPageMap != null) {
            for (int r = 0; r < results.size(); r++) {
                ClusterSearchResult result = results.get(r);
                int centroidIndex = result.centroidId - firstLeafCentroidIdMem;
                if (centroidIndex >= 0 && centroidIndex < centroidDirPageMap.length) {
                    results.set(r,
                            ClusterSearchResult.create(result.leafPageId, result.clusterIndex, result.centroid,
                                    result.distance, result.centroidId, centroidDirPageMap[centroidIndex],
                                    result.quantizedDistance));
                }
            }
        }

        return results;
    }

    /**
     * Get the raw quantization parameters, or null if no quantization is configured.
     * Format: {minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}
     */
    public float[] getQuantizationParams() {
        return quantizationParams;
    }

    public String getDistanceMetric() {
        return distanceMetric;
    }

    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Reset initialization state so that static structure directory pages
     * can be re-created after a memory component flush/recycle.
     */
    public void resetInitialization() {
        initialized = false;
        centroidDirPageMap = null;
        directoryPageIds = null;
    }

    public synchronized void setStaticStructure(VTreeAccessor staticAccessor) throws HyracksDataException {
        if (initialized) {
            return; // Already initialized, skip
        }

        VTree staticStructure = staticAccessor.getIndex();
        ITreeIndexMetadataFrame metaFrame = staticAccessor.getOpContext().getMetaFrame();

        // Store references to static structure for read-only navigation
        this.staticBufferCache = staticStructure.getBufferCache();
        this.staticFileId = staticStructure.getFileId();
        this.staticRootPage = staticStructure.rootPage;

        // Pin the static structure's metadata page onto metaFrame before reading
        // (getMaxPageId internally calls metaFrame.setPage() which initializes the frame's buffer)
        staticStructure.getPageManager().getMaxPageId(metaFrame);

        // Read metadata from static structure
        MutableArrayValueReference key1 = new MutableArrayValueReference("num_leaf_centroids".getBytes());
        LongPointable value1 = LongPointable.FACTORY.createPointable();
        MutableArrayValueReference key2 = new MutableArrayValueReference("first_leaf_centroid_id".getBytes());
        LongPointable value2 = LongPointable.FACTORY.createPointable();
        metaFrame.get(key1, value1);
        metaFrame.get(key2, value2);
        this.numLeafCentroidMem = value1.intValue();
        this.firstLeafCentroidIdMem = value2.intValue();

        // Create empty directory pages in VBC (using takePage() directly)
        ITreeIndexMetadataFrame vbcMetaFrame = freePageManager.createMetadataFrame();
        ITreeIndexFrame directoryFrame = metadataFrameFactory.createFrame();
        centroidDirPageMap = new int[numLeafCentroidMem];
        directoryPageIds = new HashSet<>();

        for (int i = 0; i < numLeafCentroidMem; i++) {
            int dirPageId = freePageManager.takePage(vbcMetaFrame);
            centroidDirPageMap[i] = dirPageId;
            directoryPageIds.add(dirPageId);

            ICachedPage targetPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), dirPageId), NEW);
            directoryFrame.setPage(targetPage);
            directoryFrame.initBuffer((byte) 0);
            bufferCache.unpin(targetPage);

            LOGGER.log(Level.TRACE, "Created directory page {} for leaf centroid {}", dirPageId, i);
        }

        initialized = true;
    }

    public void setRootPageId(int rootPageId) {
        rootPage = rootPageId;
    }

    public int[] getCentroidDirPageMap() {
        return centroidDirPageMap;
    }

    public int getFirstLeafCentroidIdMem() {
        return firstLeafCentroidIdMem;
    }

    public int getNumLeafCentroidMem() {
        return numLeafCentroidMem;
    }

    /**
     * Result of {@link #findClusterAndPrepareAccess}: carries the cluster lookup, the (still
     * write-latched) leaf page, and the metadata page id. Implements {@link AutoCloseable} so
     * callers can release the latch and unpin via try-with-resources. {@code leafPage} is
     * {@code null} for memory components — in that case {@code close()} is a no-op.
     */
    public record ClusterAccessResult(ClusterSearchResult clusterResult, ICachedPage leafPage, long metadataPageId,
            IBufferCache bufferCache) implements AutoCloseable {

    @Override
    public void close() {
        if (leafPage != null) {
            leafPage.releaseWriteLatch(true);
            bufferCache.unpin(leafPage);
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
     */
    private ClusterAccessResult findClusterAndPrepareAccess(ITupleReference tuple, VTreeOpContext ctx)
            throws HyracksDataException {
        double[] vector = VTreeTupleUtils.extractVectorFromTuple(tuple, 0, vectorAccessorFactory);
        if (vector == null) {
            throw HyracksDataException.create(ErrorCode.INDEX_NOT_UPDATABLE, "Failed to extract vector from tuple");
        }

        ClusterSearchResult clusterResult = findClosestClusterFromRoot(vector, distanceFunction);
        if (clusterResult == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No cluster found for vector");
        }

        if (centroidDirPageMap != null) {
            return new ClusterAccessResult(clusterResult, null, clusterResult.directoryPageId, bufferCache);
        }

        ICachedPage leafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), clusterResult.leafPageId));
        try {
            leafPage.acquireWriteLatch();
            ctx.getLeafFrame().setPage(leafPage);
            long metadataPageId = ctx.getLeafFrame().getMetadataPagePointer(clusterResult.clusterIndex);
            return new ClusterAccessResult(clusterResult, leafPage, metadataPageId, bufferCache);
        } catch (Exception e) {
            try {
                leafPage.releaseWriteLatch(false);
            } finally {
                bufferCache.unpin(leafPage);
            }
            throw e;
        }
    }

    public class VTreeAccessor implements ITreeIndexAccessor {

        private VTree tree;
        private VTreeOpContext ctx;
        private IIndexAccessParameters iap;
        private boolean destroyed = false;

        public VTreeAccessor(VTree tree, IIndexAccessParameters iap) {
            this.tree = tree;
            this.iap = iap;
            this.ctx = new VTreeOpContext(this, tree.interiorFrameFactory, tree.leafFrameFactory,
                    tree.metadataFrameFactory, tree.dataFrameFactory, tree.freePageManager, tree.cmpFactories,
                    tree.vectorDimensions, iap.getModificationCallback(), iap.getSearchOperationCallback(),
                    tree.dataTupleCreatorFactory, tree.quantizationParams);
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
            throw new UnsupportedOperationException();
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
            VTreeSearchCursor cursor = new VTreeSearchCursor();
            configureCursor(cursor);
            cursor.setFullScanMode(fullScanMode);
            return cursor;
        }

        private void configureCursor(VTreeSearchCursor cursor) {
            if (tree.staticBufferCache != null) {
                cursor.setBufferCache(tree.staticBufferCache);
                cursor.setFileId(tree.staticFileId);
                cursor.setRootPageId(tree.staticRootPage);
                cursor.setDataBufferCache(tree.bufferCache, tree.getFileId());
                cursor.setCentroidDirPageMap(tree.centroidDirPageMap, tree.firstLeafCentroidIdMem);
            } else {
                cursor.setBufferCache(tree.bufferCache);
                cursor.setFileId(tree.getFileId());
                cursor.setRootPageId(tree.rootPage);
                cursor.setDataBufferCache(tree.bufferCache, tree.getFileId());
            }
            cursor.setFrameFactories(tree.interiorFrameFactory, tree.leafFrameFactory, tree.metadataFrameFactory,
                    tree.dataFrameFactory);
        }

        public VTree getIndex() {
            return tree;
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            ctx.setOperation(IndexOperation.SEARCH);

            VTreeSearchCursor vectorCursor = (VTreeSearchCursor) cursor;
            configureCursor(vectorCursor);

            // Extract query vector and distance metric from predicate using the accessor factory
            // The predicate holds the tuple reference (updated per-tuple in resetSearchPredicate)
            double[] queryVector = null;
            String distanceMetric = null;
            if (searchPred instanceof VTreeSearchPredicate vectorPred) {
                ITupleReference queryTuple = vectorPred.getQueryTuple();
                int queryFieldIndex = vectorPred.getQueryFieldIndex();

                if (queryTuple != null) {
                    // Accessor factory is stored on the index accessor parameters during creation
                    IVTreeBinaryAccessorFactory factory =
                            (IVTreeBinaryAccessorFactory) iap.getParameters().get(HyracksConstants.VECTOR_QUERY);
                    queryVector = VTreeTupleUtils.extractVectorFromTuple(queryTuple, queryFieldIndex, factory);
                }

                // Extract distance metric from predicate
                distanceMetric = vectorPred.getDistanceMetric();
            }

            // Convert distance metric string to IVTreeDistanceFunction
            // Try to use factory from AsterixDB (wraps VectorDistanceArrCalculation) if available
            // Otherwise fall back to local implementation for backward compatibility
            IVTreeDistanceFunction distanceFunction = null;
            java.io.Serializable factoryObj =
                    (java.io.Serializable) iap.getParameters().get(HyracksConstants.VECTOR_DISTANCE_FUNCTION_FACTORY);

            if (factoryObj != null) {
                // Use factory from AsterixDB (wraps VectorDistanceArrCalculation)
                try {
                    // Use reflection to call createDistanceFunction() method
                    // This avoids importing AsterixDB classes in Hyracks
                    java.lang.reflect.Method createMethod =
                            factoryObj.getClass().getMethod("createDistanceFunction", String.class);
                    distanceFunction = (IVTreeDistanceFunction) createMethod.invoke(factoryObj, distanceMetric);
                } catch (Exception e) {
                    LOGGER.log(Level.TRACE,
                            "Failed to use distance function factory, falling back to local implementation: {}",
                            e.getMessage());
                    distanceFunction = VectorUtils.forMetric(distanceMetric);
                }
            } else {
                // Fallback to local implementation (for backward compatibility or when factory not provided)
                distanceFunction = VectorUtils.forMetric(distanceMetric);
            }

            // Create initial state with query vector and distance function
            VTreeCursorInitialState initialState = new VTreeCursorInitialState(ctx.getAccessor());
            // For memory components, use staticRootPage (the static structure's root);
            // for disk components, use the tree's own rootPage
            if (tree.staticBufferCache != null) {
                initialState.setRootPageId(tree.staticRootPage);
            } else {
                initialState.setRootPageId(tree.rootPage);
            }
            if (queryVector != null) {
                initialState.setQueryVector(queryVector);
            }
            initialState.setDistanceFunction(distanceFunction);

            // Create quantizer at query time using params from tree + distanceMetric from predicate
            float[] qParams = tree.getQuantizationParams();
            if (qParams != null && queryVector != null && distanceMetric != null) {
                try {
                    // Create ScalarVectorQuantizer via reflection (AsterixDB class, can't import directly)
                    // qParams = {minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount}
                    Class<?> sampleFileClass =
                            Class.forName("org.apache.asterix.common.storage.OptimizedScalarQuantizationSampleFile");
                    Class<?> paramsClass = Class
                            .forName("org.apache.asterix.common.storage.OptimizedScalarQuantizationSampleFile$Params");
                    Class<?> simFuncClass = Class.forName(
                            "org.apache.asterix.common.storage.OptimizedScalarQuantizationSampleFile$SimilarityFunction");
                    Class<?> quantizerClass = Class.forName("org.apache.asterix.common.storage.ScalarVectorQuantizer");

                    // Params(int bits, int vectorDimensions, int sampleCount, float confidenceInterval,
                    //        float minQuantile, float maxQuantile, float alpha)
                    Object params = paramsClass.getConstructor(int.class, int.class, int.class, float.class,
                            float.class, float.class, float.class).newInstance((int) qParams[4], tree.vectorDimensions,
                                    (int) qParams[5], qParams[3], qParams[0], qParams[1], qParams[2]);

                    // SimilarityFunction fromDistanceMetric(String)
                    java.lang.reflect.Method fromMetric = sampleFileClass.getMethod("fromDistanceMetric", String.class);
                    Object simFunc = fromMetric.invoke(null, distanceMetric);

                    // new ScalarVectorQuantizer(Params, SimilarityFunction)
                    IVTreeQuantizer quantizer = (IVTreeQuantizer) quantizerClass
                            .getConstructor(paramsClass, simFuncClass).newInstance(params, simFunc);

                    initialState.setQuantizedQueryVector(quantizer.quantize(queryVector));
                    initialState.setQuantizer(quantizer);
                } catch (Exception e) {
                    LOGGER.log(Level.TRACE, "Failed to create quantizer via reflection: {}", e.getMessage());
                }
            }

            // If reflection-based quantizer not available, check IAP for pre-configured quantizer
            if (initialState.getQuantizer() == null && queryVector != null) {
                IVTreeQuantizer iapQuantizer =
                        (IVTreeQuantizer) iap.getParameters().get(HyracksConstants.VECTOR_QUANTIZER);
                if (iapQuantizer != null) {
                    try {
                        initialState.setQuantizedQueryVector(iapQuantizer.quantize(queryVector));
                        initialState.setQuantizer(iapQuantizer);
                    } catch (Exception e) {
                        LOGGER.log(Level.TRACE, "Failed to use IAP quantizer: {}", e.getMessage());
                    }
                }
            }

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
                IVTreeDistanceFunction distanceFunction) throws HyracksDataException {
            return findClosestLeafCentroid(queryVector, distanceFunction, null, null);
        }

        /**
         * Find the closest leaf centroid, optionally computing quantized distance.
         *
         * @param queryVector The query vector to find the closest centroid for
         * @param distanceFunction The distance function to use for centroid finding
         * @param quantizedQueryVector Quantized form of queryVector (nullable)
         * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable)
         * @return ClusterSearchResult with optional quantizedDistance
         * @throws HyracksDataException if any error occurs during the search
         */
        public ClusterSearchResult findClosestLeafCentroid(double[] queryVector,
                IVTreeDistanceFunction distanceFunction, double[] quantizedQueryVector, IVTreeQuantizer quantizer)
                throws HyracksDataException {
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

            return tree.findClosestClusterFromRoot(queryVector, distanceFunction, quantizedQueryVector, quantizer);
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
            throw new UnsupportedOperationException("Disk order scan not yet implemented");
        }

        public VTreeOpContext getOpContext() {
            return ctx;
        }

        public ICachedPage getCachedPage(int pageId) throws HyracksDataException {
            return bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId));
        }

        public void releasePage(ICachedPage page) {
            bufferCache.unpin(page);
        }

        public List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(double[] queryVector,
                IVTreeDistanceFunction hyracksDistanceFunction, double epi) throws HyracksDataException {
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
            return tree.findCloseCentroidsLevelWiseGlobalSortFromRoot(queryVector, hyracksDistanceFunction, epi);
        }
    }
}
