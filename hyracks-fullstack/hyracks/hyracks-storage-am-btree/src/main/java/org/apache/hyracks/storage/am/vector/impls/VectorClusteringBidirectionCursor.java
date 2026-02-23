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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrame;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Bidirectional cursor for vector clustering tree operations.
 *
 * This cursor supports traversal in both directions from a pivot point where D(x, C) ≈ D(q, C).
 * - Right direction: iterates tuples with D(x, C) >= D(q, C) in ascending order
 * - Left direction: iterates tuples with D(x, C) < D(q, C) in descending order
 *
 * Key design: Left and right directions each maintain their own directory page state so they
 * can be on different directory pages simultaneously. During pivot finding, directory page IDs
 * before the pivot page are accumulated in leftDirPageStack for left-ward navigation.
 *
 * Used by LSMVCTreeBlockedCursor for optimized search with triangle inequality pruning.
 */
public class VectorClusteringBidirectionCursor implements IIndexCursor {

    // Buffer cache and file access
    private IBufferCache bufferCache;
    private int fileId;
    private ITreeIndexFrameFactory metadataFrameFactory;
    private ITreeIndexFrameFactory dataFrameFactory;

    // Left direction: stack of directory page IDs before the pivot page
    private Deque<Long> leftDirPageStack;

    // Right directory page state (independent from left)
    private long rightDirPageId;
    private ICachedPage rightDirPage;
    private IVectorClusteringMetadataFrame rightDirFrame;

    // Left directory page state (independent from right)
    private long leftDirPageId;
    private ICachedPage leftDirPage;
    private IVectorClusteringMetadataFrame leftDirFrame;

    // Position within directory pages
    private int rightEntryIndex; // Entry index for right cursor within rightDirFrame
    private int leftEntryIndex; // Entry index for left cursor within leftDirFrame

    // Right data page state
    private long rightDataPageId;
    private ICachedPage rightDataPage;
    private IVectorClusteringDataFrame rightDataFrame;
    private ITreeIndexTupleReference rightFrameTuple;
    private int rightTupleIndex;
    private boolean rightExhausted;

    // Left data page state
    private long leftDataPageId;
    private ICachedPage leftDataPage;
    private IVectorClusteringDataFrame leftDataFrame;
    private ITreeIndexTupleReference leftFrameTuple;
    private int leftTupleIndex;
    private boolean leftExhausted;

    // Current tuple references
    private ITupleReference currentRightTuple;
    private ITupleReference currentLeftTuple;

    // Cursor state
    private boolean isOpen;
    private double distanceToQueryCentroid; // D(q, C)

    public VectorClusteringBidirectionCursor() {
        this.isOpen = false;
        this.leftDirPageStack = new ArrayDeque<>();
    }

    /**
     * Set buffer cache and file ID for page access.
     */
    public void setBufferCache(IBufferCache bufferCache, int fileId) {
        this.bufferCache = bufferCache;
        this.fileId = fileId;
    }

    /**
     * Set frame factories for creating frame instances.
     */
    public void setFrameFactories(ITreeIndexFrameFactory metadataFrameFactory,
            ITreeIndexFrameFactory dataFrameFactory) {
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
    }

    /**
     * Get D(q, C) - distance from query to centroid.
     */
    public double getDistanceToQueryCentroid() {
        return this.distanceToQueryCentroid;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // Standard open() is not used - use openCluster() instead
        throw HyracksDataException.create(
                new UnsupportedOperationException("Use openCluster(directoryPageId, dqc) for bidirectional cursor"));
    }

    /**
     * Open this cursor for a specific cluster.
     * Finds the pivot point where D(x, C) ≈ D(q, C) and initializes bidirectional cursors.
     *
     * @param firstDirPageId the first directory/metadata page ID for the cluster
     * @param dqc the distance from query vector to centroid D(q, C)
     */
    public void openCluster(long firstDirPageId, double dqc) throws HyracksDataException {
        this.isOpen = true;
        this.distanceToQueryCentroid = dqc;
        this.leftDirPageStack.clear();
        this.rightExhausted = false;
        this.leftExhausted = false;

        // Close any previously open pages
        closeRightDirPage();
        closeLeftDirPage();
        closeRightDataPage();
        closeLeftDataPage();

        // Find pivot by traversing directory pages
        // Accumulate directory page IDs in stack as we go
        long dirPageId = firstDirPageId;

        while (dirPageId != -1) {
            ICachedPage dirPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) dirPageId));
            dirPage.acquireReadLatch();
            IVectorClusteringMetadataFrame dirFrame = createMetadataFrame();
            dirFrame.setPage(dirPage);

            int entryCount = dirFrame.getTupleCount();

            // Check if pivot is in this directory page
            for (int i = 0; i < entryCount; i++) {
                double maxDist = dirFrame.getMaxDistance(i);

                if (dqc <= maxDist) {
                    // Found! This directory page contains the pivot.
                    // Pin it separately for both left and right directions.
                    // Right gets the page we already have pinned.
                    rightDirPageId = dirPageId;
                    rightDirPage = dirPage; // Transfer ownership of this pin
                    rightDirFrame = dirFrame;

                    // Left needs its own independent pin of the same page.
                    leftDirPageId = dirPageId;
                    leftDirPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) dirPageId));
                    leftDirPage.acquireReadLatch();
                    leftDirFrame = createMetadataFrame();
                    leftDirFrame.setPage(leftDirPage);

                    // Initialize cursors at this entry
                    initializeCursorsAtPivot(i, dqc);
                    return;
                }
            }

            // Pivot not in this page - push to stack and continue
            leftDirPageStack.push(dirPageId);

            long nextDirPageId = dirFrame.getNextPage();
            dirPage.releaseReadLatch();
            bufferCache.unpin(dirPage);

            dirPageId = nextDirPageId;
        }

        // If we get here, dqc is larger than all max distances
        // Use the last directory page and last entry
        if (!leftDirPageStack.isEmpty()) {
            long lastDirPageId = leftDirPageStack.pop();

            // Pin for right direction
            rightDirPageId = lastDirPageId;
            rightDirPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) lastDirPageId));
            rightDirPage.acquireReadLatch();
            rightDirFrame = createMetadataFrame();
            rightDirFrame.setPage(rightDirPage);

            // Pin for left direction
            leftDirPageId = lastDirPageId;
            leftDirPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) lastDirPageId));
            leftDirPage.acquireReadLatch();
            leftDirFrame = createMetadataFrame();
            leftDirFrame.setPage(leftDirPage);

            int lastEntry = rightDirFrame.getTupleCount() - 1;
            if (lastEntry >= 0) {
                initializeCursorsAtPivot(lastEntry, dqc);
            } else {
                // Empty metadata page - no data in this cluster for this component
                closeRightDirPage();
                closeLeftDirPage();
                rightExhausted = true;
                leftExhausted = true;
            }
        } else {
            // Empty cluster
            rightExhausted = true;
            leftExhausted = true;
        }
    }

    /**
     * Initialize left and right cursors at the pivot entry.
     */
    private void initializeCursorsAtPivot(int pivotEntryIndex, double dqc) throws HyracksDataException {
        // Get data page for pivot entry (use right dir frame — both point to same page here)
        long pivotDataPageId = rightDirFrame.getDataPagePointer(pivotEntryIndex);

        // Open pivot data page to find exact tuple position
        ICachedPage pivotPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) pivotDataPageId));
        pivotPage.acquireReadLatch();
        VectorClusteringDataFrame pivotFrame = (VectorClusteringDataFrame) createDataFrame();
        pivotFrame.setPage(pivotPage);

        int pivotTupleIndex = pivotFrame.findInsertPosition(dqc);
        int tupleCount = pivotFrame.getTupleCount();

        pivotPage.releaseReadLatch();
        bufferCache.unpin(pivotPage);

        // Initialize RIGHT cursor at pivot position
        rightEntryIndex = pivotEntryIndex;
        if (pivotTupleIndex < tupleCount) {
            rightDataPageId = pivotDataPageId;
            openRightDataPage(pivotDataPageId);
            rightTupleIndex = pivotTupleIndex;
        } else {
            // Pivot is beyond this page, move to next entry
            if (!moveRightToNextEntry()) {
                rightExhausted = true;
            }
        }

        // Initialize LEFT cursor at pivot - 1 position
        leftEntryIndex = pivotEntryIndex;
        if (pivotTupleIndex > 0) {
            leftDataPageId = pivotDataPageId;
            openLeftDataPage(pivotDataPageId);
            leftTupleIndex = pivotTupleIndex - 1;
        } else {
            // Need to go to previous entry
            if (!moveLeftToPreviousEntry()) {
                leftExhausted = true;
            }
        }
    }

    // ==================== Right Direction Methods ====================

    /**
     * Check if there are more tuples in the right direction.
     */
    public boolean hasNextRight() throws HyracksDataException {
        if (!isOpen || rightExhausted) {
            return false;
        }
        return true;
    }

    /**
     * Move to the next tuple in the right direction.
     */
    public void nextRight() throws HyracksDataException {
        if (!hasNextRight()) {
            throw HyracksDataException.create(new IllegalStateException("No more tuples in right direction"));
        }

        // Return current tuple
        rightFrameTuple.resetByTupleIndex(rightDataFrame, rightTupleIndex);
        currentRightTuple = rightFrameTuple;

        // Advance to next position
        rightTupleIndex++;

        // Check if we need to move to next page
        if (rightTupleIndex >= rightDataFrame.getTupleCount()) {
            if (!moveRightToNextEntry()) {
                rightExhausted = true;
            }
        }
    }

    /**
     * Get the current tuple in the right direction.
     */
    public ITupleReference getTupleRight() {
        return currentRightTuple;
    }

    /**
     * Peek at the D(x, C) of the current right tuple without advancing.
     */
    public double peekRightDistanceToCentroid() throws HyracksDataException {
        if (!isOpen || rightExhausted || rightDataFrame == null) {
            return Double.MAX_VALUE;
        }
        if (rightTupleIndex < rightDataFrame.getTupleCount()) {
            return rightDataFrame.getDistanceToCentroid(rightTupleIndex);
        }
        return Double.MAX_VALUE;
    }

    /**
     * Move right cursor to next directory entry.
     * Uses rightDirPage/rightDirFrame exclusively — never touches left's dir page.
     */
    private boolean moveRightToNextEntry() throws HyracksDataException {
        rightEntryIndex++;

        // Check if still within current right directory page
        if (rightEntryIndex < rightDirFrame.getTupleCount()) {
            long dataPageId = rightDirFrame.getDataPagePointer(rightEntryIndex);
            closeRightDataPage();
            openRightDataPage(dataPageId);
            rightTupleIndex = 0;
            return rightDataFrame.getTupleCount() > 0;
        }

        // Need to move to next directory page
        long nextDirPageId = rightDirFrame.getNextPage();

        if (nextDirPageId == -1) {
            return false; // Exhausted
        }

        // Close right's current dir page and open the next one
        closeRightDirPage();

        rightDirPageId = nextDirPageId;
        rightDirPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) nextDirPageId));
        rightDirPage.acquireReadLatch();
        rightDirFrame = createMetadataFrame();
        rightDirFrame.setPage(rightDirPage);

        rightEntryIndex = 0;

        if (rightDirFrame.getTupleCount() == 0) {
            return false;
        }

        long dataPageId = rightDirFrame.getDataPagePointer(rightEntryIndex);
        closeRightDataPage();
        openRightDataPage(dataPageId);
        rightTupleIndex = 0;

        return rightDataFrame.getTupleCount() > 0;
    }

    // ==================== Left Direction Methods ====================

    /**
     * Check if there are more tuples in the left direction.
     */
    public boolean hasNextLeft() throws HyracksDataException {
        if (!isOpen || leftExhausted) {
            return false;
        }
        return true;
    }

    /**
     * Move to the next tuple in the left direction (decreasing D(x, C)).
     */
    public void nextLeft() throws HyracksDataException {
        if (!hasNextLeft()) {
            throw HyracksDataException.create(new IllegalStateException("No more tuples in left direction"));
        }

        // Return current tuple
        leftFrameTuple.resetByTupleIndex(leftDataFrame, leftTupleIndex);
        currentLeftTuple = leftFrameTuple;

        // Move backward
        leftTupleIndex--;

        // Check if we need to move to previous page
        if (leftTupleIndex < 0) {
            if (!moveLeftToPreviousEntry()) {
                leftExhausted = true;
            }
        }
    }

    /**
     * Get the current tuple in the left direction.
     */
    public ITupleReference getTupleLeft() {
        return currentLeftTuple;
    }

    /**
     * Peek at the D(x, C) of the current left tuple without advancing.
     */
    public double peekLeftDistanceToCentroid() throws HyracksDataException {
        if (!isOpen || leftExhausted || leftDataFrame == null) {
            return -1.0;
        }
        if (leftTupleIndex >= 0 && leftTupleIndex < leftDataFrame.getTupleCount()) {
            return leftDataFrame.getDistanceToCentroid(leftTupleIndex);
        }
        return -1.0;
    }

    /**
     * Move left cursor to previous directory entry.
     * Uses leftDirPage/leftDirFrame exclusively — never touches right's dir page.
     */
    private boolean moveLeftToPreviousEntry() throws HyracksDataException {
        leftEntryIndex--;

        // Check if still within current left directory page
        if (leftEntryIndex >= 0) {
            long dataPageId = leftDirFrame.getDataPagePointer(leftEntryIndex);
            closeLeftDataPage();
            openLeftDataPage(dataPageId);
            leftTupleIndex = leftDataFrame.getTupleCount() - 1;
            return leftTupleIndex >= 0;
        }

        // Need to move to previous directory page - pop from stack!
        if (leftDirPageStack.isEmpty()) {
            return false; // Exhausted
        }

        long prevDirPageId = leftDirPageStack.pop();

        // Close left's current dir page and open the previous one
        closeLeftDirPage();

        leftDirPageId = prevDirPageId;
        leftDirPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) prevDirPageId));
        leftDirPage.acquireReadLatch();
        leftDirFrame = createMetadataFrame();
        leftDirFrame.setPage(leftDirPage);

        // Start at last entry of previous directory page
        int lastEntryIndex = leftDirFrame.getTupleCount() - 1;

        if (lastEntryIndex < 0) {
            return false;
        }

        leftEntryIndex = lastEntryIndex;
        long dataPageId = leftDirFrame.getDataPagePointer(leftEntryIndex);

        closeLeftDataPage();
        openLeftDataPage(dataPageId);
        leftTupleIndex = leftDataFrame.getTupleCount() - 1;

        return leftTupleIndex >= 0;
    }

    // ==================== Page Management ====================

    private void openRightDataPage(long pageId) throws HyracksDataException {
        rightDataPageId = pageId;
        rightDataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) pageId));
        rightDataPage.acquireReadLatch();
        rightDataFrame = createDataFrame();
        rightDataFrame.setPage(rightDataPage);
        rightFrameTuple = rightDataFrame.createTupleReference();
    }

    private void closeRightDataPage() throws HyracksDataException {
        if (rightDataPage != null) {
            rightDataPage.releaseReadLatch();
            bufferCache.unpin(rightDataPage);
            rightDataPage = null;
            rightDataFrame = null;
        }
    }

    private void openLeftDataPage(long pageId) throws HyracksDataException {
        leftDataPageId = pageId;
        leftDataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) pageId));
        leftDataPage.acquireReadLatch();
        leftDataFrame = createDataFrame();
        leftDataFrame.setPage(leftDataPage);
        leftFrameTuple = leftDataFrame.createTupleReference();
    }

    private void closeLeftDataPage() throws HyracksDataException {
        if (leftDataPage != null) {
            leftDataPage.releaseReadLatch();
            bufferCache.unpin(leftDataPage);
            leftDataPage = null;
            leftDataFrame = null;
        }
    }

    private void closeRightDirPage() throws HyracksDataException {
        if (rightDirPage != null) {
            rightDirPage.releaseReadLatch();
            bufferCache.unpin(rightDirPage);
            rightDirPage = null;
            rightDirFrame = null;
        }
    }

    private void closeLeftDirPage() throws HyracksDataException {
        if (leftDirPage != null) {
            leftDirPage.releaseReadLatch();
            bufferCache.unpin(leftDirPage);
            leftDirPage = null;
            leftDirFrame = null;
        }
    }

    // ==================== Frame Factory Methods ====================

    private IVectorClusteringMetadataFrame createMetadataFrame() {
        if (metadataFrameFactory == null) {
            throw new IllegalStateException("Metadata frame factory not set");
        }
        return (IVectorClusteringMetadataFrame) metadataFrameFactory.createFrame();
    }

    private IVectorClusteringDataFrame createDataFrame() {
        if (dataFrameFactory == null) {
            throw new IllegalStateException("Data frame factory not set");
        }
        return (IVectorClusteringDataFrame) dataFrameFactory.createFrame();
    }

    // ==================== IIndexCursor Interface ====================

    @Override
    public boolean hasNext() throws HyracksDataException {
        return hasNextRight() || hasNextLeft();
    }

    @Override
    public void next() throws HyracksDataException {
        throw HyracksDataException
                .create(new UnsupportedOperationException("Use nextRight() or nextLeft() for bidirectional cursor"));
    }

    @Override
    public ITupleReference getTuple() {
        throw new UnsupportedOperationException("Use getTupleRight() or getTupleLeft() for bidirectional cursor");
    }

    @Override
    public void close() throws HyracksDataException {
        if (isOpen) {
            closeRightDataPage();
            closeLeftDataPage();
            closeRightDirPage();
            closeLeftDirPage();
        }
        isOpen = false;
        leftDirPageStack.clear();
        currentRightTuple = null;
        currentLeftTuple = null;
    }

    @Override
    public void destroy() throws HyracksDataException {
        close();
    }
}
