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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;

/**
 * Sequential cluster selection strategy for merge operations.
 *
 * This strategy iterates through all clusters in order (0 → 1 → 2 → ...),
 * without any early termination or distance-based selection. It is used for
 * merge operations where we need to process ALL data from all components.
 *
 * Key differences from NprobeClusterSelectionStrategy:
 * - No level-wise or DFS cluster selection
 * - No nprobe/K-based early termination (shouldStopAdvancing always returns false)
 * - Sequential cluster iteration via first cursor's advanceToNextCluster()
 * - All components advance through clusters TOGETHER in lock-step
 */
public class SequentialClusterSelectionStrategy implements IClusterSelectionStrategy {

    private static final Logger LOGGER = LogManager.getLogger();

    // First component cursor for sequential cluster iteration
    private VectorClusteringSearchCursor firstCursor;

    // Empty visited set (no deduplication needed in sequential mode)
    private final Set<Integer> visitedCentroidIds = new HashSet<>();

    // Track whether we've started iterating
    private boolean initialized;

    public SequentialClusterSelectionStrategy() {
        this.initialized = false;
    }

    @Override
    public void initialize(VectorClusteringTree vcTree, double[] queryVector, IVectorDistanceFunction distFunc, int k)
            throws HyracksDataException {
        // Sequential strategy doesn't need tree/query/distance info
        // Cluster iteration is handled by the underlying cursor in full-scan mode
        this.initialized = true;
    }

    @Override
    public ClusterSearchResult getNextCluster() throws HyracksDataException {
        if (firstCursor == null) {
            return null;
        }

        // Check if there are more clusters available
        if (!firstCursor.hasMoreClusters()) {
            LOGGER.trace("No more clusters");
            return null;
        }

        // Advance the first cursor to the next cluster (sequential iteration)
        boolean advanced = firstCursor.advanceToNextCluster();
        if (!advanced) {
            LOGGER.trace("Failed to advance to next cluster");
            return null;
        }

        // Get the cluster result after advancement
        ClusterSearchResult next = firstCursor.getCurrentClusterResult();

        if (next != null) {
            LOGGER.trace("Next cluster: cid={}, dirPage={}", next.centroidId, next.directoryPageId);
        } else {
            LOGGER.trace("advanceToNextCluster succeeded but getCurrentClusterResult is null");
        }

        return next;
    }

    @Override
    public boolean hasMoreClusters() {
        return firstCursor != null && firstCursor.hasMoreClusters();
    }

    @Override
    public boolean shouldStopAdvancing(int minClustersExplored, int resultsCollected) {
        // Never stop early in merge mode - must process ALL data
        return false;
    }

    @Override
    public Set<Integer> getVisitedCentroidIds() {
        // Return empty set - no deduplication needed in sequential mode
        // Each cluster is visited exactly once in order
        return visitedCentroidIds;
    }

    @Override
    public void setFirstCursorForDFS(VectorClusteringSearchCursor firstCursor) {
        this.firstCursor = firstCursor;
    }

    @Override
    public ClusterSearchResult getFirstCluster() {
        // First cluster is already opened by the cursor during doOpen()
        // Return current cluster result from first cursor
        return firstCursor != null ? firstCursor.getCurrentClusterResult() : null;
    }

    @Override
    public void reset() {
        this.initialized = false;
        this.visitedCentroidIds.clear();
    }

    @Override
    public int getLevelWiseClusterCount() {
        // No level-wise in sequential mode
        return 0;
    }

    @Override
    public boolean isLevelWisePhaseComplete() {
        // Always "complete" since we don't use level-wise
        return true;
    }
}