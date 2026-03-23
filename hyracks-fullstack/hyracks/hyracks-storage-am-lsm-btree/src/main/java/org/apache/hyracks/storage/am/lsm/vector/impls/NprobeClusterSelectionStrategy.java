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
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVectorQuantizer;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.utils.VCTreeNavigationUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Nprobe-based cluster selection strategy.
 *
 * Explores clusters in two phases:
 * 1. Level-wise: Computes clusters within epsilon distance threshold, sorted globally by distance
 * 2. DFS fallback: When level-wise exhausted, uses DFS backtracking from first component
 *
 * Stopping condition: minClustersExplored >= nprobe AND resultsCollected >= K
 */
public class NprobeClusterSelectionStrategy implements IClusterSelectionStrategy {
    private static final Logger LOGGER = LogManager.getLogger();

    // Parameters
    private final double minProbeFraction;
    private final double epsilon;
    private int nprobe; // Computed from minProbeFraction * totalLeafClusters at initialize time
    private int K;

    // Level-wise state
    private List<ClusterSearchResult> globalLevelWiseClusters;
    private int globalClusterIndex;
    private boolean levelWisePhaseComplete;

    // Shared state for cross-component deduplication
    private Set<Integer> visitedCentroidIds;

    // Quantizer state for computing quantized D(q,C) in ClusterSearchResult
    private double[] quantizedQueryVector;
    private IVectorQuantizer quantizer;

    // For DFS fallback
    private VectorClusteringSearchCursor firstCursor;

    public NprobeClusterSelectionStrategy(double minProbeFraction, double epsilon) {
        this.minProbeFraction = minProbeFraction;
        this.epsilon = epsilon;
        this.nprobe = 1; // Will be computed in initialize() from minProbeFraction * totalLeafClusters
        this.visitedCentroidIds = new HashSet<>();
    }

    @Override
    public void setQuantizer(double[] quantizedQueryVector, IVectorQuantizer quantizer) {
        this.quantizedQueryVector = quantizedQueryVector;
        this.quantizer = quantizer;
    }

    @Override
    public void initialize(VectorClusteringTree vcTree, double[] queryVector, IVectorDistanceFunction distFunc, int k)
            throws HyracksDataException {
        this.K = k;
        this.globalClusterIndex = 0;
        this.levelWisePhaseComplete = false;

        // Compute level-wise clusters using VCTreeNavigationUtils
        if (queryVector != null && epsilon > 0.0 && vcTree != null) {
            try {
                globalLevelWiseClusters = VCTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort(
                        vcTree.getNavigationBufferCache(), vcTree.getNavigationFileId(),
                        vcTree.getNavigationRootPageId(), vcTree.getInteriorFrameFactory(),
                        vcTree.getLeafFrameFactory(), queryVector, distFunc, epsilon, quantizedQueryVector, quantizer);

                // Compute nprobe from minProbeFraction * totalLeafClusters
                int totalLeafClusters = globalLevelWiseClusters != null ? globalLevelWiseClusters.size() : 1;
                this.nprobe = Math.max(1, (int) Math.floor(totalLeafClusters * minProbeFraction));

                // Mark first cluster as visited and start getNextCluster() from index 1
                // The cursor handles index 0 separately via getFirstCluster()
                if (globalLevelWiseClusters != null && !globalLevelWiseClusters.isEmpty()) {
                    visitedCentroidIds.add(globalLevelWiseClusters.get(0).centroidId);
                    globalClusterIndex = 1; // Skip first cluster in getNextCluster()
                }

                LOGGER.log(Level.TRACE,
                        String.format(
                                "[NprobeStrategy] Computed %d level-wise clusters with epsilon=%.2f, minProbeFraction=%.2f, nprobe=%d",
                                totalLeafClusters, epsilon, minProbeFraction, nprobe));
            } catch (Exception e) {
                LOGGER.log(Level.TRACE,
                        String.format("[NprobeStrategy] Failed to compute level-wise clusters: %s", e.getMessage()));
                globalLevelWiseClusters = null;
            }
        }
    }

    @Override
    public ClusterSearchResult getNextCluster() throws HyracksDataException {
        // Phase 1: Level-wise clusters (pre-computed, globally sorted by distance)
        if (!levelWisePhaseComplete && globalLevelWiseClusters != null
                && globalClusterIndex < globalLevelWiseClusters.size()) {

            ClusterSearchResult nextCluster = globalLevelWiseClusters.get(globalClusterIndex);
            globalClusterIndex++;

            // Mark visited for DFS fallback deduplication
            visitedCentroidIds.add(nextCluster.centroidId);

            LOGGER.log(Level.TRACE,
                    String.format("[NprobeStrategy] Level-wise: cluster %d/%d (cid=%d, distance=%.4f, dirPage=%d)",
                            globalClusterIndex, globalLevelWiseClusters.size(), nextCluster.centroidId,
                            nextCluster.distance, nextCluster.directoryPageId));

            if (globalClusterIndex >= globalLevelWiseClusters.size()) {
                levelWisePhaseComplete = true;
                LOGGER.log(Level.TRACE, String.format(
                        "[NprobeStrategy] Level-wise phase COMPLETE. Visited centroids: %s", visitedCentroidIds));
            }

            return nextCluster;
        }

        // Phase 2: DFS fallback - get next from first component's DFS
        levelWisePhaseComplete = true;

        if (firstCursor == null) {
            return null;
        }

        ClusterSearchResult next = firstCursor.findNextClusterDFS();

        if (next == null) {
            LOGGER.log(Level.TRACE, String.format(
                    "[NprobeStrategy] DFS exhausted, no more clusters. Visited centroids: %s", visitedCentroidIds));
            return null;
        }

        LOGGER.log(Level.TRACE,
                String.format("[NprobeStrategy] DFS fallback: cluster cid=%d, distance=%.4f, dirPage=%d",
                        next.centroidId, next.distance, next.directoryPageId));

        return next;
    }

    @Override
    public boolean hasMoreClusters() {
        // Check level-wise first
        if (!levelWisePhaseComplete && globalLevelWiseClusters != null
                && globalClusterIndex < globalLevelWiseClusters.size()) {
            return true;
        }

        // Check DFS via first cursor
        if (firstCursor != null) {
            return firstCursor.hasMoreClusters();
        }

        return false;
    }

    @Override
    public boolean shouldStopAdvancing(int minClustersExplored, int resultsCollected) {
        // Stop when: explored at least nprobe clusters AND collected at least K results
        return minClustersExplored >= nprobe && resultsCollected >= K;
    }

    @Override
    public Set<Integer> getVisitedCentroidIds() {
        return visitedCentroidIds;
    }

    @Override
    public void setFirstCursorForDFS(VectorClusteringSearchCursor firstCursor) {
        this.firstCursor = firstCursor;
    }

    @Override
    public ClusterSearchResult getFirstCluster() {
        if (globalLevelWiseClusters != null && !globalLevelWiseClusters.isEmpty()) {
            return globalLevelWiseClusters.get(0);
        }
        return null;
    }

    @Override
    public void reset() {
        this.globalLevelWiseClusters = null;
        this.globalClusterIndex = 0;
        this.levelWisePhaseComplete = false;
        this.visitedCentroidIds.clear();
        this.firstCursor = null;
        this.quantizedQueryVector = null;
        this.quantizer = null;
    }

    @Override
    public int getLevelWiseClusterCount() {
        return globalLevelWiseClusters != null ? globalLevelWiseClusters.size() : 0;
    }

    @Override
    public boolean isLevelWisePhaseComplete() {
        return levelWisePhaseComplete;
    }

    // Getters for logging/debugging
    public int getNprobe() {
        return nprobe;
    }

    public double getMinProbeFraction() {
        return minProbeFraction;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public int getK() {
        return K;
    }
}
