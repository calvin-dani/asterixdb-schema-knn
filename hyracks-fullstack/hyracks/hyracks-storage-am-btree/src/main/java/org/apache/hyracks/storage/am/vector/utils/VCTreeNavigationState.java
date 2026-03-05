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
package org.apache.hyracks.storage.am.vector.utils;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * State for iterative DFS navigation through the tree.
 * Maintains the navigation stack for finding clusters in distance order.
 * Also tracks visited centroids to avoid duplicates when using level-wise + DFS fallback.
 */
public class VCTreeNavigationState {
    public final IBufferCache bufferCache;
    public final int fileId;
    public final int rootPageId;
    public final ITreeIndexFrameFactory interiorFrameFactory;
    public final ITreeIndexFrameFactory leafFrameFactory;
    public final double[] queryVector;
    public final Deque<VCTreeNavigationFrame> stack;
    public boolean initialized;

    // Visited centroid tracking (can be shared across LSM components)
    private Set<Integer> visitedCentroidIds;

    public VCTreeNavigationState(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector) {
        this(bufferCache, fileId, rootPageId, interiorFrameFactory, leafFrameFactory, queryVector, new HashSet<>());
    }

    /**
     * Constructor with shared visited set.
     * Use this when sharing visited tracking across multiple LSM components.
     */
    public VCTreeNavigationState(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory, double[] queryVector,
            Set<Integer> sharedVisitedSet) {
        this.bufferCache = bufferCache;
        this.fileId = fileId;
        this.rootPageId = rootPageId;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.queryVector = queryVector;
        this.stack = new ArrayDeque<>();
        this.initialized = false;
        this.visitedCentroidIds = sharedVisitedSet != null ? sharedVisitedSet : new HashSet<>();
    }

    /**
     * Check if a centroid has already been visited.
     */
    public boolean isVisited(int centroidId) {
        return visitedCentroidIds.contains(centroidId);
    }

    /**
     * Mark a centroid as visited.
     */
    public void markVisited(int centroidId) {
        visitedCentroidIds.add(centroidId);
    }

    /**
     * Set the visited set (for sharing with other components).
     */
    public void setVisitedSet(Set<Integer> visitedSet) {
        this.visitedCentroidIds = visitedSet;
    }

    /**
     * Get the visited set.
     */
    public Set<Integer> getVisitedSet() {
        return visitedCentroidIds;
    }

    /**
     * Get the count of visited centroids.
     */
    public int getVisitedCount() {
        return visitedCentroidIds.size();
    }
}
