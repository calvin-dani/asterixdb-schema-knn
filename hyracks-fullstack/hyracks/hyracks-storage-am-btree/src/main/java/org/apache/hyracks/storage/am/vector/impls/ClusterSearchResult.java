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

/**
 * Result of cluster search operation.
 * Contains all information needed to open a cluster without re-traversing the tree.
 */
public class ClusterSearchResult {
    public final int leafPageId;
    public final int clusterIndex;
    public final double[] centroid;
    public final double distance;
    public final int centroidId;
    public final long directoryPageId;  // Direct pointer to cluster's directory/metadata page

    ClusterSearchResult(int leafPageId, int clusterIndex, double[] centroid, double distance, int centroidId,
            long directoryPageId) {
        this.leafPageId = leafPageId;
        this.clusterIndex = clusterIndex;
        this.centroid = centroid;
        this.distance = distance;
        this.centroidId = centroidId;
        this.directoryPageId = directoryPageId;
    }

    /**
     * Factory method to create ClusterSearchResult instances.
     * 
     * @param leafPageId The leaf page ID
     * @param clusterIndex The cluster index within the page
     * @param centroid The centroid vector
     * @param distance The distance to the centroid
     * @param centroidId The centroid ID
     * @param directoryPageId The directory/metadata page ID for this cluster
     * @return New ClusterSearchResult instance
     */
    public static ClusterSearchResult create(int leafPageId, int clusterIndex, double[] centroid, double distance,
            int centroidId, long directoryPageId) {
        return new ClusterSearchResult(leafPageId, clusterIndex, centroid, distance, centroidId, directoryPageId);
    }

    /**
     * Legacy factory method for backwards compatibility.
     * Creates a result with directoryPageId = -1 (unknown).
     */
    public static ClusterSearchResult create(int leafPageId, int clusterIndex, double[] centroid, double distance,
            int centroidId) {
        return new ClusterSearchResult(leafPageId, clusterIndex, centroid, distance, centroidId, -1);
    }

    /**
     * Check if this result has a valid directory page ID.
     * @return true if directoryPageId is valid (> 0)
     */
    public boolean hasDirectoryPageId() {
        return directoryPageId > 0;
    }
}