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
 */
public class ClusterSearchResult {
    public final int leafPageId;
    public final int clusterIndex;
    public final double[] centroid;
    public final double distance;
    public final int centroidId;

    ClusterSearchResult(int leafPageId, int clusterIndex, double[] centroid, double distance, int centroidId) {
        this.leafPageId = leafPageId;
        this.clusterIndex = clusterIndex;
        this.centroid = centroid;
        this.distance = distance;
        this.centroidId = centroidId;
    }

    /**
     * Factory method to create ClusterSearchResult instances.
     * 
     * @param leafPageId The leaf page ID
     * @param clusterIndex The cluster index within the page
     * @param centroid The centroid vector
     * @param distance The distance to the centroid
     * @param centroidId The centroid ID
     * @return New ClusterSearchResult instance
     */
    public static ClusterSearchResult create(int leafPageId, int clusterIndex, double[] centroid, double distance,
            int centroidId) {
        return new ClusterSearchResult(leafPageId, clusterIndex, centroid, distance, centroidId);
    }
}