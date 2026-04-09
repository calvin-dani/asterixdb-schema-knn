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

/**
 * Represents a leaf centroid with its metadata.
 * Used for iterating through centroids in a leaf page.
 */
public class VTreeLeafCentroid {
    public final int centroidId;
    public final double distance;
    public final int tupleIndex;
    public final int pageId;
    public final double[] centroid;
    public final long directoryPageId; // Direct pointer to cluster's directory page
    public final double quantizedDistance; // D(q̃, C̃) — NaN if not computed

    public VTreeLeafCentroid(int centroidId, double distance, int tupleIndex, int pageId, double[] centroid,
            long directoryPageId, double quantizedDistance) {
        this.centroidId = centroidId;
        this.distance = distance;
        this.tupleIndex = tupleIndex;
        this.pageId = pageId;
        this.centroid = centroid;
        this.directoryPageId = directoryPageId;
        this.quantizedDistance = quantizedDistance;
    }

    public VTreeLeafCentroid(int centroidId, double distance, int tupleIndex, int pageId, double[] centroid,
            long directoryPageId) {
        this(centroidId, distance, tupleIndex, pageId, centroid, directoryPageId, Double.NaN);
    }

    // Legacy constructor for backwards compatibility
    public VTreeLeafCentroid(int centroidId, double distance, int tupleIndex, int pageId, double[] centroid) {
        this(centroidId, distance, tupleIndex, pageId, centroid, -1, Double.NaN);
    }
}
