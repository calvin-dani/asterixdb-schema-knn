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

import java.util.List;

import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;

/**
 * One stack frame for the DFS cluster iterator. A frame represents either an interior page
 * (carrying {@link #sortedChildren} sorted by distance to the query) or a leaf page (carrying
 * {@link #sortedCentroids}). {@link #nextIndex} tracks the next entry to emit.
 * <p>
 * Not thread-safe: {@link #nextIndex} is mutated during iteration; a frame is confined to a single
 * search / operation context.
 */
public class VTreeNavigationFrame {

    public final int pageId;
    public final boolean isLeaf;
    /** Children sorted by distance to query; non-null iff {@code !isLeaf}. */
    public final List<VTreeChildCentroid> sortedChildren;
    /** Leaf centroids sorted by distance to query; non-null iff {@code isLeaf}. */
    public final List<ClusterSearchResult> sortedCentroids;
    /** Index of the next child / centroid to emit. */
    public int nextIndex;

    /** Constructs an interior frame. */
    public VTreeNavigationFrame(int pageId, List<VTreeChildCentroid> sortedChildren) {
        this.pageId = pageId;
        this.isLeaf = false;
        this.sortedChildren = sortedChildren;
        this.sortedCentroids = null;
        this.nextIndex = 0;
    }

    /** Constructs a leaf frame; {@code isLeaf} must be {@code true}. */
    public VTreeNavigationFrame(int pageId, List<ClusterSearchResult> sortedCentroids, boolean isLeaf) {
        this.pageId = pageId;
        this.isLeaf = isLeaf;
        this.sortedChildren = null;
        this.sortedCentroids = sortedCentroids;
        this.nextIndex = 0;
    }

    public boolean hasNext() {
        if (isLeaf) {
            return sortedCentroids != null && nextIndex < sortedCentroids.size();
        } else {
            return sortedChildren != null && nextIndex < sortedChildren.size();
        }
    }

    public VTreeChildCentroid nextChild() {
        if (!isLeaf && hasNext()) {
            return sortedChildren.get(nextIndex++);
        }
        return null;
    }

    public ClusterSearchResult nextCentroid() {
        if (isLeaf && hasNext()) {
            return sortedCentroids.get(nextIndex++);
        }
        return null;
    }
}
