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

/**
 * Stack frame for DFS navigation through the tree.
 * Each frame represents a node (interior or leaf level) in the traversal path.
 */
public class VTreeNavigationFrame {
    public final int pageId;
    public final boolean isLeaf;
    public final List<VTreeChildCentroid> sortedChildren; // For interior nodes
    public final List<VTreeLeafCentroid> sortedCentroids; // For leaf nodes
    public int nextIndex; // Next child/centroid to explore

    // Constructor for interior frame
    public VTreeNavigationFrame(int pageId, List<VTreeChildCentroid> sortedChildren) {
        this.pageId = pageId;
        this.isLeaf = false;
        this.sortedChildren = sortedChildren;
        this.sortedCentroids = null;
        this.nextIndex = 0;
    }

    // Constructor for leaf frame
    public VTreeNavigationFrame(int pageId, List<VTreeLeafCentroid> sortedCentroids, boolean isLeaf) {
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

    public VTreeLeafCentroid nextCentroid() {
        if (isLeaf && hasNext()) {
            return sortedCentroids.get(nextIndex++);
        }
        return null;
    }
}
