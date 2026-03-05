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

package org.apache.asterix.runtime.operators.hierarchical;

import org.apache.asterix.runtime.operators.HierarchicalClusterId;

/**
 * Helper class to hold centroid data with its parent information for sorting
 * in hierarchical clustering operations.
 */
public class CentroidWithParent {
    public final double[] centroid;
    public final HierarchicalClusterId clusterId;
    public final HierarchicalClusterId parentId;

    public CentroidWithParent(double[] centroid, HierarchicalClusterId clusterId, HierarchicalClusterId parentId) {
        this.centroid = centroid;
        this.clusterId = clusterId;
        this.parentId = parentId;
    }

    public double[] getCentroid() {
        return centroid;
    }

    public HierarchicalClusterId getClusterId() {
        return clusterId;
    }

    public HierarchicalClusterId getParentId() {
        return parentId;
    }
}
