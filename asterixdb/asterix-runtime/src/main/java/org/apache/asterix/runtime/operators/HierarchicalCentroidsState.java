/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
package org.apache.asterix.runtime.operators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

/**
 * State class for storing hierarchical centroids with parent-child relationships.
 * Each centroid is associated with a HierarchicalClusterId that maintains
 * the hierarchy structure.
 */
public class HierarchicalCentroidsState extends AbstractStateObject {

    /**
     * Represents a centroid with its hierarchical cluster ID
     */
    public static class HierarchicalCentroid {
        private final HierarchicalClusterId clusterId;
        private final double[] centroid;

        public HierarchicalCentroid(HierarchicalClusterId clusterId, double[] centroid) {
            this.clusterId = clusterId;
            this.centroid = centroid.clone(); // Defensive copy
        }

        public HierarchicalClusterId getClusterId() {
            return clusterId;
        }

        public double[] getCentroid() {
            return centroid.clone(); // Defensive copy
        }

        public void toBytes(DataOutput out) throws IOException {
            clusterId.toBytes(out);
            out.writeInt(centroid.length);
            for (double value : centroid) {
                out.writeDouble(value);
            }
        }

        public static HierarchicalCentroid fromBytes(DataInput in) throws IOException {
            HierarchicalClusterId clusterId = HierarchicalClusterId.fromBytes(in);
            int length = in.readInt();
            double[] centroid = new double[length];
            for (int i = 0; i < length; i++) {
                centroid[i] = in.readDouble();
            }
            return new HierarchicalCentroid(clusterId, centroid);
        }
    }

    // Map from level to list of centroids at that level
    private final Map<Integer, List<HierarchicalCentroid>> levelCentroids;

    // Map from cluster ID to its children (for quick parent-child lookups)
    private final Map<HierarchicalClusterId, List<HierarchicalClusterId>> childrenMap;

    // Map from cluster ID to its parent (for quick child-parent lookups)
    private final Map<HierarchicalClusterId, HierarchicalClusterId> parentMap;

    public HierarchicalCentroidsState(JobId jobId, PartitionedUUID objectId) {
        super(jobId, objectId);
        this.levelCentroids = new HashMap<>();
        this.childrenMap = new HashMap<>();
        this.parentMap = new HashMap<>();
    }

    /**
     * Adds a centroid to the specified level
     */
    public void addCentroid(int level, HierarchicalClusterId clusterId, double[] centroid) {
        levelCentroids.computeIfAbsent(level, k -> new ArrayList<>())
                .add(new HierarchicalCentroid(clusterId, centroid));
    }

    /**
     * Adds a centroid with parent-child relationship
     */
    public void addCentroid(int level, HierarchicalClusterId clusterId, double[] centroid,
            HierarchicalClusterId parentId) {
        addCentroid(level, clusterId, centroid);

        // Update parent-child relationships
        if (parentId != null) {
            childrenMap.computeIfAbsent(parentId, k -> new ArrayList<>()).add(clusterId);
            parentMap.put(clusterId, parentId);
        }
    }

    /**
     * Gets all centroids at a specific level
     */
    public List<HierarchicalCentroid> getCentroidsAtLevel(int level) {
        return levelCentroids.getOrDefault(level, new ArrayList<>());
    }

    /**
     * Gets all levels in the hierarchy
     */
    public List<Integer> getLevels() {
        return new ArrayList<>(levelCentroids.keySet());
    }

    /**
     * Gets children of a specific cluster
     */
    public List<HierarchicalClusterId> getChildren(HierarchicalClusterId parentId) {
        return childrenMap.getOrDefault(parentId, new ArrayList<>());
    }

    /**
     * Gets parent of a specific cluster
     */
    public HierarchicalClusterId getParent(HierarchicalClusterId clusterId) {
        return parentMap.get(clusterId);
    }

    /**
     * Gets all centroids across all levels
     */
    public List<HierarchicalCentroid> getAllCentroids() {
        List<HierarchicalCentroid> allCentroids = new ArrayList<>();
        for (List<HierarchicalCentroid> levelCentroids : this.levelCentroids.values()) {
            allCentroids.addAll(levelCentroids);
        }
        return allCentroids;
    }

    /**
     * Gets the total number of centroids across all levels
     */
    public int getTotalCentroidCount() {
        return levelCentroids.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Clears all centroids and relationships
     */
    public void clear() {
        levelCentroids.clear();
        childrenMap.clear();
        parentMap.clear();
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        // Write number of levels
        out.writeInt(levelCentroids.size());

        // Write each level
        for (Map.Entry<Integer, List<HierarchicalCentroid>> entry : levelCentroids.entrySet()) {
            out.writeInt(entry.getKey()); // level
            List<HierarchicalCentroid> centroids = entry.getValue();
            out.writeInt(centroids.size()); // number of centroids at this level

            for (HierarchicalCentroid centroid : centroids) {
                centroid.toBytes(out);
            }
        }

        // Write parent-child relationships
        out.writeInt(parentMap.size());
        for (Map.Entry<HierarchicalClusterId, HierarchicalClusterId> entry : parentMap.entrySet()) {
            entry.getKey().toBytes(out);
            entry.getValue().toBytes(out);
        }
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        levelCentroids.clear();
        childrenMap.clear();
        parentMap.clear();

        // Read levels
        int numLevels = in.readInt();
        for (int i = 0; i < numLevels; i++) {
            int level = in.readInt();
            int numCentroids = in.readInt();
            List<HierarchicalCentroid> centroids = new ArrayList<>(numCentroids);

            for (int j = 0; j < numCentroids; j++) {
                centroids.add(HierarchicalCentroid.fromBytes(in));
            }
            levelCentroids.put(level, centroids);
        }

        // Read parent-child relationships
        int numRelationships = in.readInt();
        for (int i = 0; i < numRelationships; i++) {
            HierarchicalClusterId child = HierarchicalClusterId.fromBytes(in);
            HierarchicalClusterId parent = HierarchicalClusterId.fromBytes(in);
            parentMap.put(child, parent);
            childrenMap.computeIfAbsent(parent, k -> new ArrayList<>()).add(child);
        }
    }
}
