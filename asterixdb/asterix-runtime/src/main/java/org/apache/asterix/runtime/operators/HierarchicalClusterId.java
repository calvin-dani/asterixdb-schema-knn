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
import java.util.Objects;

/**
 * Represents a hierarchical cluster ID that maintains parent-child relationships
 * in a K-means++ clustering hierarchy.
 */
public class HierarchicalClusterId implements Comparable<HierarchicalClusterId> {

    private final int level;
    private final int clusterId;
    private final int parentClusterId;
    private final long globalId;

    // Static counter for generating unique global IDs
    private static long globalIdCounter = 0;

    /**
     * Constructor for root level clusters (no parent)
     */
    public HierarchicalClusterId(int level, int clusterId) {
        this.level = level;
        this.clusterId = clusterId;
        this.parentClusterId = -1; // -1 indicates no parent
        this.globalId = generateGlobalId();
    }

    /**
     * Constructor for child clusters with parent reference
     */
    public HierarchicalClusterId(int level, int clusterId, int parentClusterId) {
        this.level = level;
        this.clusterId = clusterId;
        this.parentClusterId = parentClusterId;
        this.globalId = generateGlobalId();
    }

    /**
     * Constructor for deserialization
     */
    public HierarchicalClusterId(int level, int clusterId, int parentClusterId, long globalId) {
        this.level = level;
        this.clusterId = clusterId;
        this.parentClusterId = parentClusterId;
        this.globalId = globalId;
    }

    private static synchronized long generateGlobalId() {
        return globalIdCounter++;
    }

    public int getLevel() {
        return level;
    }

    public int getClusterId() {
        return clusterId;
    }

    public int getParentClusterId() {
        return parentClusterId;
    }

    public long getGlobalId() {
        return globalId;
    }

    public boolean hasParent() {
        return parentClusterId != -1;
    }

    public boolean isRoot() {
        return level == 0;
    }

    /**
     * Creates a child cluster ID for the next level
     */
    public HierarchicalClusterId createChild(int childClusterId) {
        return new HierarchicalClusterId(level + 1, childClusterId, this.clusterId);
    }

    /**
     * Serializes the cluster ID to a DataOutput stream
     */
    public void toBytes(DataOutput out) throws IOException {
        out.writeInt(level);
        out.writeInt(clusterId);
        out.writeInt(parentClusterId);
        out.writeLong(globalId);
    }

    /**
     * Deserializes the cluster ID from a DataInput stream
     */
    public static HierarchicalClusterId fromBytes(DataInput in) throws IOException {
        int level = in.readInt();
        int clusterId = in.readInt();
        int parentClusterId = in.readInt();
        long globalId = in.readLong();
        return new HierarchicalClusterId(level, clusterId, parentClusterId, globalId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        HierarchicalClusterId that = (HierarchicalClusterId) obj;
        return level == that.level && clusterId == that.clusterId && parentClusterId == that.parentClusterId
                && globalId == that.globalId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, clusterId, parentClusterId, globalId);
    }

    @Override
    public int compareTo(HierarchicalClusterId other) {
        // First compare by level, then by cluster ID within level
        int levelCompare = Integer.compare(this.level, other.level);
        if (levelCompare != 0) {
            return levelCompare;
        }
        return Integer.compare(this.clusterId, other.clusterId);
    }

    @Override
    public String toString() {
        return String.format("ClusterId{level=%d, id=%d, parent=%d, global=%d}", level, clusterId, parentClusterId,
                globalId);
    }
}
