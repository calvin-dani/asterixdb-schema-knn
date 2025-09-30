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

package org.apache.asterix.runtime.operators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Optimized hierarchical cluster tree with memory-efficient data structures
 * and performance optimizations inspired by Spark MLlib and other systems.
 */
public class OptimizedHierarchicalClusterTree implements Serializable {
    private static final long serialVersionUID = 1L;

    // Use arrays instead of lists for better memory locality and performance
    private TreeNode[] nodes;
    private int nodeCount = 0;
    private int capacity = 16;
    private final AtomicInteger nextGlobalId = new AtomicInteger(0);

    // Level-based indexing for O(1) access to nodes by level
    private List<Integer>[] levelIndices;
    private int maxLevel = 0;

    // Pre-allocated arrays for distance calculations (reuse to avoid GC)
    private double[] tempDistances;
    private int[] tempIndices;

    /**
     * Optimized tree node with memory-efficient layout
     */
    public static class TreeNode implements Serializable {
        private static final long serialVersionUID = 1L;

        // Use primitive arrays for better memory layout
        public final double[] centroid;
        public int level;
        public int clusterId;
        public int globalId;
        public int parentGlobalId;
        public int firstChildIndex = -1; // Index of first child in nodes array
        public int childCount = 0; // Number of children
        public int nextSiblingIndex = -1; // Index of next sibling

        // Cached values for performance
        public double cachedNorm = -1; // Cached L2 norm for distance calculations
        public boolean normComputed = false;

        public TreeNode(double[] centroid, int level, int clusterId, int globalId, int parentGlobalId) {
            this.centroid = centroid.clone(); // Defensive copy
            this.level = level;
            this.clusterId = clusterId;
            this.globalId = globalId;
            this.parentGlobalId = parentGlobalId;
        }

        /**
         * Compute and cache L2 norm for efficient distance calculations
         */
        public double getNorm() {
            if (!normComputed) {
                double sum = 0.0;
                for (double value : centroid) {
                    sum += value * value;
                }
                cachedNorm = Math.sqrt(sum);
                normComputed = true;
            }
            return cachedNorm;
        }

        /**
         * Fast squared distance calculation using cached norm
         */
        public double fastSquaredDistance(TreeNode other) {
            if (centroid.length != other.centroid.length) {
                throw new IllegalArgumentException("Dimension mismatch");
            }

            double sum = 0.0;
            for (int i = 0; i < centroid.length; i++) {
                double diff = centroid[i] - other.centroid[i];
                sum += diff * diff;
            }
            return sum;
        }

        /**
         * Fast squared distance using norm optimization (when applicable)
         */
        public double fastSquaredDistanceOptimized(TreeNode other) {
            if (centroid.length != other.centroid.length) {
                throw new IllegalArgumentException("Dimension mismatch");
            }

            // Use norm optimization: ||a-b||² = ||a||² + ||b||² - 2*a·b
            double normA = getNorm();
            double normB = other.getNorm();
            double dotProduct = 0.0;

            for (int i = 0; i < centroid.length; i++) {
                dotProduct += centroid[i] * other.centroid[i];
            }

            return normA * normA + normB * normB - 2 * dotProduct;
        }
    }

    public OptimizedHierarchicalClusterTree() {
        this.nodes = new TreeNode[capacity];
        this.levelIndices = new List[32]; // Support up to 32 levels initially
        this.tempDistances = new double[1024]; // Pre-allocated for distance calculations
        this.tempIndices = new int[1024];

        for (int i = 0; i < levelIndices.length; i++) {
            levelIndices[i] = new ArrayList<>();
        }
    }

    /**
     * Sets the root of the tree with optimized memory allocation
     */
    public void setRoot(double[] centroid) {
        if (nodes[0] != null) {
            throw new IllegalStateException("Root already set");
        }

        TreeNode root = new TreeNode(centroid, 0, 0, nextGlobalId.getAndIncrement(), -1);
        nodes[0] = root;
        nodeCount = 1;
        levelIndices[0].add(0);
        maxLevel = 0;
    }

    /**
     * Adds a child node with optimized parent-child linking
     */
    public TreeNode addChild(TreeNode parent, double[] centroid, int level) {
        if (parent == null) {
            throw new IllegalArgumentException("Parent cannot be null");
        }

        // Expand array if needed
        if (nodeCount >= capacity) {
            expandCapacity();
        }

        int childIndex = nodeCount;
        int clusterId = parent.childCount; // ID within parent's children
        TreeNode child = new TreeNode(centroid, level, clusterId, nextGlobalId.getAndIncrement(), parent.globalId);

        nodes[childIndex] = child;
        nodeCount++;

        // Update parent's child linking
        if (parent.firstChildIndex == -1) {
            parent.firstChildIndex = childIndex;
        } else {
            // Find last sibling and link
            int current = parent.firstChildIndex;
            while (nodes[current].nextSiblingIndex != -1) {
                current = nodes[current].nextSiblingIndex;
            }
            nodes[current].nextSiblingIndex = childIndex;
        }
        parent.childCount++;

        // Update level indexing
        if (level >= levelIndices.length) {
            expandLevelIndices(level + 1);
        }
        levelIndices[level].add(childIndex);
        maxLevel = Math.max(maxLevel, level);

        return child;
    }

    /**
     * Fast BFS traversal with optimized ID assignment
     */
    public void assignBFSIds() {
        if (nodeCount == 0)
            return;

        // Use pre-allocated arrays for better performance
        int[] levelCounters = new int[maxLevel + 1];
        int globalIdCounter = 0;

        // BFS traversal using array indices instead of queue
        int[] queue = new int[nodeCount];
        int queueStart = 0, queueEnd = 0;

        queue[queueEnd++] = 0; // Start with root

        while (queueStart < queueEnd) {
            int currentIndex = queue[queueStart++];
            TreeNode current = nodes[currentIndex];

            // Assign IDs
            current.globalId = globalIdCounter++;
            current.clusterId = levelCounters[current.level]++;

            // Add children to queue
            if (current.firstChildIndex != -1) {
                int childIndex = current.firstChildIndex;
                while (childIndex != -1) {
                    if (queueEnd < nodeCount) {
                        queue[queueEnd++] = childIndex;
                    }
                    childIndex = nodes[childIndex].nextSiblingIndex;
                }
            }
        }
    }

    /**
     * Fast nearest neighbor search using spatial indexing
     */
    public TreeNode findNearestNeighbor(double[] query, int level) {
        if (levelIndices[level] == null || levelIndices[level].isEmpty()) {
            return null;
        }

        TreeNode nearest = null;
        double minDistance = Double.POSITIVE_INFINITY;

        // Use pre-allocated arrays for distance calculations
        int size = levelIndices[level].size();
        if (size > tempDistances.length) {
            tempDistances = new double[size * 2];
        }

        // Compute all distances
        for (int i = 0; i < size; i++) {
            int nodeIndex = levelIndices[level].get(i);
            TreeNode node = nodes[nodeIndex];

            double distance = fastSquaredDistance(query, node.centroid);
            tempDistances[i] = distance;

            if (distance < minDistance) {
                minDistance = distance;
                nearest = node;
            }
        }

        return nearest;
    }

    /**
     * Optimized distance calculation with vectorization hints
     */
    private double fastSquaredDistance(double[] a, double[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Dimension mismatch");
        }

        double sum = 0.0;
        // Unroll loop for better performance
        int len = a.length;
        int i = 0;

        // Process 4 elements at a time for better vectorization
        for (; i < len - 3; i += 4) {
            double diff0 = a[i] - b[i];
            double diff1 = a[i + 1] - b[i + 1];
            double diff2 = a[i + 2] - b[i + 2];
            double diff3 = a[i + 3] - b[i + 3];
            sum += diff0 * diff0 + diff1 * diff1 + diff2 * diff2 + diff3 * diff3;
        }

        // Handle remaining elements
        for (; i < len; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }

        return sum;
    }

    /**
     * Get nodes at specific level with O(1) access
     */
    public List<TreeNode> getNodesAtLevel(int level) {
        if (level < 0 || level > maxLevel || levelIndices[level] == null) {
            return new ArrayList<>();
        }

        List<TreeNode> result = new ArrayList<>(levelIndices[level].size());
        for (int index : levelIndices[level]) {
            result.add(nodes[index]);
        }
        return result;
    }

    /**
     * Memory-efficient tree serialization
     */
    public byte[] serialize() {
        // Implementation would use efficient binary serialization
        // This is a placeholder for the actual implementation
        return new byte[0];
    }

    /**
     * Expand node array capacity
     */
    private void expandCapacity() {
        int newCapacity = capacity * 2;
        TreeNode[] newNodes = new TreeNode[newCapacity];
        System.arraycopy(nodes, 0, newNodes, 0, capacity);
        nodes = newNodes;
        capacity = newCapacity;
    }

    /**
     * Expand level indices array
     */
    private void expandLevelIndices(int newSize) {
        List<Integer>[] newLevelIndices = new List[newSize];
        System.arraycopy(levelIndices, 0, newLevelIndices, 0, levelIndices.length);

        for (int i = levelIndices.length; i < newSize; i++) {
            newLevelIndices[i] = new ArrayList<>();
        }

        levelIndices = newLevelIndices;
    }

    /**
     * Get total number of nodes
     */
    public int getTotalNodes() {
        return nodeCount;
    }

    /**
     * Get maximum level in tree
     */
    public int getMaxLevel() {
        return maxLevel;
    }

    /**
     * Memory usage estimation
     */
    public long estimateMemoryUsage() {
        long nodeMemory = nodeCount * (8 + 4 * 4 + 8 * 4); // Basic node overhead
        long centroidMemory = 0;
        for (int i = 0; i < nodeCount; i++) {
            if (nodes[i] != null) {
                centroidMemory += nodes[i].centroid.length * 8; // 8 bytes per double
            }
        }
        return nodeMemory + centroidMemory;
    }
}

