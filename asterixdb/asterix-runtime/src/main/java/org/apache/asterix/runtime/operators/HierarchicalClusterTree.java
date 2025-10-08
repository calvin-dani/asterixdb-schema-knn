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
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Tree structure for hierarchical clusters with BFS-based ID assignment.
 * This eliminates the need for expensive sorting operations.
 */
public class HierarchicalClusterTree implements Serializable {
    private static final long serialVersionUID = 1L;

    private TreeNode root;
    private int nextGlobalId = 0;
    private int totalNodes = 0;

    /**
     * Tree node representing a cluster in the hierarchy
     */
    public static class TreeNode implements Serializable {
        private static final long serialVersionUID = 1L;

        private double[] centroid;
        public int level;
        public int clusterId; // ID within the level
        public long globalId; // Unique global ID
        public long parentGlobalId; // Global ID of parent (-1 for root)
        private List<TreeNode> children;
        private TreeNode parent;

        public TreeNode(double[] centroid, int level, int clusterId, long globalId, long parentGlobalId) {
            this.centroid = centroid;
            this.level = level;
            this.clusterId = clusterId;
            this.globalId = globalId;
            this.parentGlobalId = parentGlobalId;
            this.children = new ArrayList<>();
        }

        public void addChild(TreeNode child) {
            children.add(child);
            child.parent = this;
        }

        public boolean isLeaf() {
            return children.isEmpty();
        }

        public boolean isRoot() {
            return parent == null;
        }

        public int getDepth() {
            int depth = 0;
            TreeNode current = this;
            while (current.parent != null) {
                depth++;
                current = current.parent;
            }
            return depth;
        }

        // Getters
        public double[] getCentroid() {
            return centroid;
        }

        public int getLevel() {
            return level;
        }

        public int getClusterId() {
            return clusterId;
        }

        public long getGlobalId() {
            return globalId;
        }

        public long getParentGlobalId() {
            return parentGlobalId;
        }

        public List<TreeNode> getChildren() {
            return children;
        }

        public TreeNode getParent() {
            return parent;
        }

        @Override
        public String toString() {
            return String.format("TreeNode{level=%d, clusterId=%d, globalId=%d, parentGlobalId=%d, children=%d}", level,
                    clusterId, globalId, parentGlobalId, children.size());
        }
    }

    /**
     * Output mode for the hierarchical clustering
     */
    public enum OutputMode {
        LEVEL_BY_LEVEL, // Output each level as it's built (with sorting)
        COMPLETE_TREE // Build complete tree first, then output with BFS IDs
    }

    public HierarchicalClusterTree() {
        this.root = null;
    }

    /**
     * Sets the root of the tree
     */
    public void setRoot(double[] centroid) {
        this.root = new TreeNode(centroid, 0, 0, nextGlobalId++, -1);
        this.totalNodes = 1;
    }

    /**
     * Sets the root of the tree to an existing node
     * 
     * FIX: This method was added to support the tree building fix where parent nodes
     * need to be connected to the tree structure. The original setRoot(double[]) method
     * creates a new node, but we need to set an existing node as the root.
     */
    public void setRoot(TreeNode node) {
        this.root = node;
    }

    /**
     * Adds multiple root nodes (Level 0 centroids) to the tree.
     * This is used for the initial leaf nodes that don't have parents yet.
     * 
     * @param centroids List of centroids to add as root nodes
     * @param level Level number (should be 0 for leaf nodes)
     * @return List of created TreeNode objects
     */
    public List<TreeNode> addRootNodes(List<double[]> centroids, int level) {
        List<TreeNode> rootNodes = new ArrayList<>();

        for (int i = 0; i < centroids.size(); i++) {
            TreeNode node = new TreeNode(centroids.get(i), level, i, nextGlobalId++, -1);
            rootNodes.add(node);
            totalNodes++;
        }

        return rootNodes;
    }

    /**
     * Adds a child to a parent node
     */
    public TreeNode addChild(TreeNode parent, double[] centroid, int level) {
        if (parent == null) {
            throw new IllegalArgumentException("Parent cannot be null");
        }

        int clusterId = parent.getChildren().size(); // ID within parent's children
        TreeNode child = new TreeNode(centroid, level, clusterId, nextGlobalId++, parent.getGlobalId());
        parent.addChild(child);
        totalNodes++;

        return child;
    }

    /**
     * Adds a node without a parent (for interior levels during tree building).
     * This is used when building the tree structure where parent nodes are created
     * before their children are assigned to them.
     * 
     * @param centroid The centroid coordinates
     * @param level The level number
     * @return The created TreeNode
     */
    public TreeNode addNode(double[] centroid, int level) {
        TreeNode node = new TreeNode(centroid, level, 0, nextGlobalId++, -1);
        totalNodes++;
        return node;
    }

    /**
     * Moves a child node to a new parent.
     * This is used during tree building to establish parent-child relationships.
     * 
     * @param childNode The child node to move
     * @param newParent The new parent node
     */
    public void moveChildToParent(TreeNode childNode, TreeNode newParent) {
        if (childNode == null || newParent == null) {
            throw new IllegalArgumentException("Child and parent cannot be null");
        }

        // Remove child from its current parent (if any)
        TreeNode currentParent = childNode.getParent();
        if (currentParent != null) {
            currentParent.getChildren().remove(childNode);
        }

        // FIX: Add child to new parent's children list directly (don't create new node)
        // The previous implementation called newParent.addChild(childNode) which creates
        // a new TreeNode instead of moving the existing childNode
        newParent.getChildren().add(childNode);

        // Update child's parent global ID
        childNode.parentGlobalId = newParent.getGlobalId();
    }

    /**
     * Assigns BFS-based IDs to all nodes in the tree
     * This replaces the expensive sorting approach
     */
    public void assignBFSIds() {
        if (root == null) {
            return;
        }

        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);

        int globalIdCounter = 0;
        int[] levelCounters = new int[1000]; // Support up to 1000 levels

        while (!queue.isEmpty()) {
            TreeNode current = queue.poll();

            // Assign new global ID based on BFS traversal order
            current.globalId = globalIdCounter++;

            // Assign cluster ID within level based on BFS order
            current.clusterId = levelCounters[current.level]++;

            // Add children to queue for next level
            for (TreeNode child : current.children) {
                queue.offer(child);
            }
        }
    }

    /**
     * Gets all nodes at a specific level (in BFS order)
     */
    public List<TreeNode> getNodesAtLevel(int level) {
        List<TreeNode> result = new ArrayList<>();
        if (root == null) {
            return result;
        }

        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);

        while (!queue.isEmpty()) {
            TreeNode current = queue.poll();

            if (current.level == level) {
                result.add(current);
            }

            // Add children to queue
            for (TreeNode child : current.children) {
                queue.offer(child);
            }
        }

        return result;
    }

    /**
     * Gets all levels in the tree
     */
    public List<Integer> getLevels() {
        List<Integer> levels = new ArrayList<>();
        if (root == null) {
            return levels;
        }

        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);

        while (!queue.isEmpty()) {
            TreeNode current = queue.poll();

            if (!levels.contains(current.level)) {
                levels.add(current.level);
            }

            // Add children to queue
            for (TreeNode child : current.children) {
                queue.offer(child);
            }
        }

        return levels;
    }

    /**
     * Gets the total number of nodes in the tree
     */
    public int getTotalNodes() {
        return totalNodes;
    }

    /**
     * Gets the maximum depth of the tree
     */
    public int getMaxDepth() {
        if (root == null) {
            return 0;
        }

        int maxDepth = 0;
        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);

        while (!queue.isEmpty()) {
            TreeNode current = queue.poll();
            maxDepth = Math.max(maxDepth, current.getDepth());

            // Add children to queue
            for (TreeNode child : current.children) {
                queue.offer(child);
            }
        }

        return maxDepth;
    }

    /**
     * Prints the tree structure in a readable format
     */
    public void printTree() {
        if (root == null) {
            System.out.println("Empty tree");
            return;
        }

        System.out.println("Hierarchical Cluster Tree Structure:");
        System.out.println("====================================");

        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);

        int currentLevel = -1;
        while (!queue.isEmpty()) {
            TreeNode current = queue.poll();

            if (current.level != currentLevel) {
                currentLevel = current.level;
                System.out.println("\nLevel " + currentLevel + ":");
            }

            String indent = "  ".repeat(current.getDepth());
            System.out.printf("%sCluster %d (Global ID: %d)", indent, current.clusterId, current.globalId);
            if (current.parent != null) {
                System.out.printf(" -> Parent: %d", current.parent.globalId);
            }
            System.out.println();

            // Add children to queue
            for (TreeNode child : current.children) {
                queue.offer(child);
            }
        }
    }

    /**
     * Converts tree to flat list of all nodes (in BFS order)
     */
    public List<TreeNode> toFlatList() {
        List<TreeNode> result = new ArrayList<>();
        if (root == null) {
            return result;
        }

        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);

        while (!queue.isEmpty()) {
            TreeNode current = queue.poll();
            result.add(current);

            // Add children to queue
            for (TreeNode child : current.children) {
                queue.offer(child);
            }
        }

        return result;
    }

    /**
     * Gets the root node
     */
    public TreeNode getRoot() {
        return root;
    }

    /**
     * Finds a node by its global ID
     */
    public TreeNode findNodeByGlobalId(long globalId) {
        if (root == null) {
            return null;
        }

        Queue<TreeNode> queue = new LinkedList<>();
        queue.offer(root);

        while (!queue.isEmpty()) {
            TreeNode current = queue.poll();

            if (current.globalId == globalId) {
                return current;
            }

            // Add children to queue
            for (TreeNode child : current.children) {
                queue.offer(child);
            }
        }

        return null;
    }
}
