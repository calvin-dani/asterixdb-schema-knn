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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Example class demonstrating how to use the hierarchical cluster index side files.
 * This shows how to read and manage the JSON side files created by the hierarchical K-means operator.
 */
public class HierarchicalClusterIndexExample {

    public static void main(String[] args) {
        // Example usage of the hierarchical cluster index reader
        HierarchicalClusterIndexReader reader = new HierarchicalClusterIndexReader();

        try {
            // Load the index file (replace with actual path)
            String indexPath = "cluster_indexes/hierarchical_cluster_index.json";
            reader.loadIndex(indexPath);

            // Print index summary
            reader.printIndexSummary();

            // Validate the index
            if (reader.validateIndex()) {
                System.out.println("Index is valid!");
            } else {
                System.out.println("Index validation failed!");
                return;
            }

            // Print details for each level
            List<Map<String, Object>> levels = reader.getClusterLevels();
            if (levels != null) {
                for (Map<String, Object> level : levels) {
                    int levelNum = (Integer) level.get("level");
                    reader.printLevelDetails(levelNum);
                }
            }

            // Example: Find a specific centroid
            List<Map<String, Object>> rootCentroids = reader.getRootCentroids();
            if (rootCentroids != null && !rootCentroids.isEmpty()) {
                Map<String, Object> firstCentroid = rootCentroids.get(0);
                long globalId = ((Number) firstCentroid.get("global_id")).longValue();

                System.out.println("=== Finding Children of First Root Centroid ===");
                System.out.println("Global ID: " + globalId);

                List<Map<String, Object>> children = reader.getChildrenOfCentroid(globalId);
                System.out.println("Number of children: " + children.size());

                for (Map<String, Object> child : children) {
                    System.out
                            .println("  Child Global ID: " + child.get("global_id") + ", Level: " + child.get("level"));
                }
            }

            // Example: Export to a different location
            reader.exportIndex("exported_cluster_index.json");

        } catch (IOException e) {
            System.err.println("Error reading index: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Example method to demonstrate programmatic access to cluster data
     */
    public static void demonstrateClusterAccess(String indexPath) {
        HierarchicalClusterIndexReader reader = new HierarchicalClusterIndexReader();

        try {
            reader.loadIndex(indexPath);

            // Get statistics
            Map<String, Object> stats = reader.getStatistics();
            if (stats != null) {
                System.out.println("Total centroids: " + stats.get("total_centroids"));
                System.out.println("Total levels: " + stats.get("total_levels"));
            }

            // Access specific level data
            Map<String, Object> level0 = reader.getClusterLevel(0);
            if (level0 != null) {
                System.out.println("Level 0 has " + level0.get("centroid_count") + " centroids");
            }

            // Find centroids by criteria
            List<Map<String, Object>> allLevels = reader.getClusterLevels();
            for (Map<String, Object> level : allLevels) {
                int levelNum = (Integer) level.get("level");
                List<Map<String, Object>> centroids = reader.getCentroidsForLevel(levelNum);

                System.out.println("Level " + levelNum + " centroids:");
                for (Map<String, Object> centroid : centroids) {
                    System.out.println(
                            "  Cluster " + centroid.get("cluster_id") + " (Global: " + centroid.get("global_id") + ")");
                }
            }

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Example method to demonstrate parent-child relationship traversal
     */
    public static void demonstrateHierarchyTraversal(String indexPath) {
        HierarchicalClusterIndexReader reader = new HierarchicalClusterIndexReader();

        try {
            reader.loadIndex(indexPath);

            // Start from root centroids and traverse down
            List<Map<String, Object>> rootCentroids = reader.getRootCentroids();

            System.out.println("=== Hierarchy Traversal ===");
            for (Map<String, Object> root : rootCentroids) {
                long rootGlobalId = ((Number) root.get("global_id")).longValue();
                System.out.println("Root centroid " + rootGlobalId + ":");

                traverseDown(reader, rootGlobalId, 0);
            }

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private static void traverseDown(HierarchicalClusterIndexReader reader, long parentGlobalId, int depth) {
        List<Map<String, Object>> children = reader.getChildrenOfCentroid(parentGlobalId);

        for (Map<String, Object> child : children) {
            long childGlobalId = ((Number) child.get("global_id")).longValue();
            int childLevel = (Integer) child.get("level");

            // Print with indentation based on depth
            for (int i = 0; i < depth + 1; i++) {
                System.out.print("  ");
            }
            System.out.println("Child " + childGlobalId + " (Level " + childLevel + ")");

            // Recursively traverse children
            traverseDown(reader, childGlobalId, depth + 1);
        }
    }
}
