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

/**
 * Example demonstrating how hierarchical cluster sorting by parent works.
 * This shows the before/after of cluster organization in the hierarchy.
 */
public class HierarchicalClusterSortingExample {

    public static void main(String[] args) {
        System.out.println("=== Hierarchical Cluster Sorting Example ===\n");

        // Demonstrate the sorting concept
        demonstrateClusterSorting();

        // Show how to read sorted clusters
        demonstrateReadingSortedClusters();
    }

    /**
     * Demonstrates how clusters are sorted by parent in the hierarchy
     */
    public static void demonstrateClusterSorting() {
        System.out.println("1. CLUSTER SORTING BY PARENT");
        System.out.println("=============================");

        System.out.println("\nBEFORE Sorting (random order):");
        System.out.println("Level 0: [C0, C1, C2]");
        System.out.println("Level 1: [C1.1, C2.1, C0.1, C2.2, C0.2, C1.2]");
        System.out.println("  - C1.1, C1.2 are children of C1");
        System.out.println("  - C2.1, C2.2 are children of C2");
        System.out.println("  - C0.1, C0.2 are children of C0");

        System.out.println("\nAFTER Sorting (grouped by parent):");
        System.out.println("Level 0: [C0, C1, C2]");
        System.out.println("Level 1: [C0.1, C0.2, C1.1, C1.2, C2.1, C2.2]");
        System.out.println("  - All children of C0 come first");
        System.out.println("  - All children of C1 come second");
        System.out.println("  - All children of C2 come third");

        System.out.println("\nBenefits of Sorting:");
        System.out.println("✓ Children of the same parent are grouped together");
        System.out.println("✓ Easier to traverse parent-child relationships");
        System.out.println("✓ Better visualization of hierarchy structure");
        System.out.println("✓ More efficient queries by parent");
    }

    /**
     * Demonstrates how to read and work with sorted clusters
     */
    public static void demonstrateReadingSortedClusters() {
        System.out.println("\n\n2. READING SORTED CLUSTERS");
        System.out.println("===========================");

        // Simulate reading from the index
        System.out.println("\nReading from hierarchical_cluster_index.json:");
        System.out.println("---------------------------------------------");

        // Example JSON structure for Level 1 (sorted)
        System.out.println("\nLevel 1 Structure (sorted by parent):");
        System.out.println("{");
        System.out.println("  \"level\": 1,");
        System.out.println("  \"centroid_count\": 6,");
        System.out.println("  \"parent_groups\": 3,");
        System.out.println("  \"centroids_by_parent\": {");
        System.out.println("    \"0\": [  // Children of C0");
        System.out.println("      {\"cluster_id\": 0, \"parent_cluster_id\": 0, \"global_id\": 3},");
        System.out.println("      {\"cluster_id\": 1, \"parent_cluster_id\": 0, \"global_id\": 4}");
        System.out.println("    ],");
        System.out.println("    \"1\": [  // Children of C1");
        System.out.println("      {\"cluster_id\": 2, \"parent_cluster_id\": 1, \"global_id\": 5},");
        System.out.println("      {\"cluster_id\": 3, \"parent_cluster_id\": 1, \"global_id\": 6}");
        System.out.println("    ],");
        System.out.println("    \"2\": [  // Children of C2");
        System.out.println("      {\"cluster_id\": 4, \"parent_cluster_id\": 2, \"global_id\": 7},");
        System.out.println("      {\"cluster_id\": 5, \"parent_cluster_id\": 2, \"global_id\": 8}");
        System.out.println("    ]");
        System.out.println("  }");
        System.out.println("}");

        System.out.println("\nCode to read sorted clusters:");
        System.out.println("-----------------------------");
        System.out.println("// Load the index");
        System.out.println("HierarchicalClusterIndexReader reader = new HierarchicalClusterIndexReader();");
        System.out.println("reader.loadIndex(\"hierarchical_cluster_index.json\");");
        System.out.println("");
        System.out.println("// Get Level 1 clusters");
        System.out.println("Map<String, Object> level1 = reader.getClusterLevel(1);");
        System.out.println("Map<Integer, List<Map<String, Object>>> centroidsByParent = ");
        System.out.println("    (Map<Integer, List<Map<String, Object>>>) level1.get(\"centroids_by_parent\");");
        System.out.println("");
        System.out.println("// Iterate through parent groups");
        System.out
                .println("for (Map.Entry<Integer, List<Map<String, Object>>> entry : centroidsByParent.entrySet()) {");
        System.out.println("    int parentId = entry.getKey();");
        System.out.println("    List<Map<String, Object>> children = entry.getValue();");
        System.out.println(
                "    System.out.println(\"Parent \" + parentId + \" has \" + children.size() + \" children\");");
        System.out.println("}");
    }

    /**
     * Shows the algorithm for sorting clusters by parent
     */
    public static void demonstrateSortingAlgorithm() {
        System.out.println("\n\n3. SORTING ALGORITHM");
        System.out.println("====================");

        System.out.println("\nAlgorithm Steps:");
        System.out.println("1. For each child centroid, find its closest parent");
        System.out.println("2. Create CentroidWithParent objects containing:");
        System.out.println("   - Centroid coordinates");
        System.out.println("   - Cluster ID");
        System.out.println("   - Parent cluster ID");
        System.out.println("3. Sort centroids by parent cluster ID:");
        System.out.println("   - Centroids with same parent are grouped together");
        System.out.println("   - Centroids without parent come first");
        System.out.println("4. Update cluster IDs with correct indices after sorting");
        System.out.println("5. Add sorted centroids to hierarchical state");

        System.out.println("\nSorting Comparator:");
        System.out.println("(a, b) -> {");
        System.out.println("    if (a.parentId == null && b.parentId == null) return 0;");
        System.out.println("    if (a.parentId == null) return 1;");
        System.out.println("    if (b.parentId == null) return -1;");
        System.out.println("    return Integer.compare(a.parentId.getClusterId(), b.parentId.getClusterId());");
        System.out.println("}");
    }

    /**
     * Shows the benefits of sorted clusters
     */
    public static void demonstrateBenefits() {
        System.out.println("\n\n4. BENEFITS OF SORTED CLUSTERS");
        System.out.println("===============================");

        System.out.println("\n1. Better Organization:");
        System.out.println("   - Children of same parent are adjacent");
        System.out.println("   - Easier to understand hierarchy structure");
        System.out.println("   - Clear parent-child groupings");

        System.out.println("\n2. Efficient Queries:");
        System.out.println("   - Find all children of a parent: O(1) with grouping");
        System.out.println("   - Traverse hierarchy: sequential access");
        System.out.println("   - Range queries by parent: contiguous memory");

        System.out.println("\n3. Better Visualization:");
        System.out.println("   - Parent groups are clearly separated");
        System.out.println("   - Hierarchy tree structure is obvious");
        System.out.println("   - Debugging and analysis is easier");

        System.out.println("\n4. Consistent Ordering:");
        System.out.println("   - Same parent always produces same child order");
        System.out.println("   - Reproducible results across runs");
        System.out.println("   - Predictable cluster indexing");
    }
}
