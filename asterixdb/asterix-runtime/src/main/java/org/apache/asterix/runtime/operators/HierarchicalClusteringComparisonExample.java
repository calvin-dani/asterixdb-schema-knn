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
 * Example demonstrating the difference between level-by-level and complete tree approaches
 * for hierarchical clustering with cluster ID assignment.
 */
public class HierarchicalClusteringComparisonExample {

    public static void main(String[] args) {
        System.out.println("=== Hierarchical Clustering Approaches Comparison ===\n");

        demonstrateLevelByLevelApproach();
        demonstrateCompleteTreeApproach();
        compareApproaches();
    }

    /**
     * Demonstrates the level-by-level approach with sorting
     */
    public static void demonstrateLevelByLevelApproach() {
        System.out.println("1. LEVEL-BY-LEVEL APPROACH (with sorting)");
        System.out.println("==========================================");

        System.out.println("\nProcess:");
        System.out.println("1. Build Level 0: [C0, C1, C2]");
        System.out.println("2. Output Level 0 immediately");
        System.out.println("3. Build Level 1: [C1.1, C2.1, C0.1, C2.2, C0.2, C1.2]");
        System.out.println("4. Sort Level 1 by parent: [C0.1, C0.2, C1.1, C1.2, C2.1, C2.2]");
        System.out.println("5. Output Level 1 immediately");
        System.out.println("6. Continue for each level...");

        System.out.println("\nOutput Order:");
        System.out.println("Level 0: C0, C1, C2");
        System.out.println("Level 1: C0.1, C0.2, C1.1, C1.2, C2.1, C2.2");
        System.out.println("Level 2: C0.1.1, C0.1.2, C1.1.1, C1.1.2, ...");

        System.out.println("\nPros:");
        System.out.println("✓ Immediate output (streaming)");
        System.out.println("✓ Lower memory usage");
        System.out.println("✓ Can stop early if needed");

        System.out.println("\nCons:");
        System.out.println("✗ Expensive sorting at each level");
        System.out.println("✗ O(n log n) complexity for sorting");
        System.out.println("✗ More complex ID management");
    }

    /**
     * Demonstrates the complete tree approach with BFS
     */
    public static void demonstrateCompleteTreeApproach() {
        System.out.println("\n\n2. COMPLETE TREE APPROACH (with BFS)");
        System.out.println("=====================================");

        System.out.println("\nProcess:");
        System.out.println("1. Build complete tree structure");
        System.out.println("2. Assign BFS-based IDs naturally");
        System.out.println("3. Output all nodes in BFS order");

        System.out.println("\nTree Structure:");
        System.out.println("        C0 (ID: 0)");
        System.out.println("       /  \\");
        System.out.println("   C0.1(1) C0.2(2)");
        System.out.println("    /  \\    /  \\");
        System.out.println("C0.1.1(3) C0.1.2(4) C0.2.1(5) C0.2.2(6)");

        System.out.println("\nBFS Traversal Order:");
        System.out.println("1. C0 (Level 0, Cluster 0, Global ID 0)");
        System.out.println("2. C0.1 (Level 1, Cluster 0, Global ID 1) -> Parent: 0");
        System.out.println("3. C0.2 (Level 1, Cluster 1, Global ID 2) -> Parent: 0");
        System.out.println("4. C0.1.1 (Level 2, Cluster 0, Global ID 3) -> Parent: 1");
        System.out.println("5. C0.1.2 (Level 2, Cluster 1, Global ID 4) -> Parent: 1");
        System.out.println("6. C0.2.1 (Level 2, Cluster 2, Global ID 5) -> Parent: 2");
        System.out.println("7. C0.2.2 (Level 2, Cluster 3, Global ID 6) -> Parent: 2");

        System.out.println("\nPros:");
        System.out.println("✓ No expensive sorting needed");
        System.out.println("✓ O(n) complexity for ID assignment");
        System.out.println("✓ Natural parent-child relationships");
        System.out.println("✓ Simpler ID management");
        System.out.println("✓ Better tree structure visualization");

        System.out.println("\nCons:");
        System.out.println("✗ Higher memory usage (stores complete tree)");
        System.out.println("✗ Delayed output (must build complete tree first)");
        System.out.println("✗ Cannot stop early");
    }

    /**
     * Compares both approaches
     */
    public static void compareApproaches() {
        System.out.println("\n\n3. COMPARISON SUMMARY");
        System.out.println("=====================");

        System.out.println("\nPerformance Comparison:");
        System.out.println("┌─────────────────────┬─────────────────┬─────────────────┐");
        System.out.println("│ Aspect              │ Level-by-Level  │ Complete Tree   │");
        System.out.println("├─────────────────────┼─────────────────┼─────────────────┤");
        System.out.println("│ ID Assignment       │ O(n log n)      │ O(n)            │");
        System.out.println("│ Memory Usage        │ Low             │ High            │");
        System.out.println("│ Output Latency      │ Immediate       │ Delayed         │");
        System.out.println("│ Complexity          │ High            │ Low             │");
        System.out.println("│ Tree Structure      │ Fragmented      │ Complete        │");
        System.out.println("│ Early Stopping      │ Yes             │ No              │");
        System.out.println("└─────────────────────┴─────────────────┴─────────────────┘");

        System.out.println("\nWhen to Use Each Approach:");
        System.out.println("\nUse Level-by-Level when:");
        System.out.println("• Memory is limited");
        System.out.println("• You need immediate output (streaming)");
        System.out.println("• You might need to stop early");
        System.out.println("• You don't mind the sorting overhead");

        System.out.println("\nUse Complete Tree when:");
        System.out.println("• Performance is critical (no sorting)");
        System.out.println("• You have sufficient memory");
        System.out.println("• You need complete tree structure");
        System.out.println("• You want simpler ID management");

        System.out.println("\nCode Usage:");
        System.out.println("// Level-by-level approach (default)");
        System.out.println("new HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(");
        System.out.println("    spec, rDesc, sampleUUID, centroidsUUID, args, K, maxIter);");
        System.out.println("");
        System.out.println("// Complete tree approach");
        System.out.println("new HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(");
        System.out.println("    spec, rDesc, sampleUUID, centroidsUUID, args, K, maxIter,");
        System.out.println("    HierarchicalClusterTree.OutputMode.COMPLETE_TREE);");
    }

    /**
     * Shows the BFS algorithm for ID assignment
     */
    public static void demonstrateBFSAlgorithm() {
        System.out.println("\n\n4. BFS ID ASSIGNMENT ALGORITHM");
        System.out.println("===============================");

        System.out.println("\nAlgorithm:");
        System.out.println("1. Create queue with root node");
        System.out.println("2. While queue is not empty:");
        System.out.println("   a. Dequeue current node");
        System.out.println("   b. Assign global ID (increment counter)");
        System.out.println("   c. Assign cluster ID within level (increment level counter)");
        System.out.println("   d. Enqueue all children");
        System.out.println("3. All nodes now have natural BFS-based IDs");

        System.out.println("\nPseudo-code:");
        System.out.println("Queue<TreeNode> queue = new LinkedList<>();");
        System.out.println("queue.offer(root);");
        System.out.println("int globalId = 0;");
        System.out.println("int[] levelCounters = new int[maxLevels];");
        System.out.println("");
        System.out.println("while (!queue.isEmpty()) {");
        System.out.println("    TreeNode current = queue.poll();");
        System.out.println("    current.globalId = globalId++;");
        System.out.println("    current.clusterId = levelCounters[current.level]++;");
        System.out.println("    for (TreeNode child : current.children) {");
        System.out.println("        queue.offer(child);");
        System.out.println("    }");
        System.out.println("}");

        System.out.println("\nBenefits:");
        System.out.println("✓ No sorting required");
        System.out.println("✓ Natural parent-child ordering");
        System.out.println("✓ O(n) time complexity");
        System.out.println("✓ Simple and efficient");
    }
}
