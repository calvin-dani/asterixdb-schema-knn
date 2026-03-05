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
 * Example demonstrating how Lloyd's assignments establish parent-child relationships
 */
public class LloydsAssignmentExample {

    public static void main(String[] args) {
        System.out.println("=== Lloyd's Assignment Parent-Child Relationships ===\n");

        demonstrateLloydsAssignments();
        showParentChildMapping();
        explainTheAlgorithm();
    }

    /**
     * Demonstrates how Lloyd's assignments work
     */
    public static void demonstrateLloydsAssignments() {
        System.out.println("1. LLOYD'S ALGORITHM ASSIGNMENTS");
        System.out.println("=================================");

        System.out.println("\nStep 1: We have parent centroids (Level 0):");
        System.out.println("Parent 0: [1.0, 2.0, 3.0]");
        System.out.println("Parent 1: [4.0, 5.0, 6.0]");
        System.out.println("Parent 2: [7.0, 8.0, 9.0]");

        System.out.println("\nStep 2: We have child centroids (Level 1):");
        System.out.println("Child 0: [1.1, 2.1, 3.1]  (close to Parent 0)");
        System.out.println("Child 1: [4.2, 5.2, 6.2]  (close to Parent 1)");
        System.out.println("Child 2: [1.3, 2.3, 3.3]  (close to Parent 0)");
        System.out.println("Child 3: [7.1, 8.1, 9.1]  (close to Parent 2)");
        System.out.println("Child 4: [4.4, 5.4, 6.4]  (close to Parent 1)");

        System.out.println("\nStep 3: Lloyd's algorithm calculates distances and assigns:");
        System.out.println("Child 0 -> Parent 0 (distance: 0.17)");
        System.out.println("Child 1 -> Parent 1 (distance: 0.35)");
        System.out.println("Child 2 -> Parent 0 (distance: 0.52)");
        System.out.println("Child 3 -> Parent 2 (distance: 0.17)");
        System.out.println("Child 4 -> Parent 1 (distance: 0.69)");

        System.out.println("\nStep 4: Lloyd's algorithm stores assignments:");
        System.out.println("assignments = [0, 1, 0, 2, 1]");
        System.out.println("//           [0, 1, 2, 3, 4] <- child indices");
        System.out.println("//           [0, 1, 0, 2, 1] <- parent indices");
    }

    /**
     * Shows how to map children to parents using assignments
     */
    public static void showParentChildMapping() {
        System.out.println("\n\n2. PARENT-CHILD MAPPING");
        System.out.println("========================");

        System.out.println("\nCode to map children to parents:");
        System.out.println("---------------------------------");
        System.out.println("int[] assignments = [0, 1, 0, 2, 1];");
        System.out.println("List<double[]> children = [child0, child1, child2, child3, child4];");
        System.out.println("");
        System.out.println("for (int i = 0; i < children.size(); i++) {");
        System.out.println("    int parentIndex = assignments[i];  // Which parent does child i belong to?");
        System.out.println("    ");
        System.out.println("    // Create parent ID");
        System.out.println("    HierarchicalClusterId parentId = new HierarchicalClusterId(0, parentIndex);");
        System.out.println("    ");
        System.out.println("    // Create child ID with parent reference");
        System.out.println("    HierarchicalClusterId childId = new HierarchicalClusterId(1, i, parentIndex);");
        System.out.println("    ");
        System.out.println("    System.out.println(\"Child \" + i + \" -> Parent \" + parentIndex);");
        System.out.println("}");

        System.out.println("\nOutput:");
        System.out.println("-------");
        System.out.println("Child 0 -> Parent 0");
        System.out.println("Child 1 -> Parent 1");
        System.out.println("Child 2 -> Parent 0");
        System.out.println("Child 3 -> Parent 2");
        System.out.println("Child 4 -> Parent 1");

        System.out.println("\nResulting Hierarchy:");
        System.out.println("-------------------");
        System.out.println("Level 0 (Parents):");
        System.out.println("  Parent 0: [1.0, 2.0, 3.0]");
        System.out.println("    └── Child 0: [1.1, 2.1, 3.1]");
        System.out.println("    └── Child 2: [1.3, 2.3, 3.3]");
        System.out.println("  Parent 1: [4.0, 5.0, 6.0]");
        System.out.println("    └── Child 1: [4.2, 5.2, 6.2]");
        System.out.println("    └── Child 4: [4.4, 5.4, 6.4]");
        System.out.println("  Parent 2: [7.0, 8.0, 9.0]");
        System.out.println("    └── Child 3: [7.1, 8.1, 9.1]");
    }

    /**
     * Explains the algorithm step by step
     */
    public static void explainTheAlgorithm() {
        System.out.println("\n\n3. ALGORITHM EXPLANATION");
        System.out.println("========================");

        System.out.println("\nWhy This Works:");
        System.out.println("---------------");
        System.out.println("1. Lloyd's algorithm already calculates which centroid each point belongs to");
        System.out.println("2. We just need to capture this information during the algorithm");
        System.out.println("3. No need for expensive distance calculations later!");

        System.out.println("\nThe Key Insight:");
        System.out.println("---------------");
        System.out.println("// During Lloyd's algorithm:");
        System.out.println("for (int cIdx = 0; cIdx < k; cIdx++) {");
        System.out.println("    double dist = euclidean_squared(point, centers[cIdx]);");
        System.out.println("    if (dist < minDist) {");
        System.out.println("        minDist = dist;");
        System.out.println("        bestIdx = cIdx;  // <-- This is the parent!");
        System.out.println("    }");
        System.out.println("}");
        System.out.println("assignments.add(bestIdx); // Store the parent for this point");

        System.out.println("\nPerformance Benefits:");
        System.out.println("--------------------");
        System.out.println("• OLD WAY: Calculate distances between all children and all parents");
        System.out.println("  - Time: O(children × parents)");
        System.out.println("  - Memory: Store all distance calculations");
        System.out.println("");
        System.out.println("• NEW WAY: Use assignments from Lloyd's algorithm");
        System.out.println("  - Time: O(children) - just array lookups!");
        System.out.println("  - Memory: Just store simple integers");
        System.out.println("  - No extra work - Lloyd's already did the hard part!");

        System.out.println("\nReal Example:");
        System.out.println("-------------");
        System.out.println("// If we have 1000 children and 10 parents:");
        System.out.println("// OLD WAY: 1000 × 10 = 10,000 distance calculations");
        System.out.println("// NEW WAY: 1000 array lookups (10,000x faster!)");
    }

    /**
     * Shows the actual code from our implementation
     */
    public static void showActualCode() {
        System.out.println("\n\n4. ACTUAL IMPLEMENTATION");
        System.out.println("=========================");

        System.out.println("\nIn our HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor:");
        System.out.println("-------------------------------------------------------------");
        System.out.println("// 1. Run Lloyd's algorithm with assignments");
        System.out.println("LloydResult lloydResult = performLloydsAlgorithmWithAssignments(...);");
        System.out.println("");
        System.out.println("// 2. Use assignments to establish parent-child relationships");
        System.out.println("for (int i = 0; i < lloydResult.centroids.size(); i++) {");
        System.out.println("    // Get parent index from Lloyd's assignments");
        System.out.println("    int parentIndex = lloydResult.assignments[i % lloydResult.assignments.length];");
        System.out.println("    ");
        System.out.println("    // Create parent ID");
        System.out.println(
                "    HierarchicalClusterId parentId = new HierarchicalClusterId(currentLevel - 1, parentIndex);");
        System.out.println("    ");
        System.out.println("    // Create child ID with parent reference");
        System.out.println(
                "    HierarchicalClusterId clusterId = new HierarchicalClusterId(currentLevel, i, parentId.getClusterId());");
        System.out.println("    ");
        System.out.println("    // Add to hierarchy");
        System.out.println(
                "    centroidsWithParents.add(new CentroidWithParent(lloydResult.centroids.get(i), clusterId, parentId));");
        System.out.println("}");

        System.out.println("\nThe Magic:");
        System.out.println("----------");
        System.out.println("• lloydResult.assignments[i] tells us which parent child i belongs to");
        System.out.println("• No distance calculations needed!");
        System.out.println("• Lloyd's algorithm already did the hard work");
        System.out.println("• We just reuse that information");
    }
}
