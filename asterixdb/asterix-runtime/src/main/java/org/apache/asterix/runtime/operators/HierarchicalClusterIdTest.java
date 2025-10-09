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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Simple test class to demonstrate the HierarchicalClusterId functionality.
 * This is not a formal unit test but rather a demonstration of the features.
 */
public class HierarchicalClusterIdTest {

    public static void main(String[] args) {
        System.out.println("=== Hierarchical Cluster ID Test ===");

        // Test 1: Create root level clusters
        System.out.println("\n1. Creating root level clusters:");
        HierarchicalClusterId root1 = new HierarchicalClusterId(0, 0);
        HierarchicalClusterId root2 = new HierarchicalClusterId(0, 1);
        System.out.println("Root cluster 1: " + root1);
        System.out.println("Root cluster 2: " + root2);
        System.out.println("Root1 has parent: " + root1.hasParent());
        System.out.println("Root1 is root: " + root1.isRoot());

        // Test 2: Create child clusters
        System.out.println("\n2. Creating child clusters:");
        HierarchicalClusterId child1 = root1.createChild(0);
        HierarchicalClusterId child2 = root1.createChild(1);
        HierarchicalClusterId child3 = root2.createChild(0);
        System.out.println("Child 1 of root1: " + child1);
        System.out.println("Child 2 of root1: " + child2);
        System.out.println("Child 1 of root2: " + child3);
        System.out.println("Child1 has parent: " + child1.hasParent());
        System.out.println("Child1 is root: " + child1.isRoot());

        // Test 3: Create grandchild clusters
        System.out.println("\n3. Creating grandchild clusters:");
        HierarchicalClusterId grandchild1 = child1.createChild(0);
        HierarchicalClusterId grandchild2 = child1.createChild(1);
        System.out.println("Grandchild 1 of child1: " + grandchild1);
        System.out.println("Grandchild 2 of child1: " + grandchild2);

        // Test 4: Test serialization/deserialization
        System.out.println("\n4. Testing serialization/deserialization:");
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            child1.toBytes(dos);
            dos.close();

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            DataInputStream dis = new DataInputStream(bais);
            HierarchicalClusterId deserialized = HierarchicalClusterId.fromBytes(dis);
            dis.close();

            System.out.println("Original: " + child1);
            System.out.println("Deserialized: " + deserialized);
            System.out.println("Are equal: " + child1.equals(deserialized));

        } catch (IOException e) {
            System.err.println("Serialization error: " + e.getMessage());
        }

        // Test 5: Test comparison
        System.out.println("\n5. Testing comparison:");
        System.out.println("root1.compareTo(root2): " + root1.compareTo(root2));
        System.out.println("child1.compareTo(child2): " + child1.compareTo(child2));
        System.out.println("root1.compareTo(child1): " + root1.compareTo(child1));

        // Test 6: Demonstrate hierarchy building
        System.out.println("\n6. Demonstrating hierarchy building:");
        demonstrateHierarchyBuilding();

        System.out.println("\n=== Test Complete ===");
    }

    private static void demonstrateHierarchyBuilding() {
        System.out.println("Building a 3-level hierarchy:");

        // Level 0: Root clusters
        HierarchicalClusterId[] level0 =
                { new HierarchicalClusterId(0, 0), new HierarchicalClusterId(0, 1), new HierarchicalClusterId(0, 2) };
        System.out.println("Level 0: " + java.util.Arrays.toString(level0));

        // Level 1: Children of level 0
        HierarchicalClusterId[] level1 = { level0[0].createChild(0), level0[0].createChild(1), level0[1].createChild(0),
                level0[2].createChild(0) };
        System.out.println("Level 1: " + java.util.Arrays.toString(level1));

        // Level 2: Children of level 1
        HierarchicalClusterId[] level2 = { level1[0].createChild(0), level1[0].createChild(1), level1[1].createChild(0),
                level1[2].createChild(0), level1[3].createChild(0) };
        System.out.println("Level 2: " + java.util.Arrays.toString(level2));

        // Show parent-child relationships
        System.out.println("\nParent-Child Relationships:");
        for (HierarchicalClusterId child : level1) {
            System.out.println("  " + child + " -> Parent: " + child.getParentClusterId());
        }
        for (HierarchicalClusterId child : level2) {
            System.out.println("  " + child + " -> Parent: " + child.getParentClusterId());
        }
    }
}
