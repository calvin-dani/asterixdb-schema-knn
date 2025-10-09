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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Simple test for Hierarchical K-means components
 * Tests core functionality without complex dependencies
 */
public class SimpleHierarchicalKMeansTest {

    public static void main(String[] args) {
        System.out.println("=== Simple Hierarchical K-means Test ===");

        int passed = 0;
        int failed = 0;

        // Test 1: HierarchicalClusterId
        System.out.println("\n1. Testing HierarchicalClusterId...");
        try {
            testHierarchicalClusterId();
            System.out.println("   ✓ HierarchicalClusterId tests passed");
            passed++;
        } catch (Exception e) {
            System.out.println("   ✗ HierarchicalClusterId tests failed: " + e.getMessage());
            e.printStackTrace();
            failed++;
        }

        // Test 2: Test Data Generation
        System.out.println("\n2. Testing Data Generation...");
        try {
            testDataGeneration();
            System.out.println("   ✓ Data generation tests passed");
            passed++;
        } catch (Exception e) {
            System.out.println("   ✗ Data generation tests failed: " + e.getMessage());
            e.printStackTrace();
            failed++;
        }

        // Test 3: Distance Calculations
        System.out.println("\n3. Testing Distance Calculations...");
        try {
            testDistanceCalculations();
            System.out.println("   ✓ Distance calculation tests passed");
            passed++;
        } catch (Exception e) {
            System.out.println("   ✗ Distance calculation tests failed: " + e.getMessage());
            e.printStackTrace();
            failed++;
        }

        // Test 4: Clustering Quality Metrics
        System.out.println("\n4. Testing Clustering Quality Metrics...");
        try {
            testClusteringQualityMetrics();
            System.out.println("   ✓ Clustering quality metric tests passed");
            passed++;
        } catch (Exception e) {
            System.out.println("   ✗ Clustering quality metric tests failed: " + e.getMessage());
            e.printStackTrace();
            failed++;
        }

        // Summary
        System.out.println("\n=== Test Summary ===");
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);
        System.out.println("Total: " + (passed + failed));

        if (failed == 0) {
            System.out.println("\n🎉 All tests passed!");
        } else {
            System.out.println("\n❌ Some tests failed!");
        }
    }

    private static void testHierarchicalClusterId() {
        // Test root cluster
        HierarchicalClusterId root = new HierarchicalClusterId(0, 0);
        assert root.isRoot() : "Root should be root";
        assert !root.hasParent() : "Root should not have parent";
        assert root.getLevel() == 0 : "Level should be 0";
        assert root.getClusterId() == 0 : "Cluster ID should be 0";
        assert root.getParentClusterId() == -1 : "Parent cluster ID should be -1";

        // Test child cluster
        HierarchicalClusterId child = root.createChild(1);
        assert !child.isRoot() : "Child should not be root";
        assert child.hasParent() : "Child should have parent";
        assert child.getLevel() == 1 : "Child level should be 1";
        assert child.getClusterId() == 1 : "Child cluster ID should be 1";
        assert child.getParentClusterId() == 0 : "Parent cluster ID should be 0";

        // Test grandchild cluster
        HierarchicalClusterId grandchild = child.createChild(2);
        assert !grandchild.isRoot() : "Grandchild should not be root";
        assert grandchild.hasParent() : "Grandchild should have parent";
        assert grandchild.getLevel() == 2 : "Grandchild level should be 2";
        assert grandchild.getClusterId() == 2 : "Grandchild cluster ID should be 2";
        assert grandchild.getParentClusterId() == 1 : "Parent cluster ID should be 1";

        // Test equality
        HierarchicalClusterId cluster1 = new HierarchicalClusterId(1, 5, 2);
        HierarchicalClusterId cluster2 = new HierarchicalClusterId(1, 5, 2);
        HierarchicalClusterId cluster3 = new HierarchicalClusterId(1, 6, 2);

        assert cluster1.equals(cluster2) : "Identical clusters should be equal";
        assert !cluster1.equals(cluster3) : "Different clusters should not be equal";
        assert cluster1.hashCode() == cluster2.hashCode() : "Equal clusters should have same hashCode";

        // Test toString
        String str = root.toString();
        assert str != null : "toString should not return null";
        assert str.contains("level=0") : "toString should contain level";
        assert str.contains("clusterId=0") : "toString should contain clusterId";

        System.out.println("   - Root cluster: " + root);
        System.out.println("   - Child cluster: " + child);
        System.out.println("   - Grandchild cluster: " + grandchild);
    }

    private static void testDataGeneration() {
        // Test Gaussian clusters
        List<double[]> gaussianData = KMeansTestDataGenerator.generateGaussianClusters(3, 10, 2);
        assert gaussianData.size() == 30 : "Should have 30 points";
        assert gaussianData.get(0).length == 2 : "Should have 2 dimensions";
        System.out.println("   - Generated " + gaussianData.size() + " Gaussian cluster points");

        // Test identical points
        List<double[]> identicalData = KMeansTestDataGenerator.generateIdenticalPoints(5, 3);
        assert identicalData.size() == 5 : "Should have 5 points";
        assert identicalData.get(0).length == 3 : "Should have 3 dimensions";

        // All points should be identical
        double[] firstPoint = identicalData.get(0);
        for (double[] point : identicalData) {
            assert Arrays.equals(firstPoint, point) : "All points should be identical";
        }
        System.out.println("   - Generated " + identicalData.size() + " identical points");

        // Test high-dimensional data
        List<double[]> highDimData = KMeansTestDataGenerator.generateHighDimensionalData(10, 20);
        assert highDimData.size() == 10 : "Should have 10 points";
        assert highDimData.get(0).length == 20 : "Should have 20 dimensions";
        System.out.println("   - Generated " + highDimData.size() + " high-dimensional points");

        // Test convergence data
        List<double[]> convergenceData = KMeansTestDataGenerator.generateConvergenceTestData(100, 3, 2);
        assert convergenceData.size() == 100 : "Should have 100 points";
        assert convergenceData.get(0).length == 3 : "Should have 3 dimensions";
        System.out.println("   - Generated " + convergenceData.size() + " convergence test points");

        // Test data with outliers
        List<double[]> dataWithOutliers = KMeansTestDataGenerator.generateDataWithOutliers(100, 10, 2, 50.0);
        assert dataWithOutliers.size() == 110 : "Should have 110 points (100 normal + 10 outliers)";
        assert dataWithOutliers.get(0).length == 2 : "Should have 2 dimensions";
        System.out.println("   - Generated " + dataWithOutliers.size() + " points with outliers");
    }

    private static void testDistanceCalculations() {
        double[] point1 = { 0.0, 0.0 };
        double[] point2 = { 3.0, 4.0 };
        double distance =
                org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared(point1, point2);
        assert Math.abs(distance - 25.0) < 1e-10 : "Distance should be 25";
        System.out.println("   - Distance between (0,0) and (3,4): " + distance);

        // Test with different points
        double[] point3 = { 1.0, 1.0 };
        double[] point4 = { 4.0, 5.0 };
        double distance2 =
                org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared(point3, point4);
        assert Math.abs(distance2 - 25.0) < 1e-10 : "Distance should be 25";
        System.out.println("   - Distance between (1,1) and (4,5): " + distance2);

        // Test with zero distance
        double[] point5 = { 2.0, 3.0 };
        double[] point6 = { 2.0, 3.0 };
        double distance3 =
                org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared(point5, point6);
        assert Math.abs(distance3 - 0.0) < 1e-10 : "Distance should be 0";
        System.out.println("   - Distance between identical points: " + distance3);
    }

    private static void testClusteringQualityMetrics() {
        // Create simple test data
        List<double[]> data = new ArrayList<>();
        data.add(new double[] { 0.0, 0.0 });
        data.add(new double[] { 1.0, 0.0 });
        data.add(new double[] { 0.0, 1.0 });
        data.add(new double[] { 10.0, 10.0 });
        data.add(new double[] { 11.0, 10.0 });
        data.add(new double[] { 10.0, 11.0 });

        // Simple cluster assignment: first 3 points in cluster 0, last 3 in cluster 1
        int[] assignments = { 0, 0, 0, 1, 1, 1 };

        // Test silhouette score
        double silhouetteScore = KMeansTestDataGenerator.calculateSilhouetteScore(data, assignments);
        assert silhouetteScore >= -1.0 && silhouetteScore <= 1.0 : "Silhouette score should be between -1 and 1";
        assert silhouetteScore > 0.0 : "Silhouette score should be positive for well-separated clusters";
        System.out.println("   - Silhouette score: " + silhouetteScore);

        // Test WCSS
        List<double[]> centroids = new ArrayList<>();
        centroids.add(new double[] { 0.33, 0.33 }); // Centroid of first cluster
        centroids.add(new double[] { 10.33, 10.33 }); // Centroid of second cluster

        double wcss = KMeansTestDataGenerator.calculateWCSS(data, centroids, assignments);
        assert wcss > 0.0 : "WCSS should be positive";
        assert wcss < 1000.0 : "WCSS should be reasonable";
        System.out.println("   - WCSS: " + wcss);

        // Test with different cluster assignments
        int[] assignments2 = { 0, 0, 1, 1, 2, 2 };
        double silhouetteScore2 = KMeansTestDataGenerator.calculateSilhouetteScore(data, assignments2);
        assert silhouetteScore2 >= -1.0 && silhouetteScore2 <= 1.0 : "Silhouette score should be between -1 and 1";
        System.out.println("   - Silhouette score (3 clusters): " + silhouetteScore2);
    }
}
