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
import java.util.Random;

/**
 * Test data generator for K-means clustering tests
 * Provides various types of synthetic datasets for comprehensive testing
 */
public class KMeansTestDataGenerator {

    private static final Random RANDOM = new Random(42); // Fixed seed for reproducibility

    /**
     * Generates Gaussian clusters with specified parameters
     * 
     * @param numClusters Number of clusters to generate
     * @param pointsPerCluster Number of points per cluster
     * @param dimensions Number of dimensions for each point
     * @return List of data points
     */
    public static List<double[]> generateGaussianClusters(int numClusters, int pointsPerCluster, int dimensions) {
        List<double[]> data = new ArrayList<>();

        for (int cluster = 0; cluster < numClusters; cluster++) {
            // Generate cluster center
            double[] center = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                center[d] = RANDOM.nextGaussian() * 10; // Scale for separation
            }

            // Generate points around center
            for (int point = 0; point < pointsPerCluster; point++) {
                double[] dataPoint = new double[dimensions];
                for (int d = 0; d < dimensions; d++) {
                    dataPoint[d] = center[d] + RANDOM.nextGaussian() * 2; // Small variance
                }
                data.add(dataPoint);
            }
        }

        return data;
    }

    /**
     * Generates linearly separable clusters
     * 
     * @param numClusters Number of clusters
     * @param pointsPerCluster Number of points per cluster
     * @param dimensions Number of dimensions
     * @return List of data points
     */
    public static List<double[]> generateLinearlySeparableClusters(int numClusters, int pointsPerCluster,
            int dimensions) {
        List<double[]> data = new ArrayList<>();

        for (int cluster = 0; cluster < numClusters; cluster++) {
            for (int point = 0; point < pointsPerCluster; point++) {
                double[] dataPoint = new double[dimensions];
                for (int d = 0; d < dimensions; d++) {
                    dataPoint[d] = cluster * 10.0 + RANDOM.nextGaussian() * 0.5;
                }
                data.add(dataPoint);
            }
        }

        return data;
    }

    /**
     * Generates high-dimensional data
     * 
     * @param numPoints Number of points
     * @param dimensions Number of dimensions
     * @return List of data points
     */
    public static List<double[]> generateHighDimensionalData(int numPoints, int dimensions) {
        List<double[]> data = new ArrayList<>();

        for (int i = 0; i < numPoints; i++) {
            double[] point = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                point[d] = RANDOM.nextGaussian() * 0.1; // Smaller values for high dimensions
            }
            data.add(point);
        }

        return data;
    }

    /**
     * Generates identical points (edge case)
     * 
     * @param numPoints Number of points
     * @param dimensions Number of dimensions
     * @return List of identical data points
     */
    public static List<double[]> generateIdenticalPoints(int numPoints, int dimensions) {
        List<double[]> data = new ArrayList<>();
        double[] template = new double[dimensions];
        Arrays.fill(template, 1.0);

        for (int i = 0; i < numPoints; i++) {
            data.add(template.clone());
        }

        return data;
    }

    /**
     * Generates noisy data around existing clean data
     * 
     * @param cleanData Original clean data
     * @param noiseLevel Standard deviation of noise to add
     * @return List of noisy data points
     */
    public static List<double[]> generateNoisyData(List<double[]> cleanData, double noiseLevel) {
        List<double[]> noisyData = new ArrayList<>();

        for (double[] cleanPoint : cleanData) {
            double[] noisyPoint = new double[cleanPoint.length];
            for (int d = 0; d < cleanPoint.length; d++) {
                noisyPoint[d] = cleanPoint[d] + RANDOM.nextGaussian() * noiseLevel;
            }
            noisyData.add(noisyPoint);
        }

        return noisyData;
    }

    /**
     * Generates data with outliers
     * 
     * @param numPoints Number of normal points
     * @param numOutliers Number of outliers
     * @param dimensions Number of dimensions
     * @param outlierDistance Distance of outliers from normal data
     * @return List of data points including outliers
     */
    public static List<double[]> generateDataWithOutliers(int numPoints, int numOutliers, int dimensions,
            double outlierDistance) {
        List<double[]> data = new ArrayList<>();

        // Generate normal data
        for (int i = 0; i < numPoints; i++) {
            double[] point = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                point[d] = RANDOM.nextGaussian();
            }
            data.add(point);
        }

        // Generate outliers
        for (int i = 0; i < numOutliers; i++) {
            double[] outlier = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                outlier[d] = RANDOM.nextGaussian() * outlierDistance;
            }
            data.add(outlier);
        }

        return data;
    }

    /**
     * Generates data with different cluster densities
     * 
     * @param clusterSizes Array of cluster sizes
     * @param dimensions Number of dimensions
     * @return List of data points
     */
    public static List<double[]> generateVariableDensityClusters(int[] clusterSizes, int dimensions) {
        List<double[]> data = new ArrayList<>();

        for (int cluster = 0; cluster < clusterSizes.length; cluster++) {
            // Generate cluster center
            double[] center = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                center[d] = cluster * 15.0; // Well separated clusters
            }

            // Generate points around center
            int pointsInCluster = clusterSizes[cluster];
            for (int point = 0; point < pointsInCluster; point++) {
                double[] dataPoint = new double[dimensions];
                for (int d = 0; d < dimensions; d++) {
                    dataPoint[d] = center[d] + RANDOM.nextGaussian() * 2;
                }
                data.add(dataPoint);
            }
        }

        return data;
    }

    /**
     * Generates data with missing values (represented as NaN)
     * 
     * @param numPoints Number of points
     * @param dimensions Number of dimensions
     * @param missingRatio Ratio of missing values (0.0 to 1.0)
     * @return List of data points with missing values
     */
    public static List<double[]> generateDataWithMissingValues(int numPoints, int dimensions, double missingRatio) {
        List<double[]> data = new ArrayList<>();

        for (int i = 0; i < numPoints; i++) {
            double[] point = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                if (RANDOM.nextDouble() < missingRatio) {
                    point[d] = Double.NaN; // Missing value
                } else {
                    point[d] = RANDOM.nextGaussian();
                }
            }
            data.add(point);
        }

        return data;
    }

    /**
     * Generates data with extreme values
     * 
     * @param numPoints Number of points
     * @param dimensions Number of dimensions
     * @param extremeValueRatio Ratio of extreme values
     * @return List of data points with extreme values
     */
    public static List<double[]> generateDataWithExtremeValues(int numPoints, int dimensions,
            double extremeValueRatio) {
        List<double[]> data = new ArrayList<>();

        for (int i = 0; i < numPoints; i++) {
            double[] point = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                if (RANDOM.nextDouble() < extremeValueRatio) {
                    // Generate extreme values
                    if (RANDOM.nextBoolean()) {
                        point[d] = Double.MAX_VALUE;
                    } else {
                        point[d] = Double.MIN_VALUE;
                    }
                } else {
                    point[d] = RANDOM.nextGaussian();
                }
            }
            data.add(point);
        }

        return data;
    }

    /**
     * Generates data with different scales across dimensions
     * 
     * @param numPoints Number of points
     * @param dimensions Number of dimensions
     * @param scales Array of scales for each dimension
     * @return List of data points with different scales
     */
    public static List<double[]> generateDataWithDifferentScales(int numPoints, int dimensions, double[] scales) {
        List<double[]> data = new ArrayList<>();

        for (int i = 0; i < numPoints; i++) {
            double[] point = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                double scale = d < scales.length ? scales[d] : 1.0;
                point[d] = RANDOM.nextGaussian() * scale;
            }
            data.add(point);
        }

        return data;
    }

    /**
     * Generates data for stress testing
     * 
     * @param numPoints Number of points
     * @param dimensions Number of dimensions
     * @return List of data points for stress testing
     */
    public static List<double[]> generateStressTestData(int numPoints, int dimensions) {
        List<double[]> data = new ArrayList<>();

        for (int i = 0; i < numPoints; i++) {
            double[] point = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                // Mix of different value ranges
                switch (d % 4) {
                    case 0:
                        point[d] = RANDOM.nextGaussian(); // Normal range
                        break;
                    case 1:
                        point[d] = RANDOM.nextGaussian() * 0.001; // Very small
                        break;
                    case 2:
                        point[d] = RANDOM.nextGaussian() * 1000; // Very large
                        break;
                    case 3:
                        point[d] = RANDOM.nextDouble() * 2 - 1; // Uniform [-1, 1]
                        break;
                }
            }
            data.add(point);
        }

        return data;
    }

    /**
     * Generates data for convergence testing
     * 
     * @param numPoints Number of points
     * @param dimensions Number of dimensions
     * @param convergenceDifficulty Difficulty level (1-5)
     * @return List of data points for convergence testing
     */
    public static List<double[]> generateConvergenceTestData(int numPoints, int dimensions, int convergenceDifficulty) {
        List<double[]> data = new ArrayList<>();

        // Adjust parameters based on difficulty
        double clusterSeparation = 5.0 * convergenceDifficulty;
        double clusterVariance = 1.0 / convergenceDifficulty;
        int numClusters = Math.max(2, convergenceDifficulty);

        for (int cluster = 0; cluster < numClusters; cluster++) {
            // Generate cluster center
            double[] center = new double[dimensions];
            for (int d = 0; d < dimensions; d++) {
                center[d] = cluster * clusterSeparation + RANDOM.nextGaussian() * 2;
            }

            // Generate points around center
            int pointsInCluster = numPoints / numClusters;
            for (int point = 0; point < pointsInCluster; point++) {
                double[] dataPoint = new double[dimensions];
                for (int d = 0; d < dimensions; d++) {
                    dataPoint[d] = center[d] + RANDOM.nextGaussian() * clusterVariance;
                }
                data.add(dataPoint);
            }
        }

        return data;
    }

    /**
     * Calculates the within-cluster sum of squares (WCSS) for validation
     * 
     * @param data Data points
     * @param centroids Cluster centroids
     * @param assignments Cluster assignments for each point
     * @return WCSS value
     */
    public static double calculateWCSS(List<double[]> data, List<double[]> centroids, int[] assignments) {
        double wcss = 0.0;

        for (int i = 0; i < data.size(); i++) {
            double[] point = data.get(i);
            double[] centroid = centroids.get(assignments[i]);

            double distance = 0.0;
            for (int d = 0; d < point.length; d++) {
                double diff = point[d] - centroid[d];
                distance += diff * diff;
            }
            wcss += distance;
        }

        return wcss;
    }

    /**
     * Calculates the silhouette score for cluster quality assessment
     * 
     * @param data Data points
     * @param assignments Cluster assignments
     * @return Silhouette score
     */
    public static double calculateSilhouetteScore(List<double[]> data, int[] assignments) {
        int n = data.size();
        if (n <= 1)
            return 0.0;

        double totalSilhouette = 0.0;

        for (int i = 0; i < n; i++) {
            double[] point = data.get(i);
            int cluster = assignments[i];

            // Calculate average distance within cluster
            double a = calculateAverageDistanceWithinCluster(data, assignments, i, cluster);

            // Calculate average distance to nearest other cluster
            double b = calculateAverageDistanceToNearestOtherCluster(data, assignments, i, cluster);

            // Calculate silhouette score for this point
            double silhouette = (b - a) / Math.max(a, b);
            totalSilhouette += silhouette;
        }

        return totalSilhouette / n;
    }

    private static double calculateAverageDistanceWithinCluster(List<double[]> data, int[] assignments, int pointIndex,
            int cluster) {
        double[] point = data.get(pointIndex);
        double totalDistance = 0.0;
        int count = 0;

        for (int i = 0; i < data.size(); i++) {
            if (i != pointIndex && assignments[i] == cluster) {
                totalDistance += calculateEuclideanDistance(point, data.get(i));
                count++;
            }
        }

        return count > 0 ? totalDistance / count : 0.0;
    }

    private static double calculateAverageDistanceToNearestOtherCluster(List<double[]> data, int[] assignments,
            int pointIndex, int currentCluster) {
        double[] point = data.get(pointIndex);
        double minAverageDistance = Double.MAX_VALUE;

        // Find all unique clusters except current one
        int[] uniqueClusters = Arrays.stream(assignments).distinct().filter(c -> c != currentCluster).toArray();

        for (int cluster : uniqueClusters) {
            double totalDistance = 0.0;
            int count = 0;

            for (int i = 0; i < data.size(); i++) {
                if (assignments[i] == cluster) {
                    totalDistance += calculateEuclideanDistance(point, data.get(i));
                    count++;
                }
            }

            if (count > 0) {
                double averageDistance = totalDistance / count;
                minAverageDistance = Math.min(minAverageDistance, averageDistance);
            }
        }

        return minAverageDistance == Double.MAX_VALUE ? 0.0 : minAverageDistance;
    }

    private static double calculateEuclideanDistance(double[] point1, double[] point2) {
        double distance = 0.0;
        for (int d = 0; d < point1.length; d++) {
            double diff = point1[d] - point2[d];
            distance += diff * diff;
        }
        return Math.sqrt(distance);
    }
}
