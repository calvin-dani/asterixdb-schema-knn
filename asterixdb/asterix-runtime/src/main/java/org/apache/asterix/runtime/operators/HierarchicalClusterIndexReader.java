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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for reading and managing hierarchical cluster index files.
 * This class provides methods to read the JSON side files created by HierarchicalClusterIndexWriter.
 */
public class HierarchicalClusterIndexReader {

    private final ObjectMapper objectMapper;
    private Map<String, Object> indexData;

    public HierarchicalClusterIndexReader() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Reads the hierarchical cluster index from a JSON file
     */
    public void loadIndex(String filePath) throws IOException {
        File indexFile = new File(filePath);
        if (!indexFile.exists()) {
            throw new IOException("Index file not found: " + filePath);
        }

        this.indexData = objectMapper.readValue(indexFile, Map.class);
        System.out.println("Successfully loaded hierarchical cluster index from: " + filePath);
    }

    /**
     * Gets the index metadata
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMetadata() {
        return indexData != null ? (Map<String, Object>) indexData.get("metadata") : null;
    }

    /**
     * Gets all cluster levels
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getClusterLevels() {
        return indexData != null ? (List<Map<String, Object>>) indexData.get("cluster_levels") : null;
    }

    /**
     * Gets a specific cluster level
     */
    public Map<String, Object> getClusterLevel(int level) {
        List<Map<String, Object>> levels = getClusterLevels();
        if (levels == null)
            return null;

        for (Map<String, Object> levelData : levels) {
            if ((Integer) levelData.get("level") == level) {
                return levelData;
            }
        }
        return null;
    }

    /**
     * Gets centroids for a specific level
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getCentroidsForLevel(int level) {
        Map<String, Object> levelData = getClusterLevel(level);
        return levelData != null ? (List<Map<String, Object>>) levelData.get("centroids") : null;
    }

    /**
     * Gets the hierarchy structure information
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getHierarchyStructure() {
        return indexData != null ? (Map<String, Object>) indexData.get("hierarchy_structure") : null;
    }

    /**
     * Gets statistics about the cluster hierarchy
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getStatistics() {
        return indexData != null ? (Map<String, Object>) indexData.get("statistics") : null;
    }

    /**
     * Gets parent-child relationships
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> getParentChildRelationships() {
        Map<String, Object> hierarchy = getHierarchyStructure();
        return hierarchy != null ? (List<Map<String, Object>>) hierarchy.get("parent_child_relationships") : null;
    }

    /**
     * Finds a centroid by its global ID
     */
    public Map<String, Object> findCentroidByGlobalId(long globalId) {
        List<Map<String, Object>> levels = getClusterLevels();
        if (levels == null)
            return null;

        for (Map<String, Object> level : levels) {
            List<Map<String, Object>> centroids = (List<Map<String, Object>>) level.get("centroids");
            for (Map<String, Object> centroid : centroids) {
                if (((Number) centroid.get("global_id")).longValue() == globalId) {
                    return centroid;
                }
            }
        }
        return null;
    }

    /**
     * Gets all children of a specific centroid
     */
    public List<Map<String, Object>> getChildrenOfCentroid(long parentGlobalId) {
        List<Map<String, Object>> children = new ArrayList<>();
        List<Map<String, Object>> relationships = getParentChildRelationships();

        if (relationships != null) {
            for (Map<String, Object> relationship : relationships) {
                if (((Number) relationship.get("parent_global_id")).longValue() == parentGlobalId) {
                    long childGlobalId = ((Number) relationship.get("child_global_id")).longValue();
                    Map<String, Object> child = findCentroidByGlobalId(childGlobalId);
                    if (child != null) {
                        children.add(child);
                    }
                }
            }
        }

        return children;
    }

    /**
     * Gets the parent of a specific centroid
     */
    public Map<String, Object> getParentOfCentroid(long childGlobalId) {
        List<Map<String, Object>> relationships = getParentChildRelationships();

        if (relationships != null) {
            for (Map<String, Object> relationship : relationships) {
                if (((Number) relationship.get("child_global_id")).longValue() == childGlobalId) {
                    long parentGlobalId = ((Number) relationship.get("parent_global_id")).longValue();
                    return findCentroidByGlobalId(parentGlobalId);
                }
            }
        }

        return null;
    }

    /**
     * Gets all centroids at the root level (level 0)
     */
    public List<Map<String, Object>> getRootCentroids() {
        return getCentroidsForLevel(0);
    }

    /**
     * Gets all centroids at the leaf level (highest level)
     */
    public List<Map<String, Object>> getLeafCentroids() {
        List<Map<String, Object>> levels = getClusterLevels();
        if (levels == null || levels.isEmpty())
            return null;

        int maxLevel = 0;
        for (Map<String, Object> level : levels) {
            maxLevel = Math.max(maxLevel, (Integer) level.get("level"));
        }

        return getCentroidsForLevel(maxLevel);
    }

    /**
     * Prints a summary of the loaded index
     */
    public void printIndexSummary() {
        if (indexData == null) {
            System.out.println("No index data loaded.");
            return;
        }

        Map<String, Object> metadata = getMetadata();
        List<Map<String, Object>> levels = getClusterLevels();
        Map<String, Object> stats = getStatistics();

        System.out.println("\n=== Hierarchical Cluster Index Summary ===");

        if (metadata != null) {
            System.out.println("Index Name: " + metadata.get("index_name"));
            System.out.println("Job ID: " + metadata.get("job_id"));
            System.out.println("Partition: " + metadata.get("partition"));
            System.out.println("Created: " + metadata.get("created_timestamp"));
        }

        if (levels != null) {
            System.out.println("\nCluster Levels:");
            for (Map<String, Object> level : levels) {
                System.out.println("  Level " + level.get("level") + ": " + level.get("centroid_count") + " centroids");
            }
        }

        if (stats != null) {
            System.out.println("\nStatistics:");
            System.out.println("  Total Centroids: " + stats.get("total_centroids"));
            System.out.println("  Total Levels: " + stats.get("total_levels"));
            System.out.println("  Max Centroids/Level: " + stats.get("max_centroids_per_level"));
            System.out.println("  Min Centroids/Level: " + stats.get("min_centroids_per_level"));
            System.out.println("  Avg Centroids/Level: " + stats.get("average_centroids_per_level"));
        }

        System.out.println("==========================================\n");
    }

    /**
     * Prints detailed information about a specific level
     */
    public void printLevelDetails(int level) {
        Map<String, Object> levelData = getClusterLevel(level);
        if (levelData == null) {
            System.out.println("Level " + level + " not found.");
            return;
        }

        System.out.println("\n=== Level " + level + " Details ===");
        System.out.println("Centroid Count: " + levelData.get("centroid_count"));

        List<Map<String, Object>> centroids = (List<Map<String, Object>>) levelData.get("centroids");
        if (centroids != null) {
            System.out.println("\nCentroids:");
            for (int i = 0; i < centroids.size(); i++) {
                Map<String, Object> centroid = centroids.get(i);
                System.out.println("  " + (i + 1) + ". Cluster ID: " + centroid.get("cluster_id") + ", Global ID: "
                        + centroid.get("global_id") + ", Has Parent: " + centroid.get("has_parent"));
                if ((Boolean) centroid.get("has_parent")) {
                    System.out.println("     Parent Cluster ID: " + centroid.get("parent_cluster_id"));
                }

                List<Double> coords = (List<Double>) centroid.get("coordinates");
                if (coords != null && !coords.isEmpty()) {
                    System.out.println("     Coordinates: [" + coords.subList(0, Math.min(3, coords.size()))
                            + (coords.size() > 3 ? "..." : "") + "]");
                }
            }
        }
        System.out.println("=====================================\n");
    }

    /**
     * Exports the index to a different format or location
     */
    public void exportIndex(String outputPath) throws IOException {
        if (indexData == null) {
            throw new IllegalStateException("No index data loaded.");
        }

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(outputPath), indexData);
        System.out.println("Index exported to: " + outputPath);
    }

    /**
     * Validates the loaded index structure
     */
    public boolean validateIndex() {
        if (indexData == null) {
            System.err.println("No index data loaded.");
            return false;
        }

        try {
            // Check required sections
            if (!indexData.containsKey("metadata") || !indexData.containsKey("cluster_levels")
                    || !indexData.containsKey("hierarchy_structure")) {
                System.err.println("Index validation failed: Missing required sections.");
                return false;
            }

            // Check metadata
            Map<String, Object> metadata = getMetadata();
            if (metadata == null || !metadata.containsKey("index_name")) {
                System.err.println("Index validation failed: Invalid metadata.");
                return false;
            }

            // Check cluster levels
            List<Map<String, Object>> levels = getClusterLevels();
            if (levels == null || levels.isEmpty()) {
                System.err.println("Index validation failed: No cluster levels found.");
                return false;
            }

            System.out.println("Index validation passed.");
            return true;

        } catch (Exception e) {
            System.err.println("Index validation failed: " + e.getMessage());
            return false;
        }
    }

    /**
     * Gets the static structure index name
     */
    public static String getStaticStructureIndexName() {
        return HierarchicalClusterIndexWriter.getStaticStructureIndexName();
    }

    /**
     * Gets the index directory
     */
    public static String getIndexDirectory() {
        return HierarchicalClusterIndexWriter.getIndexDirectory();
    }
}
