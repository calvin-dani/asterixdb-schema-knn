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
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Manages the creation and writing of hierarchical cluster index files in JSON format.
 * This class handles the static structure index file that can be manually managed.
 */
public class HierarchicalClusterIndexWriter {

    private static final String STATIC_STRUCTURE_INDEX_NAME = "hierarchical_cluster_index.json";
    private static final String INDEX_DIRECTORY = "cluster_indexes";

    private final IHyracksTaskContext ctx;
    private final JobId jobId;
    private final int partition;
    private final String basePath;
    private final ObjectMapper objectMapper;

    // Index structure
    private final Map<String, Object> indexMetadata;
    private final List<Map<String, Object>> clusterLevels;
    private final Map<String, Object> hierarchyStructure;

    public HierarchicalClusterIndexWriter(IHyracksTaskContext ctx, int partition) {
        this.ctx = ctx;
        this.jobId = ctx.getJobletContext().getJobId();
        this.partition = partition;
        this.basePath = getIndexBasePath();
        this.objectMapper = new ObjectMapper();

        // Initialize index structure
        this.indexMetadata = new HashMap<>();
        this.clusterLevels = new ArrayList<>();
        this.hierarchyStructure = new HashMap<>();

        initializeIndexMetadata();
    }

    /**
     * Initializes the index metadata with basic information
     */
    private void initializeIndexMetadata() {
        indexMetadata.put("index_name", STATIC_STRUCTURE_INDEX_NAME);
        indexMetadata.put("job_id", jobId.toString());
        indexMetadata.put("partition", partition);
        indexMetadata.put("created_timestamp", System.currentTimeMillis());
        indexMetadata.put("version", "1.0");
        indexMetadata.put("description", "Hierarchical K-means cluster index");
    }

    /**
     * Adds a cluster level to the index
     */
    public void addClusterLevel(int level, List<HierarchicalCentroidsState.HierarchicalCentroid> centroids) {
        Map<String, Object> levelData = new HashMap<>();
        levelData.put("level", level);
        levelData.put("centroid_count", centroids.size());
        levelData.put("centroids", new ArrayList<>());

        // Add centroid information
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> centroidList = new ArrayList<>();
        Map<Integer, List<Map<String, Object>>> centroidsByParent = new HashMap<>();

        for (HierarchicalCentroidsState.HierarchicalCentroid centroid : centroids) {
            Map<String, Object> centroidData = new HashMap<>();
            HierarchicalClusterId clusterId = centroid.getClusterId();

            centroidData.put("cluster_id", clusterId.getClusterId());
            centroidData.put("global_id", clusterId.getGlobalId());
            centroidData.put("level", clusterId.getLevel());
            centroidData.put("has_parent", clusterId.hasParent());
            if (clusterId.hasParent()) {
                centroidData.put("parent_cluster_id", clusterId.getParentClusterId());
            }

            // Add centroid coordinates
            double[] coordinates = centroid.getCentroid();
            List<Double> coordList = new ArrayList<>();
            for (double coord : coordinates) {
                coordList.add(coord);
            }
            centroidData.put("coordinates", coordList);
            centroidData.put("dimension", coordinates.length);

            centroidList.add(centroidData);

            // Group by parent for better organization
            int parentId = clusterId.hasParent() ? clusterId.getParentClusterId() : -1;
            centroidsByParent.computeIfAbsent(parentId, k -> new ArrayList<>()).add(centroidData);
        }

        levelData.put("centroids", centroidList);
        levelData.put("centroids_by_parent", centroidsByParent);
        levelData.put("parent_groups", centroidsByParent.keySet().size());

        clusterLevels.add(levelData);
    }

    /**
     * Adds hierarchy structure information
     */
    public void addHierarchyStructure(int totalLevels, int totalCentroids, Map<String, Object> structureInfo) {
        hierarchyStructure.put("total_levels", totalLevels);
        hierarchyStructure.put("total_centroids", totalCentroids);
        hierarchyStructure.put("structure_info", structureInfo);
        hierarchyStructure.put("parent_child_relationships", buildParentChildRelationships());
    }

    /**
     * Builds parent-child relationships from the cluster levels
     */
    private List<Map<String, Object>> buildParentChildRelationships() {
        List<Map<String, Object>> relationships = new ArrayList<>();

        for (int i = 1; i < clusterLevels.size(); i++) {
            Map<String, Object> currentLevel = clusterLevels.get(i);
            List<Map<String, Object>> currentCentroids = (List<Map<String, Object>>) currentLevel.get("centroids");

            for (Map<String, Object> centroid : currentCentroids) {
                if ((Boolean) centroid.get("has_parent")) {
                    Map<String, Object> relationship = new HashMap<>();
                    relationship.put("child_global_id", centroid.get("global_id"));
                    relationship.put("child_cluster_id", centroid.get("cluster_id"));
                    relationship.put("child_level", centroid.get("level"));
                    relationship.put("parent_cluster_id", centroid.get("parent_cluster_id"));
                    relationship.put("parent_level", (Integer) centroid.get("level") - 1);
                    relationships.add(relationship);
                }
            }
        }

        return relationships;
    }

    /**
     * Writes the complete index to a JSON file
     */
    public void writeIndex() throws HyracksDataException {
        try {
            // Create the complete index structure
            Map<String, Object> completeIndex = new HashMap<>();
            completeIndex.put("metadata", indexMetadata);
            completeIndex.put("cluster_levels", clusterLevels);
            completeIndex.put("hierarchy_structure", hierarchyStructure);
            completeIndex.put("statistics", generateStatistics());

            // Create directory if it doesn't exist
            createIndexDirectory();

            // Write to file
            String indexPath = getIndexFilePath();
            writeJsonToFile(completeIndex, indexPath);

            System.out.println("Hierarchical cluster index written to: " + indexPath);

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Writes the index to a managed workspace file (side file)
     */
    public void writeIndexToSideFile() throws HyracksDataException {
        try {
            // Create the complete index structure
            Map<String, Object> completeIndex = new HashMap<>();
            completeIndex.put("metadata", indexMetadata);
            completeIndex.put("cluster_levels", clusterLevels);
            completeIndex.put("hierarchy_structure", hierarchyStructure);
            completeIndex.put("statistics", generateStatistics());

            // Create managed workspace file
            FileReference indexFile =
                    ctx.getJobletContext().createManagedWorkspaceFile("hierarchical_cluster_index_" + partition);

            // Write JSON to the managed file
            writeJsonToFile(completeIndex, indexFile.getFile().getAbsolutePath());

            System.out.println(
                    "Hierarchical cluster index written to side file: " + indexFile.getFile().getAbsolutePath());

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Generates statistics about the cluster hierarchy
     */
    private Map<String, Object> generateStatistics() {
        Map<String, Object> stats = new HashMap<>();

        int totalCentroids = 0;
        int totalLevels = clusterLevels.size();
        int maxCentroidsPerLevel = 0;
        int minCentroidsPerLevel = Integer.MAX_VALUE;

        for (Map<String, Object> level : clusterLevels) {
            int centroidCount = (Integer) level.get("centroid_count");
            totalCentroids += centroidCount;
            maxCentroidsPerLevel = Math.max(maxCentroidsPerLevel, centroidCount);
            minCentroidsPerLevel = Math.min(minCentroidsPerLevel, centroidCount);
        }

        stats.put("total_centroids", totalCentroids);
        stats.put("total_levels", totalLevels);
        stats.put("max_centroids_per_level", maxCentroidsPerLevel);
        stats.put("min_centroids_per_level", minCentroidsPerLevel == Integer.MAX_VALUE ? 0 : minCentroidsPerLevel);
        stats.put("average_centroids_per_level", totalLevels > 0 ? (double) totalCentroids / totalLevels : 0.0);

        return stats;
    }

    /**
     * Gets the base path for index files
     */
    private String getIndexBasePath() {
        // Use a random string for workspace directory (will work out details later)
        String workspaceDir = "workspace_" + System.currentTimeMillis() + "_" + partition;
        return workspaceDir + File.separator + INDEX_DIRECTORY;
    }

    /**
     * Gets the full path for the index file
     */
    private String getIndexFilePath() {
        return basePath + File.separator + STATIC_STRUCTURE_INDEX_NAME;
    }

    /**
     * Creates the index directory if it doesn't exist
     */
    private void createIndexDirectory() throws IOException {
        Path dirPath = Paths.get(basePath);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
    }

    /**
     * Writes a Map to a JSON file
     */
    private void writeJsonToFile(Map<String, Object> data, String filePath) throws IOException {
        try (FileWriter fileWriter = new FileWriter(filePath)) {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(fileWriter, data);
        }
    }

    /**
     * Reads the index from a file
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> readIndex(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(filePath), Map.class);
    }

    /**
     * Gets the static structure index name
     */
    public static String getStaticStructureIndexName() {
        return STATIC_STRUCTURE_INDEX_NAME;
    }

    /**
     * Gets the index directory path
     */
    public static String getIndexDirectory() {
        return INDEX_DIRECTORY;
    }

    /**
     * Validates the index structure
     */
    public boolean validateIndex() {
        try {
            // Check if we have at least one level
            if (clusterLevels.isEmpty()) {
                System.err.println("Index validation failed: No cluster levels found");
                return false;
            }

            // Check if metadata is complete
            if (!indexMetadata.containsKey("index_name") || !indexMetadata.containsKey("job_id")) {
                System.err.println("Index validation failed: Incomplete metadata");
                return false;
            }

            // Check if hierarchy structure is complete
            if (!hierarchyStructure.containsKey("total_levels") || !hierarchyStructure.containsKey("total_centroids")) {
                System.err.println("Index validation failed: Incomplete hierarchy structure");
                return false;
            }

            System.out.println("Index validation passed");
            return true;

        } catch (Exception e) {
            System.err.println("Index validation failed: " + e.getMessage());
            return false;
        }
    }

    /**
     * Gets a summary of the index
     */
    public String getIndexSummary() {
        StringBuilder summary = new StringBuilder();
        summary.append("Hierarchical Cluster Index Summary:\n");
        summary.append("=====================================\n");
        summary.append("Index Name: ").append(STATIC_STRUCTURE_INDEX_NAME).append("\n");
        summary.append("Job ID: ").append(jobId).append("\n");
        summary.append("Partition: ").append(partition).append("\n");
        summary.append("Total Levels: ").append(clusterLevels.size()).append("\n");

        int totalCentroids = 0;
        for (Map<String, Object> level : clusterLevels) {
            int count = (Integer) level.get("centroid_count");
            totalCentroids += count;
            summary.append("  Level ").append(level.get("level")).append(": ").append(count).append(" centroids\n");
        }

        summary.append("Total Centroids: ").append(totalCentroids).append("\n");
        summary.append("Index Path: ").append(getIndexFilePath()).append("\n");

        return summary.toString();
    }
}
