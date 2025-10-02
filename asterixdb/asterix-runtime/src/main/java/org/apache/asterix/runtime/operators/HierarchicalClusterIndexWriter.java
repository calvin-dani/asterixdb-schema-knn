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
        // Check if level already exists and remove it first
        clusterLevels.removeIf(levelData -> (Integer) levelData.get("level") == level);

        Map<String, Object> levelData = new HashMap<>();
        levelData.put("level", level);
        levelData.put("centroid_count", centroids.size());

        // Add centroid information
        List<Map<String, Object>> centroidList = new ArrayList<>();

        for (HierarchicalCentroidsState.HierarchicalCentroid centroid : centroids) {
            Map<String, Object> centroidData = new HashMap<>();
            HierarchicalClusterId clusterId = centroid.getClusterId();

            centroidData.put("cluster_id", clusterId.getClusterId());
            centroidData.put("global_id", clusterId.getGlobalId());
            centroidData.put("level", clusterId.getLevel());
            centroidData.put("has_parent", clusterId.hasParent());
            if (clusterId.hasParent()) {
                centroidData.put("parent_cluster_id", clusterId.getParentClusterId());
                System.err.println("DEBUG: Adding centroid with parent - Level " + level + ", Cluster "
                        + clusterId.getClusterId() + ", Parent " + clusterId.getParentClusterId());
            } else {
                System.err.println("DEBUG: Adding centroid without parent - Level " + level + ", Cluster "
                        + clusterId.getClusterId());
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
        }

        levelData.put("centroids", centroidList);
        clusterLevels.add(levelData);
    }

    /**
     * Adds hierarchy structure information
     */
    public void addHierarchyStructure(int totalLevels, int totalCentroids, Map<String, Object> structureInfo) {
        hierarchyStructure.put("total_levels", totalLevels);
        hierarchyStructure.put("total_centroids", totalCentroids);
        hierarchyStructure.put("structure_info", structureInfo);
    }

    /**
     * Builds a simple tree structure with just basic information
     */
    private Map<String, Object> buildSimpleTree() {
        Map<String, Object> tree = new HashMap<>();

        // Find the root level (highest level number)
        int rootLevel = -1;
        for (Map<String, Object> level : clusterLevels) {
            int levelNum = (Integer) level.get("level");
            if (levelNum > rootLevel) {
                rootLevel = levelNum;
            }
        }

        if (rootLevel == -1) {
            tree.put("levels", new ArrayList<>());
            return tree;
        }

        // Build simple level-by-level structure
        List<Map<String, Object>> levels = new ArrayList<>();

        // Process each level from root to leaves
        for (int level = rootLevel; level >= 0; level--) {
            Map<String, Object> levelData = findLevelData(level);
            if (levelData != null) {
                Map<String, Object> levelInfo = new HashMap<>();
                levelInfo.put("level", level);
                levelInfo.put("centroids", buildBasicCentroids(levelData));
                levels.add(levelInfo);
            }
        }

        tree.put("levels", levels);
        tree.put("total_levels", levels.size());

        return tree;
    }

    /**
     * Finds level data by level number
     */
    private Map<String, Object> findLevelData(int level) {
        for (Map<String, Object> levelInfo : clusterLevels) {
            if ((Integer) levelInfo.get("level") == level) {
                return levelInfo;
            }
        }
        return null;
    }

    /**
     * Builds basic centroid structure with minimal information
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> buildBasicCentroids(Map<String, Object> levelData) {
        List<Map<String, Object>> centroids = (List<Map<String, Object>>) levelData.get("centroids");
        List<Map<String, Object>> basicCentroids = new ArrayList<>();

        for (Map<String, Object> centroid : centroids) {
            Map<String, Object> basicCentroid = new HashMap<>();
            basicCentroid.put("cluster_id", centroid.get("cluster_id"));
            basicCentroid.put("global_id", centroid.get("global_id"));

            // ALWAYS add parent information if it exists - this should match the stdout output
            if ((Boolean) centroid.get("has_parent")) {
                basicCentroid.put("parent_cluster_id", centroid.get("parent_cluster_id"));
            }

            basicCentroids.add(basicCentroid);
        }

        return basicCentroids;
    }

    /**
     * Writes the complete index to a JSON file
     */
    public void writeIndex() throws HyracksDataException {
        try {
            // Create simple tree structure
            Map<String, Object> tree = buildSimpleTree();

            // Create directory if it doesn't exist
            createIndexDirectory();

            // Write to file
            String indexPath = getIndexFilePath();
            writeJsonToFile(tree, indexPath);

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
            // Create simple tree structure
            Map<String, Object> tree = buildSimpleTree();

            // Create managed workspace file
            FileReference indexFile =
                    ctx.getJobletContext().createManagedWorkspaceFile("hierarchical_cluster_index_" + partition);

            // Write JSON to the managed file
            writeJsonToFile(tree, indexFile.getFile().getAbsolutePath());

            System.out.println(
                    "Hierarchical cluster index written to side file: " + indexFile.getFile().getAbsolutePath());

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
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
     * Gets the cluster levels
     */
    public List<Map<String, Object>> getClusterLevels() {
        return clusterLevels;
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

}
