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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reader for binary static structure files created by VCTreeStaticStructureBinaryWriter.
 * 
 * This class reads the static_structure_pages.bin file and recreates the hierarchical
 * structure information needed for LSM component creation and run file management.
 */
public class VCTreeStaticStructureReader {

    // Core infrastructure
    private final IIOManager ioManager;
    private final String indexPath;

    // Structure information loaded from binary file
    private int numLevels;
    private List<Integer> clustersPerLevel;
    private List<List<Integer>> centroidsPerCluster;
    private int maxEntriesPerPage;
    private int totalPages;
    private String binaryFileName;

    // Static structure pages loaded from binary file
    private final List<StaticStructurePage> staticStructurePages;
    private final List<LeafCentroid> leafCentroids;

    /**
     * Represents a static structure page loaded from binary file.
     */
    public static class StaticStructurePage {
        public final int pageId;
        public final int level;
        public final int clusterId;
        public final byte[] pageData;
        public final boolean isLeaf;
        public final int entryCount;

        public StaticStructurePage(int pageId, int level, int clusterId, byte[] pageData, boolean isLeaf,
                int entryCount) {
            this.pageId = pageId;
            this.level = level;
            this.clusterId = clusterId;
            this.pageData = pageData;
            this.isLeaf = isLeaf;
            this.entryCount = entryCount;
        }
    }

    /**
     * Represents a leaf centroid for run file creation.
     */
    public static class LeafCentroid {
        public final int centroidId;
        public final int level;
        public final int clusterId;
        public final double[] embedding;
        public final int pageId;

        public LeafCentroid(int centroidId, int level, int clusterId, double[] embedding, int pageId) {
            this.centroidId = centroidId;
            this.level = level;
            this.clusterId = clusterId;
            this.embedding = embedding;
            this.pageId = pageId;
        }
    }

    /**
     * Constructor for the binary reader.
     */
    public VCTreeStaticStructureReader(IIOManager ioManager, String indexPath) {
        this.ioManager = ioManager;
        this.indexPath = indexPath;
        this.staticStructurePages = new ArrayList<>();
        this.leafCentroids = new ArrayList<>();
    }

    /**
     * Reads the binary static structure file and recreates the structure information.
     */
    public void readStaticStructure() throws HyracksDataException {
        System.err.println("=== READING STATIC STRUCTURE FROM BINARY FILE ===");
        System.err.println("Index path: " + indexPath);

        try {
            // Read metadata file first
            readMetadataFile();

            // Read binary page data
            readBinaryPageFile();

            // Extract leaf centroids for run file creation
            extractLeafCentroids();

            System.err.println("Successfully read static structure:");
            System.err.println("  - Number of levels: " + numLevels);
            System.err.println("  - Clusters per level: " + clustersPerLevel);
            System.err.println("  - Total pages: " + totalPages);
            System.err.println("  - Leaf centroids: " + leafCentroids.size());

        } catch (Exception e) {
            System.err.println("ERROR: Failed to read static structure: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Reads metadata from .staticstructure file (JSON format).
     */
    private void readMetadataFile() throws IOException {
        FileReference metadataFile = ioManager.resolve(indexPath + "/.staticstructure");
        System.err.println("Reading metadata file: " + metadataFile.getAbsolutePath());

        if (!ioManager.exists(metadataFile)) {
            throw new IOException("Metadata file not found: " + metadataFile.getAbsolutePath());
        }

        byte[] jsonData = ioManager.readAllBytes(metadataFile);
        ObjectMapper mapper = new ObjectMapper();

        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = mapper.readValue(jsonData, Map.class);

        // Extract structure parameters
        this.numLevels = (Integer) metadata.get("numLevels");
        @SuppressWarnings("unchecked")
        List<Integer> clustersPerLevel = (List<Integer>) metadata.get("clustersPerLevel");
        @SuppressWarnings("unchecked")
        List<List<Integer>> centroidsPerCluster = (List<List<Integer>>) metadata.get("centroidsPerCluster");
        this.maxEntriesPerPage = (Integer) metadata.get("maxEntriesPerPage");
        this.totalPages = (Integer) metadata.get("totalPages");
        this.binaryFileName = (String) metadata.get("binaryFileName");

        System.err.println("Metadata loaded:");
        System.err.println("  - numLevels: " + numLevels);
        System.err.println("  - clustersPerLevel: " + clustersPerLevel);
        System.err.println("  - centroidsPerCluster: " + centroidsPerCluster);
        System.err.println("  - maxEntriesPerPage: " + maxEntriesPerPage);
        System.err.println("  - totalPages: " + totalPages);
        System.err.println("  - binaryFileName: " + binaryFileName);
    }

    /**
     * Reads binary page data from static_structure_pages.bin file.
     */
    private void readBinaryPageFile() throws IOException {
        FileReference binaryFile = ioManager.resolve(indexPath + "/" + binaryFileName);
        System.err.println("Reading binary file: " + binaryFile.getAbsolutePath());

        if (!ioManager.exists(binaryFile)) {
            throw new IOException("Binary file not found: " + binaryFile.getAbsolutePath());
        }

        byte[] binaryData = ioManager.readAllBytes(binaryFile);
        ByteBuffer buffer = ByteBuffer.wrap(binaryData);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read header: page count
        int pageCount = buffer.getInt();
        System.err.println("Reading " + pageCount + " pages from binary file");

        // Read each page
        for (int i = 0; i < pageCount; i++) {
            StaticStructurePage page = readPageFromBuffer(buffer);
            staticStructurePages.add(page);
        }

        System.err.println("Successfully read " + staticStructurePages.size() + " static structure pages");
    }

    /**
     * Reads a single page from the buffer.
     */
    private StaticStructurePage readPageFromBuffer(ByteBuffer buffer) {
        // Read page header
        int pageId = buffer.getInt();
        int level = buffer.getInt();
        int clusterId = buffer.getInt();
        int isLeafInt = buffer.getInt();
        boolean isLeaf = (isLeafInt == 1);
        int entryCount = buffer.getInt();

        // Read remaining page data
        int remainingDataSize = 4096 - 20; // Page size minus header
        byte[] pageData = new byte[remainingDataSize];
        buffer.get(pageData);

        return new StaticStructurePage(pageId, level, clusterId, pageData, isLeaf, entryCount);
    }

    /**
     * Extracts leaf centroids from static structure pages for run file creation.
     */
    private void extractLeafCentroids() {
        System.err.println("=== EXTRACTING LEAF CENTROIDS ===");

        // Find leaf level (highest level number)
        int leafLevel = numLevels - 1;
        System.err.println("Leaf level: " + leafLevel);

        // Extract centroids from leaf pages
        for (StaticStructurePage page : staticStructurePages) {
            if (page.isLeaf && page.level == leafLevel) {
                // For each entry in the leaf page, create a leaf centroid
                for (int entryIndex = 0; entryIndex < page.entryCount; entryIndex++) {
                    // Generate centroid ID based on page and entry
                    int centroidId = page.pageId * maxEntriesPerPage + entryIndex;

                    // Create placeholder embedding (in real implementation, extract from page data)
                    double[] embedding = createPlaceholderEmbedding(centroidId);

                    LeafCentroid leafCentroid =
                            new LeafCentroid(centroidId, page.level, page.clusterId, embedding, page.pageId);
                    leafCentroids.add(leafCentroid);

                    System.err.println("Created leaf centroid: ID=" + centroidId + ", level=" + page.level
                            + ", cluster=" + page.clusterId + ", page=" + page.pageId);
                }
            }
        }

        System.err.println("Extracted " + leafCentroids.size() + " leaf centroids for run file creation");
    }

    /**
     * Creates a placeholder embedding for testing purposes.
     * In a real implementation, this would extract the actual embedding from page data.
     */
    private double[] createPlaceholderEmbedding(int centroidId) {
        double[] embedding = new double[128]; // Standard embedding size
        for (int i = 0; i < embedding.length; i++) {
            // Use centroid ID as seed for consistent placeholder values
            embedding[i] = Math.sin(centroidId * 0.1 + i * 0.01) * 0.5;
        }
        return embedding;
    }

    /**
     * Gets the structure information for LSM component creation.
     */
    public Map<String, Object> getStructureParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("numLevels", numLevels);
        parameters.put("clustersPerLevel", clustersPerLevel);
        parameters.put("centroidsPerCluster", centroidsPerCluster);
        parameters.put("maxEntriesPerPage", maxEntriesPerPage);
        parameters.put("totalPages", totalPages);
        return parameters;
    }

    /**
     * Gets all leaf centroids for run file creation.
     */
    public List<LeafCentroid> getLeafCentroids() {
        return new ArrayList<>(leafCentroids);
    }

    /**
     * Gets static structure pages for LSM component recreation.
     */
    public List<StaticStructurePage> getStaticStructurePages() {
        return new ArrayList<>(staticStructurePages);
    }

    /**
     * Gets the number of levels in the hierarchy.
     */
    public int getNumLevels() {
        return numLevels;
    }

    /**
     * Gets clusters per level.
     */
    public List<Integer> getClustersPerLevel() {
        return new ArrayList<>(clustersPerLevel);
    }

    /**
     * Gets centroids per cluster.
     */
    public List<List<Integer>> getCentroidsPerCluster() {
        List<List<Integer>> result = new ArrayList<>();
        for (List<Integer> levelCentroids : centroidsPerCluster) {
            result.add(new ArrayList<>(levelCentroids));
        }
        return result;
    }

    /**
     * Gets maximum entries per page.
     */
    public int getMaxEntriesPerPage() {
        return maxEntriesPerPage;
    }

    /**
     * Gets total number of pages.
     */
    public int getTotalPages() {
        return totalPages;
    }
}
