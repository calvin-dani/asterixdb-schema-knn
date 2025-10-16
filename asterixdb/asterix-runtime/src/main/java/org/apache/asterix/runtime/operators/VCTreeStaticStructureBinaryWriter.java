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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManager;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Simplified binary writer for VCTree static structure pages.
 * Uses existing AsterixDB infrastructure for file I/O and page management.
 * 
 * This class creates static structure pages and writes them to binary files
 * using the standard IIOManager and FileReference APIs.
 */
public class VCTreeStaticStructureBinaryWriter {

    private static final int VECTOR_DIMENSION = 256;
    // Core infrastructure
    private final IBufferCache bufferCache;
    private final IIOManager ioManager;
    private final String indexPath;

    // Page management
    private final AppendOnlyLinkedMetadataPageManager freePageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    private final int fileId;

    // Structure configuration
    private final int numLevels;
    private final List<Integer> clustersPerLevel;
    private final List<List<Integer>> centroidsPerCluster;
    private final int maxEntriesPerPage;

    // Page tracking
    private final List<StaticStructurePage> staticStructurePages;
    private int nextPageId = 1;

    // Serializers for tuple data
    private final ISerializerDeserializer<?> intSerde = IntegerSerializerDeserializer.INSTANCE;

    /**
     * Represents a static structure page with its data and metadata.
     */
    public static class StaticStructurePage {
        public final int pageId;
        public final int level;
        public final int clusterId;
        public final byte[] pageData;
        public final boolean isLeaf;

        public StaticStructurePage(int pageId, int level, int clusterId, byte[] pageData, boolean isLeaf) {
            this.pageId = pageId;
            this.level = level;
            this.clusterId = clusterId;
            this.pageData = pageData;
            this.isLeaf = isLeaf;
        }
    }

    /**
     * Constructor for the binary writer.
     */
    public VCTreeStaticStructureBinaryWriter(IBufferCache bufferCache, IIOManager ioManager, String indexPath,
            int numLevels, List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int maxEntriesPerPage) throws HyracksDataException {

        this.bufferCache = bufferCache;
        this.ioManager = ioManager;
        this.indexPath = indexPath;
        System.err.println("DEBUG: VCTreeStaticStructureBinaryWriter initialized with indexPath: " + indexPath);
        this.numLevels = numLevels;
        this.clustersPerLevel = clustersPerLevel;
        this.centroidsPerCluster = centroidsPerCluster;
        this.maxEntriesPerPage = maxEntriesPerPage;
        this.staticStructurePages = new ArrayList<>();

        // Initialize page management infrastructure
        this.fileId = createMinimalFileReference();
        this.freePageManager = new AppendOnlyLinkedMetadataPageManager(bufferCache, new LIFOMetaDataFrameFactory());
        this.metaFrame = freePageManager.createMetadataFrame();

        System.err.println("VCTreeStaticStructureBinaryWriter initialized:");
        System.err.println("  - Index path: " + indexPath);
        System.err.println("  - File ID: " + fileId);
        System.err.println("  - Max entries per page: " + maxEntriesPerPage);
    }

    /**
     * Creates a minimal file reference for page ID management.
     */
    private int createMinimalFileReference() throws HyracksDataException {
        try {
            // Create a temporary file for page ID management
            FileReference tempFile = ioManager.resolve(indexPath + "/temp_page_manager");
            if (ioManager.exists(tempFile)) {
                ioManager.delete(tempFile);
            }
            ioManager.create(tempFile);
            return 1; // Use a simple file ID for now
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Processes hierarchical clustering tuples and creates static structure pages.
     */
    public void processTuples(List<ITupleReference> tuples) throws HyracksDataException {
        System.err.println("Processing " + tuples.size() + " tuples for static structure creation");

        for (ITupleReference tuple : tuples) {
            processTuple(tuple);
        }

        System.err.println("Created " + staticStructurePages.size() + " static structure pages");
    }

    /**
     * Processes a single tuple and creates appropriate static structure pages.
     */
    private void processTuple(ITupleReference tuple) throws HyracksDataException {
        try {
            // Extract centroid information from tuple
            CentroidInfo centroidInfo = extractCentroidInfo(tuple);

            // Determine if this is a leaf page (level 0) or interior page
            boolean isLeaf = (centroidInfo.level == 0);

            // Create or get the current page for this level/cluster
            StaticStructurePage currentPage = getCurrentPage(centroidInfo.level, centroidInfo.clusterId, isLeaf);

            // Add entry to the page
            addEntryToPage(currentPage, centroidInfo);

        } catch (Exception e) {
            System.err.println("Error processing tuple: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Extracts centroid information from a tuple.
     */
    private CentroidInfo extractCentroidInfo(ITupleReference tuple) throws HyracksDataException {
        try {
            // For now, create dummy data since we don't have access to tuple field extraction
            // In a real implementation, you would extract these from the tuple fields
            int level = 0; // Default to level 0
            int clusterId = 0; // Default to cluster 0
            int centroidId = nextPageId; // Use page ID as centroid ID

            // Create a dummy embedding (in real implementation, extract from tuple)
            double[] embedding = new double[VECTOR_DIMENSION]; // 256-dimensional embedding
            for (int i = 0; i < embedding.length; i++) {
                embedding[i] = Math.random() * 2 - 1; // Random values between -1 and 1
            }

            return new CentroidInfo(level, clusterId, centroidId, embedding);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Gets or creates the current page for the specified level and cluster.
     */
    private StaticStructurePage getCurrentPage(int level, int clusterId, boolean isLeaf) throws HyracksDataException {
        // For simplicity, create one page per level/cluster combination
        int pageId = nextPageId++;

        // Create page data buffer
        byte[] pageData = new byte[4096]; // Standard page size
        ByteBuffer buffer = ByteBuffer.wrap(pageData);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Write basic page header
        buffer.putInt(pageId);
        buffer.putInt(level);
        buffer.putInt(clusterId);
        buffer.putInt(isLeaf ? 1 : 0);
        buffer.putInt(0); // Entry count (will be updated)

        StaticStructurePage page = new StaticStructurePage(pageId, level, clusterId, pageData, isLeaf);
        staticStructurePages.add(page);

        return page;
    }

    /**
     * Adds an entry to the specified page.
     */
    private void addEntryToPage(StaticStructurePage page, CentroidInfo centroidInfo) {
        // For now, just update the entry count in the page header
        ByteBuffer buffer = ByteBuffer.wrap(page.pageData);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Skip to entry count position (after pageId, level, clusterId, isLeaf)
        buffer.position(16);
        int currentCount = buffer.getInt();
        buffer.position(16);
        buffer.putInt(currentCount + 1);

    }

    /**
     * Writes the static structure to files.
     */
    public void writeToBinaryFile() throws HyracksDataException {
        System.err.println("Writing static structure to binary files");

        try {
            // Write JSON metadata to .staticstructure file
            writeMetadataFile();

            // Write binary page data to .staticstructure.bin file
            writeBinaryPageFile();

            System.err.println("Successfully wrote static structure files");

        } catch (Exception e) {
            System.err.println("Error writing static structure files: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Writes metadata to .staticstructure file (JSON format).
     */
    private void writeMetadataFile() throws IOException {
        FileReference metadataFile = ioManager.resolve(indexPath + "/.staticstructure");
        System.err.println("DEBUG: Writing metadata file to: " + metadataFile.getAbsolutePath());

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("numLevels", numLevels);
        metadata.put("clustersPerLevel", clustersPerLevel);
        metadata.put("centroidsPerCluster", centroidsPerCluster);
        metadata.put("maxEntriesPerPage", maxEntriesPerPage);
        metadata.put("totalPages", staticStructurePages.size());
        metadata.put("binaryFileName", "static_structure_pages.bin");

        ObjectMapper mapper = new ObjectMapper();
        byte[] jsonData = mapper.writeValueAsBytes(metadata);

        ioManager.overwrite(metadataFile, jsonData);
        System.err.println("Wrote metadata to: " + metadataFile.getAbsolutePath());
    }

    /**
     * Writes binary page data to .staticstructure.bin file.
     */
    private void writeBinaryPageFile() throws IOException {
        FileReference binaryFile = ioManager.resolve(indexPath + "/static_structure_pages.bin");
        System.err.println("DEBUG: Writing binary file to: " + binaryFile.getAbsolutePath());

        // Calculate total size needed
        int totalSize = 4; // Header: page count
        for (StaticStructurePage page : staticStructurePages) {
            totalSize += page.pageData.length;
        }

        // Create buffer for all data
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Write header
        buffer.putInt(staticStructurePages.size());

        // Write each page
        for (StaticStructurePage page : staticStructurePages) {
            buffer.put(page.pageData);
        }

        // Write to file
        byte[] data = new byte[buffer.position()];
        buffer.rewind();
        buffer.get(data);

        ioManager.overwrite(binaryFile, data);
        System.err.println("Wrote binary page data to: " + binaryFile.getAbsolutePath());
        System.err.println("Total size: " + data.length + " bytes (" + staticStructurePages.size() + " pages)");
    }

    /**
     * Centroid information extracted from tuple.
     */
    private static class CentroidInfo {
        public final int level;
        public final int clusterId;
        public final int centroidId;
        public final double[] embedding;

        public CentroidInfo(int level, int clusterId, int centroidId, double[] embedding) {
            this.level = level;
            this.clusterId = clusterId;
            this.centroidId = centroidId;
            this.embedding = embedding;
        }
    }
}
