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
package org.apache.asterix.common.storage;

import java.util.Map;

import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Manages the .staticstructure file for VCTree indexes.
 * This file stores the hierarchical structure information including:
 * - Number of levels
 * - Clusters per level
 * - Centroids per cluster
 * - Structure parameters
 */
public class StaticStructureFileManager {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final IIOManager ioManager;
    private final String indexPath;

    public StaticStructureFileManager(IIOManager ioManager, String indexPath) {
        this.ioManager = ioManager;
        this.indexPath = indexPath;
    }

    /**
     * Writes the static structure information to .staticstructure file
     * 
     * @param structureData Map containing structure parameters
     * @throws HyracksDataException if writing fails
     */
    public void writeStaticStructure(Map<String, Object> structureData) throws HyracksDataException {
        try {
            FileReference structureFile = getStaticStructureFile();

            // Ensure parent directory exists
            FileReference parentDir = structureFile.getParent();
            if (!ioManager.exists(parentDir)) {
                ioManager.makeDirectories(parentDir);
            }

            // Create mask file for atomic write
            FileReference maskFile = getMaskFile();
            if (ioManager.exists(maskFile)) {
                ioManager.delete(maskFile);
            }
            ioManager.create(maskFile);

            try {
                // Write structure data as JSON
                byte[] data = OBJECT_MAPPER.writeValueAsBytes(structureData);
                ioManager.overwrite(structureFile, data);

                // Remove mask file to indicate successful write
                ioManager.delete(maskFile);

                LOGGER.info("Static structure written to: {}", structureFile.getAbsolutePath());

            } catch (Exception e) {
                // Clean up on failure
                if (ioManager.exists(structureFile)) {
                    ioManager.delete(structureFile);
                }
                if (ioManager.exists(maskFile)) {
                    ioManager.delete(maskFile);
                }
                throw HyracksDataException.create(e);
            }

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Reads the static structure information from .staticstructure file
     * 
     * @return Map containing structure parameters
     * @throws HyracksDataException if reading fails
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> readStaticStructure() throws HyracksDataException {
        try {
            FileReference structureFile = getStaticStructureFile();

            if (!ioManager.exists(structureFile)) {
                LOGGER.warn("Static structure file not found: {}", structureFile.getAbsolutePath());
                return null;
            }

            // Check for mask file (indicates incomplete write)
            FileReference maskFile = getMaskFile();
            if (ioManager.exists(maskFile)) {
                LOGGER.warn("Static structure file is being written (mask file exists): {}",
                        maskFile.getAbsolutePath());
                return null;
            }

            byte[] data = ioManager.readAllBytes(structureFile);
            return OBJECT_MAPPER.readValue(data, Map.class);

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Checks if the static structure file exists
     * 
     * @return true if file exists and is not being written
     */
    public boolean exists() {
        try {
            FileReference structureFile = getStaticStructureFile();
            FileReference maskFile = getMaskFile();

            return ioManager.exists(structureFile) && !ioManager.exists(maskFile);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Deletes the static structure file
     * 
     * @throws HyracksDataException if deletion fails
     */
    public void delete() throws HyracksDataException {
        try {
            FileReference structureFile = getStaticStructureFile();
            FileReference maskFile = getMaskFile();

            if (ioManager.exists(structureFile)) {
                ioManager.delete(structureFile);
            }
            if (ioManager.exists(maskFile)) {
                ioManager.delete(maskFile);
            }

            LOGGER.info("Static structure file deleted: {}", structureFile.getAbsolutePath());

        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Gets the FileReference for the .staticstructure file
     * Uses a simple approach that doesn't trigger AsterixDB path parsing
     */
    private FileReference getStaticStructureFile() throws HyracksDataException {
        // Use the first available IO device to avoid path parsing issues
        int deviceId = 0; // Use first device
        String structureFilePath = indexPath + "/" + StorageConstants.STATIC_STRUCTURE_FILE_NAME;
        return ioManager.getFileReference(deviceId, structureFilePath);
    }

    /**
     * Gets the FileReference for the mask file
     * Uses a simple approach that doesn't trigger AsterixDB path parsing
     */
    private FileReference getMaskFile() throws HyracksDataException {
        // Use the first available IO device to avoid path parsing issues
        int deviceId = 0; // Use first device
        String maskFilePath =
                indexPath + "/" + StorageConstants.MASK_FILE_PREFIX + StorageConstants.STATIC_STRUCTURE_FILE_NAME;
        return ioManager.getFileReference(deviceId, maskFilePath);
    }

    /**
     * Creates a StaticStructureFileManager for a given index path
     * 
     * @param ioManager IOManager instance
     * @param indexPath Path to the index directory
     * @return StaticStructureFileManager instance
     */
    public static StaticStructureFileManager create(IIOManager ioManager, String indexPath) {
        return new StaticStructureFileManager(ioManager, indexPath);
    }
}
