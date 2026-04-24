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

import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;

/**
 * Manages static structure files for VCTree indexes.
 * This class provides utilities for creating, reading, and cleaning up static structure files.
 */
public class StaticStructureFileManager {
    private static final Logger LOGGER = Logger.getLogger(StaticStructureFileManager.class.getName());

    public static final String STATIC_STRUCTURE_FILE_NAME = ".static_structure_vctree";

    private final IIOManager ioManager;

    public StaticStructureFileManager(IIOManager ioManager) {
        this.ioManager = ioManager;
    }

    /**
     * Get the static structure file path for a given index directory.
     * 
     * @param indexDir The index directory
     * @return FileReference to the static structure file
     */
    public FileReference getStaticStructureFile(FileReference indexDir) {
        return indexDir.getChild(STATIC_STRUCTURE_FILE_NAME);
    }

    /**
     * Check if a static structure file exists for the given index directory.
     * 
     * @param indexDir The index directory
     * @return true if the static structure file exists
     */
    public boolean exists(FileReference indexDir) {
        try {
            FileReference staticStructureFile = getStaticStructureFile(indexDir);
            return ioManager.exists(staticStructureFile);
        } catch (Exception e) {
            LOGGER.warning("Failed to check if static structure file exists: " + e.getMessage());
            return false;
        }
    }

    /**
     * Delete the static structure file for the given index directory.
     * 
     * @param indexDir The index directory
     * @return true if the file was deleted, false if it didn't exist
     * @throws HyracksDataException if deletion fails
     */
    public boolean delete(FileReference indexDir) throws HyracksDataException {
        try {
            FileReference staticStructureFile = getStaticStructureFile(indexDir);
            if (ioManager.exists(staticStructureFile)) {
                ioManager.delete(staticStructureFile);
                LOGGER.info("Deleted static structure file: " + staticStructureFile.getAbsolutePath());
                return true;
            }
            return false;
        } catch (Exception e) {
            LOGGER.warning("Failed to delete static structure file: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Clean up all static structure files in the given directory tree.
     * This method recursively searches for and deletes static structure files.
     * 
     * @param baseDir The base directory to search
     * @return Number of files deleted
     * @throws HyracksDataException if cleanup fails
     */
    public int cleanupAll(FileReference baseDir) throws HyracksDataException {
        int deletedCount = 0;
        try {
            // Get all files in the directory
            java.util.Set<FileReference> files = ioManager.list(baseDir, null);

            for (FileReference file : files) {
                String fileName = file.getName();
                // Look for both old format and new timestamped format
                if (STATIC_STRUCTURE_FILE_NAME.equals(fileName)
                        || fileName.startsWith(".static_structure_") && fileName.endsWith(".vctree")) {
                    // Found a static structure file, delete it
                    try {
                        ioManager.delete(file);
                        LOGGER.info("Deleted static structure file: " + file.getAbsolutePath());
                        deletedCount++;
                    } catch (Exception e) {
                        LOGGER.warning("Failed to delete static structure file " + file.getAbsolutePath() + ": "
                                + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warning(
                    "Failed to cleanup static structure files in " + baseDir.getAbsolutePath() + ": " + e.getMessage());
            throw HyracksDataException.create(e);
        }

        return deletedCount;
    }
}