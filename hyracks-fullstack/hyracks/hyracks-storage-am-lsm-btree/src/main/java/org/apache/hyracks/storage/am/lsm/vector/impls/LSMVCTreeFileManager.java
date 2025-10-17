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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * File manager for LSM Vector Clustering Trees.
 * 
 * This class manages the files associated with LSM Vector Clustering Tree components,
 * including naming conventions, file creation, and cleanup operations for both
 * in-memory and disk components.
 */
public class LSMVCTreeFileManager extends AbstractLSMIndexFileManager {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final String VCTREE_SUFFIX = "_vct";
    private static final String STATIC_STRUCTURE_SUFFIX = ".staticstructure";
    private static final String MASK_FILE_PREFIX = ".";

    private final TreeIndexFactory<? extends ITreeIndex> vcTreeFactory;

    private static final FilenameFilter vcTreeFilter =
            (dir, name) -> !name.startsWith(".") && name.endsWith(VCTREE_SUFFIX);

    public LSMVCTreeFileManager(IIOManager ioManager, FileReference file,
            TreeIndexFactory<? extends ITreeIndex> vcTreeFactory) {
        super(ioManager, file, null);
        this.vcTreeFactory = vcTreeFactory;
    }

    @Override
    public LSMComponentFileReferences getRelFlushFileReference() throws HyracksDataException {
        String baseName = getNextComponentSequence(vcTreeFilter);
        return new LSMVCTreeComponentFileReferences(baseDir.getChild(baseName + DELIMITER + VCTREE_SUFFIX), null,
                null, baseDir.getChild(STATIC_STRUCTURE_SUFFIX));
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName) {
        final String baseName = IndexComponentFileReference.getMergeSequence(firstFileName, lastFileName);
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + VCTREE_SUFFIX), null,
                baseDir.getChild(baseName + DELIMITER + STATIC_STRUCTURE_SUFFIX));
    }

    @Override
    public List<LSMComponentFileReferences> cleanupAndGetValidFiles() throws HyracksDataException {
        List<LSMComponentFileReferences> validFiles = new ArrayList<>();
        ArrayList<IndexComponentFileReference> allVCTreeFiles = new ArrayList<>();
        HashSet<String> reported = new HashSet<>();

        // Gather all VCTree files
        validateFiles(baseDir.getFile(), vcTreeFilter, allVCTreeFiles, reported, vcTreeFactory);

        // Sort all files
        Collections.sort(allVCTreeFiles);

        // Process each VCTree file and validate its corresponding .staticstructure file
        for (IndexComponentFileReference vcTreeFile : allVCTreeFiles) {
            String baseName = vcTreeFile.getSequence();
            FileReference staticStructureFile = baseDir.getChild(baseName + DELIMITER + STATIC_STRUCTURE_SUFFIX);

            // Validate .staticstructure file exists and is valid
            if (validateStaticStructureFile(staticStructureFile)) {
                LOGGER.debug("Valid VCTree component found: {} with .staticstructure", baseName);
                validFiles.add(new LSMComponentFileReferences(vcTreeFile.getFileRef(), null, staticStructureFile));
            } else {
                LOGGER.warn("Invalid or missing .staticstructure file for VCTree component: {}", baseName);
                // Clean up orphaned VCTree file if .staticstructure is missing
                cleanupOrphanedVCTreeFile(vcTreeFile.getFileRef());
            }
        }

        return validFiles;
    }

    @Override
    protected boolean areHolesAllowed() {
        return false; // VCTree components must be contiguous
    }

    /**
     * Validates that there are no invalid files in the directory.
     */
    private void validateFiles(java.io.File dir, FilenameFilter filter, List<IndexComponentFileReference> files,
            HashSet<String> reported, TreeIndexFactory<? extends ITreeIndex> factory) throws HyracksDataException {

        String[] fileNames = dir.list(filter);
        if (fileNames == null) {
            return;
        }

        for (String fileName : fileNames) {
            FileReference fileRef = baseDir.getChild(fileName);
            IndexComponentFileReference iFileRef = IndexComponentFileReference.of(fileRef);
            files.add(iFileRef);
        }
    }

    /**
     * Validates that a .staticstructure file exists and is valid.
     * 
     * @param staticStructureFile The .staticstructure file to validate
     * @return true if the file exists and is valid, false otherwise
     */
    private boolean validateStaticStructureFile(FileReference staticStructureFile) {
        try {
            // Check if file exists
            if (!ioManager.exists(staticStructureFile)) {
                LOGGER.debug("Static structure file does not exist: {}", staticStructureFile.getAbsolutePath());
                return false;
            }

            // Check for mask file (indicates incomplete write)
            FileReference maskFile = getMaskFile(staticStructureFile);
            if (ioManager.exists(maskFile)) {
                LOGGER.debug("Static structure file is being written (mask file exists): {}",
                        maskFile.getAbsolutePath());
                return false;
            }

            // Validate JSON structure by attempting to read it
            byte[] data = ioManager.readAllBytes(staticStructureFile);
            if (data == null || data.length == 0) {
                LOGGER.debug("Static structure file is empty: {}", staticStructureFile.getAbsolutePath());
                return false;
            }

            // Parse JSON to validate structure
            ObjectMapper mapper = new ObjectMapper();
            @SuppressWarnings("unchecked")
            Map<String, Object> structureData = mapper.readValue(data, Map.class);

            if (structureData == null) {
                LOGGER.debug("Static structure file is invalid JSON: {}", staticStructureFile.getAbsolutePath());
                return false;
            }

            // Validate required fields
            if (!structureData.containsKey("numLevels") || !structureData.containsKey("levelDistribution")
                    || !structureData.containsKey("clusterDistribution")) {
                LOGGER.debug("Static structure file missing required fields: {}",
                        staticStructureFile.getAbsolutePath());
                return false;
            }

            LOGGER.debug("Static structure file is valid: {}", staticStructureFile.getAbsolutePath());
            return true;

        } catch (Exception e) {
            LOGGER.debug("Error validating static structure file {}: {}", staticStructureFile.getAbsolutePath(),
                    e.getMessage());
            return false;
        }
    }

    /**
     * Gets the mask file for a .staticstructure file.
     * 
     * @param staticStructureFile The .staticstructure file
     * @return The corresponding mask file
     */
    private FileReference getMaskFile(FileReference staticStructureFile) {
        String maskFileName = MASK_FILE_PREFIX + staticStructureFile.getName();
        return staticStructureFile.getParent().getChild(maskFileName);
    }

    /**
     * Cleans up an orphaned VCTree file when its .staticstructure file is missing or invalid.
     * 
     * @param vcTreeFile The orphaned VCTree file to clean up
     */
    private void cleanupOrphanedVCTreeFile(FileReference vcTreeFile) {
        try {
            LOGGER.info("Cleaning up orphaned VCTree file: {}", vcTreeFile.getAbsolutePath());
            ioManager.delete(vcTreeFile);
        } catch (Exception e) {
            LOGGER.warn("Failed to clean up orphaned VCTree file {}: {}", vcTreeFile.getAbsolutePath(), e.getMessage());
        }
    }
}
