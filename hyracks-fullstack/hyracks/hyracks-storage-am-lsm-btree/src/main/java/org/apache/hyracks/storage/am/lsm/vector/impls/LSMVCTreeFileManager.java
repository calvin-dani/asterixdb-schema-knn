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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;

/**
 * File manager for LSM Vector Clustering Trees.
 * 
 * This class manages the files associated with LSM Vector Clustering Tree components,
 * including naming conventions, file creation, and cleanup operations for both
 * in-memory and disk components.
 */
public class LSMVCTreeFileManager extends AbstractLSMIndexFileManager {

    private static final String VCTREE_SUFFIX = "_vct";

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
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + VCTREE_SUFFIX), null, null);
    }

    @Override
    public LSMComponentFileReferences getRelMergeFileReference(String firstFileName, String lastFileName) {
        final String baseName = IndexComponentFileReference.getMergeSequence(firstFileName, lastFileName);
        return new LSMComponentFileReferences(baseDir.getChild(baseName + DELIMITER + VCTREE_SUFFIX), null, null);
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

        // Eliminate invalid files
        // There can be 0 or 1 VCTree file
        // This will only exist if a flush operation completed successfully
        for (IndexComponentFileReference vcTreeFile : allVCTreeFiles) {
            validFiles.add(new LSMComponentFileReferences(vcTreeFile.getFileRef(), null, null));
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
}
