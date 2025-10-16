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
package org.apache.hyracks.storage.am.vector.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.vector.utils.VCTreeNavigationUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Navigator for VCTree static structure files.
 * Provides access to static structure pages without requiring tree initialization.
 * Uses lazy loading - pages are loaded only when needed during traversal.
 */
public class VCTreeStaticStructureNavigator {

    private static final Logger LOGGER = LogManager.getLogger();

    // Core dependencies for page access
    private final IBufferCache bufferCache;
    private final int fileId;
    private final int rootPageId; // Always 0 for VCTreeStaticStructureBuilder
    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final ITreeIndexFrameFactory leafFrameFactory;

    // Structure validation flag
    private boolean isValidated = false;

    /**
     * Create a navigator for the given static structure file.
     * 
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param interiorFrameFactory Factory for creating interior frames
     * @param leafFrameFactory Factory for creating leaf frames
     */
    public VCTreeStaticStructureNavigator(IBufferCache bufferCache, int fileId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
        this.bufferCache = bufferCache;
        this.fileId = fileId;
        this.rootPageId = 0; // VCTreeStaticStructureBuilder always uses page 0 as root
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
    }

    /**
     * Find the closest centroid for the given query vector.
     * 
     * @param queryVector Query vector to find closest centroid for
     * @return ClusterSearchResult containing closest centroid information
     * @throws HyracksDataException if any error occurs during traversal
     */
    public ClusterSearchResult findClosestCentroid(double[] queryVector) throws HyracksDataException {
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("Query vector cannot be null or empty");
        }

        LOGGER.debug("Finding closest centroid for vector of dimension {}", queryVector.length);

        // Validate structure on first use
        if (!isValidated) {
            validateStaticStructure();
            isValidated = true;
        }

        // Use the common navigation logic
        return VCTreeNavigationUtils.findClosestCentroid(bufferCache, fileId, rootPageId, interiorFrameFactory,
                leafFrameFactory, queryVector);
    }

    /**
     * Check if the static structure is valid and accessible.
     * 
     * @return true if structure is valid, false otherwise
     */
    public boolean isStaticStructureValid() {
        try {
            validateStaticStructure();
            return true;
        } catch (HyracksDataException e) {
            LOGGER.warn("Static structure validation failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get the root page ID (always 0 for VCTreeStaticStructureBuilder).
     * 
     * @return Root page ID
     */
    public int getRootPageId() {
        return rootPageId;
    }

    /**
     * Validate that the static structure file is accessible and contains valid data.
     * 
     * @throws HyracksDataException if structure is invalid or inaccessible
     */
    private void validateStaticStructure() throws HyracksDataException {
        // TODO: Add caching support here - we could cache frequently accessed pages
        // For now, we'll just verify the root page exists and is accessible

        try {
            // Try to pin the root page to verify it exists
            long dpid = org.apache.hyracks.storage.common.file.BufferedFileHandle.getDiskPageId(fileId, rootPageId);
            org.apache.hyracks.storage.common.buffercache.ICachedPage page = bufferCache.pin(dpid);

            try {
                page.acquireReadLatch();
                // If we can acquire the latch, the page exists and is accessible
                LOGGER.debug("Static structure validation successful - root page {} is accessible", rootPageId);
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        } catch (Exception e) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Static structure validation failed - root page not accessible: " + e.getMessage());
        }
    }

    /**
     * Get debug information about the navigator configuration.
     * 
     * @return Debug information string
     */
    public String getDebugInfo() {
        return String.format("VCTreeStaticStructureNavigator{fileId=%d, rootPageId=%d, validated=%s}", fileId,
                rootPageId, isValidated);
    }
}
