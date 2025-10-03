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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheWriteContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Static structure bulk loader for VectorClusteringTree.
 * Builds predetermined hierarchical structure using protocol-based tuple stream.
 */
public class VCTreeStaticStructureLoader extends AbstractTreeIndexBulkLoader {

    // Protocol tuple types
    public enum TupleType {
        LEVEL_START,
        CLUSTER_START,
        CENTROID_DATA,
        CLUSTER_END,
        LEVEL_END
    }

    public enum PageType {
        LEAF,
        INTERIOR,
        ROOT,
        METADATA
    }

    // Loader state
    private final VectorClusteringTree vectorTree;
    private final Stack<LevelBuilder> levelStack;
    private final Map<Integer, List<Integer>> levelPageIds;
    private final Map<Integer, Integer> centroidToPageMap;

    private LevelBuilder currentLevel;
    private PageBuilder currentPage;
    private boolean structureComplete;

    /**
     * Represents a level being constructed
     */
    private static class LevelBuilder {
        final int level;
        final List<PageBuilder> pages;
        final PageType expectedPageType;

        LevelBuilder(int level, PageType expectedPageType) {
            this.level = level;
            this.pages = new ArrayList<>();
            this.expectedPageType = expectedPageType;
        }

        void addPage(PageBuilder page) {
            pages.add(page);
        }
    }

    /**
     * Page builder with overflow support
     */
    private static class PageBuilder {
        final int pageId;
        final ICachedPage page;
        final PageType pageType;
        final List<CentroidEntry> centroids;
        ITreeIndexFrame frame;

        // Overflow page support
        PageBuilder overflowPage;
        int nextPagePointer = -1;
        boolean isOverflowPage = false;
        PageBuilder primaryPage;

        PageBuilder(int pageId, ICachedPage page, PageType pageType) {
            this.pageId = pageId;
            this.page = page;
            this.pageType = pageType;
            this.centroids = new ArrayList<>();
        }

        PageBuilder(int pageId, ICachedPage page, PageType pageType, PageBuilder primaryPage) {
            this(pageId, page, pageType);
            this.isOverflowPage = true;
            this.primaryPage = primaryPage;
        }

        void addCentroid(int centroidId, float[] embedding, int childPageId) {
            centroids.add(new CentroidEntry(centroidId, embedding, childPageId));
        }

        PageBuilder createOverflowPage(IPageManager pageManager, int fileId, IBufferCache bufferCache,
                ITreeIndexMetadataFrame metaFrame) throws HyracksDataException {
            if (overflowPage != null) {
                return overflowPage.createOverflowPage(pageManager, fileId, bufferCache, metaFrame);
            }

            int overflowPageId = pageManager.takePage(metaFrame);
            long dpid = BufferedFileHandle.getDiskPageId(fileId, overflowPageId);
            ICachedPage overflowCachedPage = bufferCache.pin(dpid);

            PageBuilder overflowBuilder =
                    new PageBuilder(overflowPageId, overflowCachedPage, pageType, isOverflowPage ? primaryPage : this);

            this.overflowPage = overflowBuilder;
            this.nextPagePointer = overflowPageId;

            System.out.println("DEBUG: Created overflow page " + overflowPageId + " for primary page "
                    + (isOverflowPage ? primaryPage.pageId : pageId));

            return overflowBuilder;
        }

        PageBuilder getPrimaryPage() {
            return isOverflowPage ? primaryPage : this;
        }
    }

    /**
     * Centroid entry in a page
     */
    private static class CentroidEntry {
        final int centroidId;
        final float[] embedding;
        final int childPageId;

        CentroidEntry(int centroidId, float[] embedding, int childPageId) {
            this.centroidId = centroidId;
            this.embedding = embedding;
            this.childPageId = childPageId;
        }
    }

    public VCTreeStaticStructureLoader(float fillFactor, boolean verifyInput, IPageWriteCallback callback,
            VectorClusteringTree vectorTree, ITreeIndexFrame leafFrame, IBufferCacheWriteContext writeContext)
            throws HyracksDataException {
        super(fillFactor, callback, vectorTree, leafFrame, writeContext);

        this.vectorTree = vectorTree;
        this.levelStack = new Stack<>();
        this.levelPageIds = new HashMap<>();
        this.centroidToPageMap = new HashMap<>();
        this.structureComplete = false;

        System.out.println("DEBUG: VCTreeStaticStructureLoader initialized");
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        if (structureComplete) {
            throw new HyracksDataException("Structure loading already complete");
        }

        TupleType tupleType = parseTupleType(tuple);

        switch (tupleType) {
            case LEVEL_START:
                handleLevelStart(tuple);
                break;
            case CLUSTER_START:
                handlePageStart(tuple);
                break;
            case CENTROID_DATA:
                handleCentroidData(tuple);
                break;
            case CLUSTER_END:
                handlePageEnd();
                break;
            case LEVEL_END:
                handleLevelEnd();
                break;
            default:
                throw new HyracksDataException("Unknown tuple type: " + tupleType);
        }
    }

    /**
     * Parse tuple type from first field using TupleUtils
     */
    private TupleType parseTupleType(ITupleReference tuple) throws HyracksDataException {
        int typeCode = VCTreeProtocolTuples.parseTupleType(tuple);

        switch (typeCode) {
            case VCTreeProtocolTuples.LEVEL_START_TYPE:
                return TupleType.LEVEL_START;
            case VCTreeProtocolTuples.CLUSTER_START_TYPE:
                return TupleType.CLUSTER_START;
            case VCTreeProtocolTuples.CENTROID_DATA_TYPE:
                return TupleType.CENTROID_DATA;
            case VCTreeProtocolTuples.CLUSTER_END_TYPE:
                return TupleType.CLUSTER_END;
            case VCTreeProtocolTuples.LEVEL_END_TYPE:
                return TupleType.LEVEL_END;
            default:
                throw new HyracksDataException("Invalid tuple type code: " + typeCode);
        }
    }

    /**
     * Handle LEVEL_START tuple
     */
    private void handleLevelStart(ITupleReference tuple) throws HyracksDataException {
        VCTreeProtocolTuples.LevelStartData data = VCTreeProtocolTuples.parseLevelStartTuple(tuple);
        PageType pageType = convertToPageType(data.pageType);

        System.out.println("DEBUG: Starting level " + data.level + " with page type " + pageType);

        currentLevel = new LevelBuilder(data.level, pageType);
        levelStack.push(currentLevel);
        levelPageIds.put(data.level, new ArrayList<>());
    }

    /**
     * Handle PAGE_START tuple
     */
    private void handlePageStart(ITupleReference tuple) throws HyracksDataException {
        if (currentLevel == null) {
            throw new HyracksDataException("PAGE_START without LEVEL_START");
        }

        VCTreeProtocolTuples.PageStartData data = VCTreeProtocolTuples.parsePageStartTuple(tuple);
        PageType pageType = convertToPageType(data.pageType);

        if (pageType != currentLevel.expectedPageType) {
            throw new HyracksDataException(
                    "Page type mismatch. Expected: " + currentLevel.expectedPageType + ", got: " + pageType);
        }

        // Allocate new page
        int pageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
        ICachedPage page = bufferCache.pin(dpid);

        currentPage = new PageBuilder(pageId, page, pageType);
        currentPage.frame = createFrameForPageType(pageType);

        System.out.println("DEBUG: Starting page " + pageId + " of type " + pageType);
    }

    /**
     * Handle CENTROID_DATA tuple - lookup child page by centroid ID
     */
    private void handleCentroidData(ITupleReference tuple) throws HyracksDataException {
        if (currentPage == null) {
            throw new HyracksDataException("CENTROID_DATA without PAGE_START");
        }

        VCTreeProtocolTuples.CentroidDataTuple data = VCTreeProtocolTuples.parseCentroidDataTuple(tuple);

        // For static structure: child page ID determined by centroid ID mapping
        int childPageId = -1;
        if (currentLevel.level > 0) {
            // Interior/root page - lookup child page from lower level
            Integer childId = centroidToPageMap.get(data.centroidId);
            if (childId == null) {
                throw new HyracksDataException("No child page found for centroid " + data.centroidId + " at level "
                        + currentLevel.level + ". Centroid must exist in lower level first.");
            }
            childPageId = childId;

            System.out.println("DEBUG: Centroid " + data.centroidId + " at level " + currentLevel.level
                    + " points to child page " + childPageId);
        } else {
            System.out.println("DEBUG: Leaf centroid " + data.centroidId + " (no child page)");
        }

        // Add centroid to current page with resolved child page ID
        currentPage.addCentroid(data.centroidId, data.embedding, childPageId);
    }

    /**
     * Handle PAGE_END - create overflow pages if needed and track mappings
     */
    private void handlePageEnd() throws HyracksDataException {
        if (currentPage == null) {
            throw new HyracksDataException("PAGE_END without PAGE_START");
        }

        // Create overflow pages if needed
        List<PageBuilder> pageChain = createPageChainWithOverflow(currentPage);

        // Initialize all pages in the chain
        for (PageBuilder page : pageChain) {
            initializePageFrame(page);
        }

        // Add primary page to current level
        PageBuilder primaryPage = pageChain.get(0);
        currentLevel.addPage(primaryPage);
        levelPageIds.get(currentLevel.level).add(primaryPage.pageId);

        // Update centroid-to-page mapping for next level
        for (CentroidEntry centroid : primaryPage.centroids) {
            centroidToPageMap.put(centroid.centroidId, primaryPage.pageId);
            System.out.println("DEBUG: Mapped centroid " + centroid.centroidId + " -> page " + primaryPage.pageId
                    + " for level " + currentLevel.level);
        }

        // Map overflow centroids to PRIMARY page (upper level references primary only)
        PageBuilder currentOverflow = primaryPage.overflowPage;
        while (currentOverflow != null) {
            for (CentroidEntry centroid : currentOverflow.centroids) {
                centroidToPageMap.put(centroid.centroidId, primaryPage.pageId);
                System.out.println("DEBUG: Mapped overflow centroid " + centroid.centroidId + " -> primary page "
                        + primaryPage.pageId);
            }
            currentOverflow = currentOverflow.overflowPage;
        }

        System.out.println("DEBUG: Completed page chain starting with " + primaryPage.pageId + " (" + pageChain.size()
                + " pages total)");

        currentPage = null;
    }

    /**
     * Handle LEVEL_END tuple
     */
    private void handleLevelEnd() throws HyracksDataException {
        if (currentLevel == null) {
            throw new HyracksDataException("LEVEL_END without LEVEL_START");
        }

        System.out.println(
                "DEBUG: Completed level " + currentLevel.level + " with " + currentLevel.pages.size() + " pages");

        levelStack.pop();
        currentLevel = levelStack.isEmpty() ? null : levelStack.peek();
    }

    /**
     * Create page chain with overflow pages if needed
     */
    private List<PageBuilder> createPageChainWithOverflow(PageBuilder primaryPage) throws HyracksDataException {
        List<PageBuilder> pageChain = new ArrayList<>();
        int maxTuplesPerPage = getMaxTuplesPerPage(primaryPage.pageType);

        if (primaryPage.centroids.size() <= maxTuplesPerPage) {
            pageChain.add(primaryPage);
            return pageChain;
        }

        // Distribute centroids across multiple pages
        List<CentroidEntry> allCentroids = new ArrayList<>(primaryPage.centroids);
        primaryPage.centroids.clear();

        PageBuilder currentPageBuilder = primaryPage;
        int centroidIndex = 0;

        while (centroidIndex < allCentroids.size()) {
            int centroidsInCurrentPage = 0;
            while (centroidIndex < allCentroids.size() && centroidsInCurrentPage < maxTuplesPerPage) {
                CentroidEntry centroid = allCentroids.get(centroidIndex);
                currentPageBuilder.addCentroid(centroid.centroidId, centroid.embedding, centroid.childPageId);
                centroidIndex++;
                centroidsInCurrentPage++;
            }

            pageChain.add(currentPageBuilder);

            if (centroidIndex < allCentroids.size()) {
                currentPageBuilder =
                        currentPageBuilder.createOverflowPage(freePageManager, fileId, bufferCache, metaFrame);
            }
        }

        System.out.println("DEBUG: Created page chain with " + pageChain.size() + " pages for " + allCentroids.size()
                + " centroids");

        return pageChain;
    }

    /**
     * Get maximum tuples per page
     */
    private int getMaxTuplesPerPage(PageType pageType) {
        switch (pageType) {
            case LEAF:
            case INTERIOR:
            case ROOT:
                return 2; // Assume frames can hold 2 cluster entries
            case METADATA:
                return 10; // Metadata pages can hold more entries
            default:
                return 1;
        }
    }

    /**
     * Create frame for page type
     */
    private ITreeIndexFrame createFrameForPageType(PageType pageType) throws HyracksDataException {
        switch (pageType) {
            case LEAF:
                return vectorTree.getLeafFrameFactory().createFrame();
            case INTERIOR:
            case ROOT:
                return vectorTree.getInteriorFrameFactory().createFrame();
            case METADATA:
                return vectorTree.getMetadataFrameFactory().createFrame();
            default:
                throw new HyracksDataException("Unknown page type: " + pageType);
        }
    }

    /**
     * Initialize page frame
     */
    private void initializePageFrame(PageBuilder pageBuilder) throws HyracksDataException {
        ICachedPage page = pageBuilder.page;
        ITreeIndexFrame frame = pageBuilder.frame;

        page.acquireWriteLatch();
        try {
            frame.setPage(page);
            frame.initBuffer((byte) currentLevel.level);

            // Use first centroid as page representative
            double[] pageRepresentative = getPageRepresentative(pageBuilder);

            switch (pageBuilder.pageType) {
                case LEAF:
                    initializeLeafFrame(pageBuilder, pageRepresentative);
                    break;
                case INTERIOR:
                case ROOT:
                    initializeInteriorFrame(pageBuilder, pageRepresentative);
                    break;
                case METADATA:
                    initializeMetadataFrame(pageBuilder, pageRepresentative);
                    break;
            }

        } finally {
            page.releaseWriteLatch(true);
            bufferCache.unpin(page);
        }
    }

    /**
     * Get page representative (use first centroid)
     */
    private double[] getPageRepresentative(PageBuilder pageBuilder) {
        if (!pageBuilder.centroids.isEmpty()) {
            float[] firstCentroid = pageBuilder.centroids.get(0).embedding;
            double[] pageRepresentative = new double[firstCentroid.length];
            for (int i = 0; i < firstCentroid.length; i++) {
                pageRepresentative[i] = firstCentroid[i];
            }
            return pageRepresentative;
        }
        return new double[4]; // Default 4D zero vector
    }

    /**
     * Initialize leaf frame
     */
    private void initializeLeafFrame(PageBuilder pageBuilder, double[] pageRepresentative) throws HyracksDataException {

        // Set overflow page pointer
        if (pageBuilder.overflowPage != null) {
            System.out.println("DEBUG: Leaf page " + pageBuilder.pageId + " linked to overflow page "
                    + pageBuilder.nextPagePointer);
        }

        // Insert centroid tuples - each points to a metadata page
        for (CentroidEntry centroid : pageBuilder.centroids) {
            int metadataPageId = createMetadataPage(centroid);
            ITupleReference clusterTuple = createClusterTuple(centroid.centroidId, centroid.embedding, metadataPageId);

            System.out.println("DEBUG: Leaf cluster " + centroid.centroidId + " -> metadata page " + metadataPageId);
        }
    }

    /**
     * Initialize interior frame
     */
    private void initializeInteriorFrame(PageBuilder pageBuilder, double[] pageRepresentative)
            throws HyracksDataException {

        // Set overflow page pointer
        if (pageBuilder.overflowPage != null) {
            System.out.println("DEBUG: Interior page " + pageBuilder.pageId + " linked to overflow page "
                    + pageBuilder.nextPagePointer);
        }

        // Insert centroid tuples - each points to a child page
        for (CentroidEntry centroid : pageBuilder.centroids) {
            ITupleReference clusterTuple =
                    createClusterTuple(centroid.centroidId, centroid.embedding, centroid.childPageId);

            System.out.println(
                    "DEBUG: Interior cluster " + centroid.centroidId + " -> child page " + centroid.childPageId);
        }
    }

    /**
     * Initialize metadata frame
     */
    private void initializeMetadataFrame(PageBuilder pageBuilder, double[] pageRepresentative)
            throws HyracksDataException {
        System.out.println("DEBUG: Initialized metadata page " + pageBuilder.pageId);
    }

    /**
     * Create metadata page for a leaf cluster
     */
    private int createMetadataPage(CentroidEntry centroid) throws HyracksDataException {
        int metadataPageId = freePageManager.takePage(metaFrame);
        long dpid = BufferedFileHandle.getDiskPageId(fileId, metadataPageId);
        ICachedPage metadataPage = bufferCache.pin(dpid);

        try {
            metadataPage.acquireWriteLatch();
            System.out
                    .println("DEBUG: Created metadata page " + metadataPageId + " for cluster " + centroid.centroidId);
        } finally {
            metadataPage.releaseWriteLatch(true);
            bufferCache.unpin(metadataPage);
        }

        return metadataPageId;
    }

    /**
     * Create cluster tuple using TupleUtils
     */
    private ITupleReference createClusterTuple(int centroidId, float[] embedding, int pointer)
            throws HyracksDataException {
        return TupleUtils.createTuple(new org.apache.hyracks.api.dataflow.value.ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, FloatArraySerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE }, centroidId, embedding, pointer);
    }

    /**
     * Convert page type code to enum
     */
    private PageType convertToPageType(int pageTypeCode) throws HyracksDataException {
        switch (pageTypeCode) {
            case VCTreeProtocolTuples.LEAF_PAGE_TYPE:
                return PageType.LEAF;
            case VCTreeProtocolTuples.INTERIOR_PAGE_TYPE:
                return PageType.INTERIOR;
            case VCTreeProtocolTuples.ROOT_PAGE_TYPE:
                return PageType.ROOT;
            case VCTreeProtocolTuples.METADATA_PAGE_TYPE:
                return PageType.METADATA;
            default:
                throw new HyracksDataException("Invalid page type code: " + pageTypeCode);
        }
    }

    @Override
    public void end() throws HyracksDataException {
        if (!levelStack.isEmpty()) {
            throw new HyracksDataException("Incomplete structure: " + levelStack.size() + " levels remaining");
        }

        // Set root page ID from highest level
        if (!levelPageIds.isEmpty()) {
            int maxLevel = levelPageIds.keySet().stream().max(Integer::compareTo).orElse(-1);
            if (maxLevel >= 0) {
                List<Integer> rootPageIds = levelPageIds.get(maxLevel);
                if (!rootPageIds.isEmpty()) {
                    setRootPageId(rootPageIds.get(0));
                    System.out.println("DEBUG: Set root page ID: " + rootPageIds.get(0));
                }
            }
        }

        structureComplete = true;
        super.end();

        System.out.println("DEBUG: VCTreeStaticStructureLoader completed");
    }

    @Override
    public void abort() throws HyracksDataException {
        System.out.println("DEBUG: Aborting VCTreeStaticStructureLoader");

        // Clean up allocated pages
        for (List<Integer> pageIds : levelPageIds.values()) {
            for (Integer pageId : pageIds) {
                try {
                    long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
                    ICachedPage page = bufferCache.pin(dpid);
                    bufferCache.returnPage(page, false);
                } catch (Exception e) {
                    // Continue cleanup even if individual page cleanup fails
                }
            }
        }

        structureComplete = false;
        levelStack.clear();
        levelPageIds.clear();
        centroidToPageMap.clear();
    }
}
