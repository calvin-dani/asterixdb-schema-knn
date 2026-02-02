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

import static org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider.NEW;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrame;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringMetadataFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Static structure initializer for VectorClusteringTree for unit testing purposes. This class creates predictable
 * multi-level tree structures (root/interior/leaf/metadata/data pages) without modifying the core VectorClusteringTree
 * implementation.
 *
 * Pattern based on BTreeNSMBulkLoader for creating static test structures.
 */
public class VectorClusteringTreeStaticInitializer {

    private final VectorClusteringTree vectorTree;
    private final IBufferCache bufferCache;
    private final IPageManager pageManager;
    private final ITreeIndexMetadataFrame metaFrame;
    @SuppressWarnings("unused")
    private final int fileId;
    private final List<TestPage> testPages;

    // Tuple writers for different frame types

    /**
     * Represents a test page in the vector tree structure
     */
    public static class TestPage {
        public final int pageId;
        public final ICachedPage page;
        public final PageType type;
        public final List<ITupleReference> tuples;

        public enum PageType {
            ROOT,
            INTERIOR,
            LEAF,
            METADATA,
            VECTOR_DATA
        }

        public TestPage(int pageId, ICachedPage page, PageType type) {
            this.pageId = pageId;
            this.page = page;
            this.type = type;
            this.tuples = new ArrayList<>();
        }

        public void addTuple(ITupleReference tuple) {
            tuples.add(tuple);
        }
    }

    /**
     * Configuration for the static tree structure
     */
    public static class TreeStructureConfig {
        public final int numLeafPages;
        public final int numInteriorPages;
        public final int numVectorDataPages;
        public final int tuplesPerLeaf;
        public final boolean createMultiLevel;

        public TreeStructureConfig(int numLeafPages, int numInteriorPages, int numVectorDataPages, int tuplesPerLeaf,
                boolean createMultiLevel) {
            this.numLeafPages = numLeafPages;
            this.numInteriorPages = numInteriorPages;
            this.numVectorDataPages = numVectorDataPages;
            this.tuplesPerLeaf = tuplesPerLeaf;
            this.createMultiLevel = createMultiLevel;
        }

        // Predefined configurations for common test scenarios
        public static TreeStructureConfig singleLeaf() {
            return new TreeStructureConfig(1, 0, 1, 5, false);
        }

        public static TreeStructureConfig multipleLeaves() {
            return new TreeStructureConfig(3, 1, 3, 5, true);
        }

        public static TreeStructureConfig deepTree() {
            return new TreeStructureConfig(5, 3, 5, 10, true);
        }

        /**
         * Creates a 3-level tree structure with 4D centroids: - Root: 2 centroids - Interior: 4 centroids (2 per root
         * centroid) - Leaf: 8 clusters (2 per interior centroid)
         */
        public static TreeStructureConfig threeLevelDefault() {
            return new TreeStructureConfig(8, 4, 8, 2, true);
        }
    }

    public VectorClusteringTreeStaticInitializer(VectorClusteringTree vectorTree) {
        this.vectorTree = vectorTree;
        this.bufferCache = vectorTree.getBufferCache();
        this.pageManager = vectorTree.getPageManager();
        this.metaFrame = pageManager.createMetadataFrame();
        this.fileId = vectorTree.getFileId();
        this.testPages = new ArrayList<>();
    }

    /**
     * Initialize a static tree structure with the given configuration and tuples
     */
    public void initializeStaticStructure(TreeStructureConfig config, List<ITupleReference> tuples)
            throws HyracksDataException {

        if (tuples.isEmpty()) {
            throw new IllegalArgumentException("Cannot initialize tree with empty tuple list");
        }

        // Check if this is our special 3-level configuration
        if (config.numLeafPages == 8 && config.numInteriorPages == 4 && config.createMultiLevel) {
            // Use the specialized 3-level structure
            initializeThreeLevelStructure();
        } else {
            // Use the original general structure creation
            if (config.createMultiLevel) {
                initializeMultiLevelTree(config, tuples);
            } else {
                initializeSingleLevelTree(config, tuples);
            }
        }

        // Set the root page in the tree using a setter method
        if (!testPages.isEmpty()) {
            TestPage rootPage = findPageByType(TestPage.PageType.ROOT);
            if (rootPage == null) {
                // If no explicit root, use the first interior or leaf page as root
                rootPage = testPages.stream()
                        .filter(p -> p.type == TestPage.PageType.INTERIOR || p.type == TestPage.PageType.LEAF)
                        .findFirst().orElse(testPages.get(0));
                if (rootPage != null) {
                    setRootPageId(rootPage.pageId);
                }
            }
        }
    }

    /**
     * Initialize a single-level tree (root is a leaf page)
     */
    private void initializeSingleLevelTree(TreeStructureConfig config, List<ITupleReference> tuples)
            throws HyracksDataException {

        // Create leaf pages
        for (int i = 0; i < config.numLeafPages; i++) {
            TestPage leafPage = createPage(TestPage.PageType.LEAF);

            // Add tuples to this leaf page
            int startIndex = i * config.tuplesPerLeaf;
            int endIndex = Math.min(startIndex + config.tuplesPerLeaf, tuples.size());
            for (int j = startIndex; j < endIndex; j++) {
                leafPage.addTuple(tuples.get(j));
            }

            // Initialize the page with tuples
            initializeLeafPage(leafPage);
        }

        // Create associated vector data pages
        for (int i = 0; i < config.numVectorDataPages; i++) {
            TestPage dataPage = createPage(TestPage.PageType.VECTOR_DATA);
            initializeVectorDataPage(dataPage);
        }
    }

    /**
     * Initialize a multi-level tree with interior and leaf pages
     */
    private void initializeMultiLevelTree(TreeStructureConfig config, List<ITupleReference> tuples)
            throws HyracksDataException {

        List<TestPage> leafPages = new ArrayList<>();

        // Create leaf pages first
        for (int i = 0; i < config.numLeafPages; i++) {
            TestPage leafPage = createPage(TestPage.PageType.LEAF);
            leafPages.add(leafPage);

            // Add tuples to this leaf page
            int startIndex = i * config.tuplesPerLeaf;
            int endIndex = Math.min(startIndex + config.tuplesPerLeaf, tuples.size());
            for (int j = startIndex; j < endIndex; j++) {
                leafPage.addTuple(tuples.get(j));
            }

            initializeLeafPage(leafPage);
        }

        // Create interior pages
        List<TestPage> currentLevel = leafPages;
        while (currentLevel.size() > 1 || testPages.stream().noneMatch(p -> p.type == TestPage.PageType.ROOT)) {
            List<TestPage> nextLevel = new ArrayList<>();

            // Group current level pages under interior/root pages
            for (int i = 0; i < currentLevel.size(); i += 2) {
                TestPage.PageType pageType = (currentLevel.size() <= 2 && nextLevel.isEmpty()) ? TestPage.PageType.ROOT
                        : TestPage.PageType.INTERIOR;

                TestPage parentPage = createPage(pageType);

                // Add references to child pages
                parentPage.addTuple(currentLevel.get(i).tuples.get(0)); // First tuple of left child
                if (i + 1 < currentLevel.size()) {
                    parentPage.addTuple(currentLevel.get(i + 1).tuples.get(0)); // First tuple of right child
                }

                initializeInteriorPage(parentPage, currentLevel.get(i).pageId,
                        i + 1 < currentLevel.size() ? currentLevel.get(i + 1).pageId : -1);

                nextLevel.add(parentPage);
            }

            currentLevel = nextLevel;
        }

        // Create vector data pages
        for (int i = 0; i < config.numVectorDataPages; i++) {
            TestPage dataPage = createPage(TestPage.PageType.VECTOR_DATA);
            initializeVectorDataPage(dataPage);
        }
    }

    /**
     * Create a new page of the specified type using proper buffer management pattern.
     *
     * This method follows the proper page lifecycle pattern to ensure buffer synchronization: 1.
     * pageManager.takePage(metaFrame) - get page ID from page manager 2. BufferedFileHandle.getDiskPageId(fileId,
     * pageId) - create disk page ID 3. bufferCache.pin(dpid, NEW) - pin the page in buffer cache 4. Keep page pinned
     * for initialization - page will be unpinned by caller after modifications
     *
     * This ensures proper page ID management and buffer synchronization for tree operations.
     */
    private TestPage createPage(TestPage.PageType type) throws HyracksDataException {
        // Step 1: Allocate page ID from page manager (same as BTree pattern)
        int pageId = pageManager.takePage(metaFrame);

        // Step 2: Create disk page ID (same as BTree pattern) 
        long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);

        // Step 3: Pin the page in buffer cache (keep pinned for initialization)
        ICachedPage page = bufferCache.pin(dpid, NEW);

        // CRITICAL FIX: Do not unpin immediately - page stays pinned until after initialization
        // The caller is responsible for proper latch management during modification

        TestPage testPage = new TestPage(pageId, page, type);
        testPages.add(testPage);

        return testPage;
    }

    /**
     * Initialize a leaf page with its tuples using proper buffer management
     */
    private void initializeLeafPage(TestPage leafPage) throws HyracksDataException {
        // Use VectorClusteringLeafFrame for proper initialization
        org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrame leafFrame =
                (org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrame) vectorTree.getLeafFrameFactory()
                        .createFrame();

        leafPage.page.acquireWriteLatch();
        try {
            leafFrame.setPage(leafPage.page);
            leafFrame.initBuffer((byte) 0); // Leaf level is 0

            // Set a test centroid for this leaf page
            double[] testCentroid = generateTestCentroid(leafPage.pageId);
            leafFrame.setClusterId(leafPage.pageId); // Use page ID as cluster ID for testing

            // Insert tuples into the leaf frame using proper frame methods
            for (ITupleReference tuple : leafPage.tuples) {
                if (leafFrame.hasSpaceInsert(
                        tuple) != org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus.INSUFFICIENT_SPACE) {
                    int insertIndex = leafFrame.findInsertTupleIndex(tuple);
                    leafFrame.insert(tuple, insertIndex);
                }
            }
        } finally {
            leafPage.page.releaseWriteLatch(true); // Mark page as dirty
            bufferCache.unpin(leafPage.page); // Unpin after initialization
        }
    }

    /**
     * Initialize an interior page with child page references using proper buffer management
     */
    private void initializeInteriorPage(TestPage interiorPage, int leftChildId, int rightChildId)
            throws HyracksDataException {
        // Use VectorClusteringInteriorFrame for proper initialization
        org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrame interiorFrame =
                (org.apache.hyracks.storage.am.vector.frames.VectorClusteringInteriorFrame) vectorTree
                        .getInteriorFrameFactory().createFrame();

        interiorPage.page.acquireWriteLatch();
        try {
            interiorFrame.setPage(interiorPage.page);
            interiorFrame.initBuffer((byte) 1); // Interior level is 1 or higher

            // Set a test centroid for this interior page
            double[] testCentroid = generateTestCentroid(interiorPage.pageId);
            interiorFrame.setClusterId(interiorPage.pageId); // Use page ID as cluster ID for testing

            // Insert cluster entries with child page pointers
            for (int i = 0; i < interiorPage.tuples.size(); i++) {
                ITupleReference tuple = interiorPage.tuples.get(i);
                if (interiorFrame.hasSpaceInsert(
                        tuple) != org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus.INSUFFICIENT_SPACE) {
                    int insertIndex = interiorFrame.findInsertTupleIndex(tuple);
                    interiorFrame.insert(tuple, insertIndex);

                    // Set child page pointer - use left child for first tuple, right for second
                    int childPageId = (i == 0) ? leftChildId : rightChildId;
                    if (childPageId > 0) {
                        interiorFrame.setChildPageId(insertIndex, childPageId);
                    }
                }
            }
        } finally {
            interiorPage.page.releaseWriteLatch(true); // Mark page as dirty
            bufferCache.unpin(interiorPage.page); // Unpin after initialization
        }
    }

    /**
     * Initialize a vector data page using proper buffer management
     */
    private void initializeVectorDataPage(TestPage dataPage) throws HyracksDataException {
        // Use VectorClusteringDataFrame for vector data pages
        org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrame dataFrame =
                (org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrame) vectorTree.getDataFrameFactory()
                        .createFrame();

        dataPage.page.acquireWriteLatch();
        try {
            dataFrame.setPage(dataPage.page);
            dataFrame.initBuffer((byte) 0); // Data pages are at level 0

            // Set test centroid and cluster ID
            double[] testCentroid = generateTestCentroid(dataPage.pageId);
            dataFrame.setClusterId(dataPage.pageId);
        } finally {
            dataPage.page.releaseWriteLatch(true); // Mark page as dirty
            bufferCache.unpin(dataPage.page); // Unpin after initialization
        }
    }

    /**
     * Find a page by type
     */
    private TestPage findPageByType(TestPage.PageType type) {
        return testPages.stream().filter(page -> page.type == type).findFirst().orElse(null);
    }

    /**
     * Get all pages of a specific type
     */
    public List<TestPage> getPagesByType(TestPage.PageType type) {
        return testPages.stream().filter(page -> page.type == type).collect(ArrayList::new,
                (list, page) -> list.add(page), ArrayList::addAll);
    }

    /**
     * Get all test pages
     */
    public List<TestPage> getAllPages() {
        return new ArrayList<>(testPages);
    }

    /**
     * Generate a test centroid based on page ID for predictable testing
     */
    private double[] generateTestCentroid(int pageId) {
        return generatePredictable4DCentroid(pageId);
    }

    /**
     * Generate predictable 4D centroids for the 3-level tree structure. Creates well-separated centroids to simulate
     * realistic clustering.
     */
    private double[] generatePredictable4DCentroid(int pageId) {
        double[] centroid = new double[4];

        // Create well-separated centroids in 4D space based on page ID
        switch (pageId % 8) {
            case 0: // Root centroid 1 region
                centroid = new double[] { 10.0, 10.0, 10.0, 10.0 };
                break;
            case 1: // Root centroid 2 region  
                centroid = new double[] { -10.0, -10.0, -10.0, -10.0 };
                break;
            case 2: // Interior centroid 1.1
                centroid = new double[] { 15.0, 15.0, 5.0, 5.0 };
                break;
            case 3: // Interior centroid 1.2
                centroid = new double[] { 5.0, 5.0, 15.0, 15.0 };
                break;
            case 4: // Interior centroid 2.1
                centroid = new double[] { -15.0, -15.0, -5.0, -5.0 };
                break;
            case 5: // Interior centroid 2.2
                centroid = new double[] { -5.0, -5.0, -15.0, -15.0 };
                break;
            default: // Leaf level clusters (more specific)
                int leafIndex = pageId % 8;
                centroid[0] = (leafIndex < 4) ? 10.0 + (leafIndex * 2) : -10.0 - ((leafIndex - 4) * 2);
                centroid[1] = (leafIndex % 2 == 0) ? 10.0 : -10.0;
                centroid[2] = ((leafIndex / 2) % 2 == 0) ? 10.0 : -10.0;
                centroid[3] = (leafIndex < 4) ? 10.0 : -10.0;
                break;
        }

        return centroid;
    }

    /**
     * Set the root page ID using reflection to access the protected field
     */
    private void setRootPageId(int rootPageId) throws HyracksDataException {
        try {
            // Use reflection to access the protected rootPage field
            java.lang.reflect.Field rootPageField = AbstractTreeIndex.class.getDeclaredField("rootPage");
            rootPageField.setAccessible(true);
            rootPageField.setInt(vectorTree, rootPageId);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.CANNOT_ACTIVATE_ACTIVE_INDEX,
                    e);
        }
    }

    /**
     * Get the root page ID
     */
    public int getRootPageId() {
        TestPage rootPage = findPageByType(TestPage.PageType.ROOT);
        if (rootPage != null) {
            return rootPage.pageId;
        }
        return -1;
    }

    /**
     * Clean up all test pages with proper buffer management
     */
    public void cleanup() throws HyracksDataException {
        for (TestPage testPage : testPages) {
            if (testPage.page != null) {
                // Check if page is still pinned (shouldn't be after proper initialization)
                // If initialization methods were called correctly, pages should already be unpinned
                try {
                    bufferCache.unpin(testPage.page);
                } catch (Exception e) {
                    // Page was already unpinned - this is expected after proper initialization
                    // Continue cleanup
                }
                // Return page to buffer cache  
                bufferCache.returnPage(testPage.page, false);
            }
        }
        testPages.clear();
    }

    /**
     * Force write all pages to disk
     */
    public void flush() throws HyracksDataException {
        // In the test environment, we don't need to manually flush pages
        // The buffer cache handles this automatically
        // This method is provided for completeness
    }

    /**
     * Initialize the specialized 3-level tree structure: - Root: 2 centroids - Interior: 4 centroids (2 per root
     * centroid) - Leaf: 8 clusters (2 per interior centroid) Each leaf cluster points to an actual metadata page (not
     * data pages)
     */
    public void initializeThreeLevelStructure() throws HyracksDataException {
        if (vectorTree.isStaticStructureInitialized()) {
            return;
        }

        // Step 1: Create 8 metadata pages for leaf clusters
        List<TestPage> metadataPages = new ArrayList<>();
        List<Integer> metadataPageIds = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            TestPage metadataPage = createPage(TestPage.PageType.METADATA);
            metadataPages.add(metadataPage);
            metadataPageIds.add(metadataPage.pageId); // Track actual allocated metadata page ID

            // Initialize metadata page as empty (no data pages yet)
            initializeEmptyMetadataPage(metadataPage, i);
        }

        // Step 2: Create 8 leaf pages with hierarchical centroids pointing to metadata pages
        List<TestPage> leafPages = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            TestPage leafPage = createPage(TestPage.PageType.LEAF);
            leafPages.add(leafPage);

            // Clear the input tuples since we'll create proper cluster tuples
            leafPage.tuples.clear();

            // Create the cluster tuple that will be used in the leaf frame
            double[] leafCentroid = generateHierarchicalCentroid(i, 0); // Level 0 = leaf
            ITupleReference clusterTuple = createClusterTuple(i, leafCentroid, metadataPageIds.get(i));

            // Add the cluster tuple to the leafPage.tuples list so interior pages can reference it
            leafPage.addTuple(clusterTuple);

            // Initialize leaf with hierarchical centroid using actual metadata page ID
            initializeLeafPageWithHierarchicalCentroid(leafPage, i, metadataPageIds.get(i));
        }

        // Step 3: Create 4 interior pages (2 children each) - Track page IDs properly
        List<TestPage> interiorPages = new ArrayList<>();
        List<Integer> interiorPageIds = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            TestPage interiorPage = createPage(TestPage.PageType.INTERIOR);
            interiorPages.add(interiorPage);
            interiorPageIds.add(interiorPage.pageId); // CRITICAL: Track actual allocated page ID

            // Each interior page manages 2 leaf pages
            int leftLeafIndex = i * 2;
            int rightLeafIndex = leftLeafIndex + 1;

            // Create interior cluster tuple for left child
            if (leftLeafIndex < leafPages.size()) {
                double[] leftInteriorCentroid = generateHierarchicalCentroid(leftLeafIndex, 1); // Level 1 = interior

                ITupleReference leftInteriorTuple =
                        createClusterTuple(leftLeafIndex, leftInteriorCentroid, leafPages.get(leftLeafIndex).pageId);
                interiorPage.addTuple(leftInteriorTuple);
            }

            // Create interior cluster tuple for right child
            if (rightLeafIndex < leafPages.size()) {
                double[] rightInteriorCentroid = generateHierarchicalCentroid(rightLeafIndex, 1); // Level 1 = interior

                ITupleReference rightInteriorTuple =
                        createClusterTuple(rightLeafIndex, rightInteriorCentroid, leafPages.get(rightLeafIndex).pageId);
                interiorPage.addTuple(rightInteriorTuple);
            }

            // Initialize interior page with hierarchical centroid and child pointers
            // Use actual page IDs from page manager allocation
            initializeInteriorPageWithHierarchicalCentroid(interiorPage, i,
                    leftLeafIndex < leafPages.size() ? leafPages.get(leftLeafIndex).pageId : -1,
                    rightLeafIndex < leafPages.size() ? leafPages.get(rightLeafIndex).pageId : -1);
        }

        // Step 4: Create root page with 4 centroids - Use actual page IDs
        TestPage rootPage = createPage(TestPage.PageType.ROOT);

        // Create proper root cluster tuples
        for (int rootIndex = 0; rootIndex < 4; rootIndex++) {
            // Generate hierarchical centroid for root level
            double[] rootCentroid = generateHierarchicalCentroid(rootIndex, 2); // Level 2 = root

            int metadataPointer = interiorPageIds.get(rootIndex);
            ITupleReference rootClusterTuple = createClusterTuple(rootIndex, rootCentroid, metadataPointer);

            // Add the cluster tuple to the root page
            rootPage.addTuple(rootClusterTuple);
        }

        // Initialize root page with child pointers using actual page IDs from page manager
        initializeRootPageWith4Children(rootPage, interiorPageIds);

        // Set the root page ID
        setRootPageId(rootPage.pageId);

        vectorTree.setStaticStructureInitialized();
    }

    /**
     * Initialize a leaf page with hierarchical centroid based on its position in the 3-level structure
     */
    private void initializeLeafPageWithHierarchicalCentroid(TestPage leafPage, int leafIndex, int metadataPageId)
            throws HyracksDataException {
        org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrame leafFrame =
                (org.apache.hyracks.storage.am.vector.frames.VectorClusteringLeafFrame) vectorTree.getLeafFrameFactory()
                        .createFrame();

        leafFrame.setPage(leafPage.page);
        leafFrame.initBuffer((byte) 0); // Leaf level is 0

        // Generate hierarchical centroid for this leaf position
        double[] leafCentroid = generateHierarchicalCentroid(leafIndex, 0); // Level 0 = leaf
        leafFrame.setClusterId(leafIndex);

        // Create a single cluster tuple for this leaf (cid + centroid + metadata_pointer)
        // Use the actual allocated metadata page ID
        // Create cluster tuple with 3 fields: cid, centroid, metadata_pointer
        ITupleReference clusterTuple = createClusterTuple(leafIndex, leafCentroid, metadataPageId);

        // Insert the cluster tuple into the leaf frame
        if (leafFrame.hasSpaceInsert(
                clusterTuple) != org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus.INSUFFICIENT_SPACE) {
            int insertIndex = leafFrame.findInsertTupleIndex(clusterTuple);
            leafFrame.insert(clusterTuple, insertIndex);
        }

        // CRITICAL FIX: Ensure the frame modifications are committed to the page buffer
        leafPage.page.acquireWriteLatch();
        try {
            leafFrame.setPage(leafPage.page); // Re-associate to ensure buffer sync
        } finally {
            leafPage.page.releaseWriteLatch(true); // Mark page as dirty
            bufferCache.unpin(leafPage.page); // Unpin after initialization
        }
    }

    /**
     * Initialize an interior page with hierarchical centroid and child pointers
     */
    private void initializeInteriorPageWithHierarchicalCentroid(TestPage interiorPage, int interiorIndex,
            int leftChildPageId, int rightChildPageId) throws HyracksDataException {
        VectorClusteringInteriorFrame interiorFrame =
                (VectorClusteringInteriorFrame) vectorTree.getInteriorFrameFactory().createFrame();

        interiorFrame.setPage(interiorPage.page);
        interiorFrame.initBuffer((byte) 1); // Interior level is 1

        // Generate hierarchical centroid for this interior position
        double[] interiorCentroid = generateHierarchicalCentroid(interiorIndex, 1); // Level 1 = interior
        interiorFrame.setClusterId(interiorIndex);

        // Insert cluster tuples into the interior frame
        for (ITupleReference tuple : interiorPage.tuples) {
            if (interiorFrame.hasSpaceInsert(
                    tuple) != org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus.INSUFFICIENT_SPACE) {
                int insertIndex = interiorFrame.findInsertTupleIndex(tuple);
                interiorFrame.insert(tuple, insertIndex);
            }
        }

        // Set child page pointers
        if (leftChildPageId > 0) {
            interiorFrame.setChildPageId(0, leftChildPageId);
        }
        if (rightChildPageId > 0 && interiorFrame.getTupleCount() > 1) {
            interiorFrame.setChildPageId(1, rightChildPageId);
        }

        // CRITICAL FIX: Ensure the frame modifications are committed to the page buffer
        interiorPage.page.acquireWriteLatch();
        try {
            interiorFrame.setPage(interiorPage.page); // Re-associate to ensure buffer sync
        } finally {
            interiorPage.page.releaseWriteLatch(true); // Mark page as dirty
            bufferCache.unpin(interiorPage.page); // Unpin after initialization
        }
    }

    /**
     * Initialize root page with 4 interior children using proper page allocation pattern
     */
    private void initializeRootPageWith4Children(TestPage rootPage, List<Integer> interiorPageIds)
            throws HyracksDataException {
        VectorClusteringInteriorFrame rootFrame =
                (VectorClusteringInteriorFrame) vectorTree.getInteriorFrameFactory().createFrame();

        rootFrame.setPage(rootPage.page);
        rootFrame.initBuffer((byte) 2); // Root level is 2

        // Set root centroid (centroid of all data)

        rootFrame.setClusterId(0); // Root has cluster ID 0

        // Insert cluster tuples first, then set child pointers  
        for (ITupleReference tuple : rootPage.tuples) {
            if (rootFrame.hasSpaceInsert(
                    tuple) != org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus.INSUFFICIENT_SPACE) {
                int insertIndex = rootFrame.findInsertTupleIndex(tuple);
                rootFrame.insert(tuple, insertIndex);
            }
        }

        for (int i = 0; i < Math.min(4, interiorPageIds.size()); i++) {
            if (i < rootFrame.getTupleCount()) {
                rootFrame.setChildPageId(i, interiorPageIds.get(i));
            }
        }

        // Ensure the frame modifications are committed to the page buffer
        rootPage.page.acquireWriteLatch();
        try {
            rootFrame.setPage(rootPage.page); // Re-associate to ensure buffer sync
        } finally {
            rootPage.page.releaseWriteLatch(true); // Mark page as dirty
            bufferCache.unpin(rootPage.page); // Unpin after initialization
        }
    }

    /**
     * Generate hierarchical centroids based on the 3-level structure: - Level 0 (leaf): 8 specific cluster centroids -
     * Level 1 (interior): 4 centroids (each covering 2 leaf centroids) - Level 2 (root): 1 centroid (covering all
     * data)
     */
    private double[] generateHierarchicalCentroid(int index, int level) {
        double[] centroid = new double[4];

        switch (level) {
            case 0: // Leaf level - 8 distinct clusters
                switch (index) {
                    case 0:
                        centroid = new double[] { 20.0, 20.0, 20.0, 20.0 };
                        break;
                    case 1:
                        centroid = new double[] { 25.0, 25.0, 15.0, 15.0 };
                        break;
                    case 2:
                        centroid = new double[] { 15.0, 15.0, 25.0, 25.0 };
                        break;
                    case 3:
                        centroid = new double[] { 30.0, 10.0, 20.0, 30.0 };
                        break;
                    case 4:
                        centroid = new double[] { -20.0, -20.0, -20.0, -20.0 };
                        break;
                    case 5:
                        centroid = new double[] { -25.0, -25.0, -15.0, -15.0 };
                        break;
                    case 6:
                        centroid = new double[] { -15.0, -15.0, -25.0, -25.0 };
                        break;
                    case 7:
                        centroid = new double[] { -30.0, -10.0, -20.0, -30.0 };
                        break;
                    default:
                        centroid = new double[] { 0.0, 0.0, 0.0, 0.0 };
                        break;
                }
                break;

            case 1: // Interior level - 4 centroids, based on leaf groupings
                switch (index) {
                    case 0:
                    case 1:
                        centroid = new double[] { 22.5, 22.5, 17.5, 17.5 }; // Average of leaves 0,1
                        break;
                    case 2:
                    case 3:
                        centroid = new double[] { 22.5, 12.5, 22.5, 27.5 }; // Average of leaves 2,3
                        break;
                    case 4:
                    case 5:
                        centroid = new double[] { -22.5, -22.5, -17.5, -17.5 }; // Average of leaves 4,5
                        break;
                    case 6:
                    case 7:
                        centroid = new double[] { -22.5, -12.5, -22.5, -27.5 }; // Average of leaves 6,7
                        break;
                    default:
                        centroid = new double[] { 0.0, 0.0, 0.0, 0.0 };
                        break;
                }
                break;

            case 2: // Root level - 4 different centroids
                switch (index) {
                    case 0:
                        centroid = new double[] { 22.5, 22.5, 17.5, 17.5 };
                        break; // Covers interior 0
                    case 1:
                        centroid = new double[] { 22.5, 12.5, 22.5, 27.5 };
                        break; // Covers interior 1
                    case 2:
                        centroid = new double[] { -22.5, -22.5, -17.5, -17.5 };
                        break; // Covers interior 2
                    case 3:
                        centroid = new double[] { -22.5, -12.5, -22.5, -27.5 };
                        break; // Covers interior 3
                    default:
                        centroid = new double[] { 0.0, 0.0, 0.0, 0.0 };
                        break;
                }
                break;

            default:
                centroid = new double[] { 0.0, 0.0, 0.0, 0.0 };
        }

        return centroid;
    }

    /**
     * Create a cluster tuple with the format: <cid, centroid, metadata_pointer> This is needed for leaf frame tuples.
     */
    private ITupleReference createClusterTuple(int clusterId, double[] centroid, int metadataPageId)
            throws HyracksDataException {
        try {
            // Create tuple builder with 3 fields
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);

            // Add CID field (field 0)
            tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, clusterId);

            // Add centroid field (field 1) - using DoubleArraySerializerDeserializer.INSTANCE
            tupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, centroid);

            // Add metadata pointer field (field 2)
            tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, metadataPageId);

            // Create the tuple reference
            ArrayTupleReference tupleRef = new ArrayTupleReference();
            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

            return tupleRef;
        } catch (Exception e) {
            throw new HyracksDataException("Failed to create cluster tuple", e);
        }
    }

    /**
     * Initialize an empty metadata page for a specific cluster. The metadata page will be empty initially, and data
     * pages will be added dynamically during insertion.
     */
    private void initializeEmptyMetadataPage(TestPage metadataPage, int clusterIndex) throws HyracksDataException {
        VectorClusteringMetadataFrame metadataFrame =
                (VectorClusteringMetadataFrame) vectorTree.getMetadataFrameFactory().createFrame();

        metadataFrame.setPage(metadataPage.page);
        metadataFrame.initBuffer((byte) 0); // Metadata pages are at level 0

        // Set cluster-specific centroid and ID
        double[] clusterCentroid = generateHierarchicalCentroid(clusterIndex, 0);
        metadataFrame.setClusterId(clusterIndex);

        // Initialize as empty (no data page entries yet)
        // Data pages will be added dynamically when first records are inserted into this cluster

        // Set next page to -1 (no chaining initially)
        metadataFrame.setNextPage(-1);

        // Ensure the frame modifications are committed to the page buffer
        metadataPage.page.acquireWriteLatch();
        try {
            metadataFrame.setPage(metadataPage.page); // Re-associate to ensure buffer sync
        } finally {
            metadataPage.page.releaseWriteLatch(true); // Mark page as dirty
            bufferCache.unpin(metadataPage.page); // Unpin after initialization
        }
    }
}
