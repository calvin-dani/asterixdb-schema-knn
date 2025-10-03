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

package org.apache.hyracks.storage.am.vector;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.vector.frames.VectorTreeFrameType;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer;
import org.apache.hyracks.storage.am.vector.util.VectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.util.VectorTreeTestHarness;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Comprehensive test class for VectorClusteringTreeStaticInitializer.
 * Demonstrates different ways to create and test static tree structures.
 */
@SuppressWarnings("rawtypes")
public class VectorClusteringTreeStaticInitializerTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final VectorTreeTestHarness harness = new VectorTreeTestHarness();
    private final VectorTreeTestUtils vectorTreeTestUtils = new VectorTreeTestUtils();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        try {
            VectorTreeTestUtils.cleanupStaticInitializer();
        } catch (Exception e) {
            LOGGER.warn("Failed to cleanup static initializer", e);
        }
        harness.tearDown();
    }

    /**
     * Test single leaf page initialization
     */
    @Test
    public void testSingleLeafInitialization() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Single Leaf Static Initialization");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize with single leaf configuration
        VectorTreeTestUtils.initializeStaticStructure(ctx,
                VectorClusteringTreeStaticInitializer.TreeStructureConfig.singleLeaf());

        // Verify the static structure was created correctly
        VectorClusteringTreeStaticInitializer initializer = VectorTreeTestUtils.getStaticInitializer();
        List<VectorClusteringTreeStaticInitializer.TestPage> leafPages =
                initializer.getPagesByType(VectorClusteringTreeStaticInitializer.TestPage.PageType.LEAF);

        assertEquals("Should have 1 leaf page", 1, leafPages.size());
        assertEquals("Root page should be set correctly", leafPages.get(0).pageId, initializer.getRootPageId());

        // Test insertion into the static structure
        vectorTreeTestUtils.insertVectorTuples(ctx, 10, harness.getRandom());

        // Validate operations
        vectorTreeTestUtils.checkPointSearches(ctx);
        vectorTreeTestUtils.checkScan(ctx);
        ctx.getIndex().validate();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test multi-level tree initialization
     */
    @Test
    public void testMultiLevelTreeInitialization() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Multi-Level Tree Static Initialization");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize with multi-level configuration
        VectorTreeTestUtils.initializeStaticStructure(ctx,
                VectorClusteringTreeStaticInitializer.TreeStructureConfig.multipleLeaves());

        // Verify the structure
        VectorClusteringTreeStaticInitializer initializer = VectorTreeTestUtils.getStaticInitializer();
        List<VectorClusteringTreeStaticInitializer.TestPage> leafPages =
                initializer.getPagesByType(VectorClusteringTreeStaticInitializer.TestPage.PageType.LEAF);
        List<VectorClusteringTreeStaticInitializer.TestPage> interiorPages =
                initializer.getPagesByType(VectorClusteringTreeStaticInitializer.TestPage.PageType.INTERIOR);
        List<VectorClusteringTreeStaticInitializer.TestPage> rootPages =
                initializer.getPagesByType(VectorClusteringTreeStaticInitializer.TestPage.PageType.ROOT);

        assertEquals("Should have 3 leaf pages", 3, leafPages.size());
        assertTrue("Should have interior or root pages", interiorPages.size() + rootPages.size() > 0);

        // Test insertion and operations
        vectorTreeTestUtils.insertVectorTuples(ctx, 50, harness.getRandom());

        vectorTreeTestUtils.checkPointSearches(ctx);
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkVectorSimilaritySearches(ctx);
        ctx.getIndex().validate();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test deep tree structure initialization
     */
    @Test
    public void testDeepTreeInitialization() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Deep Tree Static Initialization");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize with deep tree configuration
        VectorTreeTestUtils.initializeStaticStructure(ctx,
                VectorClusteringTreeStaticInitializer.TreeStructureConfig.deepTree());

        // Verify the deep structure
        VectorClusteringTreeStaticInitializer initializer = VectorTreeTestUtils.getStaticInitializer();
        List<VectorClusteringTreeStaticInitializer.TestPage> allPages = initializer.getAllPages();

        assertTrue("Should have multiple pages in deep tree", allPages.size() >= 5);
        assertTrue("Root page ID should be valid", initializer.getRootPageId() > 0);

        // Test extensive operations on deep tree
        vectorTreeTestUtils.insertVectorTuples(ctx, 200, harness.getRandom());

        vectorTreeTestUtils.checkPointSearches(ctx);
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkDiskOrderScan(ctx);
        vectorTreeTestUtils.checkVectorSimilaritySearches(ctx);
        ctx.getIndex().validate();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test edge cases and error handling
     */
    @Test
    public void testStaticInitializerEdgeCases() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing Static Initializer Edge Cases");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Test with minimal configuration
        VectorClusteringTreeStaticInitializer.TreeStructureConfig minimalConfig =
                new VectorClusteringTreeStaticInitializer.TreeStructureConfig(1, 0, 1, 1, false);

        VectorTreeTestUtils.initializeStaticStructure(ctx, minimalConfig);

        // Verify minimal structure
        VectorClusteringTreeStaticInitializer initializer = VectorTreeTestUtils.getStaticInitializer();
        assertTrue("Should have at least one page", initializer.getAllPages().size() >= 1);

        // Test edge case insertions
        vectorTreeTestUtils.insertEdgeCaseVectors(ctx, 20, harness.getRandom());

        vectorTreeTestUtils.checkPointSearches(ctx);
        ctx.getIndex().validate();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Helper method to create test context
     */
    private AbstractVectorTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            VectorTreeFrameType leafType, boolean filtered) throws Exception {
        return VectorTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCache(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, numKeys, leafType,
                harness.getVectorDimensions());
    }

    /**
     * Helper method for assertions
     */
    private void assertEquals(String message, int expected, int actual) {
        if (expected != actual) {
            throw new AssertionError(message + ": expected " + expected + " but was " + actual);
        }
    }

    private void assertTrue(String message, boolean condition) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
