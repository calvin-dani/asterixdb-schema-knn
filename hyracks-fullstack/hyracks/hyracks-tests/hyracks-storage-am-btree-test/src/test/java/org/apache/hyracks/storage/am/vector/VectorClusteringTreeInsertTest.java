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

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.vector.frames.VectorTreeFrameType;
import org.apache.hyracks.storage.am.vector.util.VectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.util.VectorTreeTestHarness;
import org.junit.After;
import org.junit.Before;

/**
 * Tests the VectorClusteringTree insert operation with vector fields and string metadata fields
 * using various numbers of vector dimensions and key fields.
 * 
 * Each test first fills a VectorClusteringTree with randomly generated vector tuples. 
 * We validate the following operations against expected results:
 * 1. Point searches for all tuples using exact vector matches
 * 2. Scan operations to verify tree structure
 * 3. Vector similarity searches to validate tree organization
 * 4. Index validation to ensure tree integrity
 */
@SuppressWarnings("rawtypes")
public class VectorClusteringTreeInsertTest extends AbstractVectorTreeTestDriver {

    private final VectorTreeTestHarness harness = new VectorTreeTestHarness();
    private final VectorTreeTestUtils vectorTreeTestUtils = new VectorTreeTestUtils();

    public VectorClusteringTreeInsertTest() {
        super(VectorTreeTestHarness.LEAF_FRAMES_TO_TEST);
    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected AbstractVectorTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            VectorTreeFrameType frameType, boolean filtered) throws Exception {
        return VectorTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCache(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, numKeys, frameType,
                harness.getVectorDimensions());
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, VectorTreeFrameType frameType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey,
            ITupleReference prefixHighKey) throws Exception {

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, numKeys, frameType, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Determine field types and insert appropriate test data
        if (fieldSerdes[0] instanceof FloatArraySerializerDeserializer) {
            // Vector-based insertion tests
            vectorTreeTestUtils.insertVectorTuples(ctx, numTuplesToInsert, getRandom());
        } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
            // Mixed vector and string metadata tests
            vectorTreeTestUtils.insertMixedTuples(ctx, numTuplesToInsert, getRandom());
        }

        // Validate insertions through various operations
        vectorTreeTestUtils.checkPointSearches(ctx);
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkDiskOrderScan(ctx);

        // Vector-specific validation: similarity searches
        if (fieldSerdes[0] instanceof FloatArraySerializerDeserializer) {
            vectorTreeTestUtils.checkVectorSimilaritySearches(ctx);
        }

        // Range searches if keys are provided
        if (lowKey != null && highKey != null) {
            vectorTreeTestUtils.checkRangeSearch(ctx, lowKey, highKey, true, true);
        }
        if (prefixLowKey != null && prefixHighKey != null) {
            vectorTreeTestUtils.checkRangeSearch(ctx, prefixLowKey, prefixHighKey, true, true);
        }

        // Validate tree structure integrity
        ctx.getIndex().validate();
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Override
    protected String getTestOpName() {
        return "Insert";
    }

    /**
     * Test high-dimensional vector insertion (stress test)
     */
    public void highDimensionVectorTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorClusteringTree " + getTestOpName() + " Test With High-Dimension Vectors.");
        }

        // Set high-dimensional vectors for this test
        harness.setVectorDimensions(128);

        // High-dimensional vector + metadata
        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        for (VectorTreeFrameType frameType : frameTypesToTest) {
            runTest(fieldSerdes, 1, frameType, null, null, null, null);
        }

        // Reset dimensions
        harness.setVectorDimensions(VectorTreeTestHarness.DEFAULT_VECTOR_DIMENSIONS);
    }

    /**
     * Test insertion with pre-initialized static structure
     */
    public void staticStructureInsertTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorClusteringTree " + getTestOpName() + " Test With Static Structure.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize static structure using the initializer
        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig config =
                org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig
                        .multipleLeaves();
        VectorTreeTestUtils.initializeStaticStructure(ctx, config);

        // Insert additional tuples into the pre-built structure
        vectorTreeTestUtils.insertVectorTuples(ctx, 50, getRandom());

        // Validate all operations work with the static structure
        vectorTreeTestUtils.checkPointSearches(ctx);
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkDiskOrderScan(ctx);
        vectorTreeTestUtils.checkVectorSimilaritySearches(ctx);

        // Validate tree structure integrity
        ctx.getIndex().validate();

        // Clean up the static initializer
        VectorTreeTestUtils.cleanupStaticInitializer();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test with deep tree structure initialization
     */
    public void deepTreeStructureTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorClusteringTree " + getTestOpName() + " Test With Deep Tree Structure.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize deep tree structure
        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig config =
                org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig
                        .deepTree();
        VectorTreeTestUtils.initializeStaticStructure(ctx, config);

        // Insert tuples into the deep structure
        vectorTreeTestUtils.insertVectorTuples(ctx, 100, getRandom());

        // Validate operations with deep tree
        vectorTreeTestUtils.checkPointSearches(ctx);
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkVectorSimilaritySearches(ctx);

        ctx.getIndex().validate();

        VectorTreeTestUtils.cleanupStaticInitializer();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test edge cases with zero vectors and special values
     */
    public void edgeCaseVectorTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorClusteringTree " + getTestOpName() + " Test With Edge Case Vectors.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Insert edge case vectors (zero vectors, normalized vectors, etc.)
        vectorTreeTestUtils.insertEdgeCaseVectors(ctx, 100, getRandom());

        // Validate the insertions
        vectorTreeTestUtils.checkPointSearches(ctx);
        vectorTreeTestUtils.checkScan(ctx);

        ctx.getIndex().validate();
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test using pre-built static tree structure - Single leaf configuration
     */
    public void staticStructureSingleLeafTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                    "VectorClusteringTree " + getTestOpName() + " Test With Pre-built Static Single Leaf Structure.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize static single leaf structure
        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig config =
                org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig
                        .singleLeaf();

        VectorTreeTestUtils.initializeStaticStructure(ctx, config);

        // Validate the pre-built structure
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkDiskOrderScan(ctx);

        // Validate tree structure integrity
        ctx.getIndex().validate();

        // Clean up static initializer
        VectorTreeTestUtils.cleanupStaticInitializer();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test using pre-built static tree structure - Multiple leaves configuration
     */
    public void staticStructureMultipleLeavesTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorClusteringTree " + getTestOpName()
                    + " Test With Pre-built Static Multiple Leaves Structure.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize static multiple leaves structure
        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig config =
                org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig
                        .multipleLeaves();

        VectorTreeTestUtils.initializeStaticStructure(ctx, config);

        // Test operations on the pre-built structure
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkDiskOrderScan(ctx);

        // Test vector similarity searches on structured data
        vectorTreeTestUtils.checkVectorSimilaritySearches(ctx);

        // Validate tree structure integrity
        ctx.getIndex().validate();

        // Clean up static initializer
        VectorTreeTestUtils.cleanupStaticInitializer();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test using pre-built static tree structure - Deep tree configuration
     */
    public void staticStructureDeepTreeTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorClusteringTree " + getTestOpName() + " Test With Pre-built Static Deep Tree Structure.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize static deep tree structure
        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig config =
                org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig
                        .deepTree();

        VectorTreeTestUtils.initializeStaticStructure(ctx, config);

        // Test comprehensive operations on the deep structure
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkDiskOrderScan(ctx);
        vectorTreeTestUtils.checkVectorSimilaritySearches(ctx);

        // Validate tree structure integrity - this is important for deep trees
        ctx.getIndex().validate();

        // Test static initializer information
        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer initializer =
                VectorTreeTestUtils.getStaticInitializer();
        if (initializer != null) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Deep tree structure initialized with root page ID: " + initializer.getRootPageId());
                LOGGER.info("Total test pages created: " + initializer.getAllPages().size());
            }
        }

        // Clean up static initializer
        VectorTreeTestUtils.cleanupStaticInitializer();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    /**
     * Test mixed insertions on pre-built static structure
     */
    public void staticStructureMixedInsertionsTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                    "VectorClusteringTree " + getTestOpName() + " Test With Mixed Insertions on Pre-built Structure.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, 1, VectorTreeFrameType.REGULAR_NSM, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Initialize static structure first
        org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig config =
                org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeStaticInitializer.TreeStructureConfig
                        .multipleLeaves();

        VectorTreeTestUtils.initializeStaticStructure(ctx, config);

        // Now perform additional insertions on the pre-built structure
        vectorTreeTestUtils.insertVectorTuples(ctx, 50, getRandom());

        // Validate combined structure (static + dynamic insertions)
        vectorTreeTestUtils.checkScan(ctx);
        vectorTreeTestUtils.checkVectorSimilaritySearches(ctx);

        // Validate tree structure integrity after mixed operations
        ctx.getIndex().validate();

        // Clean up static initializer
        VectorTreeTestUtils.cleanupStaticInitializer();

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

}
