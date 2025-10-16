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

package org.apache.hyracks.storage.am.lsm.vector;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.impl.CountingIoOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestHarness;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class LSMVCTreeComponentLifecycleTest {
    private static final Logger LOGGER = LogManager.getLogger();
    private final LSMVCTreeTestHarness harness = new LSMVCTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    // Vector clustering tree specific serializers: ID field + vector field
    private final ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE, // ID field
            FloatArraySerializerDeserializer.INSTANCE // Vector field
    };

    private final int vectorDimensions = 4; // 128-dimensional vectors
    private static final int numTuplesToInsert = 100;

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    private AbstractVectorTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int vectorDimensions)
            throws Exception {
        return LSMVCTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes,
                vectorDimensions, harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory());
    }

    @Test
    public void testNormalFlushOperation() throws Exception {
        AbstractVectorTreeTestContext ctx = createTestContext(fieldSerdes, vectorDimensions);
        ILSMIndex index = (ILSMIndex) ctx.getIndex();

        // Create and activate the index
        index.create();
        index.activate();

        // Verify initial memory component index
        Assert.assertEquals(0, index.getCurrentMemoryComponentIndex());
        // Insert vector tuples into the index
        testUtils.insertRecordsIntoMultipleClusters(ctx);

        // Perform first flush
        flush(ctx);

        // After flush, memory component should switch to the next one
        Assert.assertEquals(1, index.getCurrentMemoryComponentIndex());
        // Should have 1 disk component now
        Assert.assertEquals(1, index.getDiskComponents().size());
        //
        // Verify IO operation callbacks were properly called
        CountingIoOperationCallback ioCallback = (CountingIoOperationCallback) index.getIOOperationCallback();
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        //        // Insert more vector tuples
        //        testUtils.insertVectorTuples(ctx, numTuplesToInsert, harness.getRandom());
        //
        //        // Perform second flush
        //        flush(ctx);
        //
        //        // Memory component should switch back to 0 (assuming 2 memory components)
        //        Assert.assertEquals(0, index.getCurrentMemoryComponentIndex());
        //        // Should have 2 disk components now
        //        Assert.assertEquals(2, index.getDiskComponents().size());
        //
        //        // Verify callback counts are still balanced
        //        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        //        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        //
        //        // Insert more vector tuples for third flush
        //        testUtils.insertVectorTuples(ctx, numTuplesToInsert, harness.getRandom());
        //
        //        // Perform third flush
        //        flush(ctx);
        //
        //        // Memory component should switch to 1
        //        Assert.assertEquals(1, index.getCurrentMemoryComponentIndex());
        //        // Should have 3 disk components now
        //        Assert.assertEquals(3, index.getDiskComponents().size());
        //
        //        // Final verification of callback counts
        //        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterOperationCount());
        //        Assert.assertEquals(ioCallback.getBeforeOperationCount(), ioCallback.getAfterFinalizeCount());
        //
        //        LOGGER.info("Successfully completed normal flush operation test with {} flushes and {} disk components", 3,
        //                index.getDiskComponents().size());

        // Clean up
        ctx.getIndex().deactivate();
        //ctx.getIndex().destroy();
    }

    private void flush(AbstractVectorTreeTestContext ctx) throws HyracksDataException, InterruptedException {
        ILSMIOOperation flush = scheduleFlush(ctx);
        flush.sync();
        if (flush.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(flush.getFailure());
        }
    }

    private ILSMIOOperation scheduleFlush(AbstractVectorTreeTestContext ctx)
            throws HyracksDataException, InterruptedException {
        ILSMIndexAccessor accessor =
                (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        return accessor.scheduleFlush();
    }
}
