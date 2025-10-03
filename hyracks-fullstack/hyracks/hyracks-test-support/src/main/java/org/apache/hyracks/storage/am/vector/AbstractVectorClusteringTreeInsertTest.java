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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.vector.frames.VectorTreeFrameType;

/**
 * Abstract base class for VectorClusteringTree insert tests.
 * Provides common test logic similar to OrderedIndexInsertTest for BTree.
 * 
 * Concrete test classes should extend this and provide:
 * - Test harness setup (@Before/@After methods)
 * - createTestContext() implementation
 * - getRandom() implementation
 * - Actual @Test methods
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractVectorClusteringTreeInsertTest extends AbstractVectorTreeTestDriver {

    private final VectorTreeTestUtils vectorTreeTestUtils;

    public AbstractVectorClusteringTreeInsertTest(VectorTreeFrameType[] frameTypesToTest) {
        super(frameTypesToTest);
        this.vectorTreeTestUtils = new VectorTreeTestUtils();
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
}
