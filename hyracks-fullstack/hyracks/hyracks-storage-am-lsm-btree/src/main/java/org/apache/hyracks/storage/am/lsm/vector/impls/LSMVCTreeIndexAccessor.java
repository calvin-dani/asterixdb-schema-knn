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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;

/**
 * LSM Vector Clustering Tree Index Accessor.
 * 
 * This accessor extends the standard LSM tree accessor to provide specialized support
 * for vector operations, including both standard range/point search and approximate
 * nearest neighbor (ANN) search capabilities.
 */
public class LSMVCTreeIndexAccessor extends LSMTreeIndexAccessor {

    private final LSMVCTree lsmVCTree;

    public LSMVCTreeIndexAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx, ICursorFactory cursorFactory,
            LSMVCTree lsmVCTree) {
        super(lsmHarness, ctx, cursorFactory);
        this.lsmVCTree = lsmVCTree;
    }

    @Override
    public IIndexCursor createSearchCursor(boolean exclusive) {
        // Check which cursor type is requested via IndexAccessParameters
        if (ctx instanceof LSMVCTreeOpContext) {
            LSMVCTreeOpContext opCtx = (LSMVCTreeOpContext) ctx;
            IIndexAccessParameters iap = opCtx.getIndexAccessParameters();
            if (iap != null) {
                // searchApproach=1 or 2: optimized bidirectional (with optional filtering)
                Boolean useOptimized = iap.getParameter(HyracksConstants.USE_OPTIMIZED_SEARCH, Boolean.class);
                if (Boolean.TRUE.equals(useOptimized)) {
                    return lsmVCTree.createBlockedSearchCursor(ctx);
                }
                // searchApproach=3: naive blocked (top-K window, quantized distance, no pruning)
                Boolean useNaiveBlocked = iap.getParameter(HyracksConstants.USE_NAIVE_BLOCKED_SEARCH, Boolean.class);
                if (Boolean.TRUE.equals(useNaiveBlocked)) {
                    return lsmVCTree.createNaiveBlockedSearchCursor(ctx);
                }
            }
        }
        // searchApproach=0: default naive streaming
        return super.createSearchCursor(exclusive);
    }

    /**
     * Creates a blocked cursor for optimized search with bidirectional traversal
     * and triangle inequality termination.
     *
     * @param exclusive whether the cursor should be exclusive
     * @return blocked search cursor
     * @throws HyracksDataException if cursor creation fails
     */
    public IIndexCursor createBlockedSearchCursor(boolean exclusive) throws HyracksDataException {
        return lsmVCTree.createBlockedSearchCursor(ctx);
    }
}
