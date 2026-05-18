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

import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * LSM Vector Clustering Tree Index Accessor.
 * 
 * This accessor extends the standard LSM tree accessor to provide specialized support
 * for vector operations, including both standard range/point search and approximate
 * nearest neighbor (ANN) search capabilities.
 */
public class LSMVTreeIndexAccessor extends LSMTreeIndexAccessor {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTree lsmVTree;

    public LSMVTreeIndexAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx, ICursorFactory cursorFactory,
            LSMVTree lsmVTree) {
        super(lsmHarness, ctx, cursorFactory);
        this.lsmVTree = lsmVTree;
    }

    @Override
    public IIndexCursor createSearchCursor(boolean exclusive) {
        if (ctx instanceof LSMVTreeOpContext) {
            LSMVTreeOpContext opCtx = (LSMVTreeOpContext) ctx;
            IIndexAccessParameters iap = opCtx.getIndexAccessParameters();
            if (iap != null) {
                Boolean useNaiveBlocked = iap.getParameter(HyracksConstants.USE_NAIVE_BLOCKED_SEARCH, Boolean.class);
                if (Boolean.TRUE.equals(useNaiveBlocked)) {
                    return lsmVTree.createNaiveBlockedSearchCursor(ctx);
                }
            }
        }
        return super.createSearchCursor(exclusive);
    }
}
