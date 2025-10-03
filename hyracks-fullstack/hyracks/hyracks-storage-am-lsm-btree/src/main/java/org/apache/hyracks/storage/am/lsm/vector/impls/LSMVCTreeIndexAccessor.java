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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.vector.predicates.VectorAnnPredicate;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * LSM Vector Clustering Tree Index Accessor.
 * 
 * This accessor extends the standard LSM tree accessor to provide specialized support
 * for vector operations, including both standard range/point search and approximate
 * nearest neighbor (ANN) search capabilities.
 */
public class LSMVCTreeIndexAccessor extends LSMTreeIndexAccessor {

    private final LSMVCTree lsmVCTree;

    public LSMVCTreeIndexAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx,
            LSMTreeIndexAccessor.ICursorFactory cursorFactory, LSMVCTree lsmVCTree) {
        super(lsmHarness, ctx, cursorFactory);
        this.lsmVCTree = lsmVCTree;
    }

    @Override
    public IIndexCursor createSearchCursor(boolean exclusive) {
        return super.createSearchCursor(exclusive);
    }

    /**
     * Creates an ANN search cursor for approximate nearest neighbor queries.
     * 
     * @param exclusive whether the cursor should be exclusive
     * @return ANN search cursor
     * @throws HyracksDataException if cursor creation fails
     */
    public IIndexCursor createAnnSearchCursor(boolean exclusive) throws HyracksDataException {
        return lsmVCTree.createAnnSearchCursor((AbstractLSMIndexOperationContext) ctx);
    }

    /**
     * Perform search with automatic cursor selection based on predicate type.
     * If the predicate is a VectorAnnPredicate, uses ANN cursor; otherwise uses standard cursor.
     * 
     * @param cursor the cursor to use for search
     * @param searchPred the search predicate
     * @throws HyracksDataException if search fails
     */
    @Override
    public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
        if (searchPred instanceof VectorAnnPredicate) {
            // Ensure we're using the right cursor type for ANN search
            if (!(cursor instanceof LSMVCTreeAnnCursor)) {
                throw new IllegalArgumentException(
                        "VectorAnnPredicate requires LSMVCTreeAnnCursor, got: " + cursor.getClass().getSimpleName());
            }
        }
        super.search(cursor, searchPred);
    }

    /**
     * Convenience method for ANN search that creates the appropriate cursor and performs the search.
     * 
     * @param annPredicate the ANN search predicate
     * @return ANN search cursor with results
     * @throws HyracksDataException if search fails
     */
    public IIndexCursor searchAnn(VectorAnnPredicate annPredicate) throws HyracksDataException {
        IIndexCursor annCursor = createAnnSearchCursor(false);
        search(annCursor, annPredicate);
        return annCursor;
    }
}
