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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.util.trace.ITracer;

/**
 * Operation context for LSM Vector Clustering Tree operations.
 * 
 * This context manages the state and resources needed for vector clustering
 * operations across multiple LSM components.
 */
public class LSMVCTreeOpContext extends AbstractLSMIndexOperationContext {

    private LSMVCTreeCursorInitialState searchInitialState;
    private ISearchPredicate searchPredicate;
    private int currentMutableComponentId = 0;

    public LSMVCTreeOpContext(LSMVCTree lsmTree, int[] treeFields, int[] filterFields,
            IBinaryComparatorFactory[] filterCmpFactories, IExtendedModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback, ITracer tracer) {
        super(lsmTree, treeFields, filterFields, filterCmpFactories, searchCallback, modificationCallback, tracer);
        this.searchInitialState = null; // Will be created when needed
    }

    @Override
    public void setOperation(IndexOperation newOp) throws HyracksDataException {
        super.setOperation(newOp);
    }

    @Override
    public void reset() {
        super.reset();
    }

    @Override
    public void destroy() throws HyracksDataException {
        // No additional cleanup needed beyond the base class
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        this.currentMutableComponentId = currentMutableComponentId;
    }

    public int getCurrentMutableComponentId() {
        return currentMutableComponentId;
    }

    /**
     * Gets the search initial state.
     */
    public LSMVCTreeCursorInitialState getSearchInitialState() {
        return searchInitialState;
    }

    /**
     * Gets the search predicate.
     */
    @Override
    public ISearchPredicate getSearchPredicate() {
        return searchPredicate;
    }

    /**
     * Sets the search predicate.
     */
    @Override
    public void setSearchPredicate(ISearchPredicate searchPredicate) {
        this.searchPredicate = searchPredicate;
    }

    /**
     * Gets the accessor for the current mutable VectorClusteringTree component.
     */
    public ITreeIndexAccessor getCurrentMutableVCTreeAccessor() throws HyracksDataException {
        LSMVCTree lsmVCTree = (LSMVCTree) getIndex();
        LSMVCTreeMemoryComponent memComponent = (LSMVCTreeMemoryComponent) lsmVCTree.getCurrentMemoryComponent();
        IndexAccessParameters iap = new IndexAccessParameters(getModificationCallback(), getSearchOperationCallback());
        return memComponent.getIndex().createAccessor(iap);
    }
}
