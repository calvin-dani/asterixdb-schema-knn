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

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringOpContext;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.trace.ITracer;

/**
 * Operation context for LSM Vector Clustering Tree operations.
 *
 * This context manages the state and resources needed for vector clustering
 * operations across multiple LSM components.
 */
public class LSMVCTreeOpContext extends AbstractLSMIndexOperationContext {

    /*
     * Finals - pre-created resources (LSMBTree pattern)
     */
    private final ITreeIndexFrameFactory insertDataFrameFactory;
    private final ITreeIndexFrameFactory deleteDataFrameFactory;
    private final IVectorClusteringDataFrame insertDataFrame;
    private final IVectorClusteringDataFrame deleteDataFrame;
    private final VectorClusteringTree[] mutableVCTrees;
    private final VectorClusteringTree.VectorClusteringTreeAccessor[] mutableVCTreeAccessors;
    private final VectorClusteringOpContext[] mutableVCTreeOpCtxs;
    private final MultiComparator cmp;
    private final LSMVCTreeCursorInitialState searchInitialState;
    private final IIndexAccessParameters iap;

    /*
     * Mutables - switched per operation
     */
    private VectorClusteringTree.VectorClusteringTreeAccessor currentMutableVCTreeAccessor;
    private VectorClusteringOpContext currentMutableVCTreeOpCtx;
    private boolean destroyed = false;

    public LSMVCTreeOpContext(LSMVCTree lsmTree, List<ILSMMemoryComponent> mutableComponents,
            int[] treeFields, int[] filterFields, IBinaryComparatorFactory[] filterCmpFactories,
            IExtendedModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback,
            ITracer tracer, IIndexAccessParameters iap) {
        super(lsmTree, treeFields, filterFields, filterCmpFactories, searchCallback, modificationCallback, tracer);

        this.iap = iap;
        this.insertDataFrameFactory = lsmTree.getInsertDataFrameFactory();
        this.deleteDataFrameFactory = lsmTree.getDeleteDataFrameFactory();

        // Create MultiComparator using ADM-aware comparators from LSMVCTree
        // These comparators handle the 1-byte type tag prefix for ADM-tagged types (ADOUBLE, AINT32, etc.)
        IBinaryComparatorFactory[] cmpFactories = lsmTree.getCmpFactories();
        IBinaryComparator[] comparators = new IBinaryComparator[cmpFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = cmpFactories[i].createBinaryComparator();
        }
        this.cmp = new MultiComparator(comparators);

        // Pre-create data frames for insert and delete operations (LSMBTree pattern)
        this.insertDataFrame = (IVectorClusteringDataFrame) insertDataFrameFactory.createFrame();
        this.deleteDataFrame = (IVectorClusteringDataFrame) deleteDataFrameFactory.createFrame();

        // Pre-create accessors and op contexts for ALL mutable components (LSMBTree pattern)
        mutableVCTrees = new VectorClusteringTree[mutableComponents.size()];
        mutableVCTreeAccessors = new VectorClusteringTree.VectorClusteringTreeAccessor[mutableComponents.size()];
        mutableVCTreeOpCtxs = new VectorClusteringOpContext[mutableComponents.size()];

        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMVCTreeMemoryComponent mutableComponent = (LSMVCTreeMemoryComponent) mutableComponents.get(i);
            mutableVCTrees[i] = mutableComponent.getIndex();
            IIndexAccessParameters componentIap =
                    new IndexAccessParameters(modificationCallback, NoOpOperationCallback.INSTANCE);
            mutableVCTreeAccessors[i] = (VectorClusteringTree.VectorClusteringTreeAccessor) mutableVCTrees[i].createAccessor(componentIap);
            mutableVCTreeOpCtxs[i] = mutableVCTreeAccessors[i].getOpContext();
        }

        // Initialize search state with insertDataFrameFactory (default mode)
        this.searchInitialState = new LSMVCTreeCursorInitialState(lsmTree.getInteriorFrameFactory(),
                lsmTree.getLeafFrameFactory(), lsmTree.getMetadataFrameFactory(), insertDataFrameFactory, cmp,
                lsmTree.getHarness(), null, searchCallback, componentHolder);
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    public void setInsertMode() {
        currentMutableVCTreeOpCtx.setDataFrame(insertDataFrame);
        currentMutableVCTreeOpCtx.setDataFrameFactory(insertDataFrameFactory);
    }

    public void setDeleteMode() {
        currentMutableVCTreeOpCtx.setDataFrame(deleteDataFrame);
        currentMutableVCTreeOpCtx.setDataFrameFactory(deleteDataFrameFactory);
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        setCurrentMutableVCTreeAccessor(mutableVCTreeAccessors[currentMutableComponentId]);
        currentMutableVCTreeOpCtx = mutableVCTreeOpCtxs[currentMutableComponentId];
        switch (op) {
            case SEARCH:
            case DISKORDERSCAN:
            case UPDATE:
                // Keep leafFrame as is for UPDATE (preserve previous operation semantics)
                break;
            case UPSERT:
            case INSERT:
                setInsertMode();
                break;
            case PHYSICALDELETE:
            case DELETE:
                setDeleteMode();
                break;
        }
    }

    public VectorClusteringTree.VectorClusteringTreeAccessor getCurrentMutableVCTreeAccessor() {
        return currentMutableVCTreeAccessor;
    }

    public void setCurrentMutableVCTreeAccessor(VectorClusteringTree.VectorClusteringTreeAccessor accessor) {
        this.currentMutableVCTreeAccessor = accessor;
    }

    public LSMVCTreeCursorInitialState getSearchInitialState() {
        return searchInitialState;
    }

    public MultiComparator getCmp() {
        return cmp;
    }

    public IIndexAccessParameters getIndexAccessParameters() {
        return iap;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (destroyed) {
            return;
        }
        destroyed = true;
        Throwable failure = CleanupUtils.destroy(null, mutableVCTreeAccessors);
        failure = CleanupUtils.destroy(failure, mutableVCTreeOpCtxs);
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }
}
