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

import java.util.HashMap;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrame;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.*;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.am.vector.impls.VCTreeStaticStructureBuilder;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;import org.apache.hyracks.util.trace.ITracer;

/**
 * LSM Vector Clustering Tree implementation for hierarchical vector clustering with LSM storage.
 * 
 * This implementation uses VectorClusteringTree as in-memory components
 * 
 */
public class LSMVCTree extends AbstractLSMIndex implements ITreeIndex {

    private static final ICursorFactory cursorFactory = LSMVCTreeSearchCursor::new;
    private static final ICursorFactory annCursorFactory = LSMVCTreeAnnCursor::new;

    // Vector clustering specific frame factories
    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;
    protected final ITreeIndexFrameFactory metadataFrameFactory;
    protected final ITreeIndexFrameFactory dataFrameFactory;

    // Vector clustering specific parameters
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int vectorDimensions;
    protected final boolean needKeyDupCheck;

    public LSMVCTree(NCConfig storageConfig, IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            ITreeIndexFrameFactory metadataFrameFactory, ITreeIndexFrameFactory dataFrameFactory,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, int vectorDimensions, int[] vectorFields, int[] filterFields, boolean durable,
            boolean atomic) throws HyracksDataException {

        super(storageConfig, ioManager, virtualBufferCaches, diskBufferCache, fileManager, bloomFilterFalsePositiveRate,
                mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, componentFactory,
                bulkLoadComponentFactory, filterFrameFactory, filterManager, filterFields, durable, filterHelper,
                vectorFields, ITracer.NONE, atomic);

        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
        this.cmpFactories = cmpFactories;
        this.vectorDimensions = vectorDimensions;
        this.needKeyDupCheck = needKeyDupCheck;

        // Create in-memory components using VectorClusteringTree
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            LSMVCTreeMemoryComponent mutableComponent = new LSMVCTreeMemoryComponent(this,
                    new VectorClusteringTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache),
                            interiorFrameFactory, leafFrameFactory, metadataFrameFactory, dataFrameFactory,
                            cmpFactories, 1, vectorDimensions,
                            ioManager.resolve(fileManager.getBaseDir() + "_virtual_" + i)),
                    virtualBufferCache, filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
    }

    public VCTreeStaticStructureBuilder createStaticStructureBuilder(int numLevels, List<Integer> clustersPerLevel,
            List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage) throws HyracksDataException {
        // Get the current mutable component to build the static structure
        // AbstractLSMIndexOperationContext opCtx = createOpContext(NoOpIndexAccessParameters.INSTANCE);
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        LoadOperation loadOp = new LoadOperation(componentFileRefs, ioOpCallback, getIndexIdentifier(), new HashMap<>());
        ILSMDiskComponent diskComponent = createDiskComponent(bulkLoadComponentFactory,
                componentFileRefs.getInsertIndexFileReference(), null,
                null, true);
        loadOp.setNewComponent(diskComponent);
        ioOpCallback.scheduled(loadOp);
        // opCtx.setIoOperation(loadOp);

        // Create the VCTreeStaticStructureBuilder with a NoOp page write callback for now
        return ((LSMVCTreeDiskComponent) diskComponent).createStaticStructureBuilder(storageConfig, numLevels, clustersPerLevel,
                centroidsPerCluster, maxEntriesPerPage, NoOpPageWriteCallback.INSTANCE);
    }

    @Override
    public boolean isPrimaryIndex() {
        return needKeyDupCheck;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    /**
     * Gets the vector dimensions for this index.
     */
    public int getVectorDimensions() {
        return vectorDimensions;
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMVCTreeOpContext ctx = (LSMVCTreeOpContext) ictx;
        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
        } else {
            indexTuple = tuple;
        }

        switch (ctx.getOperation()) {
            case PHYSICALDELETE:
                ctx.getCurrentMutableVCTreeAccessor().delete(indexTuple);
                break;
            case INSERT:
                insert(indexTuple, ctx);
                break;
            default:
                ctx.getCurrentMutableVCTreeAccessor().upsert(indexTuple);
                break;
        }
        updateFilter(ctx, tuple);
    }

    private boolean insert(ITupleReference tuple, LSMVCTreeOpContext ctx) throws HyracksDataException {
        // For now, implement basic insert without duplicate checking
        // TODO: Implement vector-specific duplicate checking logic
        ctx.getCurrentMutableVCTreeAccessor().insert(tuple);
        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        LSMVCTreeOpContext ctx = (LSMVCTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        cursor.open(ctx.getSearchInitialState(), pred);
    }

    @Override
    public void scanDiskComponents(ILSMIndexOperationContext ictx, IIndexCursor cursor) throws HyracksDataException {
        // Vector clustering trees don't support disk component scanning in the same way as BTrees
        throw new UnsupportedOperationException("Disk component scanning not supported for vector clustering trees");
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMVCTreeFlushOperation flushOp = (LSMVCTreeFlushOperation) operation;
        LSMVCTreeMemoryComponent flushingComponent = (LSMVCTreeMemoryComponent) flushOp.getFlushingComponent();
        IIndexAccessor accessor = flushingComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);

        ILSMDiskComponent component = null;
        LSMVCTreeDiskComponentLoader componentFlushLoader;
        try {
            component = createDiskComponent(componentFactory, flushOp.getTarget(), null, null, true);

            componentFlushLoader =
                    (LSMVCTreeDiskComponentLoader) ((LSMVCTreeDiskComponent) component).createFlushLoader(storageConfig,
                            operation, false, pageWriteCallbackFactory.createPageWriteCallback());

            try {
                VectorClusteringTree.VectorClusteringTreeAccessor vcTreeAccessor =
                        (VectorClusteringTree.VectorClusteringTreeAccessor) accessor;
                ITreeIndexMetadataFrame componentMetaFrame = (vcTreeAccessor).getOpContext().getMetaFrame();
                // Simple bulk load - just copy all pages
                int maxPageId = flushingComponent.getIndex().getPageManager().getMaxPageId(componentMetaFrame);

                for (int pageId = 0; pageId <= maxPageId; pageId++) {
                    ICachedPage sourcePage = vcTreeAccessor.getCachedPage(pageId);
                    componentFlushLoader.copyPage(sourcePage);
                    vcTreeAccessor.releasePage(sourcePage);
                }

                componentFlushLoader.end();

            } catch (Throwable e) {
                componentFlushLoader.abort();
                throw e;
            }
        } catch (Throwable e) {
            if (component != null) {
                component.destroy();
            }
            throw e;
        }
        return component;
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMVCTreeMergeOperation mergeOp = (LSMVCTreeMergeOperation) operation;

        // Create the new merged disk component
        ILSMDiskComponent component = null;
        try {
            component = createDiskComponent(componentFactory, mergeOp.getTarget(), null, null, true);

            // Create a bulk loader for the merged disk component
            ILSMDiskComponentBulkLoader componentBulkLoader = component.createBulkLoader(storageConfig, operation, 1.0f,
                    false, 0L, false, false, false, pageWriteCallbackFactory.createPageWriteCallback());

            try {
                // Get the components to be merged
                LSMVCTreeOpContext opCtx = (LSMVCTreeOpContext) mergeOp.getAccessor().getOpContext();
                List<ILSMComponent> componentsToMerge = opCtx.getComponentHolder();

                // Use a cursor to scan all components being merged
                LSMVCTreeSearchCursor cursor = new LSMVCTreeSearchCursor(opCtx);
                RangePredicate pred = new RangePredicate(null, null, true, true, null, null);

                // Create initial state for cursor spanning all components to merge
                LSMVCTreeCursorInitialState initialState = new LSMVCTreeCursorInitialState(interiorFrameFactory,
                        leafFrameFactory, metadataFrameFactory, dataFrameFactory, MultiComparator.create(cmpFactories),
                        getHarness(), pred, opCtx.getSearchOperationCallback(), componentsToMerge);

                // Merge all components
                cursor.open(initialState, pred);
                try {
                    while (cursor.hasNext()) {
                        cursor.next();
                        ITupleReference tuple = cursor.getTuple();
                        componentBulkLoader.add(tuple);
                    }
                } finally {
                    cursor.close();
                }

                componentBulkLoader.end();
            } catch (Throwable e) {
                try {
                    if (componentBulkLoader != null) {
                        componentBulkLoader.abort();
                    }
                } catch (Throwable th) {
                    e.addSuppressed(th);
                }
                throw e;
            }
        } catch (Throwable e) {
            try {
                if (component != null) {
                    component.markAsValid(false, null);
                    component.destroy();
                    component = null;
                }
            } catch (Throwable th) {
                e.addSuppressed(th);
            }
            throw e;
        }
        return component;
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback) {
        return new LSMVCTreeFlushOperation(createAccessor(opCtx), componentFileRefs.getInsertIndexFileReference(),
                callback, getIndexIdentifier());
    }

    @Override
    public LSMVCTreeOpContext createOpContext(IIndexAccessParameters iap) {
        return new LSMVCTreeOpContext(this, getTreeFields(), getFilterFields(), getFilterCmpFactories(),
                (IExtendedModificationOperationCallback) iap.getModificationCallback(),
                iap.getSearchOperationCallback(), tracer);
    }

    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return createAccessor(createOpContext(iap));
    }

    public ILSMIndexAccessor createAccessor(AbstractLSMIndexOperationContext opCtx) {
        return new LSMVCTreeIndexAccessor(getHarness(), opCtx, getCursorFactory(), this);
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMVCTreeMemoryComponent mutableComponent =
                (LSMVCTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getInteriorFrameFactory();
    }

    @Override
    public int getFieldCount() {
        LSMVCTreeMemoryComponent mutableComponent =
                (LSMVCTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFieldCount();
    }

    @Override
    public int getFileId() {
        LSMVCTreeMemoryComponent mutableComponent =
                (LSMVCTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFileId();
    }

    @Override
    public IPageManager getPageManager() {
        LSMVCTreeMemoryComponent mutableComponent =
                (LSMVCTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getPageManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMVCTreeMemoryComponent mutableComponent =
                (LSMVCTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getLeafFrameFactory();
    }

    @Override
    public int getRootPageId() {
        LSMVCTreeMemoryComponent mutableComponent =
                (LSMVCTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getRootPageId();
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException {
        return fileManager.getRelMergeFileReference(firstComponent.getId().toString(),
                lastComponent.getId().toString());
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException {
        IIndexCursor cursor = createAccessor(opCtx).createSearchCursor(false);
        return new LSMVCTreeMergeOperation(createAccessor(opCtx), cursor, null,
                mergeFileRefs.getInsertIndexFileReference(), callback, getIndexIdentifier());
    }

    protected ICursorFactory getCursorFactory() {
        return cursorFactory;
    }

    protected ICursorFactory getAnnCursorFactory() {
        return annCursorFactory;
    }

    /**
     * Creates an ANN search cursor for approximate nearest neighbor search.
     * 
     * @param opCtx the operation context
     * @return ANN search cursor
     */
    public IIndexCursor createAnnSearchCursor(AbstractLSMIndexOperationContext opCtx) {
        return new LSMVCTreeAnnCursor(opCtx);
    }
}
