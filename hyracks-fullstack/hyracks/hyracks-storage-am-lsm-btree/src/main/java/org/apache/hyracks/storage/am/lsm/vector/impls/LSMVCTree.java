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
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.vector.impls.VectorPointPredicate;
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
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
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
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMVCTreeComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LoadOperation;
import org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * LSM Vector Clustering Tree implementation for hierarchical vector clustering with LSM storage.
 * 
 * This implementation uses VectorClusteringTree as in-memory components
 * 
 */
public class LSMVCTree extends AbstractLSMIndex implements ITreeIndex {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final ICursorFactory cursorFactory = LSMVCTreeSearchCursor::new;
    private static final ICursorFactory annCursorFactory = LSMVCTreeAnnCursor::new;

    // Vector clustering specific frame factories
    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;
    protected final ITreeIndexFrameFactory metadataFrameFactory;
    protected final ITreeIndexFrameFactory insertDataFrameFactory;
    protected final ITreeIndexFrameFactory deleteDataFrameFactory;

    // Vector clustering specific parameters
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int vectorDimensions;
    protected final boolean needKeyDupCheck;
    protected final IVectorBinaryAccessorFactory vectorAccessorFactory;

    // Field count information for dynamic primary key and include field handling
    // Data tuple format: [distance, centroidId, primary_keys..., include_fields...]
    protected final int numPrimaryKeyFields;
    protected final int numIncludeFields;

    protected LSMVCTreeDiskComponent staticStructure;

    public LSMVCTree(NCConfig storageConfig, IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            ITreeIndexFrameFactory metadataFrameFactory, ITreeIndexFrameFactory insertDataFrameFactory,
            ITreeIndexFrameFactory deleteDataFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, int vectorDimensions, int[] vectorFields, int[] filterFields, boolean durable,
            boolean atomic, org.apache.hyracks.storage.am.vector.api.IVectorBinaryAccessorFactory vectorAccessorFactory,
            int numPrimaryKeyFields, int numIncludeFields)
            throws HyracksDataException {

        super(storageConfig, ioManager, virtualBufferCaches, diskBufferCache, fileManager, bloomFilterFalsePositiveRate,
                mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, componentFactory,
                bulkLoadComponentFactory, filterFrameFactory, filterManager, filterFields, durable, filterHelper,
                vectorFields, ITracer.NONE, atomic);
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.metadataFrameFactory = metadataFrameFactory;
        this.insertDataFrameFactory = insertDataFrameFactory;
        this.deleteDataFrameFactory = deleteDataFrameFactory;
        this.cmpFactories = cmpFactories;
        this.vectorDimensions = vectorDimensions;
        this.needKeyDupCheck = needKeyDupCheck;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.numPrimaryKeyFields = numPrimaryKeyFields;
        this.numIncludeFields = numIncludeFields;

        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            String baseDirPath = fileManager.getBaseDir() + "_virtual_" + i;
            FileReference virtualFileRef = ioManager.resolve(baseDirPath);
            // Memory components use insertDataFrameFactory for normal inserts
            VectorClusteringTree vcTree = new VectorClusteringTree(virtualBufferCache,
                    new VirtualFreePageManager(virtualBufferCache), interiorFrameFactory, leafFrameFactory,
                    metadataFrameFactory, insertDataFrameFactory, cmpFactories, 1, vectorDimensions, virtualFileRef,
                    vectorAccessorFactory);
            LSMVCTreeMemoryComponent mutableComponent = new LSMVCTreeMemoryComponent(this, vcTree, virtualBufferCache,
                    filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
    }

    public void setStaticStructure(LSMVCTreeDiskComponent staticStructure) {
        this.staticStructure = staticStructure;
        staticStructure.setInitialized();
    }

    public void setInitialized(LSMVCTreeDiskComponent diskComponent) {
        diskComponent.setInitialized();
    }

    protected ILSMDiskComponent createStaticStructure(ILSMDiskComponentFactory factory,
            FileReference insertFileReference, FileReference deleteIndexFileReference, FileReference bloomFilterFileRef,
            boolean createComponent) throws HyracksDataException {
        ILSMDiskComponent component = factory.createComponent(this,
                new LSMComponentFileReferences(insertFileReference, deleteIndexFileReference, bloomFilterFileRef));
        try {
            ((LSMVCTreeDiskComponent) component).setStaticStructure(true);
            component.activate(createComponent);
        } catch (HyracksDataException e) {
            component.returnPages();
            throw e;
        }
        return component;
    }

    @Override
    public void addBulkLoadedDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        LSMVCTreeDiskComponent vcTreeDiskComponent = (LSMVCTreeDiskComponent) c;
        if (vcTreeDiskComponent.isStaticStructure()) {
            setStaticStructure(vcTreeDiskComponent);
            return;
        }
        setInitialized(vcTreeDiskComponent);
        diskComponents.add(0, c);
        validateComponentIds();
    }

    public LSMVCTreeDiskComponent getStaticStructure() {
        if (staticStructure == null) {
            throw new IllegalStateException("Static structure must be built before loading records");
        }
        return staticStructure;
    }

    /**
     * Override createBulkLoader to handle static structure creation.
     * Checks parameters to determine whether to use static structure file or data file.
     */
    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        AbstractLSMIndexOperationContext opCtx = createOpContext(NoOpIndexAccessParameters.INSTANCE);
        boolean isStaticStructureLoad = parameters != null && parameters.containsKey("numLevels")
                && parameters.containsKey("clustersPerLevel") && parameters.containsKey("centroidsPerCluster");
        if (!isStaticStructureLoad) {
            // For data loading, ensure static structure is already built
            parameters.put("static_structure_component", getStaticStructure());
        }
        opCtx.setParameters(parameters);
        LSMVCTreeComponentFileReferences componentFileRefs =
                (LSMVCTreeComponentFileReferences) fileManager.getRelFlushFileReference();
        LoadOperation loadOp = new LoadOperation(componentFileRefs, ioOpCallback, getIndexIdentifier(), parameters);

        // Check if this is static structure creation or data loading

        ILSMDiskComponent component;
        if (isStaticStructureLoad) {
            // Use static structure file reference
            component = createStaticStructure(bulkLoadComponentFactory,
                    componentFileRefs.getStaticStructureFileReference(), null, null, true);
        } else {
            // Use standard data file reference
            component = createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                    null, null, true);
        }

        loadOp.setNewComponent(component);
        ioOpCallback.scheduled(loadOp);
        opCtx.setIoOperation(loadOp);
        return new LSMIndexDiskComponentBulkLoader(storageConfig, this, opCtx, fillFactor, verifyInput,
                numElementsHint);
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

        // Debug logging
        System.err.println("[LSMVCTree.modify] Operation: " + ctx.getOperation() +
                          ", Thread: " + Thread.currentThread().getId());

        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
        } else {
            indexTuple = tuple;
        }

        switch (ctx.getOperation()) {
            case PHYSICALDELETE:
                System.err.println("[LSMVCTree.modify] Executing PHYSICALDELETE");
                ctx.getCurrentMutableVCTreeAccessor().delete(indexTuple);
                break;
            case INSERT:
                System.err.println("[LSMVCTree.modify] Executing INSERT");
                insert(indexTuple, ctx);
                break;
            case DELETE:
                System.err.println("[LSMVCTree.modify] Executing DELETE");
                delete(indexTuple, ctx);
                break;
            default:
                System.err.println("[LSMVCTree.modify] Executing default (UPSERT)");
                ctx.getCurrentMutableVCTreeAccessor().upsert(indexTuple);
                break;
        }
        updateFilter(ctx, tuple);
    }

    private boolean insert(ITupleReference tuple, LSMVCTreeOpContext ctx) throws HyracksDataException {
        // TODO: Implement vector-specific duplicate checking logic
        VectorClusteringTree.VectorClusteringTreeAccessor memoryComponentAccessor =
                (VectorClusteringTree.VectorClusteringTreeAccessor) (ctx.getCurrentMutableVCTreeAccessor());
        VectorClusteringTree.VectorClusteringTreeAccessor staticAccessor =
                (VectorClusteringTree.VectorClusteringTreeAccessor) getStaticStructure().getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);

        if (!memoryComponentAccessor.getIndex().isInitialized()) {
            memoryComponentAccessor.getIndex().setStaticStructure(staticAccessor);
        }

        memoryComponentAccessor.insert(tuple);

        return true;
    }

    /**
     * Handles delete operation for vector index.
     * <p>
     * Delete flow:
     * 1. Call delete() on VectorClusteringTreeAccessor with the original input tuple
     * 2. VectorClusteringTreeAccessor.delete() sets operation to DELETE and calls insertVector()
     * 3. insertVector() navigates, calculates distance, and constructs data tuple
     * 4. Based on operation type (DELETE), creates antimatter tuple with bit 7 set
     * 5. Antimatter tuple is inserted into data pages
     *
     * This approach reuses all existing insertVector() logic and only differs in
     * the antimatter bit being set when the operation type is DELETE.
     */
    private void delete(ITupleReference tuple, LSMVCTreeOpContext ctx) throws HyracksDataException {
        // Get the current mutable VectorClusteringTree accessor
        VectorClusteringTree.VectorClusteringTreeAccessor memoryComponentAccessor = (ctx.getCurrentMutableVCTreeAccessor());
        VectorClusteringTree.VectorClusteringTreeAccessor staticAccessor =
                (VectorClusteringTree.VectorClusteringTreeAccessor) getStaticStructure().getIndex()
                        .createAccessor(NoOpIndexAccessParameters.INSTANCE);

        // Ensure static structure is initialized (same pattern as insert)
        if (!memoryComponentAccessor.getIndex().isInitialized()) {
            memoryComponentAccessor.getIndex().setStaticStructure(staticAccessor);
        }

        System.err.println("[LSMVCTree.delete] Calling VectorClusteringTreeAccessor.delete(), Thread="
                + Thread.currentThread().getId());

        // Call delete() which will set operation to DELETE and delegate to insertVector()
        // insertVector() will create an antimatter tuple based on the DELETE operation
        memoryComponentAccessor.delete(tuple);

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
        LSMVCTreeDiskComponentLoader componentFlushLoader = null;
        try {
            component = createDiskComponent(componentFactory, flushOp.getTarget(), null, null, true);

            componentFlushLoader =
                    (LSMVCTreeDiskComponentLoader) ((LSMVCTreeDiskComponent) component).createFlushLoader(storageConfig,
                            operation, false, pageWriteCallbackFactory.createPageWriteCallback());

            VectorClusteringTree.VectorClusteringTreeAccessor vcTreeAccessor =
                    (VectorClusteringTree.VectorClusteringTreeAccessor) accessor;
            ITreeIndexMetadataFrame componentMetaFrame = (vcTreeAccessor).getOpContext().getMetaFrame();
            // Simple bulk load - just copy all pages
            int maxPageId = flushingComponent.getIndex().getPageManager().getMaxPageId(componentMetaFrame);
            System.err.println();
            for (int pageId = 0; pageId <= maxPageId; pageId++) {
                ICachedPage sourcePage = vcTreeAccessor.getCachedPage(pageId);
                componentFlushLoader.copyPage(sourcePage);
                vcTreeAccessor.releasePage(sourcePage);
            }

            componentFlushLoader.end();

        } catch (Throwable e) {
            try {
                if (componentFlushLoader != null) {
                    componentFlushLoader.abort();
                }
            } catch (Throwable th) {
                e.addSuppressed(th);
            }
            throw e;
        }
        return component;
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMVCTreeMergeOperation mergeOp = (LSMVCTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ILSMDiskComponent mergedComponent;
        ILSMDiskComponentBulkLoader componentBulkLoader = null;

        try {
            try {
                // Use VectorPointPredicate for merge (full scan mode)
                // K=MAX_VALUE to scan all records, nprobe=MAX_VALUE to probe all clusters,
                // epsilon=0.0 to disable level-wise (use sequential iteration)
                VectorPointPredicate mergePred = new VectorPointPredicate();
                mergePred.setNprobe(Integer.MAX_VALUE);
                mergePred.setEpsilon(0.0);

                // Search all merging components (cursor already configured for full-scan mode)
                search(mergeOp.getAccessor().getOpContext(), cursor, mergePred);

                try {
                    // Create merged disk component
                    List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
                    long numElements = getNumberOfElements(mergedComponents);
                    mergedComponent = createDiskComponent(componentFactory, mergeOp.getTarget(), null, null, true);

                    // Set parameters for merge operation (including static structure reference)
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("static_structure_component", getStaticStructure());
                    mergeOp.getAccessor().getOpContext().setParameters(parameters);

                    // Create bulk loader (delegates to VCTreeBulkLoader)
                    IPageWriteCallback pageWriteCallback = pageWriteCallbackFactory.createPageWriteCallback();
                    componentBulkLoader = mergedComponent.createBulkLoader(storageConfig, operation, 1.0f, false,
                            numElements, false, false, false, pageWriteCallback);

                    // Iterate cursor and add tuples
                    // Cursor delivers tuples cluster-by-cluster, already reconciled
                    while (cursor.hasNext()) {
                        cursor.next();
                        ITupleReference frameTuple = cursor.getTuple();
                        componentBulkLoader.add(frameTuple);
                    }
                } finally {
                    cursor.close();
                }
            } finally {
                cursor.destroy();
            }

            componentBulkLoader.end();
        } catch (Throwable e) { // NOSONAR.. As per the contract, we should either abort or end
            try {
                if (componentBulkLoader != null) {
                    componentBulkLoader.abort();
                }
            } catch (Throwable th) { // NOSONAR Don't lose the root failure
                e.addSuppressed(th);
            }
            throw e;
        }

        return mergedComponent;
    }

    /**
     * Get number of elements in merging components.
     * Used to estimate bulk loader size.
     */
    private long getNumberOfElements(List<ILSMComponent> mergedComponents) throws HyracksDataException {
        long numElements = 0L;
        for (ILSMComponent comp : mergedComponents) {
            LSMVCTreeDiskComponent diskComp = (LSMVCTreeDiskComponent) comp;
            // Get component size if available, otherwise use default estimate
            numElements += diskComp.getComponentSize();
        }
        return numElements;
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback) {
        return new LSMVCTreeFlushOperation(createAccessor(opCtx), componentFileRefs.getInsertIndexFileReference(),
                callback, getIndexIdentifier());
    }

    @Override
    public LSMVCTreeOpContext createOpContext(IIndexAccessParameters iap) {
        return new LSMVCTreeOpContext(this, memoryComponents, getTreeFields(), getFilterFields(), getFilterCmpFactories(),
                (IExtendedModificationOperationCallback) iap.getModificationCallback(),
                iap.getSearchOperationCallback(), tracer, iap);
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

    public ITreeIndexFrameFactory getMetadataFrameFactory() {
        return metadataFrameFactory;
    }

    public ITreeIndexFrameFactory getInsertDataFrameFactory() {
        return insertDataFrameFactory;
    }

    public ITreeIndexFrameFactory getDeleteDataFrameFactory() {
        return deleteDataFrameFactory;
    }

    public IBinaryComparatorFactory[] getCmpFactories() {
        return cmpFactories;
    }

    /**
     * Gets the number of primary key fields in the data tuple.
     * Data tuple format: [distance, centroidId, primary_keys..., include_fields...]
     * Primary keys start at field index 2 and span numPrimaryKeyFields.
     *
     * @return number of primary key fields
     */
    public int getNumPrimaryKeyFields() {
        return numPrimaryKeyFields;
    }

    /**
     * Gets the number of INCLUDE fields in the data tuple.
     * Data tuple format: [distance, centroidId, primary_keys..., include_fields...]
     * Include fields start at field index (2 + numPrimaryKeyFields).
     *
     * @return number of INCLUDE fields
     */
    public int getNumIncludeFields() {
        return numIncludeFields;
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
        // Extract file names from components (e.g., "0_0" from file "0_0_vct")
        LSMVCTreeDiskComponent first = (LSMVCTreeDiskComponent) firstComponent;
        LSMVCTreeDiskComponent last = (LSMVCTreeDiskComponent) lastComponent;

        String firstName = first.getIndex().getFileReference().getFile().getName();
        String lastName = last.getIndex().getFileReference().getFile().getName();

        return fileManager.getRelMergeFileReference(firstName, lastName);
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException {

        // Determine if deleted tuples should be preserved
        List<ILSMComponent> mergingComponents = opCtx.getComponentHolder();
        boolean returnDeletedTuples = true;

        // If merging oldest component, anti-matter tuples can be discarded
        // Check if the last component in mergingComponents is the oldest disk component
        if (!diskComponents.isEmpty() && mergingComponents.size() > 0) {
            ILSMComponent lastMergingComponent = mergingComponents.get(mergingComponents.size() - 1);
            ILSMComponent oldestDiskComponent = diskComponents.get(diskComponents.size() - 1);

            if (lastMergingComponent == oldestDiskComponent) {
                returnDeletedTuples = false; // Anti-matter can be discarded
            }
        }

        // Create cursor with full-scan mode enabled for merge
        IIndexCursorStats stats = NoOpIndexCursorStats.INSTANCE;
        ILSMIndexAccessor accessor = createAccessor(opCtx);

        // Create LSMVCTreeSearchCursor in full-scan mode for merge operations
        // fullScanMode=true enables sequential cluster iteration (0→1→2→...)
        // returnDeletedTuples=true ensures antimatter tuples are visible for reconciliation
        IIndexCursor cursor = new LSMVCTreeSearchCursor((ILSMIndexOperationContext) opCtx, returnDeletedTuples, true,
                stats);

        return new LSMVCTreeMergeOperation(accessor, cursor, stats, mergeFileRefs.getInsertIndexFileReference(),
                callback, getIndexIdentifier());
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

    @Override
    public synchronized void deactivate(boolean flush) throws HyracksDataException {
        // Clean up static structure component before parent deactivation
        if (staticStructure != null) {
            try {
                staticStructure.deactivateAndPurge();
            } catch (Exception e) {
                // Log but don't fail deactivation if static structure cleanup fails
                LOGGER.warn("Failed to deactivate static structure component: {}", e.getMessage());
            }
            staticStructure = null; // Clear reference after deactivation
        }
        super.deactivate(flush);
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        // Clean up static structure component before parent destruction
        if (staticStructure != null) {
            try {
                staticStructure.destroy();
            } catch (Exception e) {
                // Log but don't fail destruction if static structure cleanup fails
                LOGGER.warn("Failed to destroy static structure component: {}", e.getMessage());
            }
            staticStructure = null; // Clear reference after destruction
        }
        super.destroy();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_ACTIVATE_ACTIVE_INDEX);
        }
        loadDiskComponents();
        completeActivation();
        isActive = true;
    }

    private void loadStaticStructure() throws HyracksDataException {
        LSMVCTreeFileManager lsmVcTreeFileManager = (LSMVCTreeFileManager) fileManager;
        LSMVCTreeComponentFileReferences ssFileRefeference = lsmVcTreeFileManager.getStaticStructureFileReference();
        if (ssFileRefeference == null) {
            return;
        }
        ILSMDiskComponent ssComponent = createStaticStructure(componentFactory,
                ssFileRefeference.getStaticStructureFileReference(), null, null, false);
        setStaticStructure((LSMVCTreeDiskComponent) ssComponent);
    }

    private void loadDiskComponents() throws HyracksDataException {
        diskComponents.clear();
        List<LSMComponentFileReferences> validFileReferences = fileManager.cleanupAndGetValidFiles();
        for (LSMComponentFileReferences lsmComponentFileReferences : validFileReferences) {
            ILSMDiskComponent component =
                    createDiskComponent(componentFactory, lsmComponentFileReferences.getInsertIndexFileReference(),
                            lsmComponentFileReferences.getDeleteIndexFileReference(),
                            lsmComponentFileReferences.getBloomFilterFileReference(), false);
            diskComponents.add(component);
        }
        loadStaticStructure();
    }
}
