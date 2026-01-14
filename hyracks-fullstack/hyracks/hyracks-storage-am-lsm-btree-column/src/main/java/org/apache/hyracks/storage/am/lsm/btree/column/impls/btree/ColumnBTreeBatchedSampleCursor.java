package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTreeCursorInitialState;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.btree.impls.BatchPredicateWithKeys;
import org.apache.hyracks.storage.am.common.api.ILSMIndexBatchPointCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

// only pin the pageZero
// once the ids are collected, project the whole tuples, when requested to the cursor.
public class ColumnBTreeBatchedSampleCursor extends EnforcedIndexCursor
        implements ITreeIndexCursor, IColumnReadMultiPageOp {

    private final ColumnBTree bTree;
    private final BTreeOpContext opCtx;
    private final ColumnBTreeReadLeafFrame leafFrame;
    private final IColumnReadContext context;
    private final IColumnTupleIterator frameTuple;

    // u64: (pageId << 32) | tupleIndex
    private final LongSet seenTupleIndexes;

    // Cardinality variables
    private static final int MAX_LEAF_FINDING_ATTEMPTS = 20; // Setting just a random value for now
    private final int componentSampleCardinality;
    // Number of LIVE tuples sampled from the component so far.
    private int sampledCount;
    private boolean continueCurrentLeaf = false;
    private int hasNextAttemptCount = 0;

    private IBufferCache bufferCache;
    private int fileId = -1;

    private int rootPageId;
    private ICachedPage page0 = null;
    private int page0Id = -1;
    private int tupleIndex = -1;

    // search predicate
    private final ILSMIndexBatchPointCursor searchCursor;
    private final BatchPredicateWithKeys batchPredicate;
    private final List<ITupleReference> searchKeys;
    private final BitSet foundIndexes;

    public ColumnBTreeBatchedSampleCursor(ColumnBTree columnBTree, ColumnBTreeReadLeafFrame leafFrame,
            BTreeOpContext opContext, IColumnReadContext context, int componentSampleCardinality, long sampleSeed,
            int index, ILSMIndexBatchPointCursor searchCursor) {
        this.bTree = columnBTree;
        this.opCtx = opContext;
        this.leafFrame = leafFrame;
        this.context = context;
        this.componentSampleCardinality = componentSampleCardinality;
        this.batchPredicate = new BatchPredicateWithKeys();
        this.searchCursor = searchCursor;
        this.frameTuple = leafFrame.createTupleReference(index, this);
        this.searchKeys = new ArrayList<>();
        this.foundIndexes = new BitSet();
        this.seenTupleIndexes = new LongOpenHashSet();
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        if (page0 != null) {
            releasePages();
        }

        page0 = initialState.getPage();
        page0Id = ((BTreeCursorInitialState) initialState).getPageId();

        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();

    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        return false;
    }

    @Override
    protected void doNext() throws HyracksDataException {

    }

    @Override
    protected void doDestroy() throws HyracksDataException {

    }

    @Override
    protected void doClose() throws HyracksDataException {

    }

    @Override
    protected ITupleReference doGetTuple() {
        return null;
    }

    private void releasePages() throws HyracksDataException {
        context.release(bufferCache);
        frameTuple.unpinColumnsPages();
        if (page0 != null) {
            bufferCache.unpin(page0);
        }
    }

    @Override
    public ICachedPage pin(int pageId) throws HyracksDataException {
        return null;
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {

    }

    @Override
    public int getPageSize() {
        return 0;
    }
}
