package org.apache.hyracks.storage.am.lsm.btree.column.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeBatchedSampleCursor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

import it.unimi.dsi.fastutil.ints.IntSet;

public class BTreeBatchedSampleCollectorOperatorDescriptorNodePushable extends BTreeSearchOperatorNodePushable {
    private final int sampleCardinalityTargetPerPartition;
    private final long sampleSeed;
    private IntSet pageTupleIds;

    public BTreeBatchedSampleCollectorOperatorDescriptorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive, int[] minFilterKeyFields, int[] maxFilterKeyFields,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, ITupleProjectorFactory tupleProjectorFactory,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap,
            int sampleCardinalityTargetPerPartition, long sampleSeed) throws HyracksDataException {
        super(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                minFilterKeyFields, maxFilterKeyFields, indexHelperFactory, retainInput, retainMissing,
                missingWriterFactory, searchCallbackFactory, false, null, tupleFilterFactory, outputLimit, false, null,
                null, tupleProjectorFactory, tuplePartitionerFactory, partitionsMap);
        this.sampleCardinalityTargetPerPartition = sampleCardinalityTargetPerPartition;
        this.sampleSeed = sampleSeed;
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException {
        super.addAdditionalIndexAccessorParams(iap);
        iap.getParameters().put(HyracksConstants.SAMPLE_CARDINALITY, sampleCardinalityTargetPerPartition);
        iap.getParameters().put(HyracksConstants.SAMPLE_SEED, sampleSeed);
    }

    @Override
    protected IIndexCursor createCursor(IIndex idx, IIndexAccessor idxAccessor) throws HyracksDataException {
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) idxAccessor;
        return ((LSMBTree) idx).createSampleCollectorCursor(lsmAccessor.getOpContext());
    }

    @Override
    protected void writeSearchResults(int tupleIndex, IIndexCursor cursor) throws Exception {
        ColumnBTreeBatchedSampleCursor batchedCursor = (ColumnBTreeBatchedSampleCursor) cursor;
        long matchingTupleCount = 0;
        while (batchedCursor.hasNext()) {
            batchedCursor.next();
            matchingTupleCount++;
            ITupleReference tuple = batchedCursor.getTuple();

        }
    }
}
