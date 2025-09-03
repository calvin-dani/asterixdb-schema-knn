package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BTreeSampleCollectorOperatorDescriptorNodePushable extends BTreeSearchOperatorNodePushable {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final String SAMPLE_OPERATION_IS_GOING = "SAMPLE_OPERATION_IS_GOING";
    private final int sampleCardinalityTargetPerPartition;
    private final long sampleSeed;
    private final IHyracksTaskContext ctx;
    private long openDuration;

    public BTreeSampleCollectorOperatorDescriptorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive, int[] minFilterKeyFields, int[] maxFilterKeyFields,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, ITupleProjectorFactory tupleProjectorFactory,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap,
            int sampleCardinalityTargetPerPartition, long sampleSeed, boolean isSampling) throws HyracksDataException {
        super(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                minFilterKeyFields, maxFilterKeyFields, indexHelperFactory, retainInput, retainMissing,
                missingWriterFactory, searchCallbackFactory, false, null, tupleFilterFactory, outputLimit, false, null,
                null, tupleProjectorFactory, tuplePartitionerFactory, partitionsMap);
        this.sampleCardinalityTargetPerPartition = sampleCardinalityTargetPerPartition;
        this.sampleSeed = sampleSeed;
        this.ctx = ctx;
        TaskUtil.put(SAMPLE_OPERATION_IS_GOING, Boolean.TRUE, ctx);
    }

    public BTreeSampleCollectorOperatorDescriptorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive, int[] minFilterKeyFields, int[] maxFilterKeyFields,
            IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput, boolean retainMissing,
            IMissingWriterFactory missingWriterFactory, ISearchOperationCallbackFactory searchCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, long outputLimit, ITupleProjectorFactory tupleProjectorFactory,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap,
            int sampleCardinalityTargetPerPartition, long sampleSeed) throws HyracksDataException {
        this(ctx, partition, inputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive,
                minFilterKeyFields, maxFilterKeyFields, indexHelperFactory, retainInput, retainMissing,
                missingWriterFactory, searchCallbackFactory, tupleFilterFactory, outputLimit, tupleProjectorFactory,
                tuplePartitionerFactory, partitionsMap, sampleCardinalityTargetPerPartition, sampleSeed, false);
    }

    @Override
    public void open() throws HyracksDataException {
        TaskUtil.put(SAMPLE_OPERATION_IS_GOING, Boolean.TRUE, ctx);
        long start = System.nanoTime();
        super.open();
        openDuration = System.nanoTime() - start;
    }

    @Override
    public void close() throws HyracksDataException {
        long start = System.nanoTime();
        try {
            super.close();
        } finally {
            long closeDuration = System.nanoTime() - start;
            if (TaskUtil.get(SAMPLE_OPERATION_IS_GOING, ctx) != null) {
                LOGGER.debug("StatsLogging: BTreeSampleCollector_Open_Time: {}ns", openDuration);
                LOGGER.debug("StatsLogging: BTreeSampleCollector_Close_Time: {}ns", closeDuration);
            }
        }
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
        long matchingTupleCount = 0;
        while (cursor.hasNext()) {
            cursor.next();
            matchingTupleCount++;
            ITupleReference tuple = cursor.getTuple();
            tb.reset();

            if (retainInput) {
                frameTuple.reset(accessor, tupleIndex);
                for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                    dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    tb.addFieldEndOffset();
                }
            }

            // tuple must be written first before the filter is applied to
            // assemble columnar tuples
            tuple = writeTupleToOutput(tuple);
            if (tuple == null) {
                continue;
            }

            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
            if (outputLimit >= 0 && ++outputCount >= outputLimit) {
                finished = true;
                break;
            }

            stats.getInputTupleCounter().update(matchingTupleCount);
        }
        cursor.print();
    }

    private static int getTupleSize(ITupleReference tuple) {
        int fields = tuple.getFieldCount();
        return tuple.getFieldStart(fields - 1) + tuple.getFieldLength(fields - 1) - tuple.getFieldStart(0);
    }

    @Override
    protected void searchPartition(int tupleCount) throws Exception {
        for (int i = 0; i < tupleCount && !finished; i++) {
            int storagePartition = tuplePartitioner.partition(accessor, i);
            int pIdx = storagePartitionId2Index.get(storagePartition);
            cursors[pIdx].close();
            ((ILSMIndexAccessor) indexAccessors[pIdx]).scanDiskComponentsForSample(cursors[pIdx]);
            writeSearchResults(i, cursors[pIdx]);
        }
    }

    @Override
    protected void searchAllPartitions(int tupleCount) throws Exception {
        for (int p = 0; p < partitions.length; p++) {
            for (int i = 0; i < tupleCount && !finished; i++) {
                cursors[p].close();
                ((ILSMIndexAccessor) indexAccessors[p]).scanDiskComponentsForSample(cursors[p]);
                writeSearchResults(i, cursors[p]);
            }
        }
    }
}
