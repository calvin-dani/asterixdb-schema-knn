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
package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback;
import org.apache.asterix.transaction.management.opcallbacks.AbstractIndexModificationOperationCallback.Operation;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspector;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.common.IModificationOperationCallback;

/**
 * This operator node is used for secondary indexes with upsert operations.
 * It works in the following way:
 * For each incoming tuple
 * -If old secondary index tuple == new secondary index tuple
 * --do nothing
 * -else
 * --perform the operation based on the operation kind
 */
public class LSMSecondaryUpsertOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    protected static final int UPSERT_NEW = LSMPrimaryUpsertOperatorNodePushable.UPSERT_NEW.getByteValue();
    protected static final int UPSERT_EXISTING = LSMPrimaryUpsertOperatorNodePushable.UPSERT_EXISTING.getByteValue();
    protected static final int DELETE_EXISTING = LSMPrimaryUpsertOperatorNodePushable.DELETE_EXISTING.getByteValue();

    private final PermutingFrameTupleReference prevTuple = new PermutingFrameTupleReference();
    private final int numberOfFields;
    private final ITupleFilterFactory prevTupleFilterFactory;
    private ITupleFilter prevTupleFilter;

    protected final int operationFieldIndex;
    protected final IBinaryIntegerInspector operationInspector;

    public LSMSecondaryUpsertOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, IModificationOperationCallbackFactory modCallbackFactory,
            ITupleFilterFactory tupleFilterFactory, ITupleFilterFactory prevTupleFilterFactory, int[] fieldPermutation,
            RecordDescriptor inputRecDesc, int operationFieldIndex,
            IBinaryIntegerInspectorFactory operationInspectorFactory, int[] prevTuplePermutation,
            ITuplePartitionerFactory tuplePartitionerFactory, int[][] partitionsMap) throws HyracksDataException {
        super(ctx, partition, indexHelperFactory, fieldPermutation, inputRecDesc, IndexOperation.UPSERT,
                modCallbackFactory, tupleFilterFactory, tuplePartitionerFactory, partitionsMap);
        this.prevTuple.setFieldPermutation(prevTuplePermutation);
        this.operationFieldIndex = operationFieldIndex;
        this.operationInspector = operationInspectorFactory.createBinaryIntegerInspector(ctx);
        this.numberOfFields = fieldPermutation.length;
        this.prevTupleFilterFactory = prevTupleFilterFactory;

        // DEBUG: Log field permutations
        System.out.println("DEBUG [LSMSecondaryUpsert Constructor]: fieldPermutation="
                + java.util.Arrays.toString(fieldPermutation));
        System.out.println("DEBUG [LSMSecondaryUpsert Constructor]: prevTuplePermutation="
                + java.util.Arrays.toString(prevTuplePermutation));
        System.out.println("DEBUG [LSMSecondaryUpsert Constructor]: operationFieldIndex=" + operationFieldIndex);
        System.out.println(
                "DEBUG [LSMSecondaryUpsert Constructor]: inputRecDesc.getFieldCount()=" + inputRecDesc.getFieldCount());
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        frameTuple = new FrameTupleReference();
        if (prevTupleFilterFactory != null) {
            prevTupleFilter = prevTupleFilterFactory.createTupleFilter(ctx);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        boolean tupleFilterIsNull = tupleFilter == null;
        boolean prevTupleFilterIsNull = prevTupleFilter == null;
        for (int i = 0; i < tupleCount; i++) {
            try {
                int storagePartition = tuplePartitioner.partition(accessor, i);
                int storageIdx = storagePartitionId2Index.get(storagePartition);
                ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessors[storageIdx];
                IModificationOperationCallback abstractModCallback = modCallbacks[storageIdx];
                frameTuple.reset(accessor, i);
                int operation = operationInspector.getIntegerValue(frameTuple.getFieldData(operationFieldIndex),
                        frameTuple.getFieldStart(operationFieldIndex), frameTuple.getFieldLength(operationFieldIndex));
                tuple.reset(accessor, i);
                prevTuple.reset(accessor, i);

                // DEBUG: Log operation type and tuple comparison
                System.out.println("DEBUG [LSMSecondaryUpsert]: operation=" + operation
                        + " (UPSERT_NEW=0, UPSERT_EXISTING=1, DELETE_EXISTING=2)");
                if (operation == UPSERT_EXISTING) {
                    boolean tuplesEqual = TupleUtils.equalTuples(tuple, prevTuple, numberOfFields);
                    System.out.println("DEBUG [LSMSecondaryUpsert]: UPSERT_EXISTING - tuplesEqual=" + tuplesEqual
                            + ", numberOfFields=" + numberOfFields);
                    System.out.println("DEBUG [LSMSecondaryUpsert]: tuple.getFieldCount()=" + tuple.getFieldCount()
                            + ", prevTuple.getFieldCount()=" + prevTuple.getFieldCount());

                    // DEBUG: Print field data for both tuples
                    System.out.println("DEBUG [LSMSecondaryUpsert]: Inspecting tuple fields:");
                    for (int f = 0; f < tuple.getFieldCount(); f++) {
                        System.out.println("  tuple field[" + f + "]: offset=" + tuple.getFieldStart(f) + ", length="
                                + tuple.getFieldLength(f));
                    }
                    System.out.println("DEBUG [LSMSecondaryUpsert]: Inspecting prevTuple fields:");
                    for (int f = 0; f < prevTuple.getFieldCount(); f++) {
                        System.out.println("  prevTuple field[" + f + "]: offset=" + prevTuple.getFieldStart(f)
                                + ", length=" + prevTuple.getFieldLength(f));
                    }
                }

                if (operation == UPSERT_NEW) {
                    if (tupleFilterIsNull || tupleFilter.accept(frameTuple)) {
                        if (abstractModCallback instanceof AbstractIndexModificationOperationCallback) {
                            ((AbstractIndexModificationOperationCallback) abstractModCallback).setOp(Operation.INSERT);
                        }
                        lsmAccessor.forceInsert(tuple);
                    }
                } else if (operation == UPSERT_EXISTING) {
                    if (!TupleUtils.equalTuples(tuple, prevTuple, numberOfFields)) {
                        System.out.println("DEBUG [LSMSecondaryUpsert]: Tuples differ, proceeding with delete+insert");
                        System.out
                                .println("DEBUG [LSMSecondaryUpsert]: prevTupleFilterIsNull=" + prevTupleFilterIsNull);

                        if (prevTupleFilterIsNull || prevTupleFilter.accept(frameTuple)) {
                            System.out.println("DEBUG [LSMSecondaryUpsert]: Calling forceDelete on prevTuple");
                            if (abstractModCallback instanceof AbstractIndexModificationOperationCallback) {
                                ((AbstractIndexModificationOperationCallback) abstractModCallback)
                                        .setOp(Operation.DELETE);
                            }
                            lsmAccessor.forceDelete(prevTuple);
                        } else {
                            System.out.println(
                                    "DEBUG [LSMSecondaryUpsert]: prevTupleFilter REJECTED frameTuple - SKIP DELETE");
                        }

                        System.out.println("DEBUG [LSMSecondaryUpsert]: tupleFilterIsNull=" + tupleFilterIsNull);
                        if (tupleFilterIsNull || tupleFilter.accept(frameTuple)) {
                            System.out.println("DEBUG [LSMSecondaryUpsert]: Calling forceInsert on tuple");
                            if (abstractModCallback instanceof AbstractIndexModificationOperationCallback) {
                                ((AbstractIndexModificationOperationCallback) abstractModCallback)
                                        .setOp(Operation.INSERT);
                            }
                            lsmAccessor.forceInsert(tuple);
                        } else {
                            System.out.println(
                                    "DEBUG [LSMSecondaryUpsert]: tupleFilter REJECTED frameTuple - SKIP INSERT");
                        }
                    }
                } else if (operation == DELETE_EXISTING) {
                    if (prevTupleFilterIsNull || prevTupleFilter.accept(frameTuple)) {
                        if (abstractModCallback instanceof AbstractIndexModificationOperationCallback) {
                            ((AbstractIndexModificationOperationCallback) abstractModCallback).setOp(Operation.DELETE);
                        }
                        lsmAccessor.forceDelete(prevTuple);
                    }
                }
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
        // No partial flushing was necessary. Forward entire frame.
        writeBuffer.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
        FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
    }

    private static boolean isNullOrMissing(FrameTupleReference tuple, int fieldIdx) {
        return TypeTagUtil.isType(tuple, fieldIdx, ATypeTag.SERIALIZED_NULL_TYPE_TAG)
                || TypeTagUtil.isType(tuple, fieldIdx, ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
    }

    protected static boolean hasNullOrMissing(FrameTupleReference tuple) {
        int fieldCount = tuple.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (isNullOrMissing(tuple, i)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void flush() throws HyracksDataException {
        // No op since nextFrame flushes by default
    }
}
