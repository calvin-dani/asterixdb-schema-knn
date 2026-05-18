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

package org.apache.hyracks.storage.am.vector.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISlotManager;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeFrame;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

/**
 * Common NSM (N-ary Storage Model) base for VTree page frames.
 * <p>
 * Page header layout extends {@link TreeIndexNSMFrame}: after the inherited reserved header bytes come a
 * 4-byte cluster ID and a 4-byte centroid ID. Subclasses append their own header fields after
 * {@link #CENTROID_ID_OFFSET} + 4 and override {@link #getPageHeaderSize()} accordingly.
 */
public abstract class VTreeNSMFrame extends TreeIndexNSMFrame implements IVTreeFrame {

    // Offset of the 4-byte cluster ID field (sentinel -1 = unassigned).
    protected static final int CLUSTER_ID_OFFSET = TreeIndexNSMFrame.RESERVED_HEADER_SIZE;
    // Offset of the 4-byte centroid ID field. Subclass headers extend from CENTROID_ID_OFFSET + 4.
    protected static final int CENTROID_ID_OFFSET = CLUSTER_ID_OFFSET + 4;

    protected MultiComparator cmp;
    protected final ITreeIndexTupleReference frameTuple;

    public VTreeNSMFrame(ITreeIndexTupleWriter tupleWriter, ISlotManager slotManager) {
        super(tupleWriter, slotManager);
        this.frameTuple = tupleWriter.createTupleReference();
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(CLUSTER_ID_OFFSET, -1);
    }

    @Override
    public int getPageHeaderSize() {
        return CENTROID_ID_OFFSET + 4;
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    @Override
    public void setMultiComparator(MultiComparator cmp) {
        this.cmp = cmp;
    }

    @Override
    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + slotManager.getSlotSize();
    }

    @Override
    public void insertSorted(ITupleReference tuple) {
        insert(tuple, getTupleCount());
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException {
        int tupleSize = getBytesRequiredToWriteTuple(tuple);
        int totalFreeSpace = buf.getInt(TOTAL_FREE_SPACE_OFFSET);

        if (totalFreeSpace >= tupleSize) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        } else if (getFreeSpaceOff()
                - ((getTupleCount() + 1) * slotManager.getSlotSize() + getPageHeaderSize()) >= tupleWriter
                        .bytesRequired(tuple)) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        } else {
            return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
        }
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException {
        // Generic split is not supported on VTree frames; subclasses that split (e.g. data/metadata frames)
        // expose their own split(...) entry points with the right argument types.
        throw new HyracksDataException("Split operation not implemented for " + this.getClass().getSimpleName());
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("clusterId:         ").append(buf.getInt(CLUSTER_ID_OFFSET)).append('\n');
        strBuilder.append("centroidId:        ").append(buf.getInt(CENTROID_ID_OFFSET)).append('\n');
        return strBuilder.toString();
    }
}
