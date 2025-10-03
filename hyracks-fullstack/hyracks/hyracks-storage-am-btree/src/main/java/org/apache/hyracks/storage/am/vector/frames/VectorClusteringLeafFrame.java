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
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringLeafFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

/**
 * Vector clustering leaf frame implementation.
 * Contains cluster entries: <cid, full_precision_centroid, pointer_to_first_metadata_page>
 */
public class VectorClusteringLeafFrame extends VectorClusteringNSMFrame implements IVectorClusteringLeafFrame {

    // Offset for next leaf pointer (4 bytes) - comes after centroid data
    private int getNextLeafOffset() {
        return CENTROID_DATA_OFFSET + (centroidDimensions * 8);
    }

    public VectorClusteringLeafFrame(ITreeIndexTupleWriter tupleWriter, int centroidDimensions) {
        super(tupleWriter, new OrderedSlotManager(), centroidDimensions);
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(getNextLeafOffset(), -1); // Initialize next leaf pointer to -1
    }

    @Override
    public int getPageHeaderSize() {
        return getNextLeafOffset() + 4; // Base header + next leaf pointer
    }

    @Override
    public void setNextLeaf(int nextLeafPage) {
        buf.putInt(getNextLeafOffset(), nextLeafPage);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(getNextLeafOffset());
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        // For leaf frames, we can insert clusters in any order since they don't need sorting
        // Insert at the end for simplicity
        return getTupleCount();
    }

    @Override
    public int getMetadataPagePointer(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Metadata page pointer is the last field in the leaf entry tuple
        int metadataPtrFieldIndex = frameTuple.getFieldCount() - 1;
        return IntegerPointable.getInteger(frameTuple.getFieldData(metadataPtrFieldIndex),
                frameTuple.getFieldStart(metadataPtrFieldIndex));
    }

    @Override
    public void setMetadataPagePointer(int tupleIndex, int metadataPageId) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Metadata page pointer is the last field in the leaf entry tuple
        int metadataPtrFieldIndex = frameTuple.getFieldCount() - 1;
        IntegerPointable.setInteger(frameTuple.getFieldData(metadataPtrFieldIndex),
                frameTuple.getFieldStart(metadataPtrFieldIndex), metadataPageId);
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        int freeSpace = buf.getInt(Constants.FREE_SPACE_OFFSET);
        slotManager.insertSlot(tupleIndex, freeSpace);
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), freeSpace);
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) + 1);
        buf.putInt(Constants.FREE_SPACE_OFFSET, buf.getInt(Constants.FREE_SPACE_OFFSET) + bytesWritten);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - bytesWritten - slotManager.getSlotSize());
    }

    public void insertSorted(ITupleReference tuple) {
        // For leaf frames, order doesn't matter, so just insert at the end
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
    public void ensureCapacity(IBufferCache bufferCache, ITupleReference tuple,
            IExtraPageBlockHelper extraPageBlockHelper) throws HyracksDataException {
        int gapBytes = getBytesRequiredToWriteTuple(tuple) - getFreeContiguousSpace();
        if (gapBytes > 0) {
            int deltaPages = (int) Math.ceil((double) gapBytes / bufferCache.getPageSize());
            growCapacity(extraPageBlockHelper, bufferCache, deltaPages);
        }
    }

    private void growCapacity(IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache, int deltaPages)
            throws HyracksDataException {
        int framePagesOld = page.getFrameSizeMultiplier();
        int newMultiplier = framePagesOld + deltaPages;

        // Get old slot offsets before growing
        int oldSlotEnd = slotManager.getSlotEndOff();
        int oldSlotStart = slotManager.getSlotStartOff() + slotManager.getSlotSize();

        bufferCache.resizePage(getPage(), newMultiplier, extraPageBlockHelper);
        buf = getPage().getBuffer();

        // Fix up the slots
        System.arraycopy(buf.array(), oldSlotEnd, buf.array(), slotManager.getSlotEndOff(), oldSlotStart - oldSlotEnd);

        // Fix up total free space counter
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) + (bufferCache.getPageSize() * deltaPages));
    }

    public int getFreeSpaceOff() {
        return buf.getInt(Constants.FREE_SPACE_OFFSET);
    }

    public int getFreeContiguousSpace() {
        return slotManager.getSlotEndOff() - getFreeSpaceOff();
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextLeaf:          " + getNextLeaf() + "\n");
        return strBuilder.toString();
    }
}
