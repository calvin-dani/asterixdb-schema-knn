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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

/**
 * VTree leaf frame implementation
 */
public class VTreeLeafFrame extends VTreeNSMFrame implements IVTreeLeafFrame {
    protected static final int NEXT_PAGE_OFFSET = CENTROID_ID_OFFSET + 4;
    protected static final int OVERFLOW_FLAG_OFFSET = NEXT_PAGE_OFFSET + 4;

    public VTreeLeafFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(NEXT_PAGE_OFFSET, -1); // Initialize next leaf pointer to -1
        buf.put(OVERFLOW_FLAG_OFFSET, (byte) 0); // Initialize overflow flag to false
    }

    @Override
    public int getPageHeaderSize() {
        return OVERFLOW_FLAG_OFFSET + 1; // Base header + next leaf pointer
    }

    @Override
    public void setNextLeaf(int nextLeafPage) {
        buf.putInt(NEXT_PAGE_OFFSET, nextLeafPage);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(NEXT_PAGE_OFFSET);
    }

    public void setOverflowFlagBit(boolean overflowFlag) {
        buf.put(OVERFLOW_FLAG_OFFSET, (byte) (overflowFlag ? 1 : 0));
    }

    public boolean getOverflowFlagBit() {
        return buf.get(OVERFLOW_FLAG_OFFSET) != 0;
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
    public int getCentroidId(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Centroid ID is the first field in the leaf entry tuple: <cid, centroid, metadata_ptr>
        int cidFieldIndex = 0;
        return IntegerPointable.getInteger(frameTuple.getFieldData(cidFieldIndex),
                frameTuple.getFieldStart(cidFieldIndex));
    }

    @Override
    public byte[] getQuantizedCentroidBytes(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        if (frameTuple.getFieldCount() < 4) {
            return null; // Non-quantized tuple
        }
        // Quantized bytes are at field 2 in the 4-field format:
        // [cid, embedding, quantizedBytes, metadataPtr]
        int fieldIndex = 2;
        byte[] fieldData = frameTuple.getFieldData(fieldIndex);
        int fieldStart = frameTuple.getFieldStart(fieldIndex);
        int fieldLength = frameTuple.getFieldLength(fieldIndex);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(fieldData, fieldStart, fieldLength));
        return ByteArraySerializerDeserializer.INSTANCE.deserialize(dis);
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
