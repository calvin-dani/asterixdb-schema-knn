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
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;

/**
 * VTree metadata frame implementation. Contains metadata entries: <max_distance, pointer_to_data_page>
 * Entries are sorted by max_distance in ascending order.
 */
public class VTreeMetadataFrame extends VTreeNSMFrame implements IVTreeMetadataFrame {

    // Offset for next page pointer (4 bytes) - comes after centroid data
    private int getNextPageOffset() {
        return CENTROID_ID_OFFSET + 4;
    }

    public VTreeMetadataFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(getNextPageOffset(), -1); // Initialize next page pointer to -1
    }

    @Override
    public int getPageHeaderSize() {
        return getNextPageOffset() + 4; // Base header + next page pointer
    }

    @Override
    public void setNextPage(int nextPage) {
        buf.putInt(getNextPageOffset(), nextPage);
    }

    @Override
    public int getNextPage() {
        return buf.getInt(getNextPageOffset());
    }

    @Override
    public int findDataPageForDistance(double distance) throws HyracksDataException {
        int tupleCount = getTupleCount();

        // Binary search for the appropriate metadata entry
        for (int i = 0; i < tupleCount; i++) {
            frameTuple.resetByTupleIndex(this, i);
            double maxDistance = getMaxDistance(i);

            if (distance <= maxDistance) {
                return getDataPagePointer(i);
            }
        }

        // If distance is greater than all max distances, return the last data page
        if (tupleCount > 0) {
            return getDataPagePointer(tupleCount - 1);
        }

        return -1; // No data pages
    }

    @Override
    public double getMaxDistance(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Max distance is the first field in metadata entries
        return DoublePointable.getDouble(frameTuple.getFieldData(0), frameTuple.getFieldStart(0));
    }

    @Override
    public int getDataPagePointer(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Data page pointer is the second field in metadata entries
        return IntegerPointable.getInteger(frameTuple.getFieldData(1), frameTuple.getFieldStart(1));
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        // Find insertion point to maintain sorted order by max_distance
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.INCLUSIVE,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
    }

    /**
     * Updates the data page pointer for an existing metadata entry.
     */
    public void updateDataPagePointer(int tupleIndex, int newDataPageId) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Data page pointer is the second field
        IntegerPointable.setInteger(frameTuple.getFieldData(1), frameTuple.getFieldStart(1), newDataPageId);
    }

    /**
     * Updates the max distance for an existing metadata entry.
     */
    public void updateMaxDistance(int tupleIndex, double newMaxDistance) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Max distance is the first field
        DoublePointable.setDouble(frameTuple.getFieldData(0), frameTuple.getFieldStart(0), newMaxDistance);
    }

    /**
     * Find the appropriate insertion position for a new metadata entry. Maintains sorted order by max distance.
     */
    public int findInsertPosition(double maxDistance) throws HyracksDataException {
        int tupleCount = getTupleCount();

        // Binary search for insertion position
        int left = 0;
        int right = tupleCount;

        while (left < right) {
            int mid = (left + right) / 2;
            double midMaxDistance = getMaxDistance(mid);

            if (midMaxDistance < maxDistance) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return left;
    }

    /**
     * Split metadata frame when it becomes full.
     * Uses a copy-and-reinitialize approach to avoid page fragmentation from in-place deletes.
     */
    public void split(IVTreeMetadataFrame rightFrame, ITupleReference tuple) throws HyracksDataException {
        int tupleCount = getTupleCount();
        int splitIndex = tupleCount / 2;

        // Copy all tuples out using TupleUtils.copyTuple (returns ArrayTupleReference
        // which works with tupleWriter.writeTuple via the ITupleReference interface)
        ITupleReference[] leftTuples = new ITupleReference[splitIndex];
        for (int i = 0; i < splitIndex; i++) {
            frameTuple.resetByTupleIndex(this, i);
            leftTuples[i] = TupleUtils.copyTuple(frameTuple);
        }
        ITupleReference[] rightTuples = new ITupleReference[tupleCount - splitIndex];
        for (int i = splitIndex; i < tupleCount; i++) {
            frameTuple.resetByTupleIndex(this, i);
            rightTuples[i - splitIndex] = TupleUtils.copyTuple(frameTuple);
        }

        // Reinitialize both frames to clean state
        initBuffer((byte) 0);
        rightFrame.initBuffer((byte) 0);

        // Re-insert left half tuples
        for (int i = 0; i < leftTuples.length; i++) {
            insert(leftTuples[i], i);
        }

        // Re-insert right half tuples
        for (int i = 0; i < rightTuples.length; i++) {
            rightFrame.insert(rightTuples[i], i);
        }

        // Insert new tuple into appropriate frame
        double newMaxDistance = extractMaxDistanceFromTuple(tuple);
        if (getTupleCount() == 0 || newMaxDistance <= getMaxDistance(getTupleCount() - 1)) {
            int insertIndex = findInsertPosition(newMaxDistance);
            insert(tuple, insertIndex);
        } else {
            int insertIndex = ((VTreeMetadataFrame) rightFrame).findInsertPosition(newMaxDistance);
            rightFrame.insert(tuple, insertIndex);
        }
    }

    /**
     * Extract max distance from tuple (first field).
     */
    private double extractMaxDistanceFromTuple(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(0);
        int offset = tuple.getFieldStart(0);
        return DoublePointable.getDouble(data, offset);
    }

    /**
     * Create metadata tuple for metadata frame.
     *
     * @param maxDistance
     *         Maximum distance for this cluster
     * @param dataPageId
     *         Pointer to the data page
     * @return ITupleReference representing the metadata tuple
     * @throws HyracksDataException
     *         if tuple creation fails
     */
    public ITupleReference createMetadataTuple(double maxDistance, int dataPageId) throws HyracksDataException {
        // Use TupleUtils for consistent tuple creation
        return TupleUtils.createTuple(
                new org.apache.hyracks.api.dataflow.value.ISerializerDeserializer[] {
                        org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer.INSTANCE,
                        org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer.INSTANCE },
                maxDistance, dataPageId);
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextPage:          " + getNextPage() + "\n");
        return strBuilder.toString();
    }
}
