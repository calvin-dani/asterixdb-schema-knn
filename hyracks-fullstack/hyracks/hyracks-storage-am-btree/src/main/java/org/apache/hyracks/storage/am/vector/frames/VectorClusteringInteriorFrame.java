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

package org.apache.hyracks.storage.am.lsm.vector.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringInteriorFrame;

/**
 * Vector clustering interior/root frame implementation.
 * Contains entries: <cid, full_precision_centroid, pointer_to_clustered_page>
 */
public class VectorClusteringInteriorFrame extends VectorClusteringNSMFrame implements IVectorClusteringInteriorFrame {

    protected static final int NEXT_PAGE_OFFSET =  CENTROID_DATA_OFFSET + 4;
    private final ITreeIndexTupleReference cmpFrameTuple;

    public VectorClusteringInteriorFrame(ITreeIndexTupleWriter tupleWriter, int centroidDimensions) {
        super(tupleWriter, new OrderedSlotManager(), centroidDimensions);
        this.cmpFrameTuple = tupleWriter.createTupleReference();
    }

    /**
     * Set the next page pointer for overflow chaining
     */
    public void setNextPage(int nextPageId) {
        buf.putInt(NEXT_PAGE_OFFSET, nextPageId);
    }

    /**
     * Get the next page pointer
     */
    public int getNextPage() {
        return buf.getInt(NEXT_PAGE_OFFSET);
    }


    @Override
    public int getChildPageId(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Child page pointer is the last field in the tuple
        int childPtrFieldIndex = frameTuple.getFieldCount() - 1;
        return IntegerPointable.getInteger(frameTuple.getFieldData(childPtrFieldIndex),
                frameTuple.getFieldStart(childPtrFieldIndex));
    }

    @Override
    public void setChildPageId(int tupleIndex, int childPageId) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Child page pointer is the last field in the tuple
        int childPtrFieldIndex = frameTuple.getFieldCount() - 1;
        IntegerPointable.setInteger(frameTuple.getFieldData(childPtrFieldIndex),
                frameTuple.getFieldStart(childPtrFieldIndex), childPageId);
    }

    @Override
    public int findChildIndex(ITupleReference searchKey) throws HyracksDataException {
        // For interior frames, we need to find the best matching cluster centroid
        // This is a simplified implementation - in practice, you might want more sophisticated matching
        int tupleCount = getTupleCount();
        if (tupleCount == 0) {
            return 0;
        }

        double bestDistance = Double.MAX_VALUE;
        int bestIndex = 0;

        // Extract search vector from the search key (assuming it's in the first field)
        double[] searchVector = extractVectorFromTuple(searchKey);

        for (int i = 0; i < tupleCount; i++) {
            frameTuple.resetByTupleIndex(this, i);
            double[] centroid = extractCentroidFromTuple(frameTuple);
            double distance = computeEuclideanDistance(searchVector, centroid);

            if (distance < bestDistance) {
                bestDistance = distance;
                bestIndex = i;
            }
        }

        return bestIndex;
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        // For interior frames, insertions typically happen during splits
        // Insert at the end for simplicity
        return getTupleCount();
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

    /**
     * Extracts vector from a tuple (assumes vector is in the centroid field).
     */
    private double[] extractVectorFromTuple(ITupleReference tuple) {
        // This is a simplified implementation
        // In practice, you'd need to properly parse the tuple structure
        // For now, assume the centroid is stored as a serialized double array
        // You would implement proper deserialization based on your tuple format
        return new double[centroidDimensions]; // Placeholder
    }

    /**
     * Extracts centroid from a cluster entry tuple.
     */
    private double[] extractCentroidFromTuple(ITreeIndexTupleReference tuple) {
        // This is a simplified implementation
        // In practice, you'd need to properly parse the tuple structure
        // The centroid should be the second field in interior node entries
        return new double[centroidDimensions]; // Placeholder
    }

    public int getFreeSpaceOff() {
        return buf.getInt(Constants.FREE_SPACE_OFFSET);
    }
}
