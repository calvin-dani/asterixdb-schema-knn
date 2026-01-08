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

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringOpContext;

/**
 * Vector clustering data frame implementation.
 * Contains vector records: <distance_to_centroid, centroid_id, PK, included_fields>
 * Records are sorted by distance_to_centroid in ascending order.
 */
public class VectorClusteringDataFrame extends VectorClusteringNSMFrame implements IVectorClusteringDataFrame {

    // Offset for next page pointer (4 bytes) - comes after centroid data
    private int getNextPageOffset() {
        return CENTROID_ID_OFFSET + 4;
    }

    public VectorClusteringDataFrame(ITreeIndexTupleWriter tupleWriter) {
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
    public int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        // Find insertion point to maintain sorted order by distance_to_centroid
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.INCLUSIVE,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
    }

    @Override
    public double getDistanceToCentroid(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Distance to centroid is the first field in data records - stored as raw double (no type tag)
        int distanceOff = frameTuple.getFieldStart(0);
        double distance = buf.getDouble(distanceOff);
        return distance;
    }


    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        // Use the parent class's insert method which correctly manages slots and space
        super.insert(tuple, tupleIndex);
    }

    public void insertSorted(ITupleReference tuple) {
        insert(tuple, getTupleCount());
    }

    @Override
    public int[] findDistanceRange(double minDistance, double maxDistance) throws HyracksDataException {
        int tupleCount = getTupleCount();
        int startIndex = -1;
        int endIndex = -1;

        // Find start index (first tuple with distance >= minDistance)
        for (int i = 0; i < tupleCount; i++) {
            double distance = getDistanceToCentroid(i);
            if (distance >= minDistance) {
                startIndex = i;
                break;
            }
        }

        if (startIndex == -1) {
            return new int[] { -1, -1 }; // No tuples in range
        }

        // Find end index (last tuple with distance <= maxDistance)
        for (int i = tupleCount - 1; i >= startIndex; i--) {
            double distance = getDistanceToCentroid(i);
            if (distance <= maxDistance) {
                endIndex = i;
                break;
            }
        }

        if (endIndex == -1) {
            return new int[] { -1, -1 }; // No tuples in range
        }

        return new int[] { startIndex, endIndex };
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException {
        int bytesRequired = tupleWriter.bytesRequired(tuple);
        // Check if we have enough contiguous space (without compaction)
        if (bytesRequired + slotManager.getSlotSize() <= buf.capacity() - buf.getInt(Constants.FREE_SPACE_OFFSET)
                - (buf.getInt(Constants.TUPLE_COUNT_OFFSET) * slotManager.getSlotSize())) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }
        // Check if we have enough space after compaction
        if (bytesRequired + slotManager.getSlotSize() <= buf.getInt(TOTAL_FREE_SPACE_OFFSET)) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        }
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }


    /**
     * Find the insertion position for a tuple based on distance to maintain sorted order.
     * Uses RIGHT boundary search: inserts AFTER all existing tuples with the same distance.
     * This preserves temporal ordering (FIFO) for tuples with equal distances.
     */
    public int findInsertPosition(double distance) throws HyracksDataException {
        int tupleCount = getTupleCount();

        // Binary search for RIGHT boundary (first tuple with distance > target)
        int left = 0;
        int right = tupleCount;

        while (left < right) {
            int mid = (left + right) / 2;
            double midDistance = getDistanceToCentroid(mid);

            if (midDistance <= distance) {
                // Include equal distances: move past them
                left = mid + 1;
            } else {
                // midDistance > distance
                right = mid;
            }
        }

        return left;
    }

    /**
     * Find tuple matching both distance and primary key using RIGHT BOUND search.
     * Returns the rightmost (most recently inserted) tuple if multiple matches exist.
     * This is critical for finding matter tuples after antimatter during deletion.
     *
     * Uses binary comparison for primary key matching - no type assumption.
     *
     * @param distance Target distance to centroid
     * @param primaryKey Primary key bytes to match (binary format)
     * @return Tuple index if found, -1 if not found
     */
    public int findTupleByDistanceAndPrimaryKey(double distance, byte[] primaryKey)
            throws HyracksDataException {

        // Step 1: Use RIGHT BOUND search to find upper boundary
        int upperBound = findInsertPosition(distance);

        // Step 2: Search BACKWARD from upperBound-1 to find matching PK
        // This ensures we find the RIGHTMOST (last inserted) tuple with this distance+PK
        for (int i = upperBound - 1; i >= 0; i--) {
            double dist = getDistanceToCentroid(i);

            // Stop when we reach a different distance zone
            if (dist < distance) {
                break;
            }

            // Check primary key match using binary comparison (no type assumption)
            byte[] pk = getPrimaryKey(i);
            if (Arrays.equals(pk, primaryKey)) {
                return i; // Found the rightmost matching tuple
            }
        }

        return -1; // Not found
    }


    /**
     * Split this data frame using BTree-style approach.
     * Follows the exact pattern from BTreeNSMLeafFrame.split().
     */
    public void split(VectorClusteringDataFrame rightFrame, ITupleReference tuple, int insertIndex)
            throws HyracksDataException {
        int tupleCount = getTupleCount();

        // Determine split point
        int tuplesToLeft = tupleCount / 2;
        int tuplesToRight = tupleCount - tuplesToLeft;

        // STEP 1: Copy entire page buffer (BTree approach)
        ByteBuffer rightBuffer = rightFrame.getBuffer();
        System.arraycopy(buf.array(), 0, rightBuffer.array(), 0, buf.capacity());

        // STEP 2: Adjust slot tables for right page
        // Copy rightmost slots to the left on right page
        int src = rightFrame.getSlotManager().getSlotEndOff();
        int dest =
                rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft * rightFrame.getSlotManager().getSlotSize();
        int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
        System.arraycopy(rightBuffer.array(), src, rightBuffer.array(), dest, length);

        // STEP 3: Update tuple counts
        rightBuffer.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToRight);
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToLeft);

        // STEP 4: Compact both pages
        rightFrame.compact();
        this.compact();

        // STEP 5: Determine target frame by comparing new tuple with split point
        // Extract distance from the new tuple
        double newTupleDistance = extractDistanceFromTuple(tuple);

        // Get distance of the last tuple in left frame (the split point)
        VectorClusteringDataFrame targetFrame;
        if (tuplesToLeft > 0) {
            double splitPointDistance = getDistanceToCentroid(tuplesToLeft - 1);

            if (newTupleDistance <= splitPointDistance) {
                targetFrame = this; // Insert into left frame
            } else {
                targetFrame = rightFrame; // Insert into right frame
            }
        } else {
            // Edge case: left frame is empty
            targetFrame = rightFrame;
        }

        // STEP 6: Recalculate insertion position in target frame
        // This ensures correct positioning with RIGHT boundary semantics
        int targetTupleIndex = targetFrame.findInsertPosition(newTupleDistance);
        targetFrame.insert(tuple, targetTupleIndex);
    }

    /**
     * Extract distance from tuple (first field).
     */
    private double extractDistanceFromTuple(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(0);
        int offset = tuple.getFieldStart(0);
        return DoublePointable.getDouble(data, offset);
    }

    /**
     * Get primary key from tuple (last field).
     */
    public byte[] getPrimaryKey(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);

        // Primary key is the last field
        int pkFieldIndex = frameTuple.getFieldCount() - 1;
        byte[] data = frameTuple.getFieldData(pkFieldIndex);
        int offset = frameTuple.getFieldStart(pkFieldIndex);
        int length = frameTuple.getFieldLength(pkFieldIndex);

        return Arrays.copyOfRange(data, offset, offset + length);
    }

    /**
     * Create a data tuple for VectorClusteringTree. For DELETE operations, sets the antimatter bit.
     *
     * @param vector Vector array
     * @param distance Distance as double
     * @param centroidId Leaf cluster centoid Id
     * @param originalTuple Original tuple containing primary key
     * @param ctx Operation context to check if this is a DELETE operation
     * @return ITupleReference representing the data tuple (with antimatter bit if DELETE)
     * @throws HyracksDataException if tuple creation fails
     */
    public ITupleReference createDataTuple(double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VectorClusteringOpContext ctx)
            throws HyracksDataException {
        // FORMAT: <distance: raw double, centroidId: raw int, primaryKey>
        try {
            ArrayTupleBuilder dataTupleBuilder = new ArrayTupleBuilder(3);
            DataOutput dos = dataTupleBuilder.getDataOutput();

            // Field 0: distance as raw double (8 bytes, no type tag)
            dos.writeDouble(distance);
            dataTupleBuilder.addFieldEndOffset();

            // Field 1: centroidId as raw int (4 bytes, no type tag)
            dos.writeInt(centroidId);
            dataTupleBuilder.addFieldEndOffset();

            // Field 2: primaryKey - copy directly from originalTuple field 1
            dataTupleBuilder.addField(originalTuple.getFieldData(1), originalTuple.getFieldStart(1),
                    originalTuple.getFieldLength(1));

            // Return tuple reference directly - let the tuple writer handle null flags byte
            // The tuple writer will add the null flags byte and set the antimatter bit if needed
            ArrayTupleReference datatupleRef = new ArrayTupleReference();
            datatupleRef.reset(dataTupleBuilder.getFieldEndOffsets(), dataTupleBuilder.getByteArray());

            return datatupleRef;

        } catch (Exception e) {
            throw new HyracksDataException("Failed to create data tuple", e);
        }
    }


    public int getFreeSpaceOff() {
        return buf.getInt(Constants.FREE_SPACE_OFFSET);
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextPage:          " + getNextPage() + "\n");
        return strBuilder.toString();
    }
}
