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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringDataFrame;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;

/**
 * Vector clustering data frame implementation.
 * Contains vector records: <distance_to_centroid, cos(θ), quantized_vector, include_fields, PK>
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
        // Distance to centroid is the first field in data records - stored as float
        return DoublePointable.getDouble(frameTuple.getFieldData(0), frameTuple.getFieldStart(0));
    }

    @Override
    public double getCosineValue(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Cosine value is the second field in data records - stored as float
        return FloatPointable.getFloat(frameTuple.getFieldData(1), frameTuple.getFieldStart(1));
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
     * Searches for vectors within a specific distance range.
     * @param targetDistance the target distance to search around
     * @param tolerance the tolerance for distance matching
     * @return array of tuple indices that match the criteria
     * @throws HyracksDataException if an error occurs
     */
    public int[] searchByDistance(double targetDistance, double tolerance) throws HyracksDataException {
        double minDistance = targetDistance - tolerance;
        double maxDistance = targetDistance + tolerance;

        int[] range = findDistanceRange(minDistance, maxDistance);
        if (range[0] == -1) {
            return new int[0]; // Empty result
        }

        int resultSize = range[1] - range[0] + 1;
        int[] results = new int[resultSize];
        for (int i = 0; i < resultSize; i++) {
            results[i] = range[0] + i;
        }

        return results;
    }

    /**
     * Gets the closest vectors to a target distance.
     * @param targetDistance the target distance
     * @param k the number of closest vectors to return
     * @return array of tuple indices of the k closest vectors
     * @throws HyracksDataException if an error occurs
     */
    public int[] getClosestVectors(double targetDistance, int k) throws HyracksDataException {
        int tupleCount = getTupleCount();
        if (tupleCount == 0 || k <= 0) {
            return new int[0];
        }

        // Simple implementation: find the k closest by scanning all
        // In practice, you might want a more efficient implementation
        double[] distances = new double[tupleCount];
        for (int i = 0; i < tupleCount; i++) {
            distances[i] = Math.abs(getDistanceToCentroid(i) - targetDistance);
        }

        // Find k smallest distances (simple selection)
        int actualK = Math.min(k, tupleCount);
        int[] results = new int[actualK];
        boolean[] used = new boolean[tupleCount];

        for (int j = 0; j < actualK; j++) {
            int minIndex = -1;
            double minDiff = Double.MAX_VALUE;

            for (int i = 0; i < tupleCount; i++) {
                if (!used[i] && distances[i] < minDiff) {
                    minDiff = distances[i];
                    minIndex = i;
                }
            }

            results[j] = minIndex;
            used[minIndex] = true;
        }

        return results;
    }

    /**
     * Find the insertion position for a tuple based on distance to maintain sorted order.
     */
    public int findInsertPosition(float distance) throws HyracksDataException {
        int tupleCount = getTupleCount();

        // Binary search for insertion position
        int left = 0;
        int right = tupleCount;

        while (left < right) {
            int mid = (left + right) / 2;
            double midDistance = getDistanceToCentroid(mid);

            if (midDistance < distance) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return left;
    }

    /**
     * Get the maximum distance in this data frame.
     */
    public double getMaxDistance() throws HyracksDataException {
        int tupleCount = getTupleCount();
        if (tupleCount == 0) {
            return 0.0;
        }

        // Since tuples are sorted by distance, max is the last tuple
        return getDistanceToCentroid(tupleCount - 1);
    }

    /**
     * Get the minimum distance in this data frame.
     */
    public double getMinDistance() throws HyracksDataException {
        int tupleCount = getTupleCount();
        if (tupleCount == 0) {
            return Double.MAX_VALUE;
        }

        // Since tuples are sorted by distance, min is the first tuple
        return getDistanceToCentroid(0);
    }

    /**
     * Range search within this data frame.
     */
    public List<Integer> rangeSearch(double minDistance, double maxDistance) throws HyracksDataException {
        List<Integer> results = new ArrayList<>();
        int tupleCount = getTupleCount();

        // Find start position using binary search
        int startIndex = findFirstTupleInRange(minDistance);

        // Collect all tuples within range
        for (int i = startIndex; i < tupleCount; i++) {
            double distance = getDistanceToCentroid(i);
            if (distance > maxDistance) {
                break; // Since sorted, no more matches
            }
            if (distance >= minDistance) {
                results.add(i);
            }
        }

        return results;
    }

    /**
     * Find k nearest neighbors within this data frame.
     */
    public List<Integer> kNearestNeighbors(int k) throws HyracksDataException {
        List<Integer> results = new ArrayList<>();
        int tupleCount = getTupleCount();
        int limit = Math.min(k, tupleCount);

        // Since tuples are sorted by distance, just take first k
        for (int i = 0; i < limit; i++) {
            results.add(i);
        }

        return results;
    }

    /**
     * Find first tuple with distance >= minDistance.
     */
    private int findFirstTupleInRange(double minDistance) throws HyracksDataException {
        int tupleCount = getTupleCount();
        int left = 0;
        int right = tupleCount;

        while (left < right) {
            int mid = (left + right) / 2;
            double midDistance = getDistanceToCentroid(mid);

            if (midDistance < minDistance) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return left;
    }

    /**
     * Split this data frame using BTree-style approach.
     * Follows the exact pattern from BTreeNSMLeafFrame.split().
     */
    public void split(VectorClusteringDataFrame rightFrame, ITupleReference tuple, int insertIndex)
            throws HyracksDataException {

        int tupleCount = getTupleCount();

        // Determine split point and target frame
        int tuplesToLeft = tupleCount / 2;
        int tuplesToRight = tupleCount - tuplesToLeft;

        // Determine which frame gets the new tuple
        VectorClusteringDataFrame targetFrame;
        if (insertIndex < tuplesToLeft) {
            targetFrame = this; // Insert into left (original) frame
        } else {
            targetFrame = rightFrame; // Insert into right (new) frame
        }

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

        // STEP 5: Insert the new tuple into appropriate frame
        int targetTupleIndex;
        if (insertIndex < tuplesToLeft) {
            // Insert into left frame
            targetTupleIndex = insertIndex;
            this.insert(tuple, targetTupleIndex);
        } else {
            // Insert into right frame
            targetTupleIndex = insertIndex - tuplesToLeft;
            rightFrame.insert(tuple, targetTupleIndex);
        }
    }

    /**
     * Split data frame when it becomes full, maintaining distance-based ordering.
     */
    public void split(IVectorClusteringDataFrame rightFrame, ITupleReference tuple) throws HyracksDataException {
        int tupleCount = getTupleCount();
        int splitIndex = tupleCount / 2;

        // Move second half of tuples to right frame
        for (int i = splitIndex; i < tupleCount; i++) {
            frameTuple.resetByTupleIndex(this, i);
            SimpleTupleReference tupleCopy = new SimpleTupleReference();
            copyTuple(frameTuple, tupleCopy);
            rightFrame.insert(tupleCopy, rightFrame.getTupleCount());
        }

        // Remove moved tuples from left frame
        for (int i = tupleCount - 1; i >= splitIndex; i--) {
            frameTuple.resetByTupleIndex(this, i);
            delete(frameTuple, i);
        }

        // Insert new tuple into appropriate frame
        float newDistance = extractDistanceFromTuple(tuple);
        if (getTupleCount() == 0 || newDistance <= getDistanceToCentroid(getTupleCount() - 1)) {
            int insertIndex = findInsertPosition(newDistance);
            insert(tuple, insertIndex);
        } else {
            // Use the findInsertPosition method instead of interface method
            int insertIndex = ((VectorClusteringDataFrame) rightFrame).findInsertPosition(newDistance);
            rightFrame.insert(tuple, insertIndex);
        }

        // Update page links - can't access getPageId from interface, so skip this for now
        int originalNextPage = getNextPage();
        setNextPage(-1); // Set to invalid page ID, will be updated by caller
        rightFrame.setNextPage(originalNextPage);
    }

    /**
     * Extract distance from tuple (first field).
     */
    private float extractDistanceFromTuple(ITupleReference tuple) {
        byte[] data = tuple.getFieldData(0);
        int offset = tuple.getFieldStart(0);
        return FloatPointable.getFloat(data, offset);
    }

    /**
     * Get vector embedding from tuple (third field).
     */
    public double[] getVectorEmbedding(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);

        // Vector embedding is the third field (index 2)
        byte[] data = frameTuple.getFieldData(2);
        int offset = frameTuple.getFieldStart(2);
        int length = frameTuple.getFieldLength(2);

        return VectorUtils.bytesToDoubleArray(Arrays.copyOfRange(data, offset, offset + length));
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
     * Update distance and cosine similarity for a tuple.
     */
    public void updateDistanceAndCosine(int tupleIndex, float newDistance, float newCosine)
            throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);

        // Extract existing data
        double[] vector = getVectorEmbedding(tupleIndex);
        byte[] primaryKey = getPrimaryKey(tupleIndex);

        // Create updated tuple
        SimpleTupleReference updatedTuple = createDataTuple(vector, newDistance, newCosine, primaryKey);

        // Replace the tuple
        frameTuple.resetByTupleIndex(this, tupleIndex);
        delete(frameTuple, tupleIndex);
        int insertIndex = findInsertPosition(newDistance);
        insert(updatedTuple, insertIndex);
    }

    /**
     * Create a data tuple with given parameters.
     */
    private SimpleTupleReference createDataTuple(double[] vector, double distance, double cosine, byte[] primaryKey)
            throws HyracksDataException {
        try {
            // Serialize fields using DataOutput
            ByteArrayOutputStream distanceStream = new ByteArrayOutputStream();
            DataOutputStream distanceOut = new DataOutputStream(distanceStream);
            DoubleSerializerDeserializer.write(distance, distanceOut);
            byte[] distanceBytes = distanceStream.toByteArray();

            ByteArrayOutputStream cosineStream = new ByteArrayOutputStream();
            DataOutputStream cosineOut = new DataOutputStream(cosineStream);
            DoubleSerializerDeserializer.write(cosine, cosineOut);
            byte[] cosineBytes = cosineStream.toByteArray();

            ByteArrayOutputStream vectorStream = new ByteArrayOutputStream();
            DataOutputStream vectorOut = new DataOutputStream(vectorStream);
            DoubleArraySerializerDeserializer.write(vector, vectorOut);
            byte[] vectorBytes = vectorStream.toByteArray();

            // Create raw tuple data manually using SimpleTupleWriter format
            int totalSize =
                    4 + 4 * 4 + distanceBytes.length + cosineBytes.length + vectorBytes.length + primaryKey.length;
            byte[] tupleData = new byte[totalSize];

            // Write null flags (4 bytes for 4 fields)
            int offset = 0;
            for (int i = 0; i < 4; i++) {
                tupleData[offset++] = 0; // no nulls
            }

            // Write field slot offsets (4 bytes each)
            int fieldOffset = 4 + 4 * 4; // after null flags and slot array
            tupleData[offset++] = (byte) (fieldOffset & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 8) & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 16) & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 24) & 0xFF);

            fieldOffset += distanceBytes.length;
            tupleData[offset++] = (byte) (fieldOffset & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 8) & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 16) & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 24) & 0xFF);

            fieldOffset += cosineBytes.length;
            tupleData[offset++] = (byte) (fieldOffset & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 8) & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 16) & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 24) & 0xFF);

            fieldOffset += vectorBytes.length;
            tupleData[offset++] = (byte) (fieldOffset & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 8) & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 16) & 0xFF);
            tupleData[offset++] = (byte) ((fieldOffset >> 24) & 0xFF);

            // Write field data
            System.arraycopy(distanceBytes, 0, tupleData, offset, distanceBytes.length);
            offset += distanceBytes.length;
            System.arraycopy(cosineBytes, 0, tupleData, offset, cosineBytes.length);
            offset += cosineBytes.length;
            System.arraycopy(vectorBytes, 0, tupleData, offset, vectorBytes.length);
            offset += vectorBytes.length;
            System.arraycopy(primaryKey, 0, tupleData, offset, primaryKey.length);

            SimpleTupleReference dataTuple = new SimpleTupleReference();
            dataTuple.resetByTupleOffset(tupleData, 0);
            dataTuple.setFieldCount(4);

            return dataTuple;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Create a data tuple with given parameters for VectorClusteringTree compatibility.
     * 
     * @param vector Vector array
     * @param distance Distance as double
     * @param cosineSim Cosine similarity as double  
     * @param originalTuple Original tuple containing primary key
     * @return ITupleReference representing the data tuple
     * @throws HyracksDataException if tuple creation fails
     */
    public ITupleReference createDataTuple(double[] vector, double distance, double cosineSim,
            ITupleReference originalTuple) throws HyracksDataException {
        // Extract primary key from original tuple (assume last field)
        try {
            // Step 1: Create tuple builder for the new computed fields (distance, cosine)
            ArrayTupleBuilder computedFieldsBuilder = new ArrayTupleBuilder(2);
            DataOutput dos = computedFieldsBuilder.getDataOutput();

            // Add distance field
            DoubleSerializerDeserializer.INSTANCE.serialize(distance, dos);
            computedFieldsBuilder.addFieldEndOffset();

            // Add cosine similarity field
            DoubleSerializerDeserializer.INSTANCE.serialize(cosineSim, dos);
            computedFieldsBuilder.addFieldEndOffset();

            // Step 2: Create tuple builder for original fields (vector, primary key)
            ArrayTupleBuilder originalFieldsBuilder = new ArrayTupleBuilder(originalTuple.getFieldCount());

            // Copy all fields from original tuple
            for (int i = 0; i < originalTuple.getFieldCount(); i++) {
                originalFieldsBuilder.addField(originalTuple.getFieldData(i), originalTuple.getFieldStart(i),
                        originalTuple.getFieldLength(i));
            }

            // Step 3: Create final data tuple builder and use addFields to combine
            ArrayTupleBuilder dataTupleBuilder = new ArrayTupleBuilder(4); // 2 computed + 2 original

            // CRITICAL: Use TupleUtils.addFields to properly combine tuple builders
            TupleUtils.addFields(computedFieldsBuilder, dataTupleBuilder); // Add distance, cosine
            TupleUtils.addFields(originalFieldsBuilder, dataTupleBuilder); // Add vector, primary_key

            // Step 4: Create the final tuple reference
            ArrayTupleReference datatupleRef = new ArrayTupleReference();
            datatupleRef.reset(dataTupleBuilder.getFieldEndOffsets(), dataTupleBuilder.getByteArray());

            return datatupleRef;

        } catch (Exception e) {
            throw new HyracksDataException("Failed to create data tuple using TupleUtils.addFields", e);
        }
    }

    /**
     * Create updated data tuple preserving vector embedding, distance, cosine, and primary key.
     * Only included fields (if any) from the update tuple are applied.
     * This method enforces the rule that vector embeddings and primary keys are immutable in updates.
     *
     * Update tuple format: <vector, included_field1, included_field2, ..., primary_key>
     * Data tuple format:   <distance, cosine, vector, primary_key, included_field1, included_field2, ...>
     *
     * @param currentVector The current vector embedding (preserved)
     * @param currentDistance The current distance to centroid (preserved)
     * @param currentCosine The current cosine similarity (preserved)
     * @param currentPK The current primary key (preserved)
     * @param updateTuple The tuple containing the included field updates
     * @return Updated data tuple with preserved vector/PK and updated included fields
     * @throws HyracksDataException if tuple creation fails
     */
    public ITupleReference createUpdatedDataTupleWithIncludedFields(double[] currentVector, double currentDistance,
            double currentCosine, byte[] currentPK, ITupleReference updateTuple) throws HyracksDataException {

        try {
            System.out.println("DEBUG: Creating updated data tuple");
            System.out.println(
                    "DEBUG: Current vector length: " + (currentVector != null ? currentVector.length : "null"));
            System.out.println("DEBUG: Current distance: " + currentDistance);
            System.out.println("DEBUG: Current cosine: " + currentCosine);
            System.out.println("DEBUG: Current PK length: " + (currentPK != null ? currentPK.length : "null"));
            System.out.println("DEBUG: Update tuple field count: " + updateTuple.getFieldCount());

            // Determine the number of included fields in the update tuple
            // Update tuple format: <vector, included_field1, included_field2, ..., primary_key>
            // So included fields are from index 1 to (fieldCount - 2)
            int numIncludedFields = Math.max(0, updateTuple.getFieldCount() - 2);

            // Data tuple format: <distance, cosine, vector, primary_key, included_field1, included_field2, ...>
            int totalFields = 4 + numIncludedFields; // distance, cosine, vector, PK + included fields

            // Use existing createDataTuple method and then append included fields
            SimpleTupleReference baseTuple =
                    createDataTuple(currentVector, currentDistance, currentCosine, currentPK);

            if (numIncludedFields == 0) {
                // No included fields to add, return base tuple
                return baseTuple;
            }

            // Create tuple with included fields using manual byte manipulation (similar to original method)
            ByteArrayOutputStream tupleStream = new ByteArrayOutputStream();
            DataOutputStream tupleOut = new DataOutputStream(tupleStream);

            // Copy base tuple data (distance, cosine, vector, PK)
            for (int i = 0; i < 4; i++) {
                byte[] fieldData = baseTuple.getFieldData(i);
                tupleOut.write(fieldData, baseTuple.getFieldStart(i), baseTuple.getFieldLength(i));
            }

            // Add included fields from update tuple
            for (int i = 1; i < updateTuple.getFieldCount() - 1; i++) {
                byte[] fieldData = updateTuple.getFieldData(i);
                int fieldLength = updateTuple.getFieldLength(i);
                if (fieldData != null && fieldLength > 0) {
                    tupleOut.write(fieldData, updateTuple.getFieldStart(i), fieldLength);
                    System.out.println("DEBUG: Added included field " + (i - 1) + " with length: " + fieldLength);
                } else {
                    // Handle empty/null included field
                    System.out.println("DEBUG: Added empty included field " + (i - 1));
                }
            }

            // Create SimpleTupleReference with the combined data
            SimpleTupleReference result = new SimpleTupleReference();
            byte[] tupleData = tupleStream.toByteArray();
            result.resetByTupleOffset(tupleData, 0);
            result.setFieldCount(totalFields);

            System.out.println("DEBUG: Created updated tuple with " + result.getFieldCount() + " fields");
            return result;

        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Copy tuple data to a new SimpleTupleReference.
     */
    private void copyTuple(ITupleReference source, SimpleTupleReference target) throws HyracksDataException {
        try {
            // Calculate total size needed
            int totalSize = 4 + source.getFieldCount() * 4; // null flags + field offsets
            for (int i = 0; i < source.getFieldCount(); i++) {
                totalSize += source.getFieldLength(i);
            }

            byte[] tupleData = new byte[totalSize];
            int offset = 0;

            // Write null flags
            for (int i = 0; i < 4; i++) {
                tupleData[offset++] = 0; // no nulls
            }

            // Write field slot offsets
            int fieldOffset = 4 + source.getFieldCount() * 4; // after null flags and slot array
            for (int i = 0; i < source.getFieldCount(); i++) {
                tupleData[offset++] = (byte) (fieldOffset & 0xFF);
                tupleData[offset++] = (byte) ((fieldOffset >> 8) & 0xFF);
                tupleData[offset++] = (byte) ((fieldOffset >> 16) & 0xFF);
                tupleData[offset++] = (byte) ((fieldOffset >> 24) & 0xFF);
                fieldOffset += source.getFieldLength(i);
            }

            // Copy field data
            for (int i = 0; i < source.getFieldCount(); i++) {
                System.arraycopy(source.getFieldData(i), source.getFieldStart(i), tupleData, offset,
                        source.getFieldLength(i));
                offset += source.getFieldLength(i);
            }

            target.setFieldCount(source.getFieldCount());
            target.resetByTupleOffset(tupleData, 0);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
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
