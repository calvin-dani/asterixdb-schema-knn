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
import org.apache.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringFrame;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

/**
 * Base class for vector clustering frames.
 */
public abstract class VectorClusteringNSMFrame extends TreeIndexNSMFrame implements IVectorClusteringFrame {

    // Offset for storing the cluster ID (4 bytes)
    protected static final int CLUSTER_ID_OFFSET = TreeIndexNSMFrame.RESERVED_HEADER_SIZE;
    // Offset for storing centroid data (variable length)
    protected static final int CENTROID_ID_OFFSET = CLUSTER_ID_OFFSET + 4;

    protected MultiComparator cmp;
    protected final ITreeIndexTupleReference frameTuple;

    public VectorClusteringNSMFrame(ITreeIndexTupleWriter tupleWriter, ISlotManager slotManager) {
        super(tupleWriter, slotManager);
        this.frameTuple = tupleWriter.createTupleReference();
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        // Initialize cluster ID to -1 (unassigned)
        buf.putInt(CLUSTER_ID_OFFSET, -1);
    }

    @Override
    public int getClusterId() {
        return buf.getInt(CLUSTER_ID_OFFSET);
    }

    @Override
    public void setClusterId(int clusterId) {
        buf.putInt(CLUSTER_ID_OFFSET, clusterId);
    }

    @Override
    public int getCentroidID() {
        return buf.getInt(CENTROID_ID_OFFSET);
    }

    @Override
    public void setCentroidID(int centroidID) {
        buf.putInt(CENTROID_ID_OFFSET, centroidID);
    }

    @Override
    public int getPageHeaderSize() {
        return CENTROID_ID_OFFSET + 4; // Base header + cluster ID + centroid ID
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    @Override
    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, null, null);
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
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException {
        // Default split implementation - can be overridden by subclasses
        throw new HyracksDataException("Split operation not implemented for " + this.getClass().getSimpleName());
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("clusterId:         " + getClusterId() + "\n");
        strBuilder.append("centroidId:      " + buf.getInt(CENTROID_ID_OFFSET) + "\n");
        return strBuilder.toString();
    }
}
