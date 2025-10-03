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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.*;
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
    // Offset for storing centroid dimension count (4 bytes)
    protected static final int CENTROID_DIM_OFFSET = CLUSTER_ID_OFFSET + 4;
    // Offset for storing centroid data (variable length)
    protected static final int CENTROID_DATA_OFFSET = CENTROID_DIM_OFFSET + 4;

    protected MultiComparator cmp;
    protected final ITreeIndexTupleReference frameTuple;
    protected int centroidDimensions;

    public VectorClusteringNSMFrame(ITreeIndexTupleWriter tupleWriter, ISlotManager slotManager,
            int centroidDimensions) {
        super(tupleWriter, slotManager);
        this.frameTuple = tupleWriter.createTupleReference();
        this.centroidDimensions = centroidDimensions;
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        // Initialize cluster ID to -1 (unassigned)
        buf.putInt(CLUSTER_ID_OFFSET, -1);
        // Initialize centroid dimensions
        buf.putInt(CENTROID_DIM_OFFSET, centroidDimensions);
        // Initialize centroid to zeros
        setCentroid(new double[centroidDimensions]);
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
    public double[] getCentroid() {
        int dimensions = buf.getInt(CENTROID_DIM_OFFSET);
        double[] centroid = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            centroid[i] = buf.getDouble(CENTROID_DATA_OFFSET + i * 8);
        }
        return centroid;
    }

    @Override
    public void setCentroid(double[] centroid) {
        buf.putInt(CENTROID_DIM_OFFSET, centroid.length);
        for (int i = 0; i < centroid.length; i++) {
            buf.putDouble(CENTROID_DATA_OFFSET + i * 8, centroid[i]);
        }
    }

    @Override
    public int getPageHeaderSize() {
        return CENTROID_DATA_OFFSET + (centroidDimensions * 8);
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

    /**
     * Computes the Euclidean distance between two vectors.
     */
    protected double computeEuclideanDistance(double[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }
        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Computes the cosine similarity between two vectors.
     */
    protected double computeCosineSimilarity(double[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }

        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            norm1 += vector1[i] * vector1[i];
            norm2 += vector2[i] * vector2[i];
        }

        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("clusterId:         " + getClusterId() + "\n");
        strBuilder.append("centroidDims:      " + buf.getInt(CENTROID_DIM_OFFSET) + "\n");
        return strBuilder.toString();
    }
}
