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

package org.apache.hyracks.storage.am.lsm.vector.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * Base interface for all vector clustering frames.
 */
public interface IVectorClusteringFrame extends ITreeIndexFrame {

    /**
     * Sets the centroid for this frame/page.
     * @param centroid the centroid vector
     */
    void setCentroid(double[] centroid);

    /**
     * Gets the centroid for this frame/page.
     * @return the centroid vector
     */
    double[] getCentroid();

    /**
     * Gets the cluster ID associated with this frame.
     * @return the cluster ID
     */
    int getClusterId();

    /**
     * Sets the cluster ID for this frame.
     * @param clusterId the cluster ID
     */
    void setClusterId(int clusterId);

    /**
     * Creates a tuple reference for this frame type.
     * @return a new tuple reference
     */
    ITreeIndexTupleReference createTupleReference();

    /**
     * Finds the index of a tuple in the frame.
     * @param tuple the tuple to find
     * @param cmp the comparator to use
     * @return the index of the tuple, or a special indicator
     * @throws HyracksDataException if an error occurs
     */
    int findTupleIndex(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException;
}
