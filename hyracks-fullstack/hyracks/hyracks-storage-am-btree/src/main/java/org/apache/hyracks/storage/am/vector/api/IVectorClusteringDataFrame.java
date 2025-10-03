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

package org.apache.hyracks.storage.am.vector.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.vector.frames.VectorClusteringDataFrame;

/**
 * Interface for vector clustering data frames.
 * Data frames contain vector records: <distance_to_centroid, cos(θ), quantized_vector, include_fields, PK>
 */
public interface IVectorClusteringDataFrame extends IVectorClusteringFrame {

    /**
     * Sets the pointer to the next data page.
     * @param nextPage the page ID of the next data page
     */
    void setNextPage(int nextPage);

    /**
     * Gets the pointer to the next data page.
     * @return the page ID of the next data page, or -1 if none
     */
    int getNextPage();

    /**
     * Finds the index where a new vector record should be inserted based on distance ordering.
     * @param tuple the vector record to insert
     * @return the insertion index
     * @throws HyracksDataException if an error occurs
     */
    int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException;

    /**
     * Gets the distance to centroid for a vector record.
     * @param tupleIndex the index of the vector record
     * @return the distance to centroid
     * @throws HyracksDataException if an error occurs
     */
    double getDistanceToCentroid(int tupleIndex) throws HyracksDataException;

    /**
     * Gets the cosine value for a vector record.
     * @param tupleIndex the index of the vector record
     * @return the cosine value
     * @throws HyracksDataException if an error occurs
     */
    double getCosineValue(int tupleIndex) throws HyracksDataException;

    /**
     * Inserts a vector record at the specified index, maintaining distance ordering.
     * @param tuple the vector record to insert
     * @param tupleIndex the index to insert at
     */
    void insert(ITupleReference tuple, int tupleIndex);

    /**
     * Creates a data tuple with the given parameters.
     * 
     * @param vector Vector array
     * @param distance Distance as double
     * @param cosineSim Cosine similarity as double  
     * @param originalTuple Original tuple containing primary key
     * @return ITupleReference representing the data tuple
     * @throws HyracksDataException if tuple creation fails
     */
    ITupleReference createDataTuple(float[] vector, double distance, double cosineSim, ITupleReference originalTuple)
            throws HyracksDataException;

    /**
     * Creates an updated data tuple with included fields while preserving vector embedding, distance, cosine, and primary key.
     * 
     * @param currentVector The current vector embedding (preserved)
     * @param currentDistance The current distance to centroid (preserved)
     * @param currentCosine The current cosine similarity (preserved)
     * @param currentPK The current primary key (preserved)
     * @param updateTuple The tuple containing the included field updates
     * @return Updated data tuple with preserved vector/PK and updated included fields
     * @throws HyracksDataException if tuple creation fails
     */
    ITupleReference createUpdatedDataTupleWithIncludedFields(float[] currentVector, double currentDistance,
            double currentCosine, byte[] currentPK, ITupleReference updateTuple) throws HyracksDataException;

    /**
     * Finds the range of tuples within a distance range.
     * @param minDistance the minimum distance
     * @param maxDistance the maximum distance
     * @return an array containing [startIndex, endIndex]
     * @throws HyracksDataException if an error occurs
     */
    int[] findDistanceRange(double minDistance, double maxDistance) throws HyracksDataException;

    public void split(VectorClusteringDataFrame rightFrame, ITupleReference tuple, int insertIndex)
            throws HyracksDataException;
}
