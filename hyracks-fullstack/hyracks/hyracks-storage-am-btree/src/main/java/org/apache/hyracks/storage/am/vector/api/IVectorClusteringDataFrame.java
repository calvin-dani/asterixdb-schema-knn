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
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringOpContext;

/**
 * Interface for vector clustering data frames.
 * Data frames contain vector records: <distance_to_centroid, centroid_id, PK, include_fields>
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
     * Inserts a vector record at the specified index, maintaining distance ordering.
     * @param tuple the vector record to insert
     * @param tupleIndex the index to insert at
     */
    void insert(ITupleReference tuple, int tupleIndex);

    /**
     * Creates a data tuple with the given parameters.
     *
     * @param vector        Vector array
     * @param distance      Distance as double
     * @param centroidId     Centroid Id as integer
     * @param originalTuple Original tuple containing primary key
     * @param ctx
     * @return ITupleReference representing the data tuple
     * @throws HyracksDataException if tuple creation fails
     */
    ITupleReference createDataTuple(double[] vector, double distance, int centroidId, ITupleReference originalTuple, VectorClusteringOpContext ctx)
            throws HyracksDataException;

    /**
     * Finds the range of tuples within a distance range.
     * @param minDistance the minimum distance
     * @param maxDistance the maximum distance
     * @return an array containing [startIndex, endIndex]
     * @throws HyracksDataException if an error occurs
     */
    int[] findDistanceRange(double minDistance, double maxDistance) throws HyracksDataException;

    void split(VectorClusteringDataFrame rightFrame, ITupleReference tuple, int insertIndex)
            throws HyracksDataException;

    int findInsertPosition(double distance) throws HyracksDataException;
}
