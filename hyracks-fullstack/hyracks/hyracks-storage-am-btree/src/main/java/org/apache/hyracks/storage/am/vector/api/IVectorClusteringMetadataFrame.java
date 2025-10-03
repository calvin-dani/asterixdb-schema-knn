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

/**
 * Interface for vector clustering metadata frames.
 * Metadata frames contain entries: <max_distance, pointer_to_data_page>
 */
public interface IVectorClusteringMetadataFrame extends IVectorClusteringFrame {

    /**
     * Sets the pointer to the next metadata page.
     * @param nextPage the page ID of the next metadata page
     */
    void setNextPage(int nextPage);

    /**
     * Gets the pointer to the next metadata page.
     * @return the page ID of the next metadata page, or -1 if none
     */
    int getNextPage();

    /**
     * Finds the data page that contains records with the given distance.
     * @param distance the distance to search for
     * @return the page ID of the data page
     * @throws HyracksDataException if an error occurs
     */
    int findDataPageForDistance(double distance) throws HyracksDataException;

    /**
     * Gets the maximum distance for a metadata entry.
     * @param tupleIndex the index of the metadata entry
     * @return the maximum distance
     * @throws HyracksDataException if an error occurs
     */
    float getMaxDistance(int tupleIndex) throws HyracksDataException;

    /**
     * Gets the data page pointer for a metadata entry.
     * @param tupleIndex the index of the metadata entry
     * @return the page ID of the data page
     * @throws HyracksDataException if an error occurs
     */
    int getDataPagePointer(int tupleIndex) throws HyracksDataException;

    /**
     * Inserts a metadata entry at the specified index.
     * @param tuple the metadata entry to insert
     * @param tupleIndex the index to insert at
     */
    void insert(ITupleReference tuple, int tupleIndex);

    /**
     * Creates a metadata tuple with the given parameters.
     * 
     * @param maxDistance Maximum distance for this cluster
     * @param dataPageId Pointer to the data page
     * @return ITupleReference representing the metadata tuple
     * @throws HyracksDataException if tuple creation fails
     */
    ITupleReference createMetadataTuple(float maxDistance, int dataPageId) throws HyracksDataException;

    /**
     * Finds the index where a new tuple should be inserted.
     * @param tuple the tuple to insert
     * @return the insertion index
     * @throws HyracksDataException if an error occurs
     */
    int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException;
}
