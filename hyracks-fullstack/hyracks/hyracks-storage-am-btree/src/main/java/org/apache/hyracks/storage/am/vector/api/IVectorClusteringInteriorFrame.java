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

/**
 * Interface for vector clustering interior/root frames.
 * Interior frames contain cluster entries: <cid, full_precision_centroid, pointer_to_clustered_page>
 */
public interface IVectorClusteringInteriorFrame extends IVectorClusteringFrame {

    /**
     * Gets the child page pointer for a given tuple.
     * @param tupleIndex the index of the tuple
     * @return the page ID of the child page
     * @throws HyracksDataException if an error occurs
     */
    int getChildPageId(int tupleIndex) throws HyracksDataException;

    /**
     * Sets the child page pointer for a given tuple.
     * @param tupleIndex the index of the tuple
     * @param childPageId the page ID of the child page
     * @throws HyracksDataException if an error occurs
     */
    void setChildPageId(int tupleIndex, int childPageId) throws HyracksDataException;

    /**
     * Finds the child page that should contain a given search key.
     * @param searchKey the key to search for
     * @return the index of the child that should contain the key
     * @throws HyracksDataException if an error occurs
     */
    int findChildIndex(ITupleReference searchKey) throws HyracksDataException;

    /**
     * Inserts a new entry into the interior frame.
     * @param tuple the entry to insert
     * @param tupleIndex the index to insert at
     */
    void insert(ITupleReference tuple, int tupleIndex);

    /**
     * Finds the index where a new tuple should be inserted.
     * @param tuple the tuple to insert
     * @return the insertion index
     * @throws HyracksDataException if an error occurs
     */
    int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException;

    void setNextPage(int nextPageId);

    int getNextPage();
}
