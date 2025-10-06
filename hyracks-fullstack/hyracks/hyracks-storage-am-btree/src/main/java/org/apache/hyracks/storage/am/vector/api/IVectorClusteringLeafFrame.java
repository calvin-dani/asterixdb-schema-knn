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
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

/**
 * Interface for vector clustering leaf frames.
 * Leaf frames contain cluster entries: <cid, full_precision_centroid, pointer_to_first_metadata_page>
 */
public interface IVectorClusteringLeafFrame extends IVectorClusteringFrame {

    /**
     * Sets the pointer to the next leaf page.
     * @param nextLeafPage the page ID of the next leaf
     */
    void setNextLeaf(int nextLeafPage);

    /**
     * Gets the pointer to the next leaf page.
     * @return the page ID of the next leaf, or -1 if none
     */
    int getNextLeaf();

    /**
     * Finds the index where a new tuple should be inserted.
     * @param tuple the tuple to insert
     * @return the insertion index
     * @throws HyracksDataException if an error occurs
     */
    int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException;

    /**
     * Gets the metadata page pointer for a cluster entry.
     * @param tupleIndex the index of the cluster entry
     * @return the page ID of the first metadata page
     * @throws HyracksDataException if an error occurs
     */
    int getMetadataPagePointer(int tupleIndex) throws HyracksDataException;

    /**
     * Sets the metadata page pointer for a cluster entry.
     * @param tupleIndex the index of the cluster entry
     * @param metadataPageId the page ID of the first metadata page
     * @throws HyracksDataException if an error occurs
     */
    void setMetadataPagePointer(int tupleIndex, int metadataPageId) throws HyracksDataException;

    /**
     * Inserts a cluster entry at the specified index.
     * @param tuple the cluster entry to insert
     * @param tupleIndex the index to insert at
     */
    void insert(ITupleReference tuple, int tupleIndex);

    /**
     * Ensures the frame has enough capacity for the given tuple.
     * @param bufferCache the buffer cache
     * @param tuple the tuple to accommodate
     * @param extraPageBlockHelper helper for extra page allocation
     * @throws HyracksDataException if an error occurs
     */
    void ensureCapacity(IBufferCache bufferCache, ITupleReference tuple, IExtraPageBlockHelper extraPageBlockHelper)
            throws HyracksDataException;

    void setOverflowFlagBit(boolean overflowFlag);
}
