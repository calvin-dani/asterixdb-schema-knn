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
 * Interface for VTree metadata/directory frames.
 * Metadata frames contain entries: <max_distance, pointer_to_data_page>
 */
public interface IVTreeMetadataFrame extends IVTreeFrame {

    void setNextPage(int nextPage);

    int getNextPage();

    double getMaxDistance(int tupleIndex) throws HyracksDataException;

    int getDataPagePointer(int tupleIndex) throws HyracksDataException;

    void insert(ITupleReference tuple, int tupleIndex);

    ITupleReference createMetadataTuple(double maxDistance, int dataPageId) throws HyracksDataException;
}
