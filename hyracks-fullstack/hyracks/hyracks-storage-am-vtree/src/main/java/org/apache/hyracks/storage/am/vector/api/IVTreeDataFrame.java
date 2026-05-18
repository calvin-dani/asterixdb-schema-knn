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
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrame;

/**
 * Interface for VTree data frames. Data frames contain vector records sorted by
 * {@code distance_to_centroid} ascending; the exact tuple shape depends on whether the index is
 * quantized:
 * <ul>
 *   <li>Non-quantized: {@code <distance_to_centroid, centroid_id, PK, include_fields>}</li>
 *   <li>Quantized:     {@code <distance_to_centroid, centroid_id, quantized_distance,
 *       quantized_embedding, PK, include_fields>} (the default in this build, since
 *       quantization is enforced at index creation; pkStartField=4 vs 2)</li>
 * </ul>
 */
public interface IVTreeDataFrame extends IVTreeFrame {

    void setNextPage(int nextPage);

    int getNextPage();

    double getDistanceToCentroid(int tupleIndex) throws HyracksDataException;

    void insert(ITupleReference tuple, int tupleIndex);

    void split(VTreeDataFrame rightFrame, ITupleReference tuple, int insertIndex) throws HyracksDataException;

    int findInsertPosition(double distance) throws HyracksDataException;
}
