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
 * Interface for creating data tuples for VectorClusteringTree storage.
 *
 * Transforms input tuples from the operator format into the storage format
 * used in data pages. Different implementations can produce different output
 * formats (e.g., with or without quantized vector embeddings).
 *
 * Follows the InvertedIndexTokenizingTupleIterator pattern: a standalone
 * tuple transformer held by the operation context, decoupled from the frame.
 */
public interface IVCTreeDataTupleCreator {

    /**
     * Create a data tuple for VectorClusteringTree storage.
     *
     * Input tuple format: [vector, include_fields..., pk]
     * Output format is implementation-specific (e.g., <distance, centroidId, pk, include_fields...>).
     *
     * @param vector the vector array extracted from the input tuple
     * @param distance the computed distance from the vector to its assigned centroid
     * @param centroidId the ID of the assigned leaf centroid
     * @param originalTuple the original input tuple containing vector, include fields, and primary key
     * @return ITupleReference representing the data tuple in storage format
     * @throws HyracksDataException if tuple creation fails
     */
    ITupleReference createDataTuple(double[] vector, double distance, int centroidId, ITupleReference originalTuple)
            throws HyracksDataException;

    /**
     * Returns the field index of the primary key in the output data tuple.
     * Callers use this to locate the PK when reading stored tuples.
     *
     * @return the field index of the primary key in the output tuple format
     */
    int getPrimaryKeyFieldIndex();
}
