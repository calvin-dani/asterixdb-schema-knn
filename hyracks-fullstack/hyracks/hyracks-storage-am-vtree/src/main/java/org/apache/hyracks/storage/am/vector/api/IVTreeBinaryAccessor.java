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

/**
 * Interface for accessing vector fields in a schema-agnostic way.
 * Similar to IBinaryComparator for comparisons and IBinaryTokenizer for text processing,
 * this interface abstracts over different vector serialization formats (e.g., AOrderedList, raw arrays).
 *
 * This allows the Hyracks storage layer to work with vectors without knowledge of
 * AsterixDB-specific type system details.
 */
public interface IVTreeBinaryAccessor {

    /**
     * Reset the accessor to point to a new vector field in serialized form.
     *
     * @param data Byte array containing the serialized vector
     * @param start Start offset of the vector field in the byte array
     * @param length Length of the vector field in bytes
     * @throws HyracksDataException if the data format is invalid
     */
    void reset(byte[] data, int start, int length) throws HyracksDataException;

    /**
     * Extract and return the vector as a double array.
     * This performs the actual deserialization from the format-specific representation.
     *
     * @return The vector as a double array
     * @throws HyracksDataException if deserialization fails
     */
    double[] getVector() throws HyracksDataException;

    /**
     * Get the dimensionality of the vector without full extraction.
     * This can be more efficient than calling getVector().length.
     *
     * @return The number of dimensions in the vector
     * @throws HyracksDataException if the dimension cannot be determined
     */
    int getDimension() throws HyracksDataException;
}
