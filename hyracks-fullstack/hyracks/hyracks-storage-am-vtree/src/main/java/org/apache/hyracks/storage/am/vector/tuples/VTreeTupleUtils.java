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

package org.apache.hyracks.storage.am.vector.tuples;

import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.tuples.SimpleTupleReference;

/**
 * Utility class for VTree tuple operations.
 * Provides common functionality for tuple copying, field extraction, and manipulation.
 *
 * Input tuple format: [vector, include_fields..., pk_field]
 * - Field 0: vector
 * - Fields 1 to (1 + numPrimaryKeyFields - 1): include fields
 * - Last field: primary key
 */
public class VTreeTupleUtils {
    /**
     * Copy tuple data to a new SimpleTupleReference using TupleUtils.
     *
     * @param source The source tuple to copy
     * @param target The target SimpleTupleReference to copy to
     * @throws HyracksDataException if copying fails
     */
    public static void copyTuple(ITupleReference source, SimpleTupleReference target) throws HyracksDataException {
        ITupleReference copiedTuple = TupleUtils.copyTuple(source);
        target.setFieldCount(copiedTuple.getFieldCount());
        target.resetByTupleOffset(copiedTuple.getFieldData(0), 0);
    }

    /**
     * Extract primary key from input tuple.
     * Input tuple format: [vector, pk_field, include_fields...]
     * Primary key field is at index 1.
     *
     * TODO: Currently only supports single primary key field. To support composite primary keys,
     * this method should accept numPrimaryKeyFields parameter and concatenate all PK fields.
     * The infrastructure (numPrimaryKeyFields passed through factory chain to LSMVTree) is already
     * in place for this enhancement.
     *
     * @param tuple The tuple to extract primary key from
     * @return Byte array containing the primary key, or null if extraction fails
     */
    public static byte[] extractPrimaryKeyFromTuple(ITupleReference tuple) {
        if (tuple == null || tuple.getFieldCount() < 2) {
            return null;
        }

        int pkFieldIndex = tuple.getFieldCount() - 1;

        byte[] data = tuple.getFieldData(pkFieldIndex);
        if (data == null) {
            return null;
        }

        int offset = tuple.getFieldStart(pkFieldIndex);
        int length = tuple.getFieldLength(pkFieldIndex);

        if (length <= 0) {
            return null;
        }

        return Arrays.copyOfRange(data, offset, offset + length);
    }
}
