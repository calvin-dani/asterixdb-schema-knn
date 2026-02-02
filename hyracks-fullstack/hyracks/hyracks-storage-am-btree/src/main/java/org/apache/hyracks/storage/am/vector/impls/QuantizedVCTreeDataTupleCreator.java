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

package org.apache.hyracks.storage.am.vector.impls;

import java.io.DataOutput;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.vector.api.IVCTreeDataTupleCreator;

/**
 * Quantized data tuple creator for VectorClusteringTree.
 *
 * Includes the vector embedding in the data tuple so that search cursors
 * (e.g., LSMVCTreeBlockedCursor) can compute D(q, x) from the stored vector
 * without needing to fetch from the primary index.
 *
 * In the unit test framework, the actual vector embedding is used as the
 * "quantized vector" (no real quantization is performed).
 *
 * Input tuple format: [vector, include_fields..., pk]
 * - Field 0: vector
 * - Fields 1 to numIncludeFields: include fields (optional)
 * - Field (1 + numIncludeFields): primary key (single field)
 *
 * Output data tuple format: <distance, centroidId, vector, pk, include_fields...>
 * - Field 0: distance (raw double, 8 bytes, no type tag)
 * - Field 1: centroidId (raw int, 4 bytes, no type tag)
 * - Field 2: vector (copied from input field 0)
 * - Field 3: primary key
 * - Fields 4+: include fields
 */
public class QuantizedVCTreeDataTupleCreator implements IVCTreeDataTupleCreator {

    private static final int PK_FIELD_INDEX = 3;
    private final int numIncludeFields;

    public QuantizedVCTreeDataTupleCreator(int numIncludeFields) {
        this.numIncludeFields = numIncludeFields;
    }

    @Override
    public ITupleReference createDataTuple(double[] vector, double distance, int centroidId,
            ITupleReference originalTuple) throws HyracksDataException {
        try {
            // Total fields: distance + centroidId + vector + pk + include_fields
            int dataFieldCount = 4 + numIncludeFields;
            ArrayTupleBuilder dataTupleBuilder = new ArrayTupleBuilder(dataFieldCount);
            DataOutput dos = dataTupleBuilder.getDataOutput();

            // Field 0: distance as raw double (8 bytes, no type tag)
            dos.writeDouble(distance);
            dataTupleBuilder.addFieldEndOffset();

            // Field 1: centroidId as raw int (4 bytes, no type tag)
            dos.writeInt(centroidId);
            dataTupleBuilder.addFieldEndOffset();

            // Field 2: vector (copied from originalTuple field 0)
            dataTupleBuilder.addField(originalTuple.getFieldData(0), originalTuple.getFieldStart(0),
                    originalTuple.getFieldLength(0));

            // Field 3: primary key (at position 1 + numIncludeFields in input)
            int pkFieldIndex = 1 + numIncludeFields;
            dataTupleBuilder.addField(originalTuple.getFieldData(pkFieldIndex),
                    originalTuple.getFieldStart(pkFieldIndex), originalTuple.getFieldLength(pkFieldIndex));

            // Fields 4+: include fields (from originalTuple fields 1 to numIncludeFields)
            for (int i = 0; i < numIncludeFields; i++) {
                int srcFieldIndex = 1 + i;
                dataTupleBuilder.addField(originalTuple.getFieldData(srcFieldIndex),
                        originalTuple.getFieldStart(srcFieldIndex), originalTuple.getFieldLength(srcFieldIndex));
            }

            ArrayTupleReference dataTupleRef = new ArrayTupleReference();
            dataTupleRef.reset(dataTupleBuilder.getFieldEndOffsets(), dataTupleBuilder.getByteArray());

            return dataTupleRef;

        } catch (Exception e) {
            throw new HyracksDataException("Failed to create quantized data tuple", e);
        }
    }

    @Override
    public int getPrimaryKeyFieldIndex() {
        return PK_FIELD_INDEX;
    }
}
