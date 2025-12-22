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

package org.apache.hyracks.storage.am.lsm.vector.tuples;

import static org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference.ANTIMATTER_BIT_OFFSET;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;

/**
 * Tuple writer for LSM Vector Clustering Tree data frames.
 *
 * Handles data tuples with format: <distance, centroid_id, primary_key>
 *
 * Anti-matter tuples (deletion markers) have the same fields as matter tuples,
 * but with bit 7 (ANTIMATTER_BIT) set in the null flags byte.
 *
 * This follows the LSM pattern where anti-matter tuples logically delete records
 * without physically removing them until merge operations.
 */
public class LSMVCTreeDataTupleWriter extends TypeAwareTupleWriter implements ILSMTreeTupleWriter {

    private boolean isAntimatter;

    // All 3 fields are "key fields" - no separate value fields in vector index
    private final int numKeyFields = 3;

    public LSMVCTreeDataTupleWriter(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        super(typeTraits, nullTypeTraits, nullIntrospector);
        this.isAntimatter = false;
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        // Anti-matter tuple has same size as matter tuple (all fields stored)
        return super.bytesRequired(tuple);
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return super.bytesRequired(tuple);
    }

    @Override
    public LSMVCTreeDataTupleReference createTupleReference() {
        return new LSMVCTreeDataTupleReference(typeTraits, nullTypeTraits);
    }

    @Override
    protected int getNullFlagsBytes(int numFields) {
        // numFields + antimatter bit (no update-aware for vector index)
        int numBits = numFields + 1;
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    protected int getNullFlagsBytes(ITupleReference tuple) {
        // # of fields + antimatter bit
        int numBits = tuple.getFieldCount() + 1;
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        // Write all fields (same for matter and antimatter)
        int bytesWritten = super.writeTuple(tuple, targetBuf, targetOff);

//        System.err.println("[LSMVCTreeDataTupleWriter.writeTuple] isAntimatter=" + isAntimatter +
//                          ", Thread=" + Thread.currentThread().getId());

        if (isAntimatter) {
            // Only difference: set antimatter bit (bit 7) in null flags byte
            BitOperationUtils.setBit(targetBuf, targetOff, ANTIMATTER_BIT_OFFSET);
//            System.err.println("[LSMVCTreeDataTupleWriter.writeTuple] ANTIMATTER BIT SET!");
        }

        return bytesWritten;
    }

    @Override
    public void setAntimatter(boolean isDelete) {
        this.isAntimatter = isDelete;
    }

    @Override
    protected int getAdjustedFieldIdx(int fieldIdx) {
        // 1 bit for antimatter (vector index doesn't use update-aware)
        return fieldIdx + 1;
    }
}