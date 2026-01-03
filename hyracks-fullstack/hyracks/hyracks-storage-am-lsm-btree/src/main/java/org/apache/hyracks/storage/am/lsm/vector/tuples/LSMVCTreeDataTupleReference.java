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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;

/**
 * Tuple reference for LSM Vector Clustering Tree data frames.
 * Handles tuples with format:
 * - Matter tuple: <distance:ADOUBLE, centroid_id:AINTEGER, primary_key:ALONG>
 * - Anti-matter tuple: <primary_key:ALONG> only (deletion marker)
 */
public class LSMVCTreeDataTupleReference extends TypeAwareTupleReference implements ILSMTreeTupleReference {

    // Indicates whether the last call to setFieldCount() was initiated by
    // the outside or whether it was called internally to set up an
    // antimatter tuple.
    private boolean resetFieldCount = false;

    // Primary key is the last field (field 2) in matter tuples, only field in antimatter tuples
    private final int numKeyFields = 1;

    // Total number of fields in a matter tuple: distance, centroid_id, primary_key
    private final int totalMatterFields;

    public LSMVCTreeDataTupleReference(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits) {
        super(typeTraits, nullTypeTraits);
        this.totalMatterFields = typeTraits.length;
    }

    @Override
    public void setFieldCount(int fieldCount) {
        super.setFieldCount(fieldCount);
        // Don't change the fieldCount in reset calls.
        resetFieldCount = false;
    }

    @Override
    public void setFieldCount(int fieldStartIndex, int fieldCount) {
        super.setFieldCount(fieldStartIndex, fieldCount);
        // Don't change the fieldCount in reset calls.
        resetFieldCount = false;
    }

    @Override
    public void resetByTupleOffset(byte[] buf, int tupleStartOff) {
        this.buf = buf;
        this.tupleStartOff = tupleStartOff;

        // NOTE: Both matter and antimatter tuples have the same structure (all fields)
        // The only difference is the antimatter bit (bit 7) in the null flags byte
        // No field count adjustment needed - both have totalMatterFields (3 fields)

        super.resetByTupleOffset(buf, tupleStartOff);
    }

    @Override
    public void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        resetByTupleOffset(frame.getBuffer().array(), frame.getTupleOffset(tupleIndex));
    }

    @Override
    protected int getNullFlagsBytes() {
        // number of fields + matter/antimatter bit
        int numBits = fieldCount + 1;
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    public boolean isAntimatter() {
        // Check antimatter bit (bit 7 in null flags byte)
        return BitOperationUtils.getBit(buf, tupleStartOff, ANTIMATTER_BIT_OFFSET);
    }

    public int getTupleStart() {
        return tupleStartOff;
    }

    @Override
    protected int getAdjustedFieldIdx(int fieldIdx) {
        // 1 bit for antimatter (vector index doesn't use update-aware)
        return fieldIdx + 1;
    }
}