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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.common.IComponentStatsAccumulator;

public class ComponentStatsAccumulator implements IComponentStatsAccumulator {

    private final ArrayBackedValueStorage serializedStats;

    private static final int MATTER_COUNT_OFFSET = 0;
    private static final int ANTI_MATTER_COUNT_OFFSET = MATTER_COUNT_OFFSET + Integer.BYTES;

    private int matterCount;
    private int antiMatterCount;

    public ComponentStatsAccumulator() {
        matterCount = 0;
        antiMatterCount = 0;
        serializedStats = new ArrayBackedValueStorage();
    }

    public void reset() {
        matterCount = 0;
        antiMatterCount = 0;
    }

    private void incrementMatterCount() {
        matterCount++;
    }

    private void incrementAntiMatterCount() {
        antiMatterCount++;
    }

    public int getMatterCount() {
        return matterCount;
    }

    @Override
    public void account(ITupleReference tuple) {
        if (tuple instanceof ILSMTreeTupleReference && ((ILSMTreeTupleReference) tuple).isAntimatter()) {
            incrementAntiMatterCount();
        }
        incrementMatterCount();
    }

    public int getAntiMatterCount() {
        return antiMatterCount;
    }

    public IValueReference serialize() throws IOException {
        serializedStats.reset();
        DataOutput output = serializedStats.getDataOutput();
        serialize(output);
        return serializedStats;
    }

    private void serialize(DataOutput output) throws IOException {
        output.writeInt(matterCount);
        output.writeInt(antiMatterCount);
    }

    public static int getMatterCount(ArrayBackedValueStorage storage) {
        return IntegerPointable.getInteger(storage.getByteArray(), storage.getStartOffset() + MATTER_COUNT_OFFSET);
    }

    public static int getAntiMatterCount(ArrayBackedValueStorage storage) {
        return IntegerPointable.getInteger(storage.getByteArray(), storage.getStartOffset() + ANTI_MATTER_COUNT_OFFSET);
    }

    @Override
    public IValueReference serializeComponentStatsMetadata() throws HyracksDataException {
        try {
            //ComponentStats
            IValueReference serializedStats = serialize();
            reset(); // since the component stats are serialized, we can reset the accumulator
            return serializedStats;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
