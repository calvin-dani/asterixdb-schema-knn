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
package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.List;
import java.util.PriorityQueue;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;

/**
 * Iterator for draining results from SpillableTopKBuffer.
 *
 * Two modes:
 * - No-spill fast path: iterates sorted VectorSearchHeapEntry[] from frame buffer (zero-copy)
 * - Merge path: k-way merge of sorted in-memory entries + sorted run files by dqx ascending
 */
public class SpillableTopKDrainIterator {

    // Shared state
    private final boolean mergeMode;
    private int outputCount;
    private final int outputLimit;

    // No-spill fast path state
    private final VectorSearchHeapEntry[] sortedInMemory;
    private final ITuplePointerAccessor bufferAccessor;
    private int inMemoryIndex;

    // Merge path state
    private PriorityQueue<MergeSource> mergeQueue;
    private MergeSource currentSource;

    // Adapter for returning tuples from in-memory buffer
    private final TuplePointerTupleReference inMemoryTupleRef;

    private SpillableTopKDrainIterator(VectorSearchHeapEntry[] sortedInMemory, ITuplePointerAccessor bufferAccessor,
            boolean mergeMode, int outputLimit) {
        this.sortedInMemory = sortedInMemory;
        this.bufferAccessor = bufferAccessor;
        this.mergeMode = mergeMode;
        this.outputLimit = outputLimit;
        this.inMemoryIndex = 0;
        this.outputCount = 0;
        this.inMemoryTupleRef = new TuplePointerTupleReference();
    }

    static SpillableTopKDrainIterator empty() {
        return new SpillableTopKDrainIterator(new VectorSearchHeapEntry[0], null, false, 0);
    }

    static SpillableTopKDrainIterator inMemoryOnly(VectorSearchHeapEntry[] sortedEntries,
            ITuplePointerAccessor accessor) {
        return new SpillableTopKDrainIterator(sortedEntries, accessor, false, sortedEntries.length);
    }

    static SpillableTopKDrainIterator withMerge(VectorSearchHeapEntry[] sortedInMemory,
            ITuplePointerAccessor bufferAccessor, List<GeneratedRunFileReader> spilledRuns,
            RecordDescriptor spillRecordDescriptor, IHyracksTaskContext ctx, int candidateLimit)
            throws HyracksDataException {

        SpillableTopKDrainIterator iter =
                new SpillableTopKDrainIterator(sortedInMemory, bufferAccessor, true, candidateLimit);
        iter.initMerge(spilledRuns, spillRecordDescriptor, ctx);
        return iter;
    }

    private void initMerge(List<GeneratedRunFileReader> spilledRuns, RecordDescriptor spillRecordDescriptor,
            IHyracksTaskContext ctx) throws HyracksDataException {
        mergeQueue = new PriorityQueue<>(spilledRuns.size() + 1, (a, b) -> Double.compare(a.currentDqx, b.currentDqx));

        // Add in-memory source
        if (sortedInMemory.length > 0) {
            InMemoryMergeSource inMemSource = new InMemoryMergeSource(sortedInMemory, bufferAccessor);
            if (inMemSource.advance()) {
                mergeQueue.offer(inMemSource);
            }
        }

        // Add run file sources
        for (GeneratedRunFileReader runReader : spilledRuns) {
            runReader.open();
            RunFileMergeSource runSource = new RunFileMergeSource(runReader, spillRecordDescriptor, ctx);
            if (runSource.advance()) {
                mergeQueue.offer(runSource);
            }
        }
    }

    public boolean hasNext() {
        if (outputCount >= outputLimit) {
            return false;
        }
        if (!mergeMode) {
            return inMemoryIndex < sortedInMemory.length;
        }
        return !mergeQueue.isEmpty() || currentSource != null;
    }

    public void next() throws HyracksDataException {
        if (!mergeMode) {
            VectorSearchHeapEntry entry = sortedInMemory[inMemoryIndex++];
            bufferAccessor.reset(entry.tuplePointer);
            inMemoryTupleRef.reset(bufferAccessor);
        } else {
            // Advance the previously consumed source and re-offer to queue if it has more
            if (currentSource != null) {
                if (currentSource.advance()) {
                    mergeQueue.offer(currentSource);
                }
            }
            // Poll the next best source (smallest dqx)
            currentSource = mergeQueue.poll();
        }
        outputCount++;
    }

    public ITupleReference getTuple() {
        if (!mergeMode) {
            return inMemoryTupleRef;
        }
        return currentSource.getCurrentTuple();
    }

    public void close() throws HyracksDataException {
        if (mergeQueue != null) {
            for (MergeSource source : mergeQueue) {
                source.close();
            }
            mergeQueue.clear();
        }
        if (currentSource != null) {
            currentSource.close();
            currentSource = null;
        }
    }

    // ==================== Merge Sources ====================

    private interface MergeSource {
        double currentDqx = 0;

        double getCurrentDqx();

        ITupleReference getCurrentTuple();

        boolean advance() throws HyracksDataException;

        void close() throws HyracksDataException;
    }

    /**
     * Merge source backed by sorted in-memory VectorSearchHeapEntry array.
     */
    private static class InMemoryMergeSource implements MergeSource {
        private final VectorSearchHeapEntry[] entries;
        private final ITuplePointerAccessor accessor;
        private final TuplePointerTupleReference tupleRef;
        private int index;
        double currentDqx;

        InMemoryMergeSource(VectorSearchHeapEntry[] entries, ITuplePointerAccessor accessor) {
            this.entries = entries;
            this.accessor = accessor;
            this.tupleRef = new TuplePointerTupleReference();
            this.index = 0;
        }

        @Override
        public double getCurrentDqx() {
            return currentDqx;
        }

        @Override
        public ITupleReference getCurrentTuple() {
            return tupleRef;
        }

        @Override
        public boolean advance() {
            if (index >= entries.length) {
                return false;
            }
            VectorSearchHeapEntry entry = entries[index++];
            accessor.reset(entry.tuplePointer);
            tupleRef.reset(accessor);
            currentDqx = entry.dqx;
            return true;
        }

        @Override
        public void close() {
        }
    }

    /**
     * Merge source backed by a sorted run file with dqx-prefixed tuples.
     * Run file tuple format: [8-byte dqx][original tuple fields]
     * This source strips the dqx prefix and exposes the original tuple.
     */
    private static class RunFileMergeSource implements MergeSource {
        private final GeneratedRunFileReader reader;
        private final FrameTupleAccessor frameAccessor;
        private final IFrame readFrame;
        private int tupleIndex;
        private int tupleCount;
        double currentDqx;
        private final RunFileTupleReference tupleRef;

        RunFileMergeSource(GeneratedRunFileReader reader, RecordDescriptor spillRecordDescriptor,
                IHyracksTaskContext ctx) throws HyracksDataException {
            this.reader = reader;
            this.readFrame = new VSizeFrame(ctx);
            this.frameAccessor = new FrameTupleAccessor(spillRecordDescriptor);
            this.tupleIndex = 0;
            this.tupleCount = 0;
            this.tupleRef = new RunFileTupleReference();
        }

        @Override
        public double getCurrentDqx() {
            return currentDqx;
        }

        @Override
        public ITupleReference getCurrentTuple() {
            return tupleRef;
        }

        @Override
        public boolean advance() throws HyracksDataException {
            while (true) {
                if (tupleIndex < tupleCount) {
                    // Read dqx from field 0 of the spill tuple
                    byte[] data = frameAccessor.getBuffer().array();
                    int fieldStart = frameAccessor.getAbsoluteFieldStartOffset(tupleIndex, 0);
                    currentDqx = DoublePointable.getDouble(data, fieldStart);
                    // Set up tuple reference that strips field 0 (dqx) and exposes fields 1..N
                    tupleRef.reset(frameAccessor, tupleIndex);
                    tupleIndex++;
                    return true;
                }
                // Need to read next frame from run file
                if (!reader.nextFrame(readFrame)) {
                    return false;
                }
                frameAccessor.reset(readFrame.getBuffer());
                tupleCount = frameAccessor.getTupleCount();
                tupleIndex = 0;
            }
        }

        @Override
        public void close() throws HyracksDataException {
            reader.close();
        }
    }

    // ==================== Tuple Reference Adapters ====================

    /**
     * Adapts ITuplePointerAccessor to ITupleReference. Points into frame buffer (zero-copy).
     */
    static class TuplePointerTupleReference implements ITupleReference {
        private ITuplePointerAccessor accessor;

        void reset(ITuplePointerAccessor accessor) {
            this.accessor = accessor;
        }

        @Override
        public int getFieldCount() {
            return accessor.getFieldCount();
        }

        @Override
        public byte[] getFieldData(int fIdx) {
            return accessor.getBuffer().array();
        }

        @Override
        public int getFieldStart(int fIdx) {
            return accessor.getAbsFieldStartOffset(fIdx);
        }

        @Override
        public int getFieldLength(int fIdx) {
            return accessor.getFieldLength(fIdx);
        }
    }

    /**
     * Adapts a dqx-prefixed run file tuple to ITupleReference, stripping field 0 (dqx).
     * Run file format: field[0]=dqx, field[1..N]=original tuple fields.
     * This reference exposes fields 1..N as fields 0..N-1.
     */
    private static class RunFileTupleReference implements ITupleReference {
        private FrameTupleAccessor accessor;
        private int tupleIndex;

        void reset(FrameTupleAccessor accessor, int tupleIndex) {
            this.accessor = accessor;
            this.tupleIndex = tupleIndex;
        }

        @Override
        public int getFieldCount() {
            return accessor.getFieldCount() - 1;
        }

        @Override
        public byte[] getFieldData(int fIdx) {
            return accessor.getBuffer().array();
        }

        @Override
        public int getFieldStart(int fIdx) {
            return accessor.getAbsoluteFieldStartOffset(tupleIndex, fIdx + 1);
        }

        @Override
        public int getFieldLength(int fIdx) {
            return accessor.getFieldLength(tupleIndex, fIdx + 1);
        }
    }
}
