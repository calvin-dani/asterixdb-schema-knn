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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.common.api.ILSMIndexBatchPointCursor;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.ComponentStatsAccumulator;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;

public final class LSMIndexSampleCursor extends EnforcedIndexCursor implements ILSMIndexCursor {
    private final ILSMIndexOperationContext opCtx;
    // Should not include the memory BTree
    // In order to do that, modify the AbstractLSMIndex.getOperationalComponents() method
    // Assume for now all the components are disk components.
    private int numDiskBTress;
    // Only create accessors for the disk components.
    private ITreeIndexCursor[] btreeCursors;
    private DiskBTree.DiskBTreeAccessor[] btreeAccessors;
    private BloomFilter[] bloomFilters;
    private ILSMHarness harness;
    private List<ILSMComponent> operationalComponents;
    private final List<ILSMComponent> searchComponents;

    // searching the newer components for the liveness check.
    private final LSMBTreeBatchPointSearchCursor searchCursor;
    private LSMBTreeCursorInitialState searchCursorInitialState;

    // Sample specific fields
    // Number of LIVE tuples to be present in the sample.
    private final int sampleCardinality;
    private final long sampleSeed;
    // Number of LIVE tuples sampled so far.
    private int sampledCount;
    private int currentDiskComponentIndex;
    private int[] proportionality;

    public LSMIndexSampleCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, new LSMBTreeBatchPointSearchCursor(opCtx));
    }

    public LSMIndexSampleCursor(ILSMIndexOperationContext opCtx, LSMBTreeBatchPointSearchCursor searchCursor) {
        this.sampleCardinality = opCtx.getIndexAccessParameter(HyracksConstants.SAMPLE_CARDINALITY, Integer.class);
        this.sampleSeed = opCtx.getIndexAccessParameter(HyracksConstants.SAMPLE_SEED, Long.class);
        assert sampleCardinality > 0;
        this.opCtx = opCtx;
        this.searchCursor = searchCursor;
        searchComponents = new ArrayList<>();
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        operationalComponents = lsmInitialState.getOperationalComponents();
        numDiskBTress = operationalComponents.size();
        // If the accessors and cursors are already created, clean up and reuse them.
        cleanUpAccessorsIfPresent();
        // create the accessors and cursors, if not already created
        if (btreeAccessors == null) {
            btreeCursors = new ITreeIndexCursor[numDiskBTress];
            btreeAccessors = new DiskBTree.DiskBTreeAccessor[numDiskBTress];
            bloomFilters = new BloomFilter[numDiskBTress];
        }

        // Set the current disk component to the first one.
        currentDiskComponentIndex = 0;
        // Initialize the sample proportionality array.
        proportionality = new int[numDiskBTress];
        // Calculate the number of total matters in the components.
        computeDiskComponentSampleProportionality();

        // open the pointSearch cursor for liveness check
        // fabricate an initial state for the search cursor
        searchCursorInitialState = new LSMBTreeCursorInitialState(lsmInitialState.getLeafFrameFactory(),
                lsmInitialState.getOriginalKeyComparator(), lsmInitialState.getBloomFilterComparator(),
                lsmInitialState.getLSMHarness(), lsmInitialState.getSearchPredicate(),
                lsmInitialState.getSearchOperationCallback(), searchComponents); // for the most recent component, there is no need to check liveness
        searchCursor.doOpen(searchCursorInitialState, searchPred);

        // Create accessors and cursors for the disk components.
        for (int i = 0; i < numDiskBTress; i++) {
            ILSMComponent component = operationalComponents.get(i);
            DiskBTree bTree = (DiskBTree) component.getIndex();
            ILSMComponent.LSMComponentType type = component.getType();
            // Remove this assert, once the change has been made to the AbstractLSMIndex.getOperationalComponents()
            assert type != ILSMComponent.LSMComponentType.MEMORY;
            bloomFilters[i] = ((LSMBTreeWithBloomFilterDiskComponent) component).getBloomFilter();

            if (btreeAccessors[i] == null) {
                btreeAccessors[i] = createAccessor(bTree, opCtx, i);
                btreeCursors[i] = createCursor(btreeAccessors[i], proportionality[i], sampleSeed, searchCursor);
            } else {
                // re-use the existing accessors and cursors
                btreeAccessors[i].reset(bTree, NoOpIndexAccessParameters.INSTANCE);
                btreeCursors[i].close();
            }
        }
        // Open the 0th component for the sample scan
        // This will open the initial accessor
        // Need to do for other cursor when their turn comes
        if (numDiskBTress > 0) {
            btreeAccessors[currentDiskComponentIndex].diskSampleScan(btreeCursors[currentDiskComponentIndex]);
        }
    }

    @Override
    public void print() {
        for (int i = 0; i < numDiskBTress; i++) {
            btreeCursors[i].print();
        }
    }

    private void computeDiskComponentSampleProportionality() throws HyracksDataException {
        int[] matterCount = new int[numDiskBTress];
        int totalMatterCount = 0;
        int i = 0;
        ArrayBackedValueStorage reference = new ArrayBackedValueStorage();
        for (ILSMComponent component : operationalComponents) {
            DiskComponentMetadata metadata = (DiskComponentMetadata) component.getMetadata();
            metadata.get(DiskComponentMetadata.STATS_KEY, reference);
            matterCount[i] = ComponentStatsAccumulator.getMatterCount(reference);
            totalMatterCount += matterCount[i];
            i++;
        }

        for (i = 0; i < numDiskBTress; i++) {
            // Calculate the proportionality of the sample size for each disk component.
            proportionality[i] = (int) Math.ceil((double) matterCount[i] / totalMatterCount * sampleCardinality);
        }
    }

    private DiskBTree.DiskBTreeAccessor createAccessor(DiskBTree bTree, ILSMIndexOperationContext opCtx, int index)
            throws HyracksDataException {
        return (DiskBTree.DiskBTreeAccessor) bTree.createAccessor(NoOpIndexAccessParameters.INSTANCE, opCtx, index);
    }

    private ITreeIndexCursor createCursor(DiskBTree.DiskBTreeAccessor bTreeAccessor, int componentSampleCardinality,
            long sampleSeed, ILSMIndexBatchPointCursor searchCursor) {
        return bTreeAccessor.createSampleCursor(componentSampleCardinality, sampleSeed, searchCursor);
    }

    private void cleanUpAccessorsIfPresent() throws HyracksDataException {
        if (btreeAccessors != null && btreeAccessors.length != numDiskBTress) {
            Throwable failure = CleanupUtils.destroy(null, btreeCursors);
            btreeCursors = null;
            failure = CleanupUtils.destroy(failure, btreeAccessors);
            btreeAccessors = null;
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }
    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        // Need to find the next tuple in the sample.
        if ((numDiskBTress == 0) || sampledCount == sampleCardinality) {
            return false; // Sample is complete.
        }
        // Else, ask the current disk component cursor for the next sampled tuple.
        // If the current disk component cursor has no next tuple,
        // move to the next disk component cursor.
        boolean hasNext = btreeCursors[currentDiskComponentIndex].hasNext();
        if (!hasNext) {
            // Move to the next disk component cursor.
            currentDiskComponentIndex++;
            if (currentDiskComponentIndex >= numDiskBTress) {
                return false; // No more disk components to sample from.
            }
            // todo: close the previous batch search cursor, and open a new one, with the components from currentDiskComponentIndex - 1 to 0
            searchCursor.doClose();
            searchComponents.add(operationalComponents.get(currentDiskComponentIndex - 1));
            searchCursor.doOpen(searchCursorInitialState, null); // search predicate is not used in the point cursor
            // open the next disk component
            btreeAccessors[currentDiskComponentIndex].diskSampleScan(btreeCursors[currentDiskComponentIndex]);
            // Check if the new disk component has a next tuple.
            return doHasNext();
        } else {
            // If we have a next tuple, increment the sampled count.
            sampledCount++;
            return true;
        }
    }

    @Override
    protected void doNext() throws HyracksDataException {
        btreeCursors[currentDiskComponentIndex].next();
    }

    @Override
    protected void doDestroy() throws HyracksDataException {
        if (btreeCursors != null) {
            Throwable failure = CleanupUtils.destroy(null, btreeCursors);
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }
        harness = null;
    }

    @Override
    protected void doClose() throws HyracksDataException {
        try {
            closeCursors();
        } finally {
            if (harness != null) {
                harness.endSearch(opCtx);
            }
        }
    }

    private void closeCursors() throws HyracksDataException {
        if (btreeCursors != null) {
            Throwable failure = null;
            for (int i = 0; i < numDiskBTress; i++) {
                if (btreeCursors[i] != null) {
                    failure = ResourceReleaseUtils.close(btreeCursors[i], failure);
                }
            }
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }
    }

    @Override
    protected ITupleReference doGetTuple() {
        return btreeCursors[currentDiskComponentIndex].getTuple();
    }

    @Override
    public ITupleReference getFilterMinTuple() {
        throw new UnsupportedOperationException("getFilterMinTuple is not supported in LSMIndexSampleCursor");
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        throw new UnsupportedOperationException("getFilterMaxTuple is not supported in LSMIndexSampleCursor");
    }

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return true;
    }

}
