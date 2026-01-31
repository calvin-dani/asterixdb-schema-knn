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
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.common.api.ILSMIndexBatchPointCursor;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaEstimator;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaSampler;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LSMIndexSampleCursor extends EnforcedIndexCursor implements ILSMIndexCursor {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ILSMIndexOperationContext opCtx;
    // count of disk btree being sampled
    private int numDiskBTrees;
    private ITreeIndexCursor[] btreeCursors;
    private DiskBTree.DiskBTreeAccessor[] btreeAccessors;
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
    private int maxSampleLeafAttempts;
    // Number of LIVE tuples sampled so far.
    private int sampledCount;
    private int currentDiskComponentIndex;
    private long[] proportionality;

    private long estimatedCardinality;

    public LSMIndexSampleCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, new LSMBTreeBatchPointSearchCursor(opCtx));
    }

    public LSMIndexSampleCursor(ILSMIndexOperationContext opCtx, LSMBTreeBatchPointSearchCursor searchCursor) {
        this.sampleCardinality = (int) opCtx.getParameters().get(HyracksConstants.SAMPLE_CARDINALITY);
        this.sampleSeed = (long) opCtx.getParameters().get(HyracksConstants.SAMPLE_SEED);
        this.opCtx = opCtx;
        this.searchCursor = searchCursor;
        searchComponents = new ArrayList<>();
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMBTreeCursorInitialState lsmInitialState = (LSMBTreeCursorInitialState) initialState;
        harness = lsmInitialState.getLSMHarness();
        operationalComponents = lsmInitialState.getOperationalComponents();
        numDiskBTrees = operationalComponents.size();

        // Read maxSampleLeafAttempts from the index
        maxSampleLeafAttempts = ((AbstractLSMIndex) opCtx.getIndex()).getMaxSampleLeafAttempts();

        // Reset state from any previous scan
        sampledCount = 0;
        searchComponents.clear();
        currentDiskComponentIndex = 0;

        // Always recreate accessors/cursors to pick up the new proportionality values
        cleanUpAccessorsIfPresent();
        btreeCursors = new ITreeIndexCursor[numDiskBTrees];
        btreeAccessors = new DiskBTree.DiskBTreeAccessor[numDiskBTrees];
        // Initialize the sample proportionality array.
        proportionality = new long[numDiskBTrees];
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
        for (int i = 0; i < numDiskBTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            DiskBTree bTree = (DiskBTree) component.getIndex();
            ILSMComponent.LSMComponentType type = component.getType();
            assert type != ILSMComponent.LSMComponentType.MEMORY;

            btreeAccessors[i] = createAccessor(bTree, opCtx, i);
            btreeCursors[i] = createCursor(btreeAccessors[i], proportionality[i], sampleSeed, searchCursor);
        }
        // Open the 0th component for the sample scan
        // This will open the initial accessor
        // Need to do for other cursor when their turn comes
        if (numDiskBTrees > 0) {
            btreeAccessors[currentDiskComponentIndex].diskSampleScan(btreeCursors[currentDiskComponentIndex]);
        }
    }

    public long getEstimatedCardinality() {
        return estimatedCardinality;
    }

    private DiskBTree.DiskBTreeAccessor createAccessor(DiskBTree bTree, ILSMIndexOperationContext opCtx, int index)
            throws HyracksDataException {
        return (DiskBTree.DiskBTreeAccessor) bTree.createAccessor(NoOpIndexAccessParameters.INSTANCE, opCtx, index);
    }

    private ITreeIndexCursor createCursor(DiskBTree.DiskBTreeAccessor bTreeAccessor, long componentSampleCardinality,
            long sampleSeed, ILSMIndexBatchPointCursor searchCursor) {
        return bTreeAccessor.createSampleCursor(componentSampleCardinality, sampleSeed, searchCursor,
                maxSampleLeafAttempts);
    }

    private void computeDiskComponentSampleProportionality() throws HyracksDataException {
        List<ThetaEstimator.ComponentStats> componentStats = new ArrayList<>();
        ArrayBackedValueStorage thetaReference = new ArrayBackedValueStorage();
        for (ILSMComponent component : operationalComponents) {
            DiskComponentMetadata metadata = (DiskComponentMetadata) component.getMetadata();
            thetaReference.reset();
            metadata.get(DiskComponentMetadata.THETA_INSERT_DELETE_SKETCH_KEY, thetaReference);
            ThetaEstimator.ComponentStats cStat = ThetaSampler.deserialize(thetaReference);
            componentStats.add(cStat);
        }

        // Compute globally-correct per-component live counts (accounts for cross-component shadowing)
        ThetaEstimator.CardinalityEstimate estimate = ThetaEstimator.estimatePerComponentCardinality(componentStats);
        estimatedCardinality = estimate.totalCardinality;
        long[] liveMatterCount = estimate.perComponentCardinality;

        long totalAllocated = 0;
        double[] remainders = new double[numDiskBTrees];
        for (int i = 0; i < numDiskBTrees; i++) {
            if (estimatedCardinality == 0) {
                proportionality[i] = 0;
                continue;
            }
            double exact = (double) sampleCardinality * liveMatterCount[i] / estimatedCardinality;
            proportionality[i] = Math.min(liveMatterCount[i], (long) exact);
            remainders[i] = exact - proportionality[i];
            totalAllocated += proportionality[i];
        }

        // Distribute leftover samples to components with largest remainders — O(N log N)
        long deficit = sampleCardinality - totalAllocated;
        if (deficit > 0) {
            Integer[] indices = new Integer[numDiskBTrees];
            for (int i = 0; i < numDiskBTrees; i++) {
                indices[i] = i;
            }
            Arrays.sort(indices, (a, b) -> Double.compare(remainders[b], remainders[a]));
            for (int j = 0; j < numDiskBTrees && deficit > 0; j++) {
                int idx = indices[j];
                if (proportionality[idx] < liveMatterCount[idx]) {
                    proportionality[idx]++;
                    deficit--;
                }
            }
        }

        // Safety check: verify allocation matches the target
        if (deficit > 0) {
            long totalCapacity = 0;
            for (long lmc : liveMatterCount) {
                totalCapacity += lmc;
            }
            if (sampleCardinality <= totalCapacity) {
                LOGGER.warn(
                        "Sample proportionality under-allocated: distributed {} of {} requested (estimated capacity: {})",
                        sampleCardinality - deficit, sampleCardinality, totalCapacity);
            }
        }
    }

    private void cleanUpAccessorsIfPresent() throws HyracksDataException {
        if (btreeAccessors != null) {
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
        if (numDiskBTrees == 0 || sampledCount == sampleCardinality) {
            return false; // Sample is complete.
        }

        // Iterate through disk components until we find a tuple or exhaust all components
        while (currentDiskComponentIndex < numDiskBTrees) {
            // Ask the current disk component cursor for the next sampled tuple
            boolean hasNext = btreeCursors[currentDiskComponentIndex].hasNext();
            if (hasNext) {
                // Found a tuple, increment the sampled count and return
                sampledCount++;
                return true;
            }

            // Current disk component cursor has no more tuples, move to the next
            currentDiskComponentIndex++;
            if (currentDiskComponentIndex >= numDiskBTrees) {
                return false; // No more disk components to sample from.
            }

            // Close the previous batch search cursor and open a new one
            // with the components from currentDiskComponentIndex - 1 to 0
            searchCursor.doClose();
            searchComponents.add(operationalComponents.get(currentDiskComponentIndex - 1));
            searchCursor.doOpen(searchCursorInitialState, null); // search predicate is not used in the point cursor

            // Open the next disk component for sampling
            btreeAccessors[currentDiskComponentIndex].diskSampleScan(btreeCursors[currentDiskComponentIndex]);
        }

        return false; // Exhausted all disk components
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
                harness.endScanDiskComponents(opCtx);
            }
        }
    }

    private void closeCursors() throws HyracksDataException {
        Throwable failure = null;
        if (btreeCursors != null) {
            for (int i = 0; i < numDiskBTrees; i++) {
                if (btreeCursors[i] != null) {
                    failure = ResourceReleaseUtils.close(btreeCursors[i], failure);
                }
            }
        }
        failure = ResourceReleaseUtils.close(searchCursor, failure);
        if (failure != null) {
            throw HyracksDataException.create(failure);
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
