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

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringSearchCursor;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VectorRangePredicate;
import org.apache.hyracks.storage.am.vector.util.VectorUtils;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * Search cursor for LSM Vector Clustering Tree.
 *
 * This cursor coordinates search operations across multiple LSM components (memory and disk),
 * merging results using a priority queue to return top-k nearest neighbors based on vector similarity.
 *
 * Algorithm Overview:
 * 1. Open cursors for all operational components (memory + disk)
 * 2. Each component cursor finds its closest cluster and iterates through cluster data
 * 3. Use a max-heap priority queue to maintain top-k results across all components
 * 4. Return results in ascending order of distance (nearest first)
 */
public class LSMVCTreeSearchCursor implements IIndexCursor {

    private AbstractLSMIndexOperationContext opCtx;
    private List<ILSMComponent> operationalComponents;
    private List<IIndexCursor> rangeCursors;
    private ISearchPredicate searchPredicate;
    private VectorRangePredicate vectorPredicate;

    // Cursor state
    private boolean open;
    private boolean exhausted;
    private ITupleReference currentTuple;

    // Priority queue for top-k results
    private PriorityQueue<VectorResult> resultHeap;
    private int k; // Number of results to return
    private double[] queryVector;

    // Result iteration
    private VectorResult[] sortedResults;
    private int resultIndex;

    /**
     * Internal class to hold vector search results with distance information.
     */
    private static class VectorResult {
        final ITupleReference tuple;
        final double distance;
        final int componentIndex;

        VectorResult(ITupleReference tuple, double distance, int componentIndex) {
            this.tuple = tuple;
            this.distance = distance;
            this.componentIndex = componentIndex;
        }
    }

    public LSMVCTreeSearchCursor(ILSMIndexOperationContext opCtx) {
        this.opCtx = (AbstractLSMIndexOperationContext) opCtx;
        this.open = false;
        this.exhausted = false;
        this.resultIndex = 0;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMVCTreeCursorInitialState lsmInitialState = (LSMVCTreeCursorInitialState) initialState;

        this.searchPredicate = searchPred;
        this.operationalComponents = lsmInitialState.getOperationalComponents();
        this.rangeCursors = lsmInitialState.getCursors();

        // Extract vector search parameters
        if (searchPred instanceof VectorRangePredicate) {
            this.vectorPredicate = (VectorRangePredicate) searchPred;
            this.queryVector = vectorPredicate.getQueryVector();
            this.k = 10;
        } else {
            throw new HyracksDataException("LSMVCTreeSearchCursor requires VectorRangePredicate");
        }

        // Initialize priority queue for top-k results (max-heap to keep smallest k distances)
        this.resultHeap = new PriorityQueue<>(k + 1, new Comparator<VectorResult>() {
            @Override
            public int compare(VectorResult a, VectorResult b) {
                // Max-heap: larger distances have higher priority (will be removed first)
                return Double.compare(b.distance, a.distance);
            }
        });

        this.open = true;
        this.exhausted = false;
        this.resultIndex = 0;
        this.sortedResults = null;

        // Open cursors for all operational components
        openComponentCursors();

        // Collect and merge results from all components
        collectAndMergeResults();
    }

    /**
     * Open cursors for all LSM components (memory and disk).
     */
    private void openComponentCursors() throws HyracksDataException {
        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);

            if (component instanceof LSMVCTreeMemoryComponent) {
                openMemoryComponentCursor(component, i);
            } else if (component instanceof LSMVCTreeDiskComponent) {
                openDiskComponentCursor(component, i);
            } else {
                throw new HyracksDataException("Unknown LSM component type: " + component.getClass());
            }
        }
    }

    /**
     * Open cursor for memory component.
     */
    private void openMemoryComponentCursor(ILSMComponent component, int componentIndex) throws HyracksDataException {
        LSMVCTreeMemoryComponent memComponent = (LSMVCTreeMemoryComponent) component;
        VectorClusteringTree vcTree = memComponent.getIndex();

        // Create and configure cursor
        VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) vcTree.createSearchCursor(false);
        vcCursor.setBufferCache(vcTree.getBufferCache());
        vcCursor.setFileId(vcTree.getFileId());
        vcCursor.setRootPageId(vcTree.getRootPageId());
        vcCursor.setFrameFactories(vcTree.getInteriorFrameFactory(), vcTree.getLeafFrameFactory(),
                vcTree.getMetadataFrameFactory(), vcTree.getDataFrameFactory());
        vcCursor.setQueryVector(queryVector);

        rangeCursors.set(componentIndex, vcCursor);
        vcCursor.open(null, searchPredicate);

        System.out.println("DEBUG: Opened memory component cursor " + componentIndex);
    }

    /**
     * Open cursor for disk component.
     */
    private void openDiskComponentCursor(ILSMComponent component, int componentIndex) throws HyracksDataException {
        LSMVCTreeDiskComponent diskComponent = (LSMVCTreeDiskComponent) component;
        VectorClusteringTree vcTree = diskComponent.getIndex();

        // Create and configure cursor (same as memory component)
        VectorClusteringSearchCursor vcCursor = (VectorClusteringSearchCursor) vcTree.createSearchCursor(false);
        vcCursor.setBufferCache(vcTree.getBufferCache());
        vcCursor.setFileId(vcTree.getFileId());
        vcCursor.setRootPageId(vcTree.getRootPageId());
        vcCursor.setFrameFactories(vcTree.getInteriorFrameFactory(), vcTree.getLeafFrameFactory(),
                vcTree.getMetadataFrameFactory(), vcTree.getDataFrameFactory());
        vcCursor.setQueryVector(queryVector);

        rangeCursors.set(componentIndex, vcCursor);
        vcCursor.open(null, searchPredicate);

        System.out.println("DEBUG: Opened disk component cursor " + componentIndex);
    }

    /**
     * Collect results from all component cursors and merge using priority queue.
     */
    private void collectAndMergeResults() throws HyracksDataException {
        System.out.println("DEBUG: Collecting results from " + rangeCursors.size() + " components");

        // Process results from each component cursor
        for (int i = 0; i < rangeCursors.size(); i++) {
            IIndexCursor cursor = rangeCursors.get(i);
            if (cursor == null)
                continue;

            System.out.println("DEBUG: Processing component " + i);

            // Collect all results from this component
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();

                // Calculate distance from query vector to this result
                double distance = calculateDistanceFromTuple(tuple, queryVector);

                // Add to priority queue (implements top-k logic)
                addToTopKResults(new VectorResult(tuple, distance, i));
            }
        }

        // Convert priority queue to sorted array for iteration
        prepareSortedResults();

        System.out.println("DEBUG: Collected " + (sortedResults != null ? sortedResults.length : 0) + " total results");
    }

    /**
     * Add a result to the top-k priority queue.
     */
    private void addToTopKResults(VectorResult result) {
        if (resultHeap.size() < k) {
            // Queue not full, just add
            resultHeap.offer(result);
        } else {
            // Queue full, check if this result is better than the worst
            VectorResult worst = resultHeap.peek();
            if (result.distance < worst.distance) {
                // This result is better, replace worst
                resultHeap.poll();
                resultHeap.offer(result);
            }
        }
    }

    /**
     * Convert priority queue to sorted array for iteration.
     */
    private void prepareSortedResults() {
        if (resultHeap.isEmpty()) {
            sortedResults = new VectorResult[0];
            return;
        }

        // Extract all results from heap
        sortedResults = resultHeap.toArray(new VectorResult[0]);

        // Sort by distance (ascending order - nearest first)
        java.util.Arrays.sort(sortedResults, new Comparator<VectorResult>() {
            @Override
            public int compare(VectorResult a, VectorResult b) {
                return Double.compare(a.distance, b.distance);
            }
        });

        System.out.println("DEBUG: Prepared " + sortedResults.length + " sorted results");
        if (sortedResults.length > 0) {
            System.out.println("DEBUG: Best distance: " + sortedResults[0].distance);
            System.out.println("DEBUG: Worst distance: " + sortedResults[sortedResults.length - 1].distance);
        }
    }

    /**
     * Calculate Euclidean distance between query vector and a result tuple.
     */
    private double calculateDistanceFromTuple(ITupleReference tuple, double[] queryVector) {
        try {
            // Assuming tuple format: [distance, cosine_similarity, vector_data, primary_key]
            // Extract vector data from field 2
            byte[] vectorData = tuple.getFieldData(2);
            int vectorOffset = tuple.getFieldStart(2);
            int vectorLength = tuple.getFieldLength(2);

            // Deserialize vector using FloatArraySerializerDeserializer
            //TODO
            double[] resultVector = new double[vectorLength / Float.BYTES];

            // Calculate Euclidean distance
            return VectorUtils.calculateEuclideanDistance(queryVector, resultVector);

        } catch (Exception e) {
            System.out.println(
                    "WARNING: Failed to calculate distance for tuple, using distance field: " + e.getMessage());

            // Fallback: try to use pre-calculated distance from field 0
            try {
                byte[] distanceData = tuple.getFieldData(0);
                int distanceOffset = tuple.getFieldStart(0);
                // TODO
                return 0;
            } catch (Exception e2) {
                System.out.println("ERROR: Could not extract distance from tuple: " + e2.getMessage());
                return Double.MAX_VALUE;
            }
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (!open) {
            return false;
        }

        if (exhausted) {
            return false;
        }

        // Check if we have more results to return
        if (sortedResults != null && resultIndex < sortedResults.length) {
            return true;
        }

        // No more results
        exhausted = true;
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        if (!hasNext()) {
            throw new IllegalStateException("No more elements");
        }

        // Get next result from sorted array
        VectorResult result = sortedResults[resultIndex];
        currentTuple = result.tuple;
        resultIndex++;

        System.out.println("DEBUG: Returning result " + resultIndex + " with distance " + result.distance
                + " from component " + result.componentIndex);
    }

    @Override
    public ITupleReference getTuple() {
        return currentTuple;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            return;
        }

        // Close all component cursors
        if (rangeCursors != null) {
            for (IIndexCursor cursor : rangeCursors) {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }

        // Clear data structures
        if (resultHeap != null) {
            resultHeap.clear();
        }

        sortedResults = null;
        open = false;
        exhausted = false;
        currentTuple = null;
        resultIndex = 0;
    }

    @Override
    public void destroy() throws HyracksDataException {
        close();

        // Destroy all component cursors
        if (rangeCursors != null) {
            for (IIndexCursor cursor : rangeCursors) {
                if (cursor != null) {
                    cursor.destroy();
                }
            }
            rangeCursors.clear();
        }
    }

    /**
     * Checks if the cursor is currently open.
     */
    public boolean isOpen() {
        return open;
    }

    /**
     * Checks if the cursor is exhausted.
     */
    public boolean isExhausted() {
        return exhausted;
    }

    /**
     * Get the number of results returned by this search.
     */
    public int getResultCount() {
        return sortedResults != null ? sortedResults.length : 0;
    }

    /**
     * Get the query vector used for this search.
     */
    public double[] getQueryVector() {
        return queryVector;
    }

    /**
     * Get the k value (number of nearest neighbors requested).
     */
    public int getK() {
        return k;
    }
}
