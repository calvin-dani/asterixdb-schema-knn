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
package org.apache.hyracks.storage.am.vector.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;

/**
 * A specialized priority queue for managing top-k nearest neighbors in ANN search.
 * Uses a max-heap to maintain the k closest vectors, automatically evicting the farthest when capacity is exceeded.
 */
public class TopKVectorQueue {

    /**
     * Container for a vector candidate with its distance.
     */
    public static class VectorCandidate {
        private final ITupleReference tuple;
        private final float distance;
        private final int componentIndex; // For LSM component tracking

        public VectorCandidate(ITupleReference tuple, float distance, int componentIndex) throws HyracksDataException {
            this.tuple = TupleUtils.copyTuple(tuple); // Create a defensive copy
            this.distance = distance;
            this.componentIndex = componentIndex;
        }

        public ITupleReference getTuple() {
            return tuple;
        }

        public float getDistance() {
            return distance;
        }

        public int getComponentIndex() {
            return componentIndex;
        }

        @Override
        public String toString() {
            return "VectorCandidate{distance=" + distance + ", componentIndex=" + componentIndex + "}";
        }
    }

    private final PriorityQueue<VectorCandidate> maxHeap;
    private final int capacity;

    /**
     * Creates a new top-k vector queue.
     * 
     * @param k The maximum number of candidates to maintain
     */
    public TopKVectorQueue(int k) {
        this.capacity = k;
        // Max-heap: candidates with larger distances have higher priority for removal
        this.maxHeap = new PriorityQueue<>(k + 1, Comparator.comparing(VectorCandidate::getDistance).reversed());
    }

    /**
     * Attempts to add a candidate to the top-k set.
     * 
     * @param tuple The vector tuple
     * @param distance Distance to the query vector
     * @param componentIndex Index of the LSM component this tuple came from
     * @return true if the candidate was added, false if it was rejected
     */
    public boolean offer(ITupleReference tuple, float distance, int componentIndex) throws HyracksDataException {
        VectorCandidate candidate = new VectorCandidate(tuple, distance, componentIndex);

        if (maxHeap.size() < capacity) {
            // Queue not full, always add
            maxHeap.offer(candidate);
            return true;
        } else {
            // Queue full, check if this candidate is better than the worst
            VectorCandidate worst = maxHeap.peek();
            if (distance < worst.getDistance()) {
                // New candidate is better, replace the worst
                maxHeap.poll();
                maxHeap.offer(candidate);
                return true;
            } else {
                // New candidate is worse, reject
                return false;
            }
        }
    }

    /**
     * Returns the current worst (farthest) distance in the queue.
     * 
     * @return The worst distance, or Float.MAX_VALUE if queue is not full
     */
    public float getWorstDistance() {
        if (maxHeap.size() >= capacity) {
            return maxHeap.peek().getDistance();
        } else {
            return Float.MAX_VALUE;
        }
    }

    /**
     * Returns the number of candidates currently in the queue.
     */
    public int size() {
        return maxHeap.size();
    }

    /**
     * Returns true if the queue is at capacity.
     */
    public boolean isFull() {
        return maxHeap.size() >= capacity;
    }

    /**
     * Returns true if the queue is empty.
     */
    public boolean isEmpty() {
        return maxHeap.isEmpty();
    }

    /**
     * Returns all candidates in ascending order of distance (closest first).
     */
    public List<VectorCandidate> getOrderedResults() {
        List<VectorCandidate> results = new ArrayList<>(maxHeap);
        Collections.sort(results, Comparator.comparing(VectorCandidate::getDistance));
        return results;
    }

    /**
     * Clears all candidates from the queue.
     */
    public void clear() {
        maxHeap.clear();
    }

    /**
     * Merges another TopKVectorQueue into this one, maintaining the top-k constraint.
     * 
     * @param other The other queue to merge
     */
    public void merge(TopKVectorQueue other) throws HyracksDataException {
        for (VectorCandidate candidate : other.maxHeap) {
            offer(candidate.getTuple(), candidate.getDistance(), candidate.getComponentIndex());
        }
    }

    @Override
    public String toString() {
        return "TopKVectorQueue{capacity=" + capacity + ", size=" + size() + ", worstDistance="
                + (isFull() ? getWorstDistance() : "N/A") + "}";
    }
}
