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

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * Search predicate for vector ANN (Approximate Nearest Neighbor) queries.
 * Holds a reference to the query tuple for per-tuple vector searches.
 *
 * Following the Reference Pattern (like RTree): The predicate holds an ITupleReference
 * that is updated per-tuple by resetSearchPredicate(). The storage layer extracts
 * the query vector when needed during search.
 *
 * For top-K ANN queries, also includes the k parameter (number of nearest neighbors).
 */
public class VectorPointPredicate implements ISearchPredicate {
    private static final long serialVersionUID = 1L;

    private ITupleReference queryTuple;
    private int queryFieldIndex;
    private String distanceMetric;
    private int k; // Number of nearest neighbors to return (for ANN queries)
    private int nprobe; // Number of clusters to probe (minimum before K-check)
    private double epsilon; // Distance threshold for level-wise cross-pollination
    private ITupleFilter tupleFilter; // Filter for INCLUDE field predicates (e.g., year > 2000)

    public VectorPointPredicate() {
        // Empty constructor for initialization
        this.distanceMetric = null;
        this.k = Integer.MAX_VALUE; // Default: no limit
        this.nprobe = 10; // Default: probe 1 cluster
        this.epsilon = 0.15; // Default: no epsilon (use nprobe count only)
    }

    public VectorPointPredicate(int k) {
        // Constructor for ANN queries with K parameter
        this.k = k;
        this.distanceMetric = null;
        this.nprobe = 10;
        this.epsilon = 0.15;
    }

    public VectorPointPredicate(int k, int nprobe, double epsilon) {
        // Constructor for ANN queries with K, nprobe, and epsilon parameters
        this.k = k;
        this.nprobe = nprobe;
        this.epsilon = epsilon;
        this.distanceMetric = null;
    }

    public VectorPointPredicate(double[] queryVector) {
        // Constructor kept for compatibility with tests
        // In runtime, query data comes via setQueryTuple()
        this.k = Integer.MAX_VALUE; // Default: no limit
        this.nprobe = 10;
        this.epsilon = 0.15;
    }

    /**
     * Set the query tuple containing the vector field.
     * Called by resetSearchPredicate() for each input tuple.
     */
    public void setQueryTuple(ITupleReference queryTuple) {
        this.queryTuple = queryTuple;
    }

    /**
     * Set the field index of the vector in the query tuple.
     */
    public void setQueryFieldIndex(int queryFieldIndex) {
        this.queryFieldIndex = queryFieldIndex;
    }

    /**
     * Get the query tuple reference.
     */
    public ITupleReference getQueryTuple() {
        return queryTuple;
    }

    /**
     * Get the query field index.
     */
    public int getQueryFieldIndex() {
        return queryFieldIndex;
    }

    /**
     * Set the distance metric string (e.g., "euclidean", "cosine similarity", etc.).
     */
    public void setDistanceMetric(String distanceMetric) {
        this.distanceMetric = distanceMetric;
    }

    /**
     * Get the distance metric string.
     */
    public String getDistanceMetric() {
        return distanceMetric;
    }

    /**
     * Set the K parameter (number of nearest neighbors to return).
     */
    public void setK(int k) {
        this.k = k;
    }

    /**
     * Get the K parameter (number of nearest neighbors to return).
     */
    public int getK() {
        return k;
    }

    /**
     * Set the nprobe parameter (number of clusters to probe).
     * This is the minimum number of clusters to explore before checking if K is satisfied.
     */
    public void setNprobe(int nprobe) {
        this.nprobe = nprobe;
    }

    /**
     * Get the nprobe parameter (number of clusters to probe).
     */
    public int getNprobe() {
        return nprobe;
    }

    /**
     * Set the epsilon parameter (distance threshold for level-wise cross-pollination).
     * Clusters within (closestDistance + epsilon) will be explored via level-wise.
     */
    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    /**
     * Get the epsilon parameter (distance threshold for level-wise cross-pollination).
     */
    public double getEpsilon() {
        return epsilon;
    }

    /**
     * Set the tuple filter for INCLUDE field predicates.
     * When set, the cursor will only return tuples that pass this filter,
     * and only count passing tuples toward K.
     */
    public void setTupleFilter(ITupleFilter tupleFilter) {
        this.tupleFilter = tupleFilter;
    }

    /**
     * Get the tuple filter for INCLUDE field predicates.
     */
    public ITupleFilter getTupleFilter() {
        return tupleFilter;
    }

    @Override
    public MultiComparator getLowKeyComparator() {
        // Vector clustering tree doesn't use traditional key comparisons
        return null;
    }

    @Override
    public MultiComparator getHighKeyComparator() {
        // Vector clustering tree doesn't use traditional key comparisons
        return null;
    }

    @Override
    public ITupleReference getLowKey() {
        // Vector clustering tree doesn't use traditional key searches
        return null;
    }

    @Override
    public String toString() {
        return "VectorPointPredicate[queryTuple=" + (queryTuple != null ? "set" : "null") + ", distanceMetric="
                + distanceMetric + ", k=" + k + ", nprobe=" + nprobe + ", epsilon=" + epsilon + "]";
    }
}
