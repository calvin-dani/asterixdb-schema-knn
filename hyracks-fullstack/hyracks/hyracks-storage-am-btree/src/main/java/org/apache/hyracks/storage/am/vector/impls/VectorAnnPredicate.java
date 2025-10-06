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
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * Search predicate for Approximate Nearest Neighbor (ANN) queries in vector clustering trees.
 * Encapsulates the query vector and the number of nearest neighbors to return (k).
 */
public class VectorAnnPredicate implements ISearchPredicate {
    private static final long serialVersionUID = 1L;

    private final float[] queryVector;
    private final int k; // Number of nearest neighbors to return
    private final String distanceMetric; // e.g., "euclidean", "cosine", "manhattan"

    public VectorAnnPredicate(float[] queryVector, int k) {
        this(queryVector, k, "euclidean");
    }

    public VectorAnnPredicate(float[] queryVector, int k, String distanceMetric) {
        this.queryVector = queryVector.clone(); // Defensive copy
        this.k = k;
        this.distanceMetric = distanceMetric;
    }

    public float[] getQueryVector() {
        return queryVector;
    }

    public int getK() {
        return k;
    }

    public String getDistanceMetric() {
        return distanceMetric;
    }

    @Override
    public MultiComparator getLowKeyComparator() {
        // Vector ANN queries don't use traditional key range comparisons
        return null;
    }

    @Override
    public MultiComparator getHighKeyComparator() {
        // Vector ANN queries don't use traditional key range comparisons
        return null;
    }

    @Override
    public ITupleReference getLowKey() {
        // Not applicable for vector ANN searches
        return null;
    }

    public ITupleReference getHighKey() {
        // Not applicable for vector ANN searches
        return null;
    }

    public boolean isLowKeyInclusive() {
        // Not applicable for vector ANN searches
        return false;
    }

    public boolean isHighKeyInclusive() {
        // Not applicable for vector ANN searches
        return false;
    }

    @Override
    public String toString() {
        return "VectorAnnPredicate{" + "queryVector.length=" + queryVector.length + ", k=" + k + ", distanceMetric='"
                + distanceMetric + '\'' + '}';
    }
}
