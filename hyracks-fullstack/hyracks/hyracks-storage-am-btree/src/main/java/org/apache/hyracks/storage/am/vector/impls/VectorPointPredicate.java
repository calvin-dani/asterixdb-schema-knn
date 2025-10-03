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
 * Search predicate for vector point queries.
 * Used for finding the closest cluster to a query vector.
 */
public class VectorPointPredicate implements ISearchPredicate {
    private static final long serialVersionUID = 1L;

    private final float[] queryVector;

    public VectorPointPredicate(float[] queryVector) {
        this.queryVector = queryVector;
    }

    public float[] getQueryVector() {
        return queryVector;
    }

    @Override
    public MultiComparator getLowKeyComparator() {
        // Vector clustering tree doesn't use traditional key comparisons
        // This method is not applicable for vector searches
        return null;
    }

    @Override
    public MultiComparator getHighKeyComparator() {
        // Vector clustering tree doesn't use traditional key comparisons
        // This method is not applicable for vector searches
        return null;
    }

    @Override
    public ITupleReference getLowKey() {
        // Vector clustering tree doesn't use traditional key searches
        // This method is not applicable for vector searches
        return null;
    }

    public ITupleReference getHighKey() {
        return null;
    }

    public boolean isLowKeyInclusive() {
        return false;
    }

    public boolean isHighKeyInclusive() {
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VectorPointPredicate[");
        if (queryVector != null) {
            sb.append("dimensions=").append(queryVector.length);
        }
        sb.append("]");
        return sb.toString();
    }
}
