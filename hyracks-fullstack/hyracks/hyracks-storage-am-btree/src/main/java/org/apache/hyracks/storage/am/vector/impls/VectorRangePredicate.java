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

import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * Range search predicate for vector clustering tree.
 * Finds all vectors within a specified distance from a query vector.
 */
public abstract class VectorRangePredicate implements ISearchPredicate {

    private final float[] queryVector;
    private final double maxDistance;
    private final DistanceFunction distanceFunction;

    public enum DistanceFunction {
        EUCLIDEAN,
        COSINE,
        MANHATTAN
    }

    public VectorRangePredicate(float[] queryVector, double maxDistance) {
        this(queryVector, maxDistance, DistanceFunction.EUCLIDEAN);
    }

    public VectorRangePredicate(float[] queryVector, double maxDistance, DistanceFunction distanceFunction) {
        this.queryVector = queryVector.clone();
        this.maxDistance = maxDistance;
        this.distanceFunction = distanceFunction;
    }

    public float[] getQueryVector() {
        return queryVector.clone();
    }

    public double getMaxDistance() {
        return maxDistance;
    }

    public DistanceFunction getDistanceFunction() {
        return distanceFunction;
    }

    @Override
    public String toString() {
        return "VectorRangePredicate{" + "queryVector.length=" + queryVector.length + ", maxDistance=" + maxDistance
                + ", distanceFunction=" + distanceFunction + '}';
    }
}
