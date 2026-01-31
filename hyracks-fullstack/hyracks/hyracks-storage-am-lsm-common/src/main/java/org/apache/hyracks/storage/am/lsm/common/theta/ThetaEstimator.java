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
package org.apache.hyracks.storage.am.lsm.common.theta;

import java.util.List;
import java.util.Set;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * Estimates the cardinality (number of distinct values) across LSM disk components using the
 * Theta Sketch algorithm, which is based on the K-Minimum Values (KMV) probabilistic data structure.
 *
 * <h2>How KMV/Theta Sketch Works</h2>
 * <p>
 * The key insight is that when you hash N items uniformly, the K smallest hash values
 * provide information about the total number of items. If the K-th smallest hash value
 * is at position θ (theta) in the hash space [0, MAX], then approximately K items
 * exist in the range [0, θ], so the total count is estimated as: {@code count ≈ K / θ}.
 * </p>
 *
 * <h2>LSM-Specific Adaptations</h2>
 * <p>
 * This implementation extends KMV to handle LSM trees with:
 * <ul>
 *   <li><b>Insert samples</b>: Hashes of inserted keys</li>
 *   <li><b>Delete samples</b>: Hashes of deleted keys (antimatter tuples)</li>
 *   <li><b>Multi-component estimation</b>: Combines sketches from multiple disk components,
 *       replaying history from newest to oldest to determine live keys</li>
 * </ul>
 * </p>
 *
 * <h2>Global Theta Calculation</h2>
 * <p>
 * When combining sketches from multiple components, we use the minimum theta (most restrictive)
 * across all components to ensure we're comparing like with like. Only hashes below this
 * global threshold are considered in the final estimation.
 * </p>
 *
 * @see <a href="https://blog.demofox.org/2015/02/03/estimating-counts-of-distinct-values-with-kmv/">
 *      Estimating Counts of Distinct Values with KMV</a>
 * @see ThetaSampler
 */
public class ThetaEstimator {

    /**
     * Holds the KMV sketch statistics for a single LSM disk component.
     * <p>
     * Each component maintains two sets of K-minimum hash values:
     * <ul>
     *   <li>{@code insertSamples}: K smallest hashes of inserted (live) tuples</li>
     *   <li>{@code deleteSamples}: K smallest hashes of deleted (antimatter) tuples</li>
     * </ul>
     * The samples are sorted in ascending order, so the last element represents theta (θ).
     * </p>
     */
    public static class ComponentStats {
        /** K smallest hash values from inserted tuples, sorted ascending */
        public final long[] insertSamples;
        /** K smallest hash values from deleted tuples, sorted ascending */
        public final long[] deleteSamples;
        /** The K parameter - maximum number of minimum values to keep */
        public final int K;

        public ComponentStats(long[] insertSamples, long[] deleteSamples, int k) {
            this.insertSamples = insertSamples;
            this.deleteSamples = deleteSamples;
            K = k;
        }

        /**
         * Calculates theta (θ) for this component's insert samples.
         * <p>
         * Theta represents the fraction of the hash space covered by our K samples.
         * If we have fewer than K samples, theta = 1.0 (we've seen everything).
         * Otherwise, theta = (K-th smallest hash) / MAX_HASH_VALUE.
         * </p>
         *
         * @return theta value between 0.0 and 1.0
         */
        public double getTheta() {
            if (insertSamples.length < K) {
                // less than the sample count, keep everything
                return 1.0;
            }
            // insertSamples[i] > 0, as while inserting clearing up the sign bit
            return (double) insertSamples[insertSamples.length - 1] / (double) Long.MAX_VALUE;
        }

        /**
         * Estimates the number of live (non-deleted) tuples in this component.
         * <p>
         * This method:
         * <ol>
         *   <li>Filters both insert and delete samples by the theta threshold</li>
         *   <li>Counts insert samples that don't have a matching delete</li>
         *   <li>Scales by 1/theta to estimate total live cardinality</li>
         * </ol>
         * </p>
         *
         * @return estimated count of live tuples in this component
         */
        public long estimateLiveCardinality() {
            double theta = getTheta();
            long threshold = (long) (theta * Long.MAX_VALUE);

            LongOpenHashSet deleteSet = new LongOpenHashSet();
            for (long hash : deleteSamples) {
                if (hash <= threshold) {
                    deleteSet.add(hash);
                }
            }

            int liveCount = 0;
            for (long hash : insertSamples) {
                if (hash <= threshold && !deleteSet.contains(hash)) {
                    liveCount++;
                }
            }

            if (theta == 0.0) {
                return 0;
            }
            return (long) (liveCount / theta);
        }
    }

    /**
     * Estimates the total cardinality of live tuples across all LSM disk components.
     * <p>
     * The algorithm:
     * <ol>
     *   <li><b>Calculate Global Theta</b>: Find the minimum theta across all components.
     *       This ensures we only compare hashes that fall within all sketches' ranges.</li>
     *   <li><b>Replay History</b>: Process components from newest to oldest, tracking which
     *       keys are alive (inserted but not yet deleted in a newer component).</li>
     *   <li><b>Estimate</b>: Divide the count of alive keys by global theta.</li>
     * </ol>
     * </p>
     *
     * @param components list of component stats, ordered from newest to oldest
     * @return estimated total cardinality of live tuples
     */
    public static long estimateCardinality(List<ComponentStats> components) {
        return estimatePerComponentCardinality(components).totalCardinality;
    }

    /**
     * Result of per-component cardinality estimation.
     */
    public static class CardinalityEstimate {
        /** Estimated total cardinality of live tuples across all components */
        public final long totalCardinality;
        /** Estimated cardinality of globally-live tuples attributed to each component */
        public final long[] perComponentCardinality;

        public CardinalityEstimate(long totalCardinality, long[] perComponentCardinality) {
            this.totalCardinality = totalCardinality;
            this.perComponentCardinality = perComponentCardinality;
        }
    }

    /**
     * Estimates per-component cardinality of globally-live tuples.
     * <p>
     * Unlike {@link ComponentStats#estimateLiveCardinality()}, which only considers
     * inserts and deletes within a single component, this method accounts for
     * cross-component shadowing: a key is attributed to the newest component that
     * contains it, and keys shadowed by newer components are not counted.
     * </p>
     * <p>
     * The sum of all per-component cardinalities equals the total cardinality estimate.
     * This makes the result suitable for proportional sample allocation across components.
     * </p>
     * <p>
     * Cost is the same as {@link #estimateCardinality(List)} &mdash; a single pass through
     * all component sketches.
     * </p>
     *
     * @param components list of component stats, ordered from newest to oldest
     * @return estimation result with total and per-component cardinalities
     */
    public static CardinalityEstimate estimatePerComponentCardinality(List<ComponentStats> components) {
        int n = components.size();
        long[] perComponent = new long[n];

        if (n == 0) {
            return new CardinalityEstimate(0, perComponent);
        }

        // 1. Calculate Global Theta (Minimum Intersection)
        double globalTheta = 1.0;
        for (ComponentStats stats : components) {
            globalTheta = Math.min(globalTheta, stats.getTheta());
        }
        long threshold = (long) (globalTheta * Long.MAX_VALUE);

        if (globalTheta == 0.0) {
            return new CardinalityEstimate(0, perComponent);
        }

        // 2. Replay History (Newest -> Oldest), tracking per-component contributions
        Set<Long> seenKeys = new LongOpenHashSet();
        int totalAliveInSample = 0;

        for (int i = 0; i < n; i++) {
            ComponentStats stats = components.get(i);
            int componentAliveCount = 0;

            // A. Process Inserts: a key is alive only if not seen in a newer component
            for (long hash : stats.insertSamples) {
                if (hash > threshold)
                    continue;
                if (!seenKeys.contains(hash)) {
                    componentAliveCount++;
                }
                seenKeys.add(hash);
            }

            // B. Process Deletes (Tombstones)
            if (stats.deleteSamples != null) {
                for (long hash : stats.deleteSamples) {
                    if (hash > threshold)
                        continue;
                    seenKeys.add(hash);
                }
            }

            perComponent[i] = (long) (componentAliveCount / globalTheta);
            totalAliveInSample += componentAliveCount;
        }

        long totalCardinality = (long) (totalAliveInSample / globalTheta);
        return new CardinalityEstimate(totalCardinality, perComponent);
    }

}
