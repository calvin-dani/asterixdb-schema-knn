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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.common.ISampler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongPriorityQueue;

/**
 * Implements a Theta Sketch sampler for LSM disk components using the K-Minimum Values (KMV) algorithm.
 *
 * <h2>Overview</h2>
 * <p>
 * This sampler maintains two separate KMV sketches during LSM flush/merge operations:
 * <ul>
 *   <li><b>Insert heap</b>: Tracks the K smallest hash values of inserted tuples</li>
 *   <li><b>Delete heap</b>: Tracks the K smallest hash values of deleted (antimatter) tuples</li>
 * </ul>
 * These sketches are stored in the disk component metadata and used by {@link ThetaEstimator}
 * to estimate cardinality without scanning the entire component.
 * </p>
 *
 * <h2>How It Works</h2>
 * <p>
 * For each tuple written to disk:
 * <ol>
 *   <li>Compute a 64-bit hash of the tuple's key fields using MurmurHash3</li>
 *   <li>If the hash is smaller than the current K-th smallest, add it to the appropriate heap</li>
 *   <li>If the heap exceeds K elements, remove the largest (maintaining K-minimum invariant)</li>
 * </ol>
 * The K-th smallest hash value defines "theta" (θ), which represents what fraction of the
 * hash space is covered. Cardinality is then estimated as: {@code count ≈ K / θ}.
 * </p>
 *
 * <h2>Memory Efficiency</h2>
 * <p>
 * With the default K=1024, each sketch uses only ~8KB of memory (1024 * 8 bytes) regardless of how many
 * tuples are processed. This provides O(1) space complexity for cardinality estimation.
 * K is configurable via the storage property {@code storage.lsm.theta.sketch.k} and is passed through
 * the LSM index constructor chain.
 * </p>
 *
 * @see ThetaEstimator
 * @see <a href="https://blog.demofox.org/2015/02/03/estimating-counts-of-distinct-values-with-kmv/">
 *      Estimating Counts of Distinct Values with KMV</a>
 */
public class ThetaSampler implements ISampler {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Global flag to disable theta sampling for tests.
     * When disabled, createSampler() returns NoOpSampler.INSTANCE instead of ThetaSampler.
     * This is useful for unit tests with small buffer cache page sizes that cannot
     * accommodate the serialized theta data (~16KB with K=1024).
     * <p>
     * <b>Thread Safety:</b> This flag should only be modified before any LSM operations
     * start (e.g., in test setup/teardown). It is not thread-safe for concurrent modification.
     */
    private static boolean samplingEnabled = true;

    /**
     * Enables or disables theta sampling globally.
     * When disabled, {@link #createSampler(int[])} returns {@link NoOpSampler#INSTANCE}.
     * <p>
     * <b>Warning:</b> This method is intended for test use only. Disabling sampling
     * in production will prevent cardinality estimation from working correctly.
     *
     * @param enabled true to enable sampling, false to disable
     */
    public static void setSamplingEnabled(boolean enabled) {
        if (!enabled) {
            LOGGER.warn("Theta sampling has been DISABLED. This should only be used in tests. "
                    + "Cardinality estimation will not work correctly.");
        }
        samplingEnabled = enabled;
    }

    /**
     * @return true if theta sampling is enabled, false otherwise
     */
    public static boolean isSamplingEnabled() {
        return samplingEnabled;
    }

    /**
     * Factory method to create a sampler based on the global sampling enabled flag.
     * Uses the default K value ({@link #DEFAULT_K}).
     *
     * @param keyFields indices of the tuple fields that form the primary key
     * @return a ThetaSampler if sampling is enabled, NoOpSampler.INSTANCE otherwise
     */
    public static ISampler createSampler(int[] keyFields) {
        return createSampler(keyFields, DEFAULT_K);
    }

    /**
     * Factory method to create a sampler based on the global sampling enabled flag.
     *
     * @param keyFields indices of the tuple fields that form the primary key
     * @param k the K parameter (number of minimum hash values to keep)
     * @return a ThetaSampler if sampling is enabled, NoOpSampler.INSTANCE otherwise
     */
    public static ISampler createSampler(int[] keyFields, int k) {
        return samplingEnabled ? new ThetaSampler(keyFields, k)
                : org.apache.hyracks.storage.common.NoOpSampler.INSTANCE;
    }

    /**
     * The default K parameter - number of minimum hash values to keep.
     * Higher K = more accuracy but more memory. K=1024 provides ~3% error rate.
     */
    public static final int DEFAULT_K = 1024;

    /** The K value for this instance */
    private final int k;
    /** Buffer for serializing the sketch to disk component metadata */
    private final ArrayBackedValueStorage serializedTheta;
    /** Max-heap of K smallest hashes for inserted tuples (max-heap for efficient eviction of largest) */
    private final LongPriorityQueue insertHeap;
    /** Max-heap of K smallest hashes for deleted tuples */
    private final LongPriorityQueue deleteHeap;
    /** Indices of key fields to hash for tuple identification */
    private final int[] keyFields;
    /** Reusable buffer for 128-bit MurmurHash output */
    private final long[] hashes;

    /** Comparator for max-heap behavior (largest element at head for easy eviction) */
    public static final LongComparator thresholdComparator = (f, s) -> Long.compare(s, f);

    /**
     * Current theta limit for inserts - hashes larger than this are immediately rejected.
     * This optimization avoids heap operations for hashes that would be evicted anyway.
     */
    private long insertThetaLimit = Long.MAX_VALUE;
    /** Current theta limit for deletes */
    private long deleteThetaLimit = Long.MAX_VALUE;
    /** Seed for MurmurHash - fixed for deterministic hashing */
    private static final long SEED = 0L;

    /**
     * Creates a new ThetaSampler for tracking tuple cardinality using the default K value.
     *
     * @param keyFields indices of the tuple fields that form the primary key
     */
    public ThetaSampler(int[] keyFields) {
        this(keyFields, DEFAULT_K);
    }

    /**
     * Creates a new ThetaSampler for tracking tuple cardinality with a specified K value.
     *
     * @param keyFields indices of the tuple fields that form the primary key
     * @param k the K parameter (number of minimum hash values to keep)
     */
    public ThetaSampler(int[] keyFields, int k) {
        this.k = k;
        // ReverseOrder makes it a Max-Heap
        this.insertHeap = new LongHeapPriorityQueue(k, thresholdComparator);
        this.deleteHeap = new LongHeapPriorityQueue(k, thresholdComparator);
        this.keyFields = keyFields;
        this.serializedTheta = new ArrayBackedValueStorage();
        this.hashes = new long[2];
    }

    /**
     * Updates the sketch with a tuple being written to the disk component.
     * <p>
     * Call this for every tuple during LSM flush or merge operations.
     * The tuple is hashed and added to either the insert or delete heap
     * based on whether it's an antimatter tuple.
     * </p>
     *
     * @param tuple        the tuple being written to disk
     */
    public void addTuple(ITupleReference tuple) {
        // 1. Compute Hash (Use MurmurHash3 or similar high-quality hash)
        long hash = computeHash(tuple);
        boolean isAntiMatter =
                tuple instanceof ILSMTreeTupleReference && ((ILSMTreeTupleReference) tuple).isAntimatter();

        // 2. Route to appropriate heap
        if (isAntiMatter) {
            updateHeap(deleteHeap, hash, deleteThetaLimit, true);
        } else {
            updateHeap(insertHeap, hash, insertThetaLimit, false);
        }
    }

    /**
     * Computes a 63-bit positive hash of the tuple's key fields.
     * <p>
     * Uses MurmurHash3 128-bit variant and XORs both halves together,
     * then masks to ensure the result is positive (required for theta calculation).
     * </p>
     */
    private long computeHash(ITupleReference tuple) {
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
        // Force all hashes to be positive (0 to Long.MAX_VALUE)
        // Effectively using 63 bits of entropy instead of 64.
        // For 1M keys, 63 bits is still PLENTY (collision chance is effectively zero).
        return (hashes[0] ^ hashes[1]) & 0x7FFFFFFFFFFFFFFFL;
    }

    /**
     * Updates a KMV heap with a new hash value.
     * <p>
     * If the hash is larger than the current K-th smallest (limit), it's ignored.
     * Otherwise, it's added to the heap, and if the heap exceeds K elements,
     * the largest is evicted.
     * </p>
     */
    private void updateHeap(LongPriorityQueue heap, long hash, long limit, boolean isDelete) {
        // Optimization: If hash is already bigger than our worst sample, skip it.
        if (heap.size() >= k && hash >= limit) {
            return;
        }

        // Add to heap
        // Note: We allow duplicates in the heap for simplicity (Min-Hash variant).
        // For stricter accuracy with few keys, you could check .contains(),
        // but for 1M+ keys it doesn't matter much.
        heap.enqueue(hash);

        // Enforce Size Limit
        if (heap.size() > k) {
            heap.dequeueLong(); // Remove the largest value

            // Update the limit so we can fast-fail future large hashes
            long newLimit = heap.firstLong();
            if (isDelete) {
                deleteThetaLimit = newLimit;
            } else {
                insertThetaLimit = newLimit;
            }
        }
    }

    /**
     * Serializes the theta sketch to binary format for storage in component metadata.
     * <p>
     * Format: [K (int)] [insertHeap size + values] [deleteHeap size + values]
     * </p>
     *
     * @return serialized sketch as a value reference
     * @throws IOException if serialization fails
     */
    public IValueReference serialize() throws IOException {
        serializedTheta.reset();
        DataOutput out = serializedTheta.getDataOutput();
        out.writeInt(k);
        serializePQ(insertHeap, out);
        serializePQ(deleteHeap, out);
        return serializedTheta;
    }

    /**
     * Deserializes a theta sketch from component metadata into a ComponentStats object.
     * <p>
     * The samples are sorted in ascending order after deserialization, so the last
     * element in each array represents the theta threshold for that sample set.
     * </p>
     *
     * @param thetaStorage the serialized sketch data
     * @return ComponentStats containing the insert/delete samples and K value
     */
    public static ThetaEstimator.ComponentStats deserialize(ArrayBackedValueStorage thetaStorage) {
        int start = thetaStorage.getStartOffset();
        byte[] thetaBytes = thetaStorage.getByteArray();
        int K = IntegerPointable.getInteger(thetaBytes, start);
        start += Integer.BYTES;

        int insertCount = IntegerPointable.getInteger(thetaBytes, start);
        start += Integer.BYTES;

        long[] insertSamples = new long[insertCount];
        for (int i = 0; i < insertCount; i++) {
            insertSamples[i] = LongPointable.getLong(thetaBytes, start);
            start += Long.BYTES;
        }

        int deleteCount = IntegerPointable.getInteger(thetaBytes, start);
        start += Integer.BYTES;

        long[] deleteSamples = new long[deleteCount];
        for (int i = 0; i < deleteCount; i++) {
            deleteSamples[i] = LongPointable.getLong(thetaBytes, start);
            start += Long.BYTES;
        }

        Arrays.sort(insertSamples);
        Arrays.sort(deleteSamples);
        return new ThetaEstimator.ComponentStats(insertSamples, deleteSamples, K);
    }

    public void serializePQ(LongPriorityQueue heap, DataOutput out) throws IOException {
        int size = heap.size();
        out.writeInt(size);

        // Dequeue gives Largest -> Smallest
        long[] temp = new long[size];
        for (int i = 0; i < size; i++) {
            temp[i] = heap.dequeueLong();
        }

        // We want Smallest -> Largest (Ascending)
        // temp is [Max, ... Min].
        // We can write it backwards.
        for (int i = size - 1; i >= 0; i--) {
            out.writeLong(temp[i]);
        }
    }

    @Override
    public IValueReference serializeSamplingMetadata() throws IOException {
        return serialize();
    }
}
