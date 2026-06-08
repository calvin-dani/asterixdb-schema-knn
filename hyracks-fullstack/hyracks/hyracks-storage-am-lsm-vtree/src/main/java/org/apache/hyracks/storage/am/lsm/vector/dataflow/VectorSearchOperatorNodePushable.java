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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeDiskComponent;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.am.vector.utils.VectorUtils;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Runtime operator for vector index search (ANN search).
 * Extends IndexSearchOperatorNodePushable which handles the heavy lifting:
 * - Opening/closing indexes
 * - Frame/tuple iteration
 * - Output buffering
 * - Transaction callbacks
 *
 * This class implements the schema-agnostic pattern using IVTreeBinaryAccessor
 * to abstract over different vector serialization formats (e.g., AOrderedList).
 *
 * This class only needs to implement:
 * 1. createSearchPredicate() - Create VectorPointPredicate with accessor
 * 2. resetSearchPredicate() - Set query tuple reference (no deserialization!)
 * 3. getFieldCount() - Return number of output fields
 * 4. addAdditionalIndexAccessorParams() - Add vector-specific params (if any)
 */
public class VectorSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final int BFS_PRINT_MAX_PAGES = 16;
    private static final int BFS_PRINT_MAX_TUPLES = 64;
    private static final int MAX_ROUTING_EMBEDDING_DIMENSION = 32768;

    // Field indexes in input tuple: [query_vector_field, k_field, metric_field]
    protected final int[] queryFields;

    // Factory for creating vector accessors (passed from AsterixDB layer)
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;

    // Factory for creating distance functions (passed from AsterixDB layer, wraps VectorDistanceArrCalculation)
    protected final java.io.Serializable distanceFunctionFactory;

    // Tuple reference for extracting query parameters
    protected PermutingFrameTupleReference queryParamsTuple;

    // Reusable pointable for extracting string values
    private final UTF8StringPointable stringPointable = new UTF8StringPointable();

    // Factory for creating tuple filters for INCLUDE field predicates (e.g., year > 2000)
    // When set, the filter is pushed down to the cursor level for proper K counting
    protected final ITupleFilterFactory tupleFilterFactory;

    // The actual tuple filter, created from the factory
    protected ITupleFilter tupleFilter;

    // Search approach: 0 = naive (LSMVTreeSearchCursor), 1 = optimized (LSMVTreePrunedTopKSearchCursor)
    // Compile-time constant passed from descriptor for cursor selection at open() time
    protected final int searchApproach;

    // Number of secondary key fields before PKs in data tuples (2 for non-quantized, 4 for quantized)
    protected final int numSecondaryKeys;

    // Multiplier for candidate limit: K * kMultiplier candidates sent to PK for reranking
    protected final int kMultiplier;

    /** Epsilon from vector index metadata (default 0.3 when absent in catalog). */
    protected final double indexEpsilon;

    /** When true, dump capped BFS view of static structure on first query tuple. */
    protected final boolean printTreeOnSearch;

    private final int computePartition;

    private boolean treePrinted;

    public VectorSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            int[] queryFields, IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput,
            ISearchOperationCallbackFactory searchCallbackFactory, ITupleProjectorFactory projectorFactory,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, java.io.Serializable distanceFunctionFactory,
            int[][] partitionsMap, ITupleFilterFactory tupleFilterFactory, int searchApproach, int numSecondaryKeys,
            int kMultiplier, double indexEpsilon, boolean printTreeOnSearch) throws HyracksDataException {
        // Call parent constructor
        // Note: Vector search doesn't need min/max filter fields (pass null)
        // Note: Vector search doesn't need missing writer (pass null for retainMissing)
        // Note: No index filter for now (pass false for appendIndexFilter)
        // Note: We pass null for tupleFilterFactory to parent - we handle filtering in cursor
        // Note: No output limit for now (pass -1)
        // Note: No search callback result needed (pass false)
        super(ctx, inputRecDesc, partition, null, // minFilterFieldIndexes
                null, // maxFilterFieldIndexes
                indexHelperFactory, retainInput, false, // retainMissing
                null, // nonMatchWriterFactory
                searchCallbackFactory, false, // appendIndexFilter
                null, // nonFilterWriterFactory
                null, // tupleFilterFactory - we handle this at cursor level, not operator level
                -1, // outputLimit
                false, // appendOpCallbackProceedResult
                null, // searchCallbackProceedResultFalseValue
                null, // searchCallbackProceedResultTrueValue
                projectorFactory, // ← PKOnlyTupleProjectorFactory (extracts only PK fields)
                null, // tuplePartitionerFactory
                partitionsMap);

        this.queryFields = queryFields;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.tupleFilterFactory = tupleFilterFactory;
        this.searchApproach = searchApproach;
        this.numSecondaryKeys = numSecondaryKeys;
        this.kMultiplier = kMultiplier;
        this.indexEpsilon = indexEpsilon;
        this.printTreeOnSearch = printTreeOnSearch;
        this.computePartition = partition;

        // Setup permuting tuple reference to extract query parameters
        if (queryFields != null && queryFields.length > 0) {
            queryParamsTuple = new PermutingFrameTupleReference();
            queryParamsTuple.setFieldPermutation(queryFields);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();

        // Create tuple filter from factory if available
        // This filter is pushed down to the cursor level for proper K counting
        if (tupleFilterFactory != null) {
            tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
        }
    }

    @Override
    protected ISearchPredicate createSearchPredicate(IIndex index) {
        // Create simple marker predicate
        // The actual query vector is passed via IIndexAccessParameters in addAdditionalIndexAccessorParams()
        return new VTreeSearchPredicate();
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        // Update queryParamsTuple to point to current input tuple
        if (queryParamsTuple != null) {
            queryParamsTuple.reset(accessor, tupleIndex);

            // Update predicate with current tuple reference
            // Following RTree pattern: predicate holds reference, updated per-tuple
            VTreeSearchPredicate vectorPred = (VTreeSearchPredicate) searchPred;
            vectorPred.setQueryTuple(queryParamsTuple);
            vectorPred.setQueryFieldIndex(0); // Field 0 is the vector field
            vectorPred.setPkStartField(numSecondaryKeys);

            // Extract K value from field 1 if available
            if (queryFields.length > 1) {
                // K is at field index 1 in queryParamsTuple (after permutation)
                // Skip type tag (1 byte) to get the actual integer value
                int k = IntegerPointable.getInteger(queryParamsTuple.getFieldData(1),
                        queryParamsTuple.getFieldStart(1) + 1 // +1 to skip type tag
                );
                vectorPred.setK(k);
            }

            // Extract distance metric from field index 2 (after query vector at 0, k at 1)
            if (queryFields != null && queryFields.length > 2) {
                String distanceMetric = extractDistanceMetricFromTuple(queryParamsTuple, 2);
                vectorPred.setDistanceMetric(distanceMetric);
            }

            // Extract min_probe_fraction from field 3 (double, +1 to skip type tag)
            // Fraction of leaf clusters to probe (0.0-1.0). 0 means use default (0.1).
            if (queryFields.length > 3) {
                double minProbeFraction = DoublePointable.getDouble(queryParamsTuple.getFieldData(3),
                        queryParamsTuple.getFieldStart(3) + 1);
                if (minProbeFraction > 0.0) {
                    vectorPred.setMinProbeFraction(minProbeFraction);
                }
            }

            // Extract k_multiplier from field 4 (int, +1 to skip type tag)
            if (queryFields.length > 4) {
                int queryKMult = IntegerPointable.getInteger(queryParamsTuple.getFieldData(4),
                        queryParamsTuple.getFieldStart(4) + 1);
                vectorPred.setKMultiplier(Math.max(1, queryKMult));
            }

            // Set tuple filter for INCLUDE field predicates (e.g., year > 2000)
            // This filter is applied at cursor level for proper K counting
            if (tupleFilter != null) {
                vectorPred.setTupleFilter(tupleFilter);
            }

            // Session config compiler.vector.kmultiplier overrides query arg if set (kMultiplier > 1 from constructor)
            if (kMultiplier > 1) {
                vectorPred.setKMultiplier(kMultiplier);
            }

            vectorPred.setEpsilon(indexEpsilon);

            maybePrintStaticStructureOnSearch();
        }
    }

    private void maybePrintStaticStructureOnSearch() {
        if (!printTreeOnSearch || treePrinted || computePartition != 0 || indexes == null || indexes.length == 0) {
            return;
        }
        try {
            IIndex index = indexes[0];
            if (!(index instanceof LSMVTree)) {
                return;
            }
            LSMVTreeDiskComponent component = ((LSMVTree) index).getStaticStructure();
            IVTreeBinaryAccessor acc = vectorAccessorFactory.createAccessor();
            acc.reset(queryParamsTuple.getFieldData(0), queryParamsTuple.getFieldStart(0),
                    queryParamsTuple.getFieldLength(0));
            printStaticStructureBFS(component, acc.getVector());
            treePrinted = true;
        } catch (Throwable t) {
            LOGGER.warn("BFS structure print on search failed (non-fatal)", t);
        }
    }

    private void printStaticStructureBFS(LSMVTreeDiskComponent component, double[] queryVector)
            throws HyracksDataException {
        VTree vcTree = component.getIndex();
        IBufferCache bufferCache = vcTree.getBufferCache();
        int fileId = vcTree.getFileId();
        int rootPageId = vcTree.getRootPageId();
        ITreeIndexFrameFactory interiorFrameFactory = vcTree.getInteriorFrameFactory();
        ITreeIndexFrameFactory leafFrameFactory = vcTree.getLeafFrameFactory();

        if (bufferCache == null || interiorFrameFactory == null || leafFrameFactory == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Required components are not initialized");
        }

        final int printLimit = 8;

        Queue<int[]> queue = new ArrayDeque<>();
        Set<Integer> visited = new HashSet<>();
        queue.add(new int[] { rootPageId, 0 });
        visited.add(rootPageId);

        int visitedPages = 0;
        long processedTuples = 0L;
        boolean truncated = false;

        while (!queue.isEmpty()) {
            if (visitedPages >= BFS_PRINT_MAX_PAGES) {
                truncated = true;
                break;
            }
            int[] entry = queue.poll();
            int currentPageId = entry[0];
            int level = entry[1];

            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
            try {
                page.acquireReadLatch();

                IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                boolean isLeaf = leafFrame.isLeaf();

                if (isLeaf) {
                    LOGGER.info("=== LEVEL {} | PAGE {} | TYPE: LEAF ===", level, currentPageId);
                    int tupleCount = leafFrame.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        if (processedTuples >= BFS_PRINT_MAX_TUPLES) {
                            truncated = true;
                            break;
                        }
                        try {
                            ITreeIndexTupleReference frameTuple = leafFrame.createTupleReference();
                            frameTuple.resetByTupleIndex(leafFrame, i);

                            int cid = IntegerPointable.getInteger(frameTuple.getFieldData(0),
                                    frameTuple.getFieldStart(0));
                            double[] centroid = deserializeRoutingEmbedding(frameTuple);
                            int metadataPtr = leafFrame.getMetadataPagePointer(i);
                            int centroidId = leafFrame.getCentroidId(i);

                            String centroidStr = formatCentroid(centroid, printLimit);
                            String distStr = computeDistanceString(queryVector, centroid);

                            LOGGER.info("tuple={} | cid={} | centroidId={} | centroid={} | dist={} | metadata={}", i,
                                    cid, centroidId, centroidStr, distStr, metadataPtr);
                            processedTuples++;
                        } catch (Exception e) {
                            // skip bad tuple
                        }
                    }

                    if (!truncated) {
                        boolean hasOverflow = leafFrame.getOverflowFlagBit();
                        if (hasOverflow) {
                            int nextLeaf = leafFrame.getNextLeaf();
                            if (visited.add(nextLeaf)) {
                                queue.add(new int[] { nextLeaf, level });
                            }
                        }
                    }

                } else {
                    IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);
                    LOGGER.info("=== LEVEL {} | PAGE {} | TYPE: INTERIOR ===", level, currentPageId);
                    int tupleCount = interiorFrame.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        if (processedTuples >= BFS_PRINT_MAX_TUPLES) {
                            truncated = true;
                            break;
                        }
                        try {
                            ITreeIndexTupleReference frameTuple = interiorFrame.createTupleReference();
                            frameTuple.resetByTupleIndex(interiorFrame, i);

                            int cid = IntegerPointable.getInteger(frameTuple.getFieldData(0),
                                    frameTuple.getFieldStart(0));
                            double[] centroid = deserializeRoutingEmbedding(frameTuple);
                            int childPageId = interiorFrame.getChildPageId(i);

                            String centroidStr = formatCentroid(centroid, printLimit);
                            String distStr = computeDistanceString(queryVector, centroid);

                            LOGGER.info("tuple={} | cid={} | centroid={} | dist={} | child={}", i, cid, centroidStr,
                                    distStr, childPageId);
                            processedTuples++;

                            if (childPageId != -1 && visited.add(childPageId)) {
                                queue.add(new int[] { childPageId, level + 1 });
                            }
                        } catch (Exception e) {
                            // skip bad tuple
                        }
                    }

                    if (!truncated) {
                        boolean hasOverflow = interiorFrame.getOverflowFlagBit();
                        if (hasOverflow) {
                            int nextPage = interiorFrame.getNextPage();
                            if (visited.add(nextPage)) {
                                queue.add(new int[] { nextPage, level });
                            }
                        }
                    }
                }

                visitedPages++;
                if (truncated) {
                    break;
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }

        if (truncated) {
            LOGGER.info("=== BFS PRINT TRUNCATED | pages={} | tuples={} (maxPages={} maxTuples={}) ===", visitedPages,
                    processedTuples, BFS_PRINT_MAX_PAGES, BFS_PRINT_MAX_TUPLES);
        } else {
            LOGGER.info("=== BFS PRINT COMPLETE | pages={} | tuples={} ===", visitedPages, processedTuples);
        }
    }

    private double[] deserializeRoutingEmbedding(ITreeIndexTupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() < 2) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "Routing tuple has fewer than 2 fields (fieldCount=" + tuple.getFieldCount() + ")");
        }
        byte[] data = tuple.getFieldData(1);
        int start = tuple.getFieldStart(1);
        int length = tuple.getFieldLength(1);
        if (length < 4) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Embedding field too short: " + length);
        }
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data, start, length));
            int len = dis.readInt();
            if (len <= 0 || len > MAX_ROUTING_EMBEDDING_DIMENSION) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "Invalid routing embedding length: " + len + " (max " + MAX_ROUTING_EMBEDDING_DIMENSION + ")");
            }
            long requiredBytes = 4L + (long) len * 8;
            if (length < requiredBytes) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "Embedding field length " + length + " < declared " + requiredBytes + " bytes");
            }
            double[] array = new double[len];
            for (int i = 0; i < len; i++) {
                array[i] = dis.readDouble();
            }
            return array;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private String formatCentroid(double[] centroid, int limit) {
        if (centroid == null) {
            return "null";
        }
        int n = centroid.length;
        int toPrint = Math.min(limit, n);
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < toPrint; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(String.format("%.4f", centroid[i]));
        }
        sb.append(']');
        if (n > toPrint) {
            sb.append(" (+").append(n - toPrint).append(" more)");
        }
        return sb.toString();
    }

    private String computeDistanceString(double[] queryVector, double[] centroid) {
        if (queryVector == null || centroid == null) {
            return "NA";
        }
        if (centroid.length != queryVector.length) {
            return "NA (dim mismatch)";
        }
        double d = VectorUtils.calculateEuclideanDistance(queryVector, centroid);
        return String.format("%.4f", d);
    }

    /**
     * Extract distance metric string from tuple field.
     *
     * @param tuple Input tuple containing the distance metric
     * @param fieldIndex Field index containing the distance metric string
     * @return Distance metric string, or "euclidean" as default
     */
    private String extractDistanceMetricFromTuple(ITupleReference tuple, int fieldIndex) {
        try {
            if (tuple.getFieldCount() > fieldIndex) {
                byte[] fieldData = tuple.getFieldData(fieldIndex);
                int fieldStart = tuple.getFieldStart(fieldIndex);
                int fieldLength = tuple.getFieldLength(fieldIndex);

                // Reset pointable to read the string value
                stringPointable.set(fieldData, fieldStart + 1, fieldLength);
                return stringPointable.toString();
            }
        } catch (Exception e) {
            // If extraction fails, default to euclidean
            LOGGER.trace("Failed to extract distance metric from tuple, defaulting to euclidean: {}", e.getMessage());
        }
        return "euclidean"; // Default fallback
    }

    @Override
    protected int getFieldCount(IIndex index) {
        // For vector index, we only output primary keys (no secondary keys/embeddings)
        // The number of fields is determined by the dataset's primary key count
        //
        // TODO: Get actual PK count from index metadata
        // For now, assume single PK field (common case)
        return 1;

        // When implementing properly:
        // LSMVTree lsmvTree = (LSMVTree) index;
        // return lsmvTree.getNumPrimaryKeys();  // Or similar method
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) {
        // Store the vector accessor factory in parameters
        // The VTree accessor will extract the query vector from the predicate during search()
        // This maintains layer separation: extraction happens in storage layer using the factory
        iap.getParameters().put(HyracksConstants.VECTOR_QUERY, vectorAccessorFactory);

        // Store the K field index (field 1 in queryFields: [vector, k, metric])
        // The cursor will extract K from the query tuple using this index
        if (queryFields != null && queryFields.length > 1) {
            iap.getParameters().put(HyracksConstants.VECTOR_K, 1); // K is at field index 1
        }

        // Store the distance function factory in parameters
        // The VTree will use this factory to create IVTreeDistanceFunction implementations
        // that wrap VectorDistanceArrCalculation from AsterixDB
        iap.getParameters().put(HyracksConstants.VECTOR_DISTANCE_FUNCTION_FACTORY, distanceFunctionFactory);

        // Set cursor selection flags based on compile-time searchApproach constant
        // 0 = naive streaming (LSMVTreeSearchCursor)
        // 1 = optimized bidirectional (LSMVTreePrunedTopKSearchCursor)
        // 2 = optimized bidirectional with inline filtering (LSMVTreePrunedTopKSearchCursor + ITupleFilter)
        // 3 = naive blocked (LSMVTreePrunedTopKSearchCursorNaive - top-K window, quantized distance, no pruning)
        // 4 = index-driven KNN (LSMVTreePrunedTopKSearchCursor + SequentialClusterSelectionStrategy)
        if (searchApproach == 1 || searchApproach == 2 || searchApproach == 4) {
            iap.getParameters().put(HyracksConstants.USE_OPTIMIZED_SEARCH, Boolean.TRUE);
        }
        if (searchApproach == 4) {
            iap.getParameters().put(HyracksConstants.USE_SEQUENTIAL_SCAN, Boolean.TRUE);
        }
        if (searchApproach == 3) {
            iap.getParameters().put(HyracksConstants.USE_NAIVE_BLOCKED_SEARCH, Boolean.TRUE);
        }

        // Pass task context for spillable top-K buffer (follows inverted index pattern)
        iap.getParameters().put(HyracksConstants.HYRACKS_TASK_CONTEXT, ctx);
    }
}
