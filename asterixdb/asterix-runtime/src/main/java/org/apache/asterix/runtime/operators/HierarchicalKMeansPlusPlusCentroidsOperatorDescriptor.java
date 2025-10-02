/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
package org.apache.asterix.runtime.operators;

import static org.apache.asterix.om.types.BuiltinType.ADOUBLE;
import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;
import static org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

/**
 * Enhanced version of LocalKMeansPlusPlusCentroidsOperatorDescriptor that maintains
 * hierarchical cluster relationships with parent-child associations.
 */
public final class HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor extends AbstractOperatorDescriptor {

    /**
     * Helper class to hold centroid data with its parent information for sorting
     */
    private static class CentroidWithParent {
        final double[] centroid;
        @SuppressWarnings("unused")
        final HierarchicalClusterId clusterId;
        final HierarchicalClusterId parentId;

        CentroidWithParent(double[] centroid, HierarchicalClusterId clusterId, HierarchicalClusterId parentId) {
            this.centroid = centroid;
            this.clusterId = clusterId;
            this.parentId = parentId;
        }
    }

    /**
     * Result class to hold Lloyd's algorithm results with assignments
     */
    private static class LloydResult {
        final List<double[]> centroids;
        @SuppressWarnings("unused")
        final int[] assignments;

        LloydResult(List<double[]> centroids, int[] assignments) {
            this.centroids = centroids;
            this.assignments = assignments;
        }
    }

    private static final long serialVersionUID = 1L;

    private final UUID sampleUUID;
    private final UUID centroidsUUID;

    private IScalarEvaluatorFactory args;
    private int K;
    private int maxScalableKmeansIter;
    private HierarchicalClusterTree.OutputMode outputMode;

    public HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor rDesc, UUID sampleUUID, UUID centroidsUUID, IScalarEvaluatorFactory args, int K,
            int maxScalableKmeansIter) {
        this(spec, rDesc, sampleUUID, centroidsUUID, args, K, maxScalableKmeansIter,
                HierarchicalClusterTree.OutputMode.LEVEL_BY_LEVEL);
    }

    public HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor rDesc, UUID sampleUUID, UUID centroidsUUID, IScalarEvaluatorFactory args, int K,
            int maxScalableKmeansIter, HierarchicalClusterTree.OutputMode outputMode) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.sampleUUID = sampleUUID;
        this.centroidsUUID = centroidsUUID;
        this.args = args;
        this.K = K;
        this.maxScalableKmeansIter = maxScalableKmeansIter;
        this.outputMode = outputMode;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StoreCentroidsActivity sa = new StoreCentroidsActivity(new ActivityId(odId, 0));
        FindCandidatesActivity ca = new FindCandidatesActivity(new ActivityId(odId, 1));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ca);
        builder.addTargetEdge(0, ca, 0);

        builder.addBlockingEdge(sa, ca);
        builder.addTargetEdge(0, ca, 0);
    }

    protected class StoreCentroidsActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private FrameTupleAccessor fta;
        private FrameTupleReference tuple;

        protected StoreCentroidsActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                IScalarEvaluator eval;
                IPointable inputVal;
                HierarchicalCentroidsState hierarchicalState;
                boolean first;
                private MaterializerTaskState materializedSample;
                KMeansUtils kMeansUtils;

                @Override
                public void open() throws HyracksDataException {
                    materializedSample = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(sampleUUID, partition));
                    materializedSample.open(ctx);
                    hierarchicalState = new HierarchicalCentroidsState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(centroidsUUID, partition));
                    eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                    inputVal = new VoidPointable();
                    fta = new FrameTupleAccessor(outRecDescs[0]);
                    tuple = new FrameTupleReference();
                    first = true;
                    kMeansUtils = new KMeansUtils(new VoidPointable(), new ArrayBackedValueStorage());
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    fta.reset(buffer);
                    if (first) {
                        tuple.reset(fta, 0);
                        eval.evaluate(tuple, inputVal);
                        ListAccessor listAccessorConstant = new ListAccessor();
                        if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                .isListType()) {
                            // Handle unsupported type
                        }
                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                        try {
                            double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);
                            // Create root level cluster ID (level 0, cluster 0)
                            HierarchicalClusterId rootClusterId = new HierarchicalClusterId(0, 0);
                            hierarchicalState.addCentroid(0, rootClusterId, point);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        first = false;
                    }
                    materializedSample.appendFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (hierarchicalState != null) {
                        ctx.setStateObject(hierarchicalState);
                    }
                    if (materializedSample != null) {
                        materializedSample.close();
                        ctx.setStateObject(materializedSample);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    protected class FindCandidatesActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected FindCandidatesActivity(ActivityId id) {
            super(id);
        }

        @Override
        @SuppressWarnings("squid:S1188")
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    // Get file reader for written samples
                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader in = sampleState.creatReader();
                    try {
                        // Get hierarchical state
                        PartitionedUUID centroidsKey = new PartitionedUUID(centroidsUUID, partition);
                        HierarchicalCentroidsState hierarchicalState =
                                (HierarchicalCentroidsState) ctx.getStateObject(centroidsKey);

                        FrameTupleAccessor fta;
                        FrameTupleReference tuple;
                        IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                        IPointable inputVal = new VoidPointable();
                        IPointable tempVal = new VoidPointable();
                        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                        KMeansUtils KMeansUtils = new KMeansUtils(tempVal, storage);
                        fta = new FrameTupleAccessor(outRecDescs[0]);
                        tuple = new FrameTupleReference();
                        VSizeFrame vSizeFrame = new VSizeFrame(ctx);
                        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                        ListAccessor listAccessorConstant = new ListAccessor();

                        writer.open();

                        // Create hierarchical cluster index writer
                        HierarchicalClusterIndexWriter indexWriter = new HierarchicalClusterIndexWriter(ctx, partition);

                        // Build hierarchical clustering with parent-child relationships
                        if (outputMode == HierarchicalClusterTree.OutputMode.COMPLETE_TREE) {
                            buildCompleteTreeClustering(ctx, in, fta, tuple, eval, inputVal, listAccessorConstant,
                                    KMeansUtils, vSizeFrame, appender, partition, indexWriter);
                        } else {
                            buildLevelByLevelClustering(ctx, in, hierarchicalState, fta, tuple, eval, inputVal,
                                    listAccessorConstant, KMeansUtils, vSizeFrame, appender, partition, indexWriter);
                        }

                        // Write the hierarchical cluster index to side file
                        indexWriter.writeIndexToSideFile();

                        // Also write to static location for manual management
                        indexWriter.writeIndex();

                        // Print simple summary
                        System.out.println("Hierarchical cluster index created with "
                                + indexWriter.getClusterLevels().size() + " levels");

                        FrameUtils.flushFrame(appender.getBuffer(), writer);

                    } catch (Throwable e) {
                        writer.fail();
                        throw new RuntimeException(e);
                    } finally {
                        in.close();
                        writer.close();
                    }
                }

                /**
                 * Builds complete tree first, then assigns BFS-based IDs and outputs
                 * This is more efficient than sorting as it avoids expensive sorting operations
                 */
                private void buildCompleteTreeClustering(IHyracksTaskContext ctx, GeneratedRunFileReader in,
                        FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, VSizeFrame vSizeFrame,
                        FrameTupleAppender appender, int partition, HierarchicalClusterIndexWriter indexWriter)
                        throws HyracksDataException, IOException {

                    System.err.println("Building complete tree with BFS-based ID assignment...");

                    // Create tree structure
                    HierarchicalClusterTree tree = new HierarchicalClusterTree();

                    // Build hierarchy levels using the original memory-efficient approach
                    int currentLevel = 0;
                    int currentK = K;
                    Random rand = new Random();
                    int maxKMeansIterations = 20;

                    // Start with the original data (not loaded into memory)
                    List<double[]> currentLevelCentroids = new ArrayList<>();

                    // First level: Use original probabilistic K-means++ on data
                    currentLevelCentroids = performMemoryEfficientKMeansPlusPlus(ctx, in, fta, tuple, eval, inputVal,
                            listAccessorConstant, kMeansUtils, vSizeFrame, currentK, rand, maxKMeansIterations,
                            partition);

                    if (currentLevelCentroids.isEmpty()) {
                        System.err.println("No centroids available for tree building");
                        return;
                    }

                    // Add Level 0 centroids (children) to tree - these are the leaf nodes
                    List<HierarchicalClusterTree.TreeNode> currentLevelNodes = new ArrayList<>();
                    for (int i = 0; i < currentLevelCentroids.size(); i++) {
                        HierarchicalClusterTree.TreeNode leafNode =
                                tree.addChild(null, currentLevelCentroids.get(i), currentLevel);
                        currentLevelNodes.add(leafNode);
                    }

                    System.err.println("Starting tree building with " + currentLevelCentroids.size()
                            + " centroids at level " + currentLevel);

                    currentLevel++;
                    currentK = Math.max(1, currentK / 2);

                    // Build subsequent levels until centroids fit into one frame
                    while (currentLevelCentroids.size() > 1 && currentK > 0) {
                        System.err.println("Building level " + currentLevel + " with " + currentLevelCentroids.size()
                                + " centroids, target K = " + currentK);

                        // Use scalable K-means++ on centroids from previous level
                        List<double[]> nextLevelCentroids = performScalableKMeansPlusPlusOnCentroids(
                                currentLevelCentroids, currentK, rand, kMeansUtils, maxKMeansIterations);

                        // Use Lloyd's algorithm approach to assign current level centroids (children) to next level centroids (parents)
                        int[] parentAssignments = assignCentroidsToParents(currentLevelCentroids, nextLevelCentroids);

                        // Add next level centroids (parents) to tree
                        List<HierarchicalClusterTree.TreeNode> nextLevelNodes = new ArrayList<>();
                        for (int i = 0; i < nextLevelCentroids.size(); i++) {
                            HierarchicalClusterTree.TreeNode parentNode =
                                    tree.addChild(null, nextLevelCentroids.get(i), currentLevel);
                            nextLevelNodes.add(parentNode);
                        }

                        // Add current level centroids (children) to their parent nodes
                        for (int j = 0; j < currentLevelCentroids.size(); j++) {
                            int parentIndex = parentAssignments[j];
                            HierarchicalClusterTree.TreeNode parentNode = nextLevelNodes.get(parentIndex);
                            // Create new child node under the parent
                            tree.addChild(parentNode, currentLevelCentroids.get(j), currentLevel - 1);
                        }

                        // Test if all centroids from this level can fit in one frame
                        boolean canFitInOneFrame = testTreeLevelFitsInFrame(ctx, nextLevelNodes, appender);

                        if (canFitInOneFrame) {
                            System.err.println(
                                    "Level " + currentLevel + " centroids fit in one frame! Stopping tree building.");
                            break;
                        }

                        // Prepare for next iteration
                        currentLevelCentroids = nextLevelCentroids;
                        currentLevelNodes = nextLevelNodes;
                        currentLevel++;
                        currentK = Math.max(1, currentK / 2);
                    }

                    System.err.println("Tree building complete. Assigning BFS-based IDs...");

                    // Assign BFS-based IDs to all nodes (no sorting needed!)
                    tree.assignBFSIds();

                    // Print tree structure
                    tree.printTree();

                    // Output all nodes in BFS order
                    outputCompleteTree(tree, appender, indexWriter, partition);

                    System.err.println("Complete tree output finished with " + tree.getTotalNodes() + " total nodes");
                }

                /**
                 * Builds hierarchical clustering level by level without sorting
                 * Assigns cluster IDs as we build each level and maintains parent-child relationships
                 */
                private void buildLevelByLevelClustering(IHyracksTaskContext ctx, GeneratedRunFileReader in,
                        HierarchicalCentroidsState hierarchicalState, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, FrameTupleAppender appender, int partition,
                        HierarchicalClusterIndexWriter indexWriter) throws HyracksDataException, IOException {

                    System.err.println("Building level-by-level clustering without sorting...");

                    // Build hierarchy levels using the original memory-efficient approach
                    int currentLevel = 0;
                    int currentK = K;
                    Random rand = new Random();
                    int maxKMeansIterations = 20;

                    // Start with the original data (not loaded into memory)
                    List<double[]> currentLevelCentroids = new ArrayList<>();

                    // First level: Use original probabilistic K-means++ on data
                    currentLevelCentroids = performMemoryEfficientKMeansPlusPlus(ctx, in, fta, tuple, eval, inputVal,
                            listAccessorConstant, kMeansUtils, vSizeFrame, currentK, rand, maxKMeansIterations,
                            partition);

                    // Store and output first level centroids (leaf nodes)
                    for (int i = 0; i < currentLevelCentroids.size(); i++) {
                        HierarchicalClusterId clusterId = new HierarchicalClusterId(currentLevel, i);
                        hierarchicalState.addCentroid(currentLevel, clusterId, currentLevelCentroids.get(i));
                    }

                    // Add first level to index
                    indexWriter.addClusterLevel(currentLevel, hierarchicalState.getCentroidsAtLevel(currentLevel));

                    // Output first level centroids immediately
                    outputLevelCentroids(hierarchicalState, currentLevel, appender);

                    currentLevel++;
                    currentK = Math.max(1, currentK / 2);

                    // Build subsequent levels until centroids fit into one frame
                    if (currentLevelCentroids.isEmpty()) {
                        System.err.println("No centroids available for hierarchy building");
                        return;
                    }

                    System.err.println("Starting hierarchy building with " + currentLevelCentroids.size()
                            + " centroids at level " + currentLevel);

                    while (currentLevelCentroids.size() > 1 && currentK > 0) {
                        System.err.println("Building level " + currentLevel + " with " + currentLevelCentroids.size()
                                + " centroids, target K = " + currentK);

                        // Use scalable K-means++ on centroids from previous level
                        List<double[]> nextLevelCentroids = performScalableKMeansPlusPlusOnCentroids(
                                currentLevelCentroids, currentK, rand, kMeansUtils, maxKMeansIterations);

                        // Use Lloyd's algorithm to assign current level centroids (children) to next level centroids (parents)
                        int[] parentAssignments = assignCentroidsToParents(currentLevelCentroids, nextLevelCentroids);

                        // Store next level centroids (parents) with sequential cluster IDs
                        for (int i = 0; i < nextLevelCentroids.size(); i++) {
                            HierarchicalClusterId parentId = new HierarchicalClusterId(currentLevel, i);
                            hierarchicalState.addCentroid(currentLevel, parentId, nextLevelCentroids.get(i));
                        }

                        // Store current level centroids (children) with their parent relationships
                        // We need to clear the previous level and re-add with parent relationships
                        hierarchicalState.clearLevel(currentLevel - 1);

                        for (int j = 0; j < currentLevelCentroids.size(); j++) {
                            int parentIndex = parentAssignments[j];
                            HierarchicalClusterId parentId = new HierarchicalClusterId(currentLevel, parentIndex);

                            // Create cluster ID with parent relationship
                            HierarchicalClusterId childClusterId =
                                    new HierarchicalClusterId(currentLevel - 1, j, parentId.getClusterId());

                            // Add child centroid with parent relationship
                            hierarchicalState.addCentroid(currentLevel - 1, childClusterId,
                                    currentLevelCentroids.get(j));
                        }

                        // Add this level to index
                        indexWriter.addClusterLevel(currentLevel, hierarchicalState.getCentroidsAtLevel(currentLevel));

                        // Add the child level (with parent relationships) to index
                        indexWriter.addClusterLevel(currentLevel - 1,
                                hierarchicalState.getCentroidsAtLevel(currentLevel - 1));

                        // Output this level centroids immediately
                        outputLevelCentroids(hierarchicalState, currentLevel, appender);

                        // Test if all centroids from this level can fit in one frame
                        boolean canFitInOneFrame =
                                testCentroidsFitInFrame(ctx, hierarchicalState, currentLevel, appender);

                        if (canFitInOneFrame) {
                            System.err.println(
                                    "Level " + currentLevel + " centroids fit in one frame! Stopping hierarchy.");
                            break;
                        }

                        // Prepare for next iteration
                        currentLevelCentroids = nextLevelCentroids;
                        currentLevel++;
                        currentK = Math.max(1, currentK / 2);
                    }

                    System.err.println("Final hierarchy: " + currentLevel + " levels with "
                            + currentLevelCentroids.size() + " centroids at top level");

                    // Add final hierarchy structure to index
                    Map<String, Object> structureInfo = new HashMap<>();
                    structureInfo.put("final_level", currentLevel);
                    structureInfo.put("final_centroid_count", currentLevelCentroids.size());
                    structureInfo.put("stopped_reason", "centroids_fit_in_frame");

                    int totalCentroids = 0;
                    for (int level : hierarchicalState.getLevels()) {
                        totalCentroids += hierarchicalState.getCentroidsAtLevel(level).size();
                    }

                    indexWriter.addHierarchyStructure(currentLevel + 1, totalCentroids, structureInfo);

                    // Print hierarchy information
                    printHierarchyInfo(hierarchicalState, partition);
                }

                /**
                 * Builds the hierarchical clustering structure with parent-child relationships
                 * using memory-efficient probabilistic selection (not loading all data points)
                 */
                @SuppressWarnings("unused")
                private void buildHierarchicalClustering(IHyracksTaskContext ctx, GeneratedRunFileReader in,
                        HierarchicalCentroidsState hierarchicalState, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, FrameTupleAppender appender, int partition,
                        HierarchicalClusterIndexWriter indexWriter) throws HyracksDataException, IOException {

                    // Build hierarchy levels using the original memory-efficient approach
                    int currentLevel = 0;
                    int currentK = K;
                    Random rand = new Random();
                    int maxKMeansIterations = 20;

                    // Start with the original data (not loaded into memory)
                    List<double[]> currentLevelCentroids = new ArrayList<>();

                    // First level: Use original probabilistic K-means++ on data
                    currentLevelCentroids = performMemoryEfficientKMeansPlusPlus(ctx, in, fta, tuple, eval, inputVal,
                            listAccessorConstant, kMeansUtils, vSizeFrame, currentK, rand, maxKMeansIterations,
                            partition);

                    // Store and output first level centroids
                    for (int i = 0; i < currentLevelCentroids.size(); i++) {
                        HierarchicalClusterId clusterId = new HierarchicalClusterId(currentLevel, i);
                        hierarchicalState.addCentroid(currentLevel, clusterId, currentLevelCentroids.get(i));
                    }

                    // Add first level to index
                    indexWriter.addClusterLevel(currentLevel, hierarchicalState.getCentroidsAtLevel(currentLevel));

                    // Output first level centroids immediately
                    outputLevelCentroids(hierarchicalState, currentLevel, appender);

                    currentLevel++;
                    currentK = Math.max(1, currentK / 2);

                    // Build subsequent levels until centroids fit into one frame using actual frame capacity check
                    if (currentLevelCentroids.isEmpty()) {
                        System.err.println("No centroids available for hierarchy building");
                        return;
                    }

                    System.err.println("Starting hierarchy building with " + currentLevelCentroids.size()
                            + " centroids at level " + currentLevel);

                    while (currentLevelCentroids.size() > 1 && currentK > 0) {
                        System.err.println("Building level " + currentLevel + " with " + currentLevelCentroids.size()
                                + " centroids, target K = " + currentK);

                        // Use scalable K-means++ on centroids from previous level
                        List<double[]> nextLevelCentroids = performScalableKMeansPlusPlusOnCentroids(
                                currentLevelCentroids, currentK, rand, kMeansUtils, maxKMeansIterations);

                        // Use Lloyd's algorithm to assign current level centroids (children) to next level centroids (parents)
                        // This is bottom-up: Level 0 centroids are children, Level 1 centroids are parents
                        int[] parentAssignments = assignCentroidsToParents(currentLevelCentroids, nextLevelCentroids);

                        // Store the next level centroids (parents) in hierarchical state
                        for (int i = 0; i < nextLevelCentroids.size(); i++) {
                            HierarchicalClusterId parentId = new HierarchicalClusterId(currentLevel, i);
                            hierarchicalState.addCentroid(currentLevel, parentId, nextLevelCentroids.get(i));
                        }

                        // Store the current level centroids (children) with their parent relationships
                        List<CentroidWithParent> centroidsWithParents = new ArrayList<>();
                        for (int j = 0; j < currentLevelCentroids.size(); j++) {
                            int parentIndex = parentAssignments[j];
                            HierarchicalClusterId parentId = new HierarchicalClusterId(currentLevel, parentIndex);
                            HierarchicalClusterId clusterId =
                                    new HierarchicalClusterId(currentLevel - 1, j, parentId.getClusterId());
                            centroidsWithParents
                                    .add(new CentroidWithParent(currentLevelCentroids.get(j), clusterId, parentId));
                        }

                        // Sort centroids by parent cluster ID to group children together
                        centroidsWithParents.sort((a, b) -> {
                            if (a.parentId == null && b.parentId == null)
                                return 0;
                            if (a.parentId == null)
                                return 1;
                            if (b.parentId == null)
                                return -1;
                            return Integer.compare(a.parentId.getClusterId(), b.parentId.getClusterId());
                        });

                        // Add sorted centroids to hierarchical state
                        for (int i = 0; i < centroidsWithParents.size(); i++) {
                            CentroidWithParent cwp = centroidsWithParents.get(i);
                            // Update cluster ID with correct index after sorting
                            HierarchicalClusterId sortedClusterId = new HierarchicalClusterId(currentLevel, i,
                                    cwp.parentId != null ? cwp.parentId.getClusterId() : -1);
                            hierarchicalState.addCentroid(currentLevel, sortedClusterId, cwp.centroid);
                        }

                        // Add this level to index
                        indexWriter.addClusterLevel(currentLevel, hierarchicalState.getCentroidsAtLevel(currentLevel));

                        // Output this level centroids immediately
                        outputLevelCentroids(hierarchicalState, currentLevel, appender);

                        // Test if all centroids from this level can fit in one frame
                        boolean canFitInOneFrame =
                                testCentroidsFitInFrame(ctx, hierarchicalState, currentLevel, appender);

                        if (canFitInOneFrame) {
                            System.err.println(
                                    "Level " + currentLevel + " centroids fit in one frame! Stopping hierarchy.");
                            break;
                        }

                        // Prepare for next iteration
                        currentLevelCentroids = nextLevelCentroids;
                        currentLevel++;
                        currentK = Math.max(1, currentK / 2);
                    }

                    System.err.println("Final hierarchy: " + currentLevel + " levels with "
                            + currentLevelCentroids.size() + " centroids at top level");

                    // Add final hierarchy structure to index
                    Map<String, Object> structureInfo = new HashMap<>();
                    structureInfo.put("final_level", currentLevel);
                    structureInfo.put("final_centroid_count", currentLevelCentroids.size());
                    structureInfo.put("stopped_reason", "centroids_fit_in_frame");

                    int totalCentroids = 0;
                    for (int level : hierarchicalState.getLevels()) {
                        totalCentroids += hierarchicalState.getCentroidsAtLevel(level).size();
                    }

                    indexWriter.addHierarchyStructure(currentLevel + 1, totalCentroids, structureInfo);

                    // Print hierarchy information
                    printHierarchyInfo(hierarchicalState, partition);
                }

                /**
                 * Performs memory-efficient K-means++ on the original data using probabilistic selection
                 * This is the original approach that doesn't load all data into memory
                 */
                private List<double[]> performMemoryEfficientKMeansPlusPlus(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, int k, Random rand, int maxIterations,
                        int partition) throws HyracksDataException, IOException {

                    if (k <= 0) {
                        return new ArrayList<>();
                    }

                    // Get the first centroid that was stored in StoreCentroidsActivity
                    HierarchicalCentroidsState hierarchicalState = (HierarchicalCentroidsState) ctx
                            .getStateObject(new PartitionedUUID(centroidsUUID, partition));
                    List<HierarchicalCentroidsState.HierarchicalCentroid> existingCentroids =
                            hierarchicalState.getCentroidsAtLevel(0);

                    if (existingCentroids.isEmpty()) {
                        return new ArrayList<>();
                    }

                    // Initialize candidate set with existing centroids
                    List<double[]> currentCandidates = new ArrayList<>();
                    for (HierarchicalCentroidsState.HierarchicalCentroid centroid : existingCentroids) {
                        currentCandidates.add(centroid.getCentroid());
                    }

                    // SCALABLE KMEANS ++ to select candidates (iterative approach like original)
                    for (int step = 0; step < maxScalableKmeansIter; step++) {

                        List<Double> preCosts = new ArrayList<>();

                        // First pass: Calculate costs for all points (memory efficient - stream through data)
                        vSizeFrame.reset();
                        in.open();

                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                tuple.reset(fta, tupleIndex);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    continue;
                                }
                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Calculate minimum cost to current candidates
                                double minCost = Double.POSITIVE_INFINITY;
                                for (double[] candidate : currentCandidates) {
                                    double cost = euclidean_squared(point, candidate);
                                    if (cost < minCost) {
                                        minCost = cost;
                                    }
                                }
                                preCosts.add(minCost);
                            }
                        }

                        // Compute sumCosts
                        double sumCosts = 0.0;
                        for (double c : preCosts) {
                            sumCosts += c;
                        }

                        // Second pass: Probabilistic selection
                        List<double[]> chosen = new ArrayList<>();
                        int globalTupleIndex = 0;
                        double l = k * 5; // oversampling factor

                        vSizeFrame.reset();
                        in.seek(0);
                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                                double prob = l * preCosts.get(globalTupleIndex) / sumCosts;
                                if (prob > 1.0)
                                    prob = 1.0;

                                if (rand.nextDouble() < prob) {
                                    tuple.reset(fta, tupleIndex);
                                    eval.evaluate(tuple, inputVal);
                                    if (!ATYPETAGDESERIALIZER
                                            .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                            .isListType()) {
                                        continue;
                                    }
                                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);
                                    chosen.add(point);
                                }
                            }
                        }

                        // Add chosen candidates to current set
                        for (double[] c : chosen) {
                            currentCandidates.add(c);
                        }
                    }

                    // Now perform weighted K-means++ on ALL accumulated candidates
                    List<double[]> weightedCentroids =
                            performWeightedKMeansPlusPlus(currentCandidates, k, rand, kMeansUtils, maxIterations);

                    // Run Lloyd's algorithm with assignments to improve the centroids and get parent-child relationships
                    LloydResult lloydResult = performLloydsAlgorithmWithAssignments(in, fta, tuple, eval, inputVal,
                            listAccessorConstant, kMeansUtils, vSizeFrame, weightedCentroids, k, maxIterations);

                    // Store assignments for parent-child relationships (we'll use this later)
                    // For now, just return the centroids
                    return lloydResult.centroids;
                }

                /**
                 * Performs Lloyd's algorithm to improve centroids by iteratively assigning points to nearest centroids
                 * This is the same as the original implementation
                 */
                @SuppressWarnings("unused")
                private List<double[]> performLloydsAlgorithm(GeneratedRunFileReader in, FrameTupleAccessor fta,
                        FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, VSizeFrame vSizeFrame,
                        List<double[]> initialCentroids, int k, int maxIterations)
                        throws HyracksDataException, IOException {

                    if (initialCentroids.isEmpty() || k <= 0) {
                        return initialCentroids;
                    }

                    int dim = initialCentroids.get(0).length;
                    double[][] centers = new double[k][dim];
                    for (int i = 0; i < k && i < initialCentroids.size(); i++) {
                        centers[i] = Arrays.copyOf(initialCentroids.get(i), dim);
                    }

                    double[][] newCenters = new double[k][dim];
                    int[] counts = new int[k];
                    double epsilon = 1e-4; // convergence threshold
                    int step = 0;
                    boolean converged = false;

                    while (!converged && step < maxIterations) {
                        // Reset accumulators
                        for (int i = 0; i < k; i++) {
                            counts[i] = 0;
                            for (int d = 0; d < dim; d++) {
                                newCenters[i][d] = 0.0;
                            }
                        }

                        // Assign points to nearest centroid and sum
                        vSizeFrame.reset();
                        in.seek(0);
                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                tuple.reset(fta, tupleIndex);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    continue;
                                }
                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Assign to nearest centroid
                                int bestIdx = 0;
                                double minDist = Double.POSITIVE_INFINITY;
                                for (int cIdx = 0; cIdx < k; cIdx++) {
                                    double dist = euclidean_squared(point, centers[cIdx]);
                                    if (dist < minDist) {
                                        minDist = dist;
                                        bestIdx = cIdx;
                                    }
                                }
                                for (int d = 0; d < dim; d++) {
                                    newCenters[bestIdx][d] += point[d];
                                }
                                counts[bestIdx]++;
                            }
                        }

                        // Update centroids & check for convergence
                        converged = true;
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            if (counts[cIdx] > 0) {
                                for (int d = 0; d < dim; d++) {
                                    newCenters[cIdx][d] /= counts[cIdx];
                                }
                            } else {
                                // Optionally keep old center or set to zero
                                newCenters[cIdx] = Arrays.copyOf(centers[cIdx], dim);
                            }
                            // Check movement
                            double dist = euclidean_squared(centers[cIdx], newCenters[cIdx]);
                            if (dist > epsilon) {
                                converged = false;
                            }
                        }

                        // Copy newCenters to centers for next iteration
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            System.arraycopy(newCenters[cIdx], 0, centers[cIdx], 0, dim);
                        }

                        step++;
                    }

                    // Build final centroids
                    List<double[]> finalCentroids = new ArrayList<>(k);
                    for (int cIdx = 0; cIdx < k; cIdx++) {
                        double[] centroid = new double[dim];
                        System.arraycopy(centers[cIdx], 0, centroid, 0, dim);
                        finalCentroids.add(centroid);
                    }

                    return finalCentroids;
                }

                /**
                 * Performs Lloyd's algorithm with assignment tracking for parent-child relationships
                 * This eliminates the need for expensive parent finding later
                 */
                private LloydResult performLloydsAlgorithmWithAssignments(GeneratedRunFileReader in,
                        FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, VSizeFrame vSizeFrame,
                        List<double[]> initialCentroids, int k, int maxIterations)
                        throws HyracksDataException, IOException {

                    if (initialCentroids.isEmpty() || k <= 0) {
                        return new LloydResult(initialCentroids, new int[0]);
                    }

                    int dim = initialCentroids.get(0).length;
                    double[][] centers = new double[k][dim];
                    for (int i = 0; i < k && i < initialCentroids.size(); i++) {
                        centers[i] = Arrays.copyOf(initialCentroids.get(i), dim);
                    }

                    double[][] newCenters = new double[k][dim];
                    int[] counts = new int[k];
                    double epsilon = 1e-4; // convergence threshold
                    int step = 0;
                    boolean converged = false;

                    // Track assignments for parent-child relationships
                    List<Integer> assignments = new ArrayList<>();

                    while (!converged && step < maxIterations) {
                        // Reset accumulators
                        for (int i = 0; i < k; i++) {
                            counts[i] = 0;
                            for (int d = 0; d < dim; d++) {
                                newCenters[i][d] = 0.0;
                            }
                        }

                        // Clear assignments for this iteration
                        assignments.clear();

                        // Assign points to nearest centroid and sum
                        vSizeFrame.reset();
                        in.seek(0);
                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                tuple.reset(fta, tupleIndex);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    continue;
                                }
                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Assign to nearest centroid
                                int bestIdx = 0;
                                double minDist = Double.POSITIVE_INFINITY;
                                for (int cIdx = 0; cIdx < k; cIdx++) {
                                    double dist = euclidean_squared(point, centers[cIdx]);
                                    if (dist < minDist) {
                                        minDist = dist;
                                        bestIdx = cIdx;
                                    }
                                }

                                // Store assignment for parent-child relationships
                                assignments.add(bestIdx);

                                for (int d = 0; d < dim; d++) {
                                    newCenters[bestIdx][d] += point[d];
                                }
                                counts[bestIdx]++;
                            }
                        }

                        // Update centroids & check for convergence
                        converged = true;
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            if (counts[cIdx] > 0) {
                                for (int d = 0; d < dim; d++) {
                                    newCenters[cIdx][d] /= counts[cIdx];
                                }
                            } else {
                                // Optionally keep old center or set to zero
                                newCenters[cIdx] = Arrays.copyOf(centers[cIdx], dim);
                            }
                            // Check movement
                            double dist = euclidean_squared(centers[cIdx], newCenters[cIdx]);
                            if (dist > epsilon) {
                                converged = false;
                            }
                        }

                        // Copy newCenters to centers for next iteration
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            System.arraycopy(newCenters[cIdx], 0, centers[cIdx], 0, dim);
                        }

                        step++;
                    }

                    // Build final centroids
                    List<double[]> finalCentroids = new ArrayList<>(k);
                    for (int cIdx = 0; cIdx < k; cIdx++) {
                        double[] centroid = new double[dim];
                        System.arraycopy(centers[cIdx], 0, centroid, 0, dim);
                        finalCentroids.add(centroid);
                    }

                    // Convert assignments to array
                    int[] assignmentArray = new int[assignments.size()];
                    for (int i = 0; i < assignments.size(); i++) {
                        assignmentArray[i] = assignments.get(i);
                    }

                    return new LloydResult(finalCentroids, assignmentArray);
                }

                /**
                 * Performs weighted K-means++ on a set of candidate points
                 * Uses the efficient approach from LocalKMeansPlusPlusCentroidsOperatorDescriptor
                 */
                private List<double[]> performWeightedKMeansPlusPlus(List<double[]> candidates, int k, Random rand,
                        KMeansUtils kMeansUtils, int maxIterations) {
                    if (candidates.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    int dim = candidates.get(0).length;
                    int n = candidates.size();
                    double[][] centers = new double[k][dim];

                    // Assume equal weights for candidates (since we don't have centroidCounts here)
                    int[] candidateCounts = new int[n];
                    Arrays.fill(candidateCounts, 1); // Each candidate has weight 1

                    // Pick first center using weighted selection
                    int idx = kMeansUtils.pickWeightedIndex(rand, candidates, candidateCounts);
                    centers[0] = Arrays.copyOf(candidates.get(idx), dim);

                    // Initialize costArray efficiently
                    double[] costArray = new double[n];
                    for (int i = 0; i < n; i++) {
                        costArray[i] = euclidean_squared(candidates.get(i), centers[0]);
                    }

                    // Weighted K-means++ initialization (efficient approach)
                    for (int i = 1; i < k; i++) {
                        double sum = 0.0;
                        for (int j = 0; j < n; j++) {
                            sum += costArray[j] * candidateCounts[j]; // Weighted sum
                        }
                        double r = rand.nextDouble() * sum;
                        double cumulativeScore = 0.0;
                        int j = 0;
                        while (j < n && cumulativeScore < r) {
                            cumulativeScore += candidateCounts[j] * costArray[j]; // Weighted selection
                            j++;
                        }
                        if (j == 0) {
                            centers[i] = Arrays.copyOf(candidates.get(0), dim);
                        } else {
                            centers[i] = Arrays.copyOf(candidates.get(j - 1), dim);
                        }

                        // Update costArray efficiently (only 2 nested loops)
                        for (int p = 0; p < n; p++) {
                            costArray[p] = Math.min(euclidean_squared(candidates.get(p), centers[i]), costArray[p]);
                        }
                    }

                    // Weighted Lloyd's algorithm
                    int[] oldClosest = new int[n];
                    Arrays.fill(oldClosest, -1);
                    int iteration = 0;
                    boolean moved = true;

                    while (moved && iteration < maxIterations) {
                        moved = false;
                        double[] counts = new double[k];
                        double[][] sums = new double[k][dim];

                        // Assign points to closest centroids with weights
                        for (int i = 0; i < n; i++) {
                            int index = kMeansUtils.findClosest(centers, candidates.get(i));
                            kMeansUtils.addWeighted(sums[index], candidates.get(i), candidateCounts[i]);
                            counts[index] += candidateCounts[i];
                            if (index != oldClosest[i]) {
                                moved = true;
                                oldClosest[i] = index;
                            }
                        }

                        // Update centers with weighted averages
                        for (int j = 0; j < k; j++) {
                            if (counts[j] == 0.0) {
                                // Handle empty cluster - select random point
                                int selectedIdx = rand.nextInt(n);
                                centers[j] = Arrays.copyOf(candidates.get(selectedIdx), dim);
                            } else {
                                kMeansUtils.scale(sums[j], 1.0 / counts[j]);
                                centers[j] = Arrays.copyOf(sums[j], dim);
                            }
                        }
                        iteration++;
                    }

                    // Convert to list
                    List<double[]> result = new ArrayList<>();
                    for (double[] center : centers) {
                        result.add(center);
                    }
                    return result;
                }

                /**
                 * Performs scalable K-means++ clustering on centroids from previous level
                 * This matches the original implementation's approach for subsequent levels
                 */
                private List<double[]> performScalableKMeansPlusPlusOnCentroids(List<double[]> centroids, int k,
                        Random rand, KMeansUtils kMeansUtils, int maxIterations) {
                    if (centroids.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    // Initialize candidate set with first centroid
                    List<double[]> currentCandidates = new ArrayList<>();
                    currentCandidates.add(centroids.get(0));

                    // SCALABLE KMEANS ++ to select candidates (iterative approach like original)
                    for (int stepMultilevel = 1; stepMultilevel < maxScalableKmeansIter; stepMultilevel++) {
                        List<Double> preCosts = new ArrayList<>();

                        int tupleCount = centroids.size();
                        for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                            double[] point = centroids.get(tupleIndex);

                            double minCost = Double.POSITIVE_INFINITY;
                            for (double[] center : currentCandidates) {
                                double cost = euclidean_squared(point, center);
                                if (cost < minCost)
                                    minCost = cost;
                            }
                            preCosts.add(minCost);
                        }

                        // Compute sumCosts
                        double sumCosts = 0.0;
                        for (double c : preCosts)
                            sumCosts += c;

                        // Oversampling factor
                        double l = k * 5; // expected number of candidates per round
                        List<double[]> chosen = new ArrayList<>();

                        int pointCount = centroids.size();
                        for (int tupleIndex = 0; tupleIndex < pointCount; tupleIndex++) {
                            double prob = l * preCosts.get(tupleIndex) / sumCosts;
                            if (prob > 1.0)
                                prob = 1.0; // cap at 1

                            if (rand.nextDouble() < prob) {
                                double[] point = centroids.get(tupleIndex);
                                chosen.add(point);
                            }
                        }

                        for (double[] c : chosen) {
                            currentCandidates.add(c);
                        }
                    }

                    // Calculate weights for each candidate
                    int[] centroidCounts = new int[currentCandidates.size()];
                    for (int i = 0; i < currentCandidates.size(); i++) {
                        double[] candidate = currentCandidates.get(i);
                        int closestIdx = -1;
                        double minCost = Double.POSITIVE_INFINITY;
                        for (int j = 0; j < centroids.size(); j++) {
                            double cost = euclidean_squared(candidate, centroids.get(j));
                            if (cost < minCost) {
                                minCost = cost;
                                closestIdx = j;
                            }
                        }
                        if (closestIdx >= 0) {
                            centroidCounts[i]++;
                        }
                    }

                    // Now perform weighted K-means++ on the chosen candidates
                    List<double[]> weightedCentroids = performWeightedKMeansPlusPlusWithCounts(currentCandidates, k,
                            rand, kMeansUtils, maxIterations, centroidCounts);

                    // Run Lloyd's algorithm to improve the centroids (like original implementation)
                    return performLloydsAlgorithmOnCentroids(centroids, weightedCentroids, k, maxIterations);
                }

                /**
                 * Performs Lloyd's algorithm on centroids (for subsequent levels)
                 * This is similar to the original but works on centroids instead of raw data
                 */
                private List<double[]> performLloydsAlgorithmOnCentroids(List<double[]> originalCentroids,
                        List<double[]> initialCentroids, int k, int maxIterations) {
                    if (initialCentroids.isEmpty() || k <= 0) {
                        return initialCentroids;
                    }

                    int dim = initialCentroids.get(0).length;
                    double[][] centers = new double[k][dim];
                    for (int i = 0; i < k && i < initialCentroids.size(); i++) {
                        centers[i] = Arrays.copyOf(initialCentroids.get(i), dim);
                    }

                    double[][] newCenters = new double[k][dim];
                    int[] counts = new int[k];
                    double epsilon = 1e-4; // convergence threshold
                    int step = 0;
                    boolean converged = false;

                    while (!converged && step < maxIterations) {
                        // Reset accumulators
                        for (int i = 0; i < k; i++) {
                            counts[i] = 0;
                            for (int d = 0; d < dim; d++) {
                                newCenters[i][d] = 0.0;
                            }
                        }

                        // Assign centroids to nearest center and sum
                        for (double[] centroid : originalCentroids) {
                            // Assign to nearest center
                            int bestIdx = 0;
                            double minDist = Double.POSITIVE_INFINITY;
                            for (int cIdx = 0; cIdx < k; cIdx++) {
                                double dist = euclidean_squared(centroid, centers[cIdx]);
                                if (dist < minDist) {
                                    minDist = dist;
                                    bestIdx = cIdx;
                                }
                            }
                            for (int d = 0; d < dim; d++) {
                                newCenters[bestIdx][d] += centroid[d];
                            }
                            counts[bestIdx]++;
                        }

                        // Update centers & check for convergence
                        converged = true;
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            if (counts[cIdx] > 0) {
                                for (int d = 0; d < dim; d++) {
                                    newCenters[cIdx][d] /= counts[cIdx];
                                }
                            } else {
                                // Optionally keep old center or set to zero
                                newCenters[cIdx] = Arrays.copyOf(centers[cIdx], dim);
                            }
                            // Check movement
                            double dist = euclidean_squared(centers[cIdx], newCenters[cIdx]);
                            if (dist > epsilon) {
                                converged = false;
                            }
                        }

                        // Copy newCenters to centers for next iteration
                        for (int cIdx = 0; cIdx < k; cIdx++) {
                            System.arraycopy(newCenters[cIdx], 0, centers[cIdx], 0, dim);
                        }

                        step++;
                    }

                    // Build final centroids
                    List<double[]> finalCentroids = new ArrayList<>(k);
                    for (int cIdx = 0; cIdx < k; cIdx++) {
                        double[] centroid = new double[dim];
                        System.arraycopy(centers[cIdx], 0, centroid, 0, dim);
                        finalCentroids.add(centroid);
                    }

                    return finalCentroids;
                }

                /**
                 * Performs weighted K-means++ with provided counts (for centroids)
                 */
                private List<double[]> performWeightedKMeansPlusPlusWithCounts(List<double[]> candidates, int k,
                        Random rand, KMeansUtils kMeansUtils, int maxIterations, int[] candidateCounts) {
                    if (candidates.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    int dim = candidates.get(0).length;
                    int n = candidates.size();
                    double[][] centers = new double[k][dim];

                    // Pick first center using weighted selection
                    int idx = kMeansUtils.pickWeightedIndex(rand, candidates, candidateCounts);
                    centers[0] = Arrays.copyOf(candidates.get(idx), dim);

                    // Initialize costArray efficiently
                    double[] costArray = new double[n];
                    for (int i = 0; i < n; i++) {
                        costArray[i] = euclidean_squared(candidates.get(i), centers[0]);
                    }

                    // Weighted K-means++ initialization (efficient approach)
                    for (int i = 1; i < k; i++) {
                        double sum = 0.0;
                        for (int j = 0; j < n; j++) {
                            sum += costArray[j] * candidateCounts[j]; // Weighted sum
                        }
                        double r = rand.nextDouble() * sum;
                        double cumulativeScore = 0.0;
                        int j = 0;
                        while (j < n && cumulativeScore < r) {
                            cumulativeScore += candidateCounts[j] * costArray[j]; // Weighted selection
                            j++;
                        }
                        if (j == 0) {
                            centers[i] = Arrays.copyOf(candidates.get(0), dim);
                        } else {
                            centers[i] = Arrays.copyOf(candidates.get(j - 1), dim);
                        }

                        // Update costArray efficiently (only 2 nested loops)
                        for (int p = 0; p < n; p++) {
                            costArray[p] = Math.min(euclidean_squared(candidates.get(p), centers[i]), costArray[p]);
                        }
                    }

                    // Weighted Lloyd's algorithm
                    int[] oldClosest = new int[n];
                    Arrays.fill(oldClosest, -1);
                    int iteration = 0;
                    boolean moved = true;

                    while (moved && iteration < maxIterations) {
                        moved = false;
                        double[] counts = new double[k];
                        double[][] sums = new double[k][dim];

                        // Assign points to closest centroids with weights
                        for (int i = 0; i < n; i++) {
                            int index = kMeansUtils.findClosest(centers, candidates.get(i));
                            kMeansUtils.addWeighted(sums[index], candidates.get(i), candidateCounts[i]);
                            counts[index] += candidateCounts[i];
                            if (index != oldClosest[i]) {
                                moved = true;
                                oldClosest[i] = index;
                            }
                        }

                        // Update centers with weighted averages
                        for (int j = 0; j < k; j++) {
                            if (counts[j] == 0.0) {
                                // Handle empty cluster - select random point
                                int selectedIdx = rand.nextInt(n);
                                centers[j] = Arrays.copyOf(candidates.get(selectedIdx), dim);
                            } else {
                                kMeansUtils.scale(sums[j], 1.0 / counts[j]);
                                centers[j] = Arrays.copyOf(sums[j], dim);
                            }
                        }
                        iteration++;
                    }

                    // Convert to list
                    List<double[]> result = new ArrayList<>();
                    for (double[] center : centers) {
                        result.add(center);
                    }
                    return result;
                }

                /**
                 * Tests if all centroids from a specific level can fit in one frame
                 * Uses actual FrameUtils.appendToWriter to check capacity
                 */
                @SuppressWarnings("deprecation")
                private boolean testCentroidsFitInFrame(IHyracksTaskContext ctx,
                        HierarchicalCentroidsState hierarchicalState, int level, FrameTupleAppender appender) {
                    try {
                        // Use the same appender to test capacity
                        // We'll reset it after testing

                        // Get centroids for this level
                        List<HierarchicalCentroidsState.HierarchicalCentroid> centroids =
                                hierarchicalState.getCentroidsAtLevel(level);

                        if (centroids.isEmpty()) {
                            return true; // Empty level fits in any frame
                        }

                        // Store current appender state
                        ByteBuffer originalBuffer = appender.getBuffer();
                        int originalPosition = originalBuffer.position();

                        try {
                            // Try to append all centroids from this level
                            for (HierarchicalCentroidsState.HierarchicalCentroid centroid : centroids) {
                                double[] arr = centroid.getCentroid();

                                // Create tuple data (same as outputHierarchicalCentroids)
                                ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                                DataOutput embBytesOutput = new DataOutputStream(embBytes);
                                AMutableDouble aDouble = new AMutableDouble(0);
                                OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                                ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                                orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));

                                for (double value : arr) {
                                    aDouble.setValue(value);
                                    listStorage.reset();
                                    listStorage.getDataOutput().writeByte(ATypeTag.FLOAT.serialize());
                                    ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble,
                                            listStorage.getDataOutput());
                                    orderedListBuilder.addItem(listStorage);
                                }

                                embBytes.reset();
                                orderedListBuilder.write(embBytesOutput, true);

                                // Create tuple builder
                                ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
                                tupleBuilder.reset();
                                tupleBuilder.addField(embBytes.getByteArray(), 0, embBytes.getLength());

                                // Try to append - if this fails, centroids don't fit in one frame
                                if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                        tupleBuilder.getSize())) {
                                    System.err.println(
                                            "Level " + level + " centroids do NOT fit in one frame (failed at centroid "
                                                    + centroids.indexOf(centroid) + " of " + centroids.size() + ")");
                                    return false;
                                }
                            }

                            System.err.println("Level " + level + " centroids fit in one frame (" + centroids.size()
                                    + " centroids)");
                            return true;

                        } finally {
                            // Reset appender to original state
                            originalBuffer.position(originalPosition);
                            appender.reset(new VSizeFrame(ctx), true);
                        }

                    } catch (Exception e) {
                        System.err.println("Error testing frame capacity: " + e.getMessage());
                        return false; // If we can't test, assume it doesn't fit
                    }
                }

                /**
                 * Finds the closest parent centroid for establishing parent-child relationships
                 */
                @SuppressWarnings("unused")
                private HierarchicalClusterId findClosestParentCentroid(double[] childCentroid,
                        HierarchicalCentroidsState hierarchicalState, int parentLevel) {
                    List<HierarchicalCentroidsState.HierarchicalCentroid> parentCentroids =
                            hierarchicalState.getCentroidsAtLevel(parentLevel);

                    if (parentCentroids.isEmpty()) {
                        return null;
                    }

                    double minDistance = Double.POSITIVE_INFINITY;
                    HierarchicalClusterId closestParent = null;

                    for (HierarchicalCentroidsState.HierarchicalCentroid parent : parentCentroids) {
                        double distance = euclidean_squared(childCentroid, parent.getCentroid());
                        if (distance < minDistance) {
                            minDistance = distance;
                            closestParent = parent.getClusterId();
                        }
                    }

                    return closestParent;
                }

                /**
                 * Outputs centroids from a specific level immediately
                 */
                @SuppressWarnings("deprecation")
                private void outputLevelCentroids(HierarchicalCentroidsState hierarchicalState, int level,
                        FrameTupleAppender appender) throws HyracksDataException, IOException {
                    ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                    DataOutput embBytesOutput = new DataOutputStream(embBytes);
                    AMutableDouble aDouble = new AMutableDouble(0);
                    OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                    ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                    orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);

                    // Get centroids for this level
                    List<HierarchicalCentroidsState.HierarchicalCentroid> centroids =
                            hierarchicalState.getCentroidsAtLevel(level);
                    System.err.println("Outputting " + centroids.size() + " centroids from level " + level);

                    for (HierarchicalCentroidsState.HierarchicalCentroid centroid : centroids) {
                        double[] arr = centroid.getCentroid();
                        orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));
                        for (double value : arr) {
                            aDouble.setValue(value);
                            listStorage.reset();
                            listStorage.getDataOutput().writeByte(ATypeTag.FLOAT.serialize());
                            ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, listStorage.getDataOutput());
                            orderedListBuilder.addItem(listStorage);
                        }
                        embBytes.reset();
                        orderedListBuilder.write(embBytesOutput, true);
                        tupleBuilder.reset();
                        tupleBuilder.addField(embBytes.getByteArray(), 0, embBytes.getLength());

                        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            FrameUtils.flushFrame(appender.getBuffer(), writer);
                            appender.reset(new VSizeFrame(ctx), true);
                            appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                    tupleBuilder.getSize());
                        }
                    }
                }

                /**
                 * Assigns child centroids to parent centroids using Lloyd's algorithm approach
                 * This is more efficient than individual distance calculations
                 */
                private int[] assignCentroidsToParents(List<double[]> childCentroids, List<double[]> parentCentroids) {
                    if (childCentroids.isEmpty() || parentCentroids.isEmpty()) {
                        return new int[0];
                    }

                    int[] assignments = new int[childCentroids.size()];

                    // For each child centroid, find its closest parent
                    for (int i = 0; i < childCentroids.size(); i++) {
                        double[] childCentroid = childCentroids.get(i);
                        int closestParentIndex = 0;
                        double minDistance = Double.POSITIVE_INFINITY;

                        for (int j = 0; j < parentCentroids.size(); j++) {
                            double distance = euclidean_squared(childCentroid, parentCentroids.get(j));
                            if (distance < minDistance) {
                                minDistance = distance;
                                closestParentIndex = j;
                            }
                        }

                        assignments[i] = closestParentIndex;
                    }

                    return assignments;
                }

                /**
                 * Finds the closest parent index for a child centroid
                 */
                @SuppressWarnings("unused")
                private int findClosestParentIndex(double[] childCentroid, List<double[]> parentCentroids) {
                    if (parentCentroids.isEmpty()) {
                        return 0;
                    }

                    int closestIndex = 0;
                    double minDistance = Double.POSITIVE_INFINITY;

                    for (int i = 0; i < parentCentroids.size(); i++) {
                        double distance = euclidean_squared(childCentroid, parentCentroids.get(i));
                        if (distance < minDistance) {
                            minDistance = distance;
                            closestIndex = i;
                        }
                    }

                    return closestIndex;
                }

                /**
                 * Finds parent node by index (for Lloyd's assignments)
                 */
                @SuppressWarnings("unused")
                private HierarchicalClusterTree.TreeNode findParentNodeByIndex(int parentIndex,
                        HierarchicalClusterTree.TreeNode currentLevelNodes) {
                    if (currentLevelNodes == null) {
                        return null;
                    }

                    // Search through all nodes at the same level as currentLevelNodes
                    Queue<HierarchicalClusterTree.TreeNode> queue = new LinkedList<>();
                    queue.offer(currentLevelNodes);

                    int currentIndex = 0;
                    while (!queue.isEmpty()) {
                        HierarchicalClusterTree.TreeNode current = queue.poll();

                        // Check if this node is at the parent level
                        if (current.level == currentLevelNodes.level) {
                            if (currentIndex == parentIndex) {
                                return current;
                            }
                            currentIndex++;
                        }

                        // Add children to queue
                        for (HierarchicalClusterTree.TreeNode child : current.getChildren()) {
                            queue.offer(child);
                        }
                    }

                    // If not found, return the first node as fallback
                    return currentLevelNodes;
                }

                /**
                 * Finds the closest parent node for tree-based clustering
                 */
                @SuppressWarnings("unused")
                private HierarchicalClusterTree.TreeNode findClosestParentNode(double[] childCentroid,
                        HierarchicalClusterTree.TreeNode parentNode) {
                    if (parentNode == null) {
                        return null;
                    }

                    double minDistance = Double.POSITIVE_INFINITY;
                    HierarchicalClusterTree.TreeNode closestParent = parentNode;

                    // Search through all nodes at the same level as parentNode
                    Queue<HierarchicalClusterTree.TreeNode> queue = new LinkedList<>();
                    queue.offer(parentNode);

                    while (!queue.isEmpty()) {
                        HierarchicalClusterTree.TreeNode current = queue.poll();

                        // Check if this node is at the parent level
                        if (current.level == parentNode.level) {
                            double distance = euclidean_squared(childCentroid, current.getCentroid());
                            if (distance < minDistance) {
                                minDistance = distance;
                                closestParent = current;
                            }
                        }

                        // Add children to queue
                        for (HierarchicalClusterTree.TreeNode child : current.getChildren()) {
                            queue.offer(child);
                        }
                    }

                    return closestParent;
                }

                /**
                 * Tests if all nodes at a tree level can fit in one frame
                 */
                @SuppressWarnings("deprecation")
                private boolean testTreeLevelFitsInFrame(IHyracksTaskContext ctx,
                        List<HierarchicalClusterTree.TreeNode> nodes, FrameTupleAppender appender) {
                    try {
                        if (nodes.isEmpty()) {
                            return true;
                        }

                        // Store current appender state
                        ByteBuffer originalBuffer = appender.getBuffer();
                        int originalPosition = originalBuffer.position();

                        try {
                            // Try to append all nodes from this level
                            for (HierarchicalClusterTree.TreeNode node : nodes) {
                                double[] arr = node.getCentroid();

                                // Create tuple data (same as outputHierarchicalCentroids)
                                ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                                DataOutput embBytesOutput = new DataOutputStream(embBytes);
                                AMutableDouble aDouble = new AMutableDouble(0);
                                OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                                ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                                orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));

                                for (double value : arr) {
                                    aDouble.setValue(value);
                                    listStorage.reset();
                                    listStorage.getDataOutput().writeByte(ATypeTag.FLOAT.serialize());
                                    ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble,
                                            listStorage.getDataOutput());
                                    orderedListBuilder.addItem(listStorage);
                                }

                                embBytes.reset();
                                orderedListBuilder.write(embBytesOutput, true);

                                // Create tuple builder
                                ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
                                tupleBuilder.reset();
                                tupleBuilder.addField(embBytes.getByteArray(), 0, embBytes.getLength());

                                // Try to append - if this fails, nodes don't fit in one frame
                                if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                        tupleBuilder.getSize())) {
                                    System.err.println("Tree level nodes do NOT fit in one frame (failed at node "
                                            + nodes.indexOf(node) + " of " + nodes.size() + ")");
                                    return false;
                                }
                            }

                            System.err.println("Tree level nodes fit in one frame (" + nodes.size() + " nodes)");
                            return true;

                        } finally {
                            // Reset appender to original state
                            originalBuffer.position(originalPosition);
                            appender.reset(new VSizeFrame(ctx), true);
                        }

                    } catch (Exception e) {
                        System.err.println("Error testing tree level frame capacity: " + e.getMessage());
                        return false;
                    }
                }

                /**
                 * Outputs the complete tree in BFS order
                 */
                @SuppressWarnings("deprecation")
                private void outputCompleteTree(HierarchicalClusterTree tree, FrameTupleAppender appender,
                        HierarchicalClusterIndexWriter indexWriter, int partition)
                        throws HyracksDataException, IOException {

                    System.err.println("Outputting complete tree in BFS order...");

                    // Get all nodes in BFS order
                    List<HierarchicalClusterTree.TreeNode> allNodes = tree.toFlatList();

                    // Add all nodes to index
                    for (HierarchicalClusterTree.TreeNode node : allNodes) {
                        // Convert tree node to hierarchical centroid for index
                        HierarchicalClusterId clusterId = new HierarchicalClusterId(node.getLevel(),
                                node.getClusterId(), (int) node.getParentGlobalId());
                        HierarchicalCentroidsState.HierarchicalCentroid centroid =
                                new HierarchicalCentroidsState.HierarchicalCentroid(clusterId, node.getCentroid());

                        // Add to index by level
                        indexWriter.addClusterLevel(node.getLevel(), List.of(centroid));
                    }

                    // Output all nodes
                    ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                    DataOutput embBytesOutput = new DataOutputStream(embBytes);
                    AMutableDouble aDouble = new AMutableDouble(0);
                    OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                    ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                    orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);

                    System.err.println("Outputting " + allNodes.size() + " nodes in BFS order:");

                    for (HierarchicalClusterTree.TreeNode node : allNodes) {
                        double[] arr = node.getCentroid();
                        orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));
                        for (double value : arr) {
                            aDouble.setValue(value);
                            listStorage.reset();
                            listStorage.getDataOutput().writeByte(ATypeTag.FLOAT.serialize());
                            ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, listStorage.getDataOutput());
                            orderedListBuilder.addItem(listStorage);
                        }
                        embBytes.reset();
                        orderedListBuilder.write(embBytesOutput, true);
                        tupleBuilder.reset();
                        tupleBuilder.addField(embBytes.getByteArray(), 0, embBytes.getLength());

                        System.err.println("  Node Level " + node.getLevel() + ", Cluster " + node.getClusterId()
                                + ", Global ID " + node.getGlobalId()
                                + (node.getParentGlobalId() != -1 ? ", Parent " + node.getParentGlobalId() : ""));

                        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                tupleBuilder.getSize())) {
                            FrameUtils.flushFrame(appender.getBuffer(), writer);
                            appender.reset(new VSizeFrame(ctx), true);
                            appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                    tupleBuilder.getSize());
                        }
                    }
                }

                /**
                 * Prints hierarchy information for debugging
                 */
                private void printHierarchyInfo(HierarchicalCentroidsState hierarchicalState, int partition) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Hierarchical Clustering Results for Partition ").append(partition).append(":\n");
                    sb.append("================================================\n");

                    for (int level : hierarchicalState.getLevels()) {
                        List<HierarchicalCentroidsState.HierarchicalCentroid> centroids =
                                hierarchicalState.getCentroidsAtLevel(level);
                        sb.append("\nLevel ").append(level).append(": ").append(centroids.size())
                                .append(" centroids\n");
                        sb.append("-".repeat(50)).append("\n");

                        // Group centroids by parent for better visualization
                        Map<Integer, List<HierarchicalCentroidsState.HierarchicalCentroid>> centroidsByParent =
                                new HashMap<>();

                        for (HierarchicalCentroidsState.HierarchicalCentroid centroid : centroids) {
                            HierarchicalClusterId clusterId = centroid.getClusterId();
                            int parentId = clusterId.hasParent() ? clusterId.getParentClusterId() : -1;
                            centroidsByParent.computeIfAbsent(parentId, k -> new ArrayList<>()).add(centroid);
                        }

                        // Print centroids grouped by parent
                        for (Map.Entry<Integer, List<HierarchicalCentroidsState.HierarchicalCentroid>> entry : centroidsByParent
                                .entrySet()) {
                            int parentId = entry.getKey();
                            List<HierarchicalCentroidsState.HierarchicalCentroid> children = entry.getValue();

                            if (parentId == -1) {
                                sb.append("  Root clusters:\n");
                            } else {
                                sb.append("  Children of Parent ").append(parentId).append(":\n");
                            }

                            for (HierarchicalCentroidsState.HierarchicalCentroid centroid : children) {
                                HierarchicalClusterId clusterId = centroid.getClusterId();
                                double[] centroidValues = centroid.getCentroid();

                                sb.append("    Cluster ").append(clusterId.getClusterId()).append(" (Global ID: ")
                                        .append(clusterId.getGlobalId()).append(")");
                                if (clusterId.hasParent()) {
                                    sb.append(" -> Parent: ").append(clusterId.getParentClusterId());
                                }
                                sb.append("\n");

                                // Print the actual centroid values
                                sb.append("      Centroid: [");
                                for (int i = 0; i < centroidValues.length; i++) {
                                    sb.append(String.format("%.4f", centroidValues[i]));
                                    if (i < centroidValues.length - 1) {
                                        sb.append(", ");
                                    }
                                }
                                sb.append("]\n");
                            }
                        }
                    }

                    // Print summary
                    sb.append("\n").append("=".repeat(50)).append("\n");
                    sb.append("Summary:\n");
                    int totalCentroids = 0;
                    for (int level : hierarchicalState.getLevels()) {
                        int levelCount = hierarchicalState.getCentroidsAtLevel(level).size();
                        totalCentroids += levelCount;
                        sb.append("  Level ").append(level).append(": ").append(levelCount).append(" centroids\n");
                    }
                    sb.append("  Total centroids: ").append(totalCentroids).append("\n");
                    sb.append("  Total levels: ").append(hierarchicalState.getLevels().size()).append("\n");

                    System.err.println(sb.toString());
                }
            };
        }
    }
}
