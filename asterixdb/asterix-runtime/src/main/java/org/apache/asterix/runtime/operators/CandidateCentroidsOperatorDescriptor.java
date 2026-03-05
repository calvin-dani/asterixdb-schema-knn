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
import java.util.List;
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

public final class CandidateCentroidsOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final UUID sampleUUID;
    private final UUID centroidsUUID;
    private final UUID permitUUID;

    private RecordDescriptor embeddingDesc;
    private IScalarEvaluatorFactory args;
    private int K;
    private int maxScalableKmeansIter;

    public CandidateCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            UUID sampleUUID, UUID centroidsUUID, UUID permitUUID, IScalarEvaluatorFactory args, int K,
            int maxScalableKmeansIter) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.sampleUUID = sampleUUID;
        this.centroidsUUID = centroidsUUID;
        this.args = args;
        this.permitUUID = permitUUID;
        this.K = K;
        this.maxScalableKmeansIter = maxScalableKmeansIter;
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
                CentroidsState state;
                boolean first;
                private MaterializerTaskState materializedSample;
                KMeansUtils kMeansUtils;

                @Override
                public void open() throws HyracksDataException {

                    materializedSample = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(sampleUUID, partition));
                    materializedSample.open(ctx);
                    state = new CentroidsState(ctx.getJobletContext().getJobId(),
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
                            //                            ExceptionUtil.warnUnsupportedType(ctx, sourceLoc, getIdentifier().getName(), ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]));
                        }
                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                        try {
                            double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);
                            state.addCentroid(point);

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        first = false;
                    }
                    materializedSample.appendFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (state != null) {
                        ctx.setStateObject(state);
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

                        // To write the embedding to the next operator
                        ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                        DataOutput embBytesOutput = new DataOutputStream(embBytes);

                        AMutableDouble aDouble = new AMutableDouble(0);

                        // For building the list of doubles
                        OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                        ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                        orderedListBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));

                        // To get the state where centroids are stored for this partition
                        PartitionedUUID centroidsKey = new PartitionedUUID(centroidsUUID, partition);
                        CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsKey);

                        // SCALABLE KMEANS ++ to select candidates
                        for (int step = 0; step < maxScalableKmeansIter; step++) {

                            List<Double> preCosts = new ArrayList<>();

                            // Calculating the cost of chosen centroids and the rest of the points
                            vSizeFrame.reset();
                            in.open();

                            while (in.nextFrame(vSizeFrame)) {
                                fta.reset(vSizeFrame.getBuffer());
                                int tupleCount = fta.getTupleCount();
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                    tuple.reset(fta, tupleIndex);
                                    eval.evaluate(tuple, inputVal);
                                    if (!ATYPETAGDESERIALIZER
                                            .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                            .isListType()) {
                                        continue;
                                    }
                                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    double[] point = KMeansUtils.createPrimitveList(listAccessorConstant);
                                    double minCost = Double.POSITIVE_INFINITY;
                                    for (double[] center : currentCentroids.getCentroids()) {
                                        double cost = euclidean_squared(point, center);
                                        if (cost < minCost)
                                            minCost = cost;
                                    }
                                    preCosts.add(minCost);
                                }
                            }

                            // Compute sumCosts
                            float sumCosts = 0f;
                            for (double c : preCosts)
                                sumCosts += c;

                            // Second pass: probabilistic selection
                            List<double[]> chosen = new ArrayList<>();
                            int globalTupleIndex = 0;

                            Random rand = new Random();
                            double cumulative = 0.0;

                            // Oversampling factor of 2
                            double l = K * 2; // expected number of candidates per round

                            // Selecting new centroids based on probability based on their cost
                            vSizeFrame.reset();
                            in.seek(0);
                            while (in.nextFrame(vSizeFrame)) {
                                fta.reset(vSizeFrame.getBuffer());
                                int tupleCount = fta.getTupleCount();
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                                    cumulative += preCosts.get(globalTupleIndex);

                                    double prob = l * preCosts.get(globalTupleIndex) / sumCosts;
                                    if (prob > 1.0)
                                        prob = 1.0; // cap at 1

                                    if (rand.nextDouble() < prob) {
                                        tuple.reset(fta, tupleIndex);
                                        eval.evaluate(tuple, inputVal);
                                        if (!ATYPETAGDESERIALIZER
                                                .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                                .isListType()) {
                                            continue;
                                        }
                                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                        double[] point = KMeansUtils.createPrimitveList(listAccessorConstant);
                                        chosen.add(point);
                                    }
                                }
                            }

                            for (double[] c : chosen) {
                                currentCentroids.addCentroid(c);
                            }

                        }

                        int[] centroidCounts = new int[currentCentroids.getCentroids().size()];

                        int globalTupleIndex = 0;

                        // Calculate the weights for each centroid
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
                                double[] point = KMeansUtils.createPrimitveList(listAccessorConstant);

                                // Find closest centroid
                                int closestIdx = -1;
                                double minCost = Double.POSITIVE_INFINITY;
                                List<double[]> centroids = currentCentroids.getCentroids();
                                for (int i = 0; i < centroids.size(); i++) {
                                    double cost = euclidean_squared(point, centroids.get(i));
                                    if (cost < minCost) {
                                        minCost = cost;
                                        closestIdx = i;
                                    }
                                }

                                // Increment count for the closest centroid
                                if (closestIdx >= 0) {
                                    centroidCounts[closestIdx]++;
                                }
                            }
                        }

                        int dim = currentCentroids.getCentroids().get(0).length;
                        int n = currentCentroids.getCentroids().size();

                        double[][] centers = new double[K][dim];
                        List<double[]> points = currentCentroids.getCentroids();
                        double[] costArray = new double[currentCentroids.getCentroids().size()];
                        Random rand = new Random();
                        int maxKMeansIterations = 20;
                        // Pick first center
                        int idx = KMeansUtils.pickWeightedIndex(rand, points, centroidCounts);
                        centers[0] = Arrays.copyOf(points.get(idx), points.get(idx).length);

                        // Initialize costArray
                        for (int i = 0; i < n; i++) {
                            costArray[i] = euclidean_squared(points.get(i), centers[0]);
                        }

                        // Weighted KMEANS++ initialization
                        for (int i = 1; i < K; i++) {
                            double sum = 0.0;
                            for (int j = 0; j < n; j++) {
                                sum += costArray[j] * centroidCounts[j];
                            }
                            double r = rand.nextDouble() * sum;
                            double cumulativeScore = 0.0;
                            int j = 0;
                            while (j < n && cumulativeScore < r) {
                                cumulativeScore += centroidCounts[j] * costArray[j];
                                j++;
                            }
                            if (j == 0) {
                                centers[i] = Arrays.copyOf(points.get(0), dim);
                            } else {
                                centers[i] = Arrays.copyOf(points.get(j - 1), dim);
                            }
                            // Update costArray
                            for (int p = 0; p < n; p++) {
                                costArray[p] = Math.min(euclidean_squared(points.get(p), centers[i]), costArray[p]);
                            }
                        }

                        // Weighted KMEANS++
                        int[] oldClosest = new int[n];
                        Arrays.fill(oldClosest, -1);
                        int iteration = 0;
                        boolean moved = true;
                        while (moved && iteration < maxKMeansIterations) {
                            moved = false;
                            double[] counts = new double[K];
                            double[][] sums = new double[K][dim];
                            for (int i = 0; i < n; i++) {
                                int index = KMeansUtils.findClosest(centers, points.get(i));
                                KMeansUtils.addWeighted(sums[index], points.get(i), centroidCounts[i]);
                                counts[index] += centroidCounts[i];
                                if (index != oldClosest[i]) {
                                    moved = true;
                                    oldClosest[i] = index;
                                }
                            }
                            // Update centers
                            for (int j = 0; j < K; j++) {
                                if (counts[j] == 0.0) {
                                    // Weighted selection for empty cluster
                                    double totalWeight = 0.0;
                                    for (double w : centroidCounts)
                                        totalWeight += w;
                                    double r = rand.nextDouble() * totalWeight;
                                    double cumulative = 0.0;
                                    int selectedIdx = 0;
                                    for (; selectedIdx < n; selectedIdx++) {
                                        cumulative += centroidCounts[selectedIdx];
                                        if (cumulative >= r)
                                            break;
                                    }
                                    centers[j] = Arrays.copyOf(points.get(Math.min(selectedIdx, n - 1)), dim);
                                } else {
                                    KMeansUtils.scale(sums[j], 1.0 / counts[j]);
                                    centers[j] = Arrays.copyOf(sums[j], dim);
                                }
                            }
                            iteration++;
                        }

                        currentCentroids.clearCentroids();

                        // Now run Lloyd's algorithm on each partition

                        double[][] newCenters = new double[K][dim];
                        int[] counts = new int[K];
                        double epsilon = 1e-4; // convergence threshold
                        int step = 0;
                        boolean converged = false;

                        while (!converged && step < maxKMeansIterations) {
                            // Reset accumulators
                            for (int i = 0; i < K; i++) {
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
                                            .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                            .isListType()) {
                                        continue;
                                    }
                                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    double[] point = KMeansUtils.createPrimitveList(listAccessorConstant);

                                    // Assign to nearest centroid
                                    int bestIdx = 0;
                                    double minDist = Double.POSITIVE_INFINITY;
                                    for (int cIdx = 0; cIdx < K; cIdx++) {
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
                            for (int cIdx = 0; cIdx < K; cIdx++) {
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
                            for (int cIdx = 0; cIdx < K; cIdx++) {
                                System.arraycopy(newCenters[cIdx], 0, centers[cIdx], 0, dim);
                            }

                            step++;
                        }

                        // Build final centroids
                        List<double[]> finalCentroids = new ArrayList<>(K);
                        for (int cIdx = 0; cIdx < K; cIdx++) {
                            double[] centroid = new double[dim];
                            System.arraycopy(centers[cIdx], 0, centroid, 0, dim);
                            finalCentroids.add(centroid);
                        }

                        double[][] sumVectors = new double[K][dim];

                        List<List<double[]>> allClusters = new ArrayList<>();
                        allClusters.add(finalCentroids);

                        StringBuilder sb = new StringBuilder(
                                " Final centroids at  partition " + partition + " : with level " + allClusters.size());
                        for (List<double[]> centroids : allClusters) {
                            sb.append(" Level with ").append(0).append(" with K centroids ").append(centroids.size())
                                    .append(" for partition ").append(partition).append("\n");
                            for (double[] c : centroids) {
                                sb.append(Arrays.toString(c)).append(" ,  \n");
                            }
                        }
                        System.err.println(sb.toString());

                        int requiedSpace = K * dim * Double.BYTES;
                        int minFrameSize = ctx.getJobletContext().getInitialFrameSize();
                        //                        int maxFrameSize = ctx.getJobletContext().getMaxFrameSize();

                        int iterations = 0;
                        int currentK = K;
                        int frameSize = minFrameSize;

                        while (currentK * dim * Double.BYTES > frameSize) {
                            currentK /= 2;
                            iterations++;

                            List<double[]> currentPoints = allClusters.get(allClusters.size() - 1);
                            List<double[]> currentLvlCentroids = new ArrayList<>();
                            currentLvlCentroids.add(currentPoints.get(0));

                            for (int stepMultilevel = 1; stepMultilevel < maxScalableKmeansIter; stepMultilevel++) {
                                List<Double> preCosts = new ArrayList<>();

                                int tupleCount = currentPoints.size();
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                    double[] point = currentPoints.get(tupleIndex);

                                    double minCost = Double.POSITIVE_INFINITY;
                                    for (double[] center : currentLvlCentroids) {
                                        double cost = euclidean_squared(point, center);
                                        if (cost < minCost)
                                            minCost = cost;
                                    }
                                    preCosts.add(minCost);
                                    // Accumulate minCost for each tuple
                                }

                                // Compute sumCosts
                                float sumCosts = 0f;
                                for (double c : preCosts)
                                    sumCosts += c;

                                // Oversampling factor of 2
                                double l = currentK * 2; // expected number of candidates per round
                                List<double[]> chosen = new ArrayList<>();

                                int pointCount = currentPoints.size();
                                for (int tupleIndex = 0; tupleIndex < pointCount; tupleIndex++) {

                                    double prob = l * preCosts.get(tupleIndex) / sumCosts;
                                    if (prob > 1.0)
                                        prob = 1.0; // cap at 1

                                    if (rand.nextDouble() < prob) {
                                        double[] point = currentPoints.get(tupleIndex);
                                        chosen.add(point);
                                    }
                                }

                                for (double[] c : chosen) {
                                    currentLvlCentroids.add(c);
                                }

                            }

                            centroidCounts = new int[currentPoints.size()];
                            int pointCount = currentPoints.size();
                            for (int tupleIndex = 0; tupleIndex < pointCount; tupleIndex++) {

                                double[] point = currentPoints.get(tupleIndex);

                                // Find closest centroid
                                int closestIdx = -1;
                                double minCost = Double.POSITIVE_INFINITY;
                                List<double[]> centroids = currentLvlCentroids;
                                for (int i = 0; i < centroids.size(); i++) {
                                    double cost = euclidean_squared(point, centroids.get(i));
                                    if (cost < minCost) {
                                        minCost = cost;
                                        closestIdx = i;
                                    }
                                }
                                // Increment count for the closest centroid
                                if (closestIdx >= 0) {
                                    centroidCounts[closestIdx]++;
                                }
                            }

                            dim = currentLvlCentroids.get(0).length;
                            n = currentLvlCentroids.size();

                            centers = new double[currentK][dim];
                            points = currentLvlCentroids;
                            costArray = new double[currentLvlCentroids.size()];

                            // Pick first center
                            idx = KMeansUtils.pickWeightedIndex(rand, points, centroidCounts);
                            centers[0] = Arrays.copyOf(points.get(idx), points.get(idx).length);

                            // Initialize costArray
                            for (int i = 0; i < n; i++) {
                                costArray[i] = euclidean_squared(points.get(i), centers[0]);
                            }

                            // Weighted KMEANS++ initialization
                            for (int i = 1; i < currentK; i++) {
                                double sum = 0.0;
                                for (int j = 0; j < n; j++) {
                                    sum += costArray[j] * centroidCounts[j];
                                }
                                double r = rand.nextDouble() * sum;
                                double cumulativeScore = 0.0;
                                int j = 0;
                                while (j < n && cumulativeScore < r) {
                                    cumulativeScore += centroidCounts[j] * costArray[j];
                                    j++;
                                }
                                if (j == 0) {
                                    centers[i] = Arrays.copyOf(points.get(0), dim);
                                } else {
                                    centers[i] = Arrays.copyOf(points.get(j - 1), dim);
                                }
                                // Update costArray
                                for (int p = 0; p < n; p++) {
                                    costArray[p] = Math.min(euclidean_squared(points.get(p), centers[i]), costArray[p]);
                                }
                            }

                            // Weighted KMEANS++ initialization
                            oldClosest = new int[n];
                            Arrays.fill(oldClosest, -1);
                            iteration = 0;
                            moved = true;
                            while (moved && iteration < maxKMeansIterations) {
                                moved = false;
                                counts = new int[currentK];
                                double[][] sums = new double[currentK][dim];
                                for (int i = 0; i < n; i++) {
                                    int index = KMeansUtils.findClosest(centers, points.get(i));
                                    KMeansUtils.addWeighted(sums[index], points.get(i), centroidCounts[i]);
                                    counts[index] += centroidCounts[i];
                                    if (index != oldClosest[i]) {
                                        moved = true;
                                        oldClosest[i] = index;
                                    }
                                }
                                // Update centers
                                for (int j = 0; j < currentK; j++) {
                                    if (counts[j] == 0.0) {
                                        // Weighted selection for empty cluster
                                        double totalWeight = 0.0;
                                        for (double w : centroidCounts)
                                            totalWeight += w;
                                        double r = rand.nextDouble() * totalWeight;
                                        double cumulative = 0.0;
                                        int selectedIdx = 0;
                                        for (; selectedIdx < n; selectedIdx++) {
                                            cumulative += centroidCounts[selectedIdx];
                                            if (cumulative >= r)
                                                break;
                                        }
                                        centers[j] = Arrays.copyOf(points.get(Math.min(selectedIdx, n - 1)), dim);
                                    } else {
                                        KMeansUtils.scale(sums[j], 1.0 / counts[j]);
                                        centers[j] = Arrays.copyOf(sums[j], dim);
                                    }
                                }
                                iteration++;
                            }

                            newCenters = new double[currentK][dim];
                            counts = new int[currentK];
                            step = 0;
                            converged = false;

                            while (!converged && step < maxKMeansIterations) {
                                // Reset accumulators
                                for (int i = 0; i < currentK; i++) {
                                    counts[i] = 0;
                                    for (int d = 0; d < dim; d++) {
                                        newCenters[i][d] = 0.0;
                                    }
                                }

                                // Assign points to nearest centroid and sum
                                pointCount = currentPoints.size();
                                for (int tupleIndex = 0; tupleIndex < pointCount; tupleIndex++) {

                                    double[] point = currentPoints.get(tupleIndex);

                                    // Assign to nearest centroid
                                    int bestIdx = 0;
                                    double minDist = Double.POSITIVE_INFINITY;
                                    for (int cIdx = 0; cIdx < currentK; cIdx++) {
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

                                // Update centroids & check for convergence
                                converged = true;
                                for (int cIdx = 0; cIdx < currentK; cIdx++) {
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
                                for (int cIdx = 0; cIdx < currentK; cIdx++) {
                                    System.arraycopy(newCenters[cIdx], 0, centers[cIdx], 0, dim);
                                }

                                step++;
                            }

                            // Build final centroids
                            List<double[]> finalCentroidsLvl = new ArrayList<>(currentK);
                            for (int cIdx = 0; cIdx < currentK; cIdx++) {
                                double[] centroid = new double[dim];
                                System.arraycopy(centers[cIdx], 0, centroid, 0, dim);
                                finalCentroidsLvl.add(centroid);
                            }

                            allClusters.add(finalCentroidsLvl);
                        }

                        int level = allClusters.size();
                        //                        StringBuilder sb = new StringBuilder(
                        //                                " Final centroids at  partition " + partition + " : with level " + allClusters.size()
                        //                                        + " cost " + currentK * dim * Double.BYTES + " frameSize " + frameSize);
                        //                        for (List<double[]> centroids : allClusters) {
                        //                            sb.append(" Level with ").append(level).append(" with K centroids ")
                        //                                    .append(centroids.size()).append(" for partition ").append(partition).append("\n");
                        //                            level--;
                        //                            for (double[] c : centroids) {
                        //                                sb.append(Arrays.toString(c)).append(" ,  \n");
                        //                            }
                        //                        }
                        //                        System.err.println(sb.toString());

                        System.err.println(" Ending");

                        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1); // 1 field: the record
                        for (int i = 0; i < currentCentroids.getCentroids().size(); i++) {
                            double[] arr = currentCentroids.getCentroids().get(i);
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
                                // Frame is full, flush and reset
                                FrameUtils.flushFrame(appender.getBuffer(), writer);
                                appender.reset(new VSizeFrame(ctx), true);
                                appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                                        tupleBuilder.getSize());
                            }
                        }

                        FrameUtils.flushFrame(appender.getBuffer(), writer);

                    } catch (Throwable e) {
                        writer.fail();
                        throw new RuntimeException(e);
                    } finally {
                        in.close();
                        writer.close();
                    }
                }

            };
        }
    }
}
