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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
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
import org.apache.hyracks.control.cc.job.Task;
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
                                                UUID sampleUUID, UUID centroidsUUID, UUID permitUUID, IScalarEvaluatorFactory args, int K, int maxScalableKmeansIter) {
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

                @Override
                public void open() throws HyracksDataException {

                    System.err.println(" Cand Centroids Activity Opened partition " + partition + " nPartitions " + nPartitions);
                    materializedSample = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(sampleUUID, partition));
                    materializedSample.open(ctx);
                    state = new CentroidsState(ctx.getJobletContext().getJobId(), new PartitionedUUID(centroidsUUID, partition));
                    eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                    inputVal = new VoidPointable();
                    fta = new FrameTupleAccessor(outRecDescs[0]);
                    tuple = new FrameTupleReference();
                    first = true;

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    fta.reset(buffer);
                    if (first) {
                        tuple.reset(fta, 0);
                        eval.evaluate(tuple, inputVal);
                        ListAccessor listAccessorConstant = new ListAccessor();
                        if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
//                            ExceptionUtil.warnUnsupportedType(ctx, sourceLoc, getIdentifier().getName(), ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]));
                        }
                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                        try {
                            double[] point = createPrimitveList(listAccessorConstant);
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

                protected double[] createPrimitveList(ListAccessor listAccessor) throws IOException {
                    ATypeTag typeTag = listAccessor.getItemType();
                    double[] primitiveArray = new double[listAccessor.size()];
                    IPointable tempVal = new VoidPointable();
                    ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                    for (int i = 0; i < listAccessor.size(); i++) {
                        listAccessor.getOrWriteItem(i, tempVal, storage);
                        primitiveArray[i] = extractNumericVector(tempVal, typeTag);
                    }
                    return primitiveArray;
                }

                protected double extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws HyracksDataException {
                    byte[] data = pointable.getByteArray();
                    int offset = pointable.getStartOffset();
                    if (derivedTypeTag.isNumericType()) {
                        return getValueFromTag(derivedTypeTag, data, offset);
                    } else if (derivedTypeTag == ATypeTag.ANY) {
                        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
                        return getValueFromTag(typeTag, data, offset);
                    } else {
                        throw new HyracksDataException("Unsupported type tag for numeric vector extraction: " + derivedTypeTag);
                    }
                }


                protected double getValueFromTag(ATypeTag typeTag, byte[] data, int offset) {
                    return switch (typeTag) {
                        case TINYINT -> AInt8SerializerDeserializer.getByte(data, offset + 1);
                        case SMALLINT -> AInt16SerializerDeserializer.getShort(data, offset + 1);
                        case INTEGER -> AInt32SerializerDeserializer.getInt(data, offset + 1);
                        case BIGINT -> AInt64SerializerDeserializer.getLong(data, offset + 1);
                        case FLOAT -> AFloatSerializerDeserializer.getFloat(data, offset + 1);
                        case DOUBLE -> ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                        default -> Float.NaN;
                    };
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
                        fta = new FrameTupleAccessor(outRecDescs[0]);
                        tuple = new FrameTupleReference();
                        VSizeFrame vSizeFrame = new VSizeFrame(ctx);
                        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));



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
                        for (int step = 0; step < maxScalableKmeansIter ; step++) {

                            List<Double> preCosts = new ArrayList<>();

                            int globalTupleIndex = 0;
                            vSizeFrame.reset();

                            // Calculating the cost of chosen centroids and the rest of the points
                            in.open();
                            while (in.nextFrame(vSizeFrame)) {
                                fta.reset(vSizeFrame.getBuffer());
                                int tupleCount = fta.getTupleCount();
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                                    tuple.reset(fta, tupleIndex);
                                    eval.evaluate(tuple, inputVal);
                                    ListAccessor listAccessorConstant = new ListAccessor();
                                    if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                        continue;
                                    }
                                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    double[] point = createPrimitveList(listAccessorConstant);
                                    double minCost = Double.POSITIVE_INFINITY;
                                    for (double[] center : currentCentroids.getCentroids()) {
                                        double cost = euclideanDistance(point, center);
                                        if (cost < minCost) minCost = cost;
                                    }
                                    preCosts.add(minCost);
                                }
                            }
                            in.close();



                            // Compute sumCosts
                            float sumCosts = 0f;
                            for (double c : preCosts) sumCosts += c;


                            // Second pass: probabilistic selection
                            List<double[]> chosen = new ArrayList<>();
                            globalTupleIndex = 0;
                            in.seek(0); // or reopen reader
                            vSizeFrame.reset();
                            Random rand = new Random();
                            double cumulative = 0.0;

                            // Oversampling factor of 2
                            double l = K * 2; // expected number of candidates per round

                            // Selecting new centroids based on probability based on their cost
                            in.open();
                            while (in.nextFrame(vSizeFrame)) {
                                fta.reset(vSizeFrame.getBuffer());
                                int tupleCount = fta.getTupleCount();
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                                    cumulative += preCosts.get(globalTupleIndex);

                                    double prob = l * preCosts.get(globalTupleIndex) / sumCosts;
                                    if (prob > 1.0) prob = 1.0;  // cap at 1

                                    if (rand.nextDouble() < prob) {
                                        tuple.reset(fta, tupleIndex);
                                        eval.evaluate(tuple, inputVal);
                                        ListAccessor listAccessorConstant = new ListAccessor();
                                        if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                            continue;
                                        }
                                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                        double[] point = createPrimitveList(listAccessorConstant);
                                        chosen.add(point);
                                    }
                                }
                            }
                            in.close();


                            for (double[] c : chosen) {
                                currentCentroids.addCentroid(c);
                            }

                        }


                        double[] centroidCounts = new double[currentCentroids.getCentroids().size()];


                        vSizeFrame.reset();
                        int globalTupleIndex = 0;

                        // Calculate the weights for each centroid
                        in.open();
                        while (in.nextFrame(vSizeFrame)) {
                            fta.reset(vSizeFrame.getBuffer());
                            int tupleCount = fta.getTupleCount();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {

                                tuple.reset(fta, tupleIndex);
                                eval.evaluate(tuple, inputVal);
                                ListAccessor listAccessorConstant = new ListAccessor();
                                if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    continue;
                                }
                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                double[] point = createPrimitveList(listAccessorConstant);

                                // Find closest centroid
                                int closestIdx = -1;
                                double minCost = Double.POSITIVE_INFINITY;
                                List<double[]> centroids = currentCentroids.getCentroids();
                                for (int i = 0; i < centroids.size(); i++) {
                                    double cost = euclideanDistance(point, centroids.get(i));
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
                        in.close();




                        int dim = currentCentroids.getCentroids().get(0).length;
                        int n = currentCentroids.getCentroids().size();

                        double[][] centers = new double[K][dim];
                        List<double[]> points = currentCentroids.getCentroids();
                        double[] costArray = new double[currentCentroids.getCentroids().size()];

                        Random rand = new Random();
                        int maxKMeansIterations = 20;
                        // Pick first center
                        int idx = pickWeightedIndex(rand, points, centroidCounts);
                        centers[0] = Arrays.copyOf(points.get(idx), points.get(idx).length);

                        // Initialize costArray
                        for (int i = 0; i < n; i++) {
                            costArray[i] = euclideanDistance(points.get(i), centers[0]);
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
                                costArray[p] = Math.min(euclideanDistance(points.get(p), centers[i]), costArray[p]);
                            }
                        }

                        // Weighted KMEANS++ initialization
                        int[] oldClosest = new int[n];
                        Arrays.fill(oldClosest, -1);
                        int iteration = 0;
                        boolean moved = true;
                        while (moved && iteration < maxKMeansIterations) {
                            moved = false;
                            double[] counts = new double[K];
                            double[][] sums = new double[K][dim];
                            for (int i = 0; i < n; i++) {
                                int index = findClosest(centers, points.get(i));
                                addWeighted(sums[index], points.get(i), centroidCounts[i]);
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
                                    for (double w : centroidCounts) totalWeight += w;
                                    double r = rand.nextDouble() * totalWeight;
                                    double cumulative = 0.0;
                                    int selectedIdx = 0;
                                    for (; selectedIdx < n; selectedIdx++) {
                                        cumulative += centroidCounts[selectedIdx];
                                        if (cumulative >= r) break;
                                    }
                                    centers[j] = Arrays.copyOf(points.get(Math.min(selectedIdx, n - 1)), dim);
                                } else {
                                    scale(sums[j], 1.0 / counts[j]);
                                    centers[j] = Arrays.copyOf(sums[j], dim);
                                }
                            }
                            iteration++;
                        }



                        currentCentroids.clearCentroids();


//                        System.err.println(sb.toString());

                        // Now run Lloyd's algorithm in parallel on each partition

                        double[][] sumVectors = new double[K][dim];
                        int[] counts = new int[K];
                        for (int step = 0; step < 20; step++) {
//                            System.err.println(" Step at Lloyds" + step + " for partition " + partition);

                            // Each partition runs Lloyd's algorithm using initialCentroids
                            List<List<float[]>> clusters = new ArrayList<>(K);
                            for (int i = 0; i < K; i++) clusters.add(new ArrayList<>());

                            vSizeFrame.reset();
                            in.open();
                            while (in.nextFrame(vSizeFrame)) {
                                fta.reset(vSizeFrame.getBuffer());
                                int tupleCount = fta.getTupleCount();
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                    tuple.reset(fta, tupleIndex);
                                    eval.evaluate(tuple, inputVal);
                                    ListAccessor listAccessorConstant = new ListAccessor();
                                    if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                        continue;
                                    }
                                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    double[] point = createPrimitveList(listAccessorConstant);

                                    // Assign to nearest centroid
                                    int bestIdx = 0;
                                    double minDist = Float.POSITIVE_INFINITY;
                                    for (int cIdx = 0; cIdx < K; cIdx++) {
                                        double dist = euclideanDistance(point, centers[cIdx]);
                                        if (dist < minDist) {
                                            minDist = dist;
                                            bestIdx = cIdx;
                                        }
                                    }
                                    for (int d = 0; d < dim; d++) {
                                        sumVectors[bestIdx][d] += point[d];
                                    }
                                    counts[bestIdx]++;
                                }
                            }
                            in.close();
                            // Update centroids
                        }


                        List<double[]> finalCentroids = new ArrayList<>(K);
                        for (int cIdx = 0; cIdx < K; cIdx++) {
                            double[] centroid = new double[dim];
                            if (counts[cIdx] > 0) {
                                for (int d = 0; d < dim; d++) {
                                    centroid[d] = sumVectors[cIdx][d] / counts[cIdx];
                                }
                            } else {
                                // Handle empty cluster (optional: keep previous centroid or set to zero)
                                for (int d = 0; d < dim; d++) {
                                    centroid[d] = 0f;
                                }
                            }
                            finalCentroids.add(centroid);
                        }

                        List<List<double[]>> allClusters = new ArrayList<>();
                        allClusters.add(finalCentroids);

                        int requiedSpace = K * dim * Double.BYTES;
                        int minFrameSize = ctx.getJobletContext().getInitialFrameSize();
                        int maxFrameSize = ctx.getJobletContext().getMaxFrameSize();

                        int iterations = 0;
                        int currentK = K;
                        int frameSize = minFrameSize;

                        while (currentK * dim * Double.BYTES > frameSize) {
                            currentK /= 2;
                            iterations++;

                            List<double[]> currentPoints = allClusters.get(allClusters.size() - 1);
                            List<double[]> currentLvlCentroids = new ArrayList<>();
                            currentLvlCentroids.add(currentPoints.get(0));

                            for (int step = 1; step < currentK; step++) {

//                                System.err.println(" Step " + step + " for partition " + partition + " with current centroids " + currentLvlCentroids.getCentroids().size());
                                List<Double> preCosts = new ArrayList<>();

                                globalTupleIndex = 0;
                                // TODO CALVINDANI: CHECK reader reset in original code


                                int tupleCount = currentPoints.size();
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                                    double[] point = currentPoints.get(globalTupleIndex);

                                    double minCost = Double.POSITIVE_INFINITY;
                                    for (double[] center : currentLvlCentroids) {
                                        double cost = euclideanDistance(point, center);
                                        if (cost < minCost) minCost = cost;
                                    }
                                    preCosts.add(minCost);
                                    // Accumulate minCost for each tuple
                                }
                                double l = K * 2; // expected number of candidates per round

                                // Compute sumCosts
                                float sumCosts = 0f;
                                for (double c : preCosts) sumCosts += c;


//                                System.err.println(" Step after pre cost" + step + " precost size " + preCosts.size() + "globalindex" + globalTupleIndex + " for partition " + partition);

                                // Second pass: probabilistic selection
                                List<double[]> chosen = new ArrayList<>();
                                globalTupleIndex = 0;
//                                in.seek(0); // or reopen reader
                                vSizeFrame.reset();
                                rand = new Random();
                                double cumulative = 0.0;
                                double prob = rand.nextDouble() * sumCosts;
                                int pointCount = currentPoints.size();
                                for (int tupleIndex = 0; tupleIndex < pointCount; tupleIndex++, globalTupleIndex++) {
                                    cumulative += preCosts.get(globalTupleIndex);
                                    if (cumulative >= prob) {
                                        double[] point = currentPoints.get(globalTupleIndex);
                                        chosen.add(point);
                                        break;
                                    }
                                }
//                                }


                                for (double[] c : chosen) {
                                    currentLvlCentroids.add(c);
                                }
//                                sb = new StringBuilder(" Current centroids at step " + step + " partition " + partition + " : ");
//                                System.err.println(sb.toString());

                            }


                            float[][] sumLvlVectors = new float[currentK][dim];
                            int[] countsLvl = new int[currentK];
                            for (int stepLvl = 0; stepLvl < 20; stepLvl++) {
//                                System.err.println(" Step at Lloyds" + stepLvl + " for partition " + partition);

                                // Each partition runs Lloyd's algorithm using initialCentroids
                                List<List<float[]>> clusters = new ArrayList<>(currentK);
                                for (int i = 0; i < currentK; i++) clusters.add(new ArrayList<>());

                                vSizeFrame.reset();
                                in.open();
                                int tupleCount = currentLvlCentroids.size();
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++) {
                                    double[] point = currentLvlCentroids.get(tupleIndex);

                                    // Assign to nearest centroid
                                    int bestIdx = 0;
                                    double minDist = Float.POSITIVE_INFINITY;
                                    for (int cIdx = 0; cIdx < currentK; cIdx++) {
                                        double dist = euclideanDistance(point, currentLvlCentroids.get(cIdx));
                                        if (dist < minDist) {
                                            minDist = dist;
                                            bestIdx = cIdx;
                                        }
                                    }
                                    for (int d = 0; d < dim; d++) {
                                        sumLvlVectors[bestIdx][d] += point[d];
                                    }
                                    countsLvl[bestIdx]++;
                                }
                            }

                            List<double[]> finalLvlCentroids = new ArrayList<>(K);
                            for (int cIdx = 0; cIdx < currentK; cIdx++) {
                                double[] centroid = new double[dim];
                                if (counts[cIdx] > 0) {
                                    for (int d = 0; d < dim; d++) {
                                        centroid[d] = sumVectors[cIdx][d] / counts[cIdx];
                                    }
                                } else {
                                    // Handle empty cluster (optional: keep previous centroid or set to zero)
                                    for (int d = 0; d < dim; d++) {
                                        centroid[d] = 0f;
                                    }
                                }
                                finalLvlCentroids.add(centroid);
                            }


                            allClusters.add(finalLvlCentroids);


                        }

                        int level =  allClusters.size();
                        StringBuilder sb = new StringBuilder(" Final centroids at  partition " + partition + " : with level " + allClusters.size() + " cost " + currentK * dim * Double.BYTES + " frameSize " + frameSize);
                        for (List<double[]> centroids : allClusters) {
                            sb.append(" Level with ").append(level).append(" with K centroids ").append(centroids.size()).append(" for partition ").append(partition).append("\n");
                            level--;
                            for (double[] c : centroids) {
                                sb.append(Arrays.toString(c)).append(" ,  \n");
                            }
                        }
                        System.err.println(sb.toString());


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
                            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                                // Frame is full, flush and reset
                                FrameUtils.flushFrame(appender.getBuffer(), writer);
                                appender.reset(new VSizeFrame(ctx), true);
                                appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
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

                private int findClosest(double[][] centers, double[] point) {
                    int best = 0;
                    double bestDist = euclideanDistance(centers[0], point);
                    for (int i = 1; i < centers.length; i++) {
                        double dist = euclideanDistance(centers[i], point);
                        if (dist < bestDist) {
                            bestDist = dist;
                            best = i;
                        }
                    }
                    return best;
                }


                private int pickWeightedIndex(Random rand, List<double[]> data, double[] weights) {
                    float sum = 0f;
                    for (double w : weights) {
                        sum += w;
                    }
                    double r = rand.nextDouble() * sum;
                    double curWeight = 0.0;
                    int i = 0;
                    while (i < data.size() && curWeight < r) {
                        curWeight += weights[i];
                        i++;
                    }
                    return Math.max(0, i - 1);
                }

                private void addWeighted(double[] sum, double[] point, double weight) {
                    for (int i = 0; i < sum.length; i++) {
                        sum[i] += point[i] * weight;
                    }
                }

                private void scale(double[] vec, double factor) {
                    for (int i = 0; i < vec.length; i++) {
                        vec[i] *= factor;
                    }
                }


                private double euclideanDistance(double[] point, double[] center) {
                    float sum = 0;
                    for (int i = 0; i < point.length; i++) {
                        double diff = point[i] - center[i];
                        sum += diff * diff;
                    }
                    return sum;
                }

                protected double[] createPrimitveList(ListAccessor listAccessor) throws IOException {
                    ATypeTag typeTag = listAccessor.getItemType();
                    double[] primitiveArray = new double[listAccessor.size()];
                    IPointable tempVal = new VoidPointable();
                    ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                    for (int i = 0; i < listAccessor.size(); i++) {
                        listAccessor.getOrWriteItem(i, tempVal, storage);
                        primitiveArray[i] = extractNumericVector(tempVal, typeTag);
                    }
                    return primitiveArray;
                }

                protected double extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws
                        HyracksDataException {
                    byte[] data = pointable.getByteArray();
                    int offset = pointable.getStartOffset();
                    if (derivedTypeTag.isNumericType()) {
                        return getValueFromTag(derivedTypeTag, data, offset);
                    } else if (derivedTypeTag == ATypeTag.ANY) {
                        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
                        return getValueFromTag(typeTag, data, offset);
                    } else {
                        throw new HyracksDataException("Unsupported type tag for numeric vector extraction: " + derivedTypeTag);
                    }
                }

                protected double getValueFromTag(ATypeTag typeTag, byte[] data, int offset) throws HyracksDataException {
                    return switch (typeTag) {
                        case TINYINT -> AInt8SerializerDeserializer.getByte(data, offset + 1);
                        case SMALLINT -> AInt16SerializerDeserializer.getShort(data, offset + 1);
                        case INTEGER -> AInt32SerializerDeserializer.getInt(data, offset + 1);
                        case BIGINT -> AInt64SerializerDeserializer.getLong(data, offset + 1);
                        case FLOAT -> AFloatSerializerDeserializer.getFloat(data, offset + 1);
                        case DOUBLE -> ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                        default -> Float.NaN;
                    };
                }

            };
        }
    }
}
