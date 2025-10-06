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

import static org.apache.asterix.om.types.BuiltinType.AFLOAT;
import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

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
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableFloat;
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
import org.apache.hyracks.dataflow.std.misc.KCentroidPartitionedUUID;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

public final class CandidateCentroidsOperatorDescriptor2 extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final UUID sampleUUID;
    private final UUID centroidsUUID;
    private final UUID permitUUID;

    private RecordDescriptor embeddingDesc;
    private IScalarEvaluatorFactory args;
    private int K;

    public CandidateCentroidsOperatorDescriptor2(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            UUID sampleUUID, UUID centroidsUUID, UUID permitUUID, IScalarEvaluatorFactory args, int K) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.sampleUUID = sampleUUID;
        this.centroidsUUID = centroidsUUID;
        this.args = args;
        this.permitUUID = permitUUID;
        this.K = K;
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
                KCentroidsState allCentroidsState;
                boolean first;
                private MaterializerTaskState materializedSample;

                @Override
                public void open() throws HyracksDataException {

                    System.err.println(" Cand Centroids Activity Opened partition " + partition + " nPartitions " + nPartitions);
                    materializedSample = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(sampleUUID, partition));
                    materializedSample.open(ctx);
                    state = new CentroidsState(ctx.getJobletContext().getJobId(), new PartitionedUUID(centroidsUUID, partition));
                    allCentroidsState = new KCentroidsState(ctx.getJobletContext().getJobId(), new KCentroidPartitionedUUID(centroidsUUID, partition));
                    eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                    inputVal = new VoidPointable();
                    fta = new FrameTupleAccessor(outRecDescs[0]);
                    tuple = new FrameTupleReference();
                    first = true;

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    fta.reset(buffer);
                    for (int i = 0; i < fta.getTupleCount(); i++) {
                        // broadcast the first centroid to all partitions from partition 0
                        tuple.reset(fta, i);
                        eval.evaluate(tuple, inputVal);
                        ListAccessor listAccessorConstant = new ListAccessor();
                        if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                            //TODO: handle error
                        }
                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                        try {
                            double[] point = createPrimitveList(listAccessorConstant);
                            if (i == 0 && first) {
                                state.addCentroid(point);
                                first = false;
                            }
                            allCentroidsState.addCentroid(point);

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    if (state != null) {
                        ctx.setStateObject(state);
                    }
                    if(allCentroidsState != null) {
                        ctx.setStateObject(allCentroidsState);
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

                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader in = sampleState.creatReader();
                    try {

                        writer.open();
                        ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                        DataOutput embBytesOutput = new DataOutputStream(embBytes);
                        ByteArrayAccessibleOutputStream partitionBytes = new ByteArrayAccessibleOutputStream();
                        DataOutput partitionBytesOutput = new DataOutputStream(partitionBytes);


                        AMutableFloat aFloat = new AMutableFloat(0);
                        AMutableDouble aDouble = new AMutableDouble(0);


                        OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                        ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                        orderedListBuilder.reset(new AOrderedListType(AFLOAT, "embedding"));

                        FrameTupleAccessor fta;
                        FrameTupleReference tuple;
                        IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                        IPointable inputVal = new VoidPointable();
                        fta = new FrameTupleAccessor(outRecDescs[0]);
                        tuple = new FrameTupleReference();

                        VSizeFrame vSizeFrame = new VSizeFrame(ctx);

                        PartitionedUUID centroidsKey = new PartitionedUUID(centroidsUUID, partition);
                        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));

                        CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsKey);
                        KCentroidsState allCentroids = (KCentroidsState) ctx.getStateObject(new KCentroidPartitionedUUID(centroidsUUID, partition));
                        for (int step = 0; step < 5; step++) {

                            List<Double> preCosts = new ArrayList<>();

                            int globalTupleIndex = 0;
                            // TODO CALVINDANI: CHECK reader reset in original code
                            vSizeFrame.reset();
                            in.open();
//                                fta.reset(vSizeFrame.getBuffer(), "cost step " + step + " partition " + partition);

                            int tupleCount = allCentroids.getCentroids().size();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                                tuple.reset(fta, tupleIndex);

                                double[] point = allCentroids.getCentroids().get(tupleIndex);

                                double minCost = Double.POSITIVE_INFINITY;
                                for (double[] center : currentCentroids.getCentroids()) {
//                                        float cost =  VectorDistanceArrCalculation.euclidean_squared(point, center);
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


                            // Second pass: probabilistic selection
                            List<double[]> chosen = new ArrayList<>();
                            globalTupleIndex = 0;
                            in.seek(0); // or reopen reader
                            vSizeFrame.reset();
                            Random rand = new Random();
                            in.open();

                            tupleCount = allCentroids.getCentroids().size();
                            for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {

                                double[] point = allCentroids.getCentroids().get(tupleIndex);
//                                    float prob = 2.0f * 2 * preCosts[globalTupleIndex] / sumCosts;
                                double prob = preCosts.get(globalTupleIndex) / sumCosts;
                                if (rand.nextDouble() < prob) {
                                    chosen.add(point);
                                }
                            }

                            for (double[] c : chosen) {
                                currentCentroids.addCentroid(c);
                            }

                        }

                        int[] centroidCounts = new int[currentCentroids.getCentroids().size()];
                        vSizeFrame.reset();
                        in.open();
                        int globalTupleIndex = 0;
                        List<Double> preCosts = new ArrayList<>();


                        int tupleCount = allCentroids.getCentroids().size();
                        for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {

                            double[] point = allCentroids.getCentroids().get(tupleIndex);

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
                            preCosts.add(minCost);

                            // Increment count for the closest centroid
                            if (closestIdx >= 0) {
                                centroidCounts[closestIdx]++;
                            }
                        }


                        int dimensions = 1;
                        int n = currentCentroids.getCentroids().size();
                        double[][] centers = new double[K][dimensions];
                        List<double[]> points = currentCentroids.getCentroids();
                        double[] costArray = new double[currentCentroids.getCentroids().size()];
                        double[] preCostsArray = new double[preCosts.size()];
                        for (int i = 0; i < preCosts.size(); i++) {
                            preCostsArray[i] = preCosts.get(i);
                        }
                        Random rand = new Random();
                        int maxIterations = 10;
                        // Pick first center
                        int idx = pickWeightedIndex(rand, points, preCostsArray);
                        centers[0] = Arrays.copyOf(points.get(idx), points.get(idx).length);

                        // Initialize costArray
                        for (int i = 0; i < n; i++) {
                            costArray[i] = euclideanDistance(points.get(i), centers[0]);
                        }

                        for (int i = 1; i < K; i++) {
                            double sum = 0.0;
                            for (int j = 0; j < n; j++) {
                                sum += costArray[j] * preCostsArray[j];
                            }
                            double r = rand.nextDouble() * sum;
                            double cumulativeScore = 0.0;
                            int j = 0;
                            while (j < n && cumulativeScore < r) {
                                cumulativeScore += preCostsArray[j] * costArray[j];
                                j++;
                            }
                            if (j == 0) {
                                centers[i] = Arrays.copyOf(points.get(0), dimensions);
                            } else {
                                centers[i] = Arrays.copyOf(points.get(j - 1), dimensions);
                            }
                            // Update costArray
                            for (int p = 0; p < n; p++) {
                                costArray[p] = Math.min(euclideanDistance(points.get(p), centers[i]), costArray[p]);
                            }
                        }

                        int[] oldClosest = new int[n];
                        Arrays.fill(oldClosest, -1);
                        int iteration = 0;
                        boolean moved = true;
                        while (moved && iteration < maxIterations) {
                            moved = false;
                            double[] counts = new double[K];
                            double[][] sums = new double[K][dimensions];
                            for (int i = 0; i < n; i++) {
                                int index = findClosest(centers, points.get(i));
                                addWeighted(sums[index], points.get(i), preCostsArray[i]);
                                counts[index] += preCostsArray[i];
                                if (index != oldClosest[i]) {
                                    moved = true;
                                    oldClosest[i] = index;
                                }
                            }
                            // Update centers
                            for (int j = 0; j < K; j++) {
                                if (counts[j] == 0.0) {
                                    centers[j] = Arrays.copyOf(points.get(rand.nextInt(n)), dimensions);
                                } else {
                                    scale(sums[j], 1.0 / counts[j]);
                                    centers[j] = Arrays.copyOf(sums[j], dimensions);
                                }
                            }
                            iteration++;
                        }

                        aFloat.setValue(partition);
                        AFloatSerializerDeserializer.INSTANCE.serialize(aFloat, partitionBytesOutput);


                        StringBuilder sb = new StringBuilder("CENTROIDS CAND 1");
                        for (int i = 0; i < centers.length; i++) {
                            sb.append("\nCENTROID ").append(i).append(" COUNT ").append(centroidCounts[i]).append(" ");
                            double[] arr = centers[i];
                            for (double v : arr) {
                                sb.append(v).append(" ");
                            }
                        }
                        System.err.println(sb);

                        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1); // 1 field: the record
                        for (int i = 0; i < centers.length; i++) {
                            double[] arr = centers[i];
                            orderedListBuilder.reset(new AOrderedListType(AFLOAT, "embedding"));
                            for (double value : arr) {
//                                aFloat.setValue(value);
                                aDouble.setValue(value);
                                listStorage.reset();
                                listStorage.getDataOutput().writeByte(ATypeTag.DOUBLE.serialize());
                                ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, listStorage.getDataOutput());
                                orderedListBuilder.addItem(listStorage);
                            }
                            embBytes.reset();
                            orderedListBuilder.write(embBytesOutput, true);
                            tupleBuilder.reset();
                            tupleBuilder.addField(embBytes.getByteArray(), 0, embBytes.getLength());
//                            tupleBuilder.addField(partitionBytes.getByteArray(), 0, partitionBytes.getLength());
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
                    double sum = 0;
                    for (int i = 0; i < point.length; i++) {
                        double diff = point[i] - center[i];
                        sum += diff * diff;
                    }
                    return sum;
                }

                protected float[] createPrimitveList(ListAccessor listAccessor) throws IOException {
                    ATypeTag typeTag = listAccessor.getItemType();
                    float[] primitiveArray = new float[listAccessor.size()];
                    IPointable tempVal = new VoidPointable();
                    ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                    for (int i = 0; i < listAccessor.size(); i++) {
                        listAccessor.getOrWriteItem(i, tempVal, storage);
                        primitiveArray[i] = extractNumericVector(tempVal, typeTag);
                    }
                    return primitiveArray;
                }

                protected float extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws
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

                protected float getValueFromTag(ATypeTag typeTag, byte[] data, int offset) throws HyracksDataException {
                    return switch (typeTag) {
                        case TINYINT -> AInt8SerializerDeserializer.getByte(data, offset + 1);
                        case SMALLINT -> AInt16SerializerDeserializer.getShort(data, offset + 1);
                        case INTEGER -> AInt32SerializerDeserializer.getInt(data, offset + 1);
                        case BIGINT -> AInt64SerializerDeserializer.getLong(data, offset + 1);
                        case FLOAT -> AFloatSerializerDeserializer.getFloat(data, offset + 1);
//                    case DOUBLE -> ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                        default -> Float.NaN;
                    };
                }

            };
        }
    }
}
