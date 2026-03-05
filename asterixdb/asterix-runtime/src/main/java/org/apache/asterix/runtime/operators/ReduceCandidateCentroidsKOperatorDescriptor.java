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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
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
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public final class ReduceCandidateCentroidsKOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final UUID centroidsUUID;
    private final UUID permitUUID;
    private final int dimensions = 1;
    private final int n = 55;
    double[] accumulatedWeights;
    RecordDescriptor onlyCentroids;
    IScalarEvaluatorFactory args;

    public ReduceCandidateCentroidsKOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            RecordDescriptor onlyCentroids, UUID centroidsUUID, UUID permitUUID, IScalarEvaluatorFactory args) {
        super(spec, 1, 1);
        this.centroidsUUID = centroidsUUID;
        this.permitUUID = permitUUID;
        outRecDescs[0] = rDesc;
        accumulatedWeights = new double[n];
        this.onlyCentroids = onlyCentroids;
        this.args = args;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {


            FrameTupleAccessor fta;
            FrameTupleReference tRef;
            IScalarEvaluator eval;
            VoidPointable inputVal;
            OrderedListBuilder orderedListBuilder;
            AMutableFloat aFloat;
            AMutableDouble aDouble;
            ArrayBackedValueStorage listStorage;
            ByteArrayAccessibleOutputStream embBytes;
            DataOutput embBytesOutput;


            @Override
            public void open() throws HyracksDataException {
                fta = new FrameTupleAccessor(onlyCentroids);
                tRef = new FrameTupleReference();
                writer.open();
                eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                inputVal = new VoidPointable();
                orderedListBuilder = new OrderedListBuilder();
                aFloat = new AMutableFloat(0);
                listStorage = new ArrayBackedValueStorage();
                embBytes = new ByteArrayAccessibleOutputStream();
                embBytesOutput = new DataOutputStream(embBytes);

            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    fta.reset(buffer);
                    tRef.reset(fta,0);
                    eval.evaluate(tRef,inputVal);
                    ListAccessor listAccessorConstant = new ListAccessor();
                    if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
//                        continue;
                    }
                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                    float[] point = createPrimitveList(listAccessorConstant);

                    List<Float> weights = deserializeWeight(buffer.array());
                    for (int i = 0; i < point.length; i++) {
                        accumulatedWeights[i] += point[i];
                    }

                } catch (Exception e) {
                }

            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void flush() throws HyracksDataException {
                writer.flush();
            }

            @Override
            public void close() throws HyracksDataException {
                CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsUUID);
                // compute weights and send to next writer

                try {
                    int dimensions = 1;
                    int k = 2;
                    int n = currentCentroids.getCentroids().size();
                    double[][] centers = new double[k][dimensions];
                    List<double[]> points = currentCentroids.getCentroids();
                    double[] costArray = new double[currentCentroids.getCentroids().size()];

                    Random rand = new Random();
                    int maxIterations = 10;
                    // Pick first center
                    int idx = pickWeightedIndex(rand, points, accumulatedWeights);
                    centers[0] = Arrays.copyOf(points.get(idx), dimensions);

                    // Initialize costArray
                    for (int i = 0; i < n; i++) {
                        costArray[i] = fastSquaredDistance(points.get(i), centers[0]);
                    }

                    for (int i = 1; i < k; i++) {
                        double sum = 0.0;
                        for (int j = 0; j < n; j++) {
                            sum += costArray[j] * accumulatedWeights[j];
                        }
                        double r = rand.nextDouble() * sum;
                        double cumulativeScore = 0.0;
                        int j = 0;
                        while (j < n && cumulativeScore < r) {
                            cumulativeScore += accumulatedWeights[j] * costArray[j];
                            j++;
                        }
                        if (j == 0) {
                            centers[i] = Arrays.copyOf(points.get(0), dimensions);
                        } else {
                            centers[i] = Arrays.copyOf(points.get(j - 1), dimensions);
                        }
                        // Update costArray
                        for (int p = 0; p < n; p++) {
                            costArray[p] = Math.min(fastSquaredDistance(points.get(p), centers[i]), costArray[p]);
                        }
                    }

                    int[] oldClosest = new int[n];
                    Arrays.fill(oldClosest, -1);
                    int iteration = 0;
                    boolean moved = true;
                    while (moved && iteration < maxIterations) {
                        moved = false;
                        double[] counts = new double[k];
                        double[][] sums = new double[k][dimensions];
                        for (int i = 0; i < n; i++) {
                            int index = findClosest(centers, points.get(i));
                            addWeighted(sums[index], points.get(i), accumulatedWeights[i]);
                            counts[index] += accumulatedWeights[i];
                            if (index != oldClosest[i]) {
                                moved = true;
                                oldClosest[i] = index;
                            }
                        }
                        // Update centers
                        for (int j = 0; j < k; j++) {
                            if (counts[j] == 0.0) {
                                centers[j] = Arrays.copyOf(points.get(rand.nextInt(n)), dimensions);
                            } else {
                                scale(sums[j], 1.0 / counts[j]);
                                centers[j] = Arrays.copyOf(sums[j], dimensions);
                            }
                        }
                        iteration++;
                    }
//
//                    for (float[] f : centers) {
//                        System.err.println("final centroids " + Arrays.toString(f));
//                        currentCentroids.addCentroid(f);
//                    }
                    ctx.setStateObject(currentCentroids);
                    FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1); // 1 field: the record
                    for (int i = 0; i < k; i++) {
                        double[] arr = centers[i];

                        orderedListBuilder.reset(new AOrderedListType(AFLOAT, "embedding"));
                        for (double value : arr) {
//                            aFloat.setValue(value);
                            aDouble.setValue(value);
                            listStorage.reset();
                            listStorage.getDataOutput().writeByte(ATypeTag.FLOAT.serialize());
                            AFloatSerializerDeserializer.INSTANCE.serialize(aFloat, listStorage.getDataOutput());
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

                } catch (Exception e) {
                }

                writer.close();
            }

            public static List<Float> deserializeWeight(byte[] bytes) throws IOException {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes, 9, bytes.length - 9));
                List<Float> floatList = new ArrayList<>();
                //                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                int size = dis.readInt();
                for (int i = 0; i < size; i++) {
                    floatList.add(dis.readFloat());
                }
                return floatList;
            }

            private int pickWeightedIndex(Random rand, List<double[]> data, double[] weights) {
                double sum = 0f;
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

            private double fastSquaredDistance(double[] a, double[] b) {
                double sum = 0.0;
                for (int i = 0; i < a.length; i++) {
                    double diff = a[i] - b[i];
                    sum += diff * diff;
                }
                return sum;
            }

            private int findClosest(double[][] centers, double[] point) {
                int best = 0;
                double bestDist = fastSquaredDistance(centers[0], point);
                for (int i = 1; i < centers.length; i++) {
                    double dist = fastSquaredDistance(centers[i], point);
                    if (dist < bestDist) {
                        bestDist = dist;
                        best = i;
                    }
                }
                return best;
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
