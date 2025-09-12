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

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
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
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

public final class StoreMergedCentroidsOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final UUID centroidsUUID;
    private final UUID permitUUID;
    private final UUID sampleUUID;
    private final IScalarEvaluatorFactory args;

    public StoreMergedCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            UUID centroidsUUID, UUID permitUUID, UUID sampleUUID, IScalarEvaluatorFactory args) {
        super(spec, 1, 1);
        this.centroidsUUID = centroidsUUID;
        this.permitUUID = permitUUID;
        this.sampleUUID = sampleUUID;
        this.args = args;
        outRecDescs[0] = rDesc;

    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private FrameTupleAccessor fta;
            private FrameTupleReference tuple;
            IScalarEvaluator eval;
            @Override
            public void open() throws HyracksDataException {
                eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                writer.open();
                fta = new FrameTupleAccessor(outRecDescs[0]);
                tuple = new FrameTupleReference();

            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    System.err.println(
                            "Storing merged centroids partiton: " + partition + " total n partiton : " + nPartitions);
                    List<float[]> floats = deserializeFloatList(buffer.array());
                    CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsUUID);
                    for (float[] f : floats) {
                        currentCentroids.addCentroid(f);
                    }
                    ctx.setStateObject(currentCentroids);
                    System.err.println("taking the next permit");
                    IterationPermitState iterPermitState =
                            (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));
                    Semaphore proceed = iterPermitState.getPermit();
                    proceed.release();
                    System.err.println("released the next permit");
                } catch (Exception e) {
                }

                //                IterationPermitState permitState =
                //                        (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));

                // store the merged centroids
                //                ctx.setStateObject(new CentroidsState(ctx.getJobletContext().getJobId(), centroidsUUID));

                //                permitState.getPermit().release(1);
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
                PartitionedUUID partitionUUID = new PartitionedUUID(sampleUUID,partition);

                CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsUUID);
                MaterializerTaskState materializerTaskState = (MaterializerTaskState) ctx.getStateObject(partitionUUID);
                GeneratedRunFileReader in = materializerTaskState.creatReader();
                int[] centroidCounts = new int[currentCentroids.getCentroids().size()];
                VSizeFrame vSizeFrame = new VSizeFrame(ctx);
                vSizeFrame.reset();
                in.open();
                int globalTupleIndex = 0;
                VoidPointable inputVal = new VoidPointable();
                List<Float> preCosts = new ArrayList<>();
                FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                try {

                while (in.nextFrame(vSizeFrame)) {
                    fta.reset(vSizeFrame.getBuffer(), "cost step ");
                    int tupleCount = fta.getTupleCount();
                    for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                        tuple.reset(fta, tupleIndex);
                        eval.evaluate(tuple, inputVal);
                        ListAccessor listAccessorConstant = new ListAccessor();
                        if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                            continue;
                        }
                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                        float[] point = createPrimitveList(listAccessorConstant);

                        // Find closest centroid
                        int closestIdx = -1;
                        float minCost = Float.POSITIVE_INFINITY;
                        List<float[]> centroids = currentCentroids.getCentroids();
                        for (int i = 0; i < centroids.size(); i++) {
//                            System.out.println("-------centroid------------- " + i + " " + java.util.Arrays.toString(centroids.get(i)));
                            float cost = euclideanDistance(point, centroids.get(i));
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
                }

                    byte[] serialized = serializeFloat(preCosts);
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
                    tupleBuilder.addField(serialized, 0, serialized.length);

// 3. Write tuple to frame
                    FrameUtils.appendToWriter(writer, appender, tupleBuilder.getFieldEndOffsets(),
                            tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                    // TODO CALVIN DANI: check the API and correct design?
                    appender.write(writer, true);
                }
                catch (Exception e) {
                    System.err.println("failed to store centroids " + e.getMessage());
                }
                finally {
                    in.close();

                    System.err.println("CLOSING STORE MERGED CENTROIDS partition" + partition + " total n partiton : " + nPartitions);
                    System.err.println("current centroids  " + currentCentroids.getCentroids().toString());
                    // compute weights and send to next writer
                    writer.close();
                }

// 2. Build tuple with one field



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

            public static byte[] serializeFloat(List<Float> floatList) throws IOException {
                // TODO CALVIN DANI: to chnage
                // 1 FRAME PER LOOP or 1 TUPLE PER LOOP?
                // KEEP TRACK OF ALL PARTITON END SIGNAL
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeInt(floatList.size());
                for (Float cost : floatList) {
//                    for (float f : arr) {
                        dos.writeFloat(cost.floatValue());
//                    }
                }
                dos.flush();
                return baos.toByteArray();
            }

            private float euclideanDistance(float[] point, float[] center) {
                float sum = 0;
                for (int i = 0; i < point.length; i++) {
                    float diff = point[i] - center[i];
                    sum += diff * diff;
                }
                return (float) Math.sqrt(sum);
            }


            // Deserialization
            public static List<float[]> deserializeFloatList(byte[] bytes) throws IOException {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes, 9, bytes.length - 9));
                int numArrays = dis.readInt();
                StringBuilder sb = new StringBuilder();
                sb.append("numArrays ");
                sb.append(numArrays);
                List<float[]> result = new ArrayList<>(numArrays);
                for (int i = 0; i < numArrays; i++) {
                    int arrLen = dis.readInt();
                    sb.append("| arrLen ");
                    sb.append(arrLen);
                    sb.append('[');
                    float[] arr = new float[arrLen];
                    for (int j = 0; j < arrLen; j++) {
                        arr[j] = dis.readFloat();
                        sb.append(arr[j]);
                        if (j < arrLen - 1) {
                            sb.append(',');
                        }
                    }
                    sb.append(']');
                    result.add(arr);
                }
                System.err.println("merging centroids " + sb.toString());

                return result;
            }
        };
    }
}
