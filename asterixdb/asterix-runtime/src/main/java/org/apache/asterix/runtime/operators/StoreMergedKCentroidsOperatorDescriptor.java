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
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
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
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

public final class StoreMergedKCentroidsOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final UUID centroidsUUID;
    private final UUID permitUUID;
    private final UUID sampleUUID;
    private final IScalarEvaluatorFactory args;
    private final RecordDescriptor centroidDesc;

    public StoreMergedKCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            RecordDescriptor centroidDesc, UUID centroidsUUID, UUID permitUUID, UUID sampleUUID,
            IScalarEvaluatorFactory args) {
        super(spec, 1, 1);
        this.centroidsUUID = centroidsUUID;
        this.permitUUID = permitUUID;
        this.sampleUUID = sampleUUID;
        this.args = args;
        outRecDescs[0] = rDesc;
        this.centroidDesc = centroidDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                                                   IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private FrameTupleAccessor fta;
            private FrameTupleAccessor fta2;
            private FrameTupleReference tuple;
            VoidPointable inputVal;
            IScalarEvaluator eval;
            CentroidsState currentCentroids;
            OrderedListBuilder orderedListBuilder;
            AMutableFloat aFloat;
            ArrayBackedValueStorage listStorage;
            ByteArrayAccessibleOutputStream embBytes;
            DataOutput embBytesOutput;

            @Override
            public void open() throws HyracksDataException {

                System.err.println(" Store Centroids Activity Opened partition " + partition + " nPartitions " + nPartitions);

                eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                writer.open();
                fta = new FrameTupleAccessor(outRecDescs[0]);
                fta2 = new FrameTupleAccessor(centroidDesc);
                tuple = new FrameTupleReference();
                inputVal = new VoidPointable();
                currentCentroids = (CentroidsState) ctx.getStateObject(centroidsUUID);
                orderedListBuilder = new OrderedListBuilder();
                aFloat = new AMutableFloat(0);
                listStorage = new ArrayBackedValueStorage();
                embBytes = new ByteArrayAccessibleOutputStream();
                embBytesOutput = new DataOutputStream(embBytes);

            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    fta2.reset(buffer, "Store merged");
                    currentCentroids.getCentroids().clear();
                    for (int i = 0; i < fta2.getTupleCount(); i++) {
                        tuple.reset(fta2, i);

                        eval.evaluate(tuple, inputVal);
                        ListAccessor listAccessorConstant = new ListAccessor();
                        if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                            continue;
                        }
                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                        float[] point = createPrimitveList(listAccessorConstant);
                        currentCentroids.addCentroid(point);
                    }
                    ctx.setStateObject(currentCentroids);
                    IterationPermitState iterPermitState =
                            (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));
                    Semaphore proceed = iterPermitState.getPermit();
                    proceed.release();
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
                PartitionedUUID partitionUUID = new PartitionedUUID(sampleUUID, partition);

                CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsUUID);

                System.err.println(" Storing centroids " + currentCentroids.getCentroids().size() + " for partition " + partition);
                for (float[] centroid : currentCentroids.getCentroids()) {
                    System.err.print("Centroid: ");
                    for (float value : centroid) {
                        System.err.print(value + " ");
                    }
                    System.err.println();
                }
                FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));

                try {
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1); // 1 field: the record
                    orderedListBuilder.reset(new AOrderedListType(AFLOAT, "embedding"));
                    for(float[] point : currentCentroids.getCentroids()) {
                        for (float value : point) {
                            aFloat.setValue(value);
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
                    System.err.println("failed to store centroids " + e.getMessage());
                    writer.fail();
                } finally {
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

            private float euclideanDistance(float[] point, float[] center) {
                float sum = 0;
                for (int i = 0; i < point.length; i++) {
                    float diff = point[i] - center[i];
                    sum += diff * diff;
                }
                return (float) Math.sqrt(sum);
            }

        };
    }
}
