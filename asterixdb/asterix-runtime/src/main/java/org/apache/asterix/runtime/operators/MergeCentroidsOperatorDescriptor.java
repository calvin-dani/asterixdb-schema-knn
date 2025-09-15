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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public final class MergeCentroidsOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final RecordDescriptor centroidDesc;
    private IScalarEvaluatorFactory args;
    private IScalarEvaluatorFactory part;
    private IScalarEvaluatorFactory isLast;
    private ListAccessor listAccessorConstant;
    private List<float[]> accumulator;
    private FrameTupleAccessor fta;
    private FrameTupleAccessor fta2;
    private int totalPartitions;

    public MergeCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            RecordDescriptor centroidDesc, IScalarEvaluatorFactory args, IScalarEvaluatorFactory part,
            IScalarEvaluatorFactory isLast, int totalPartitions) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.centroidDesc = centroidDesc;
        this.args = args;
        this.part = part;
        this.isLast = isLast;
        this.totalPartitions = totalPartitions;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            IScalarEvaluator eval;
            IScalarEvaluator partEval;
            IScalarEvaluator isLastEval;
            VoidPointable inputVal = new VoidPointable();
            VoidPointable partVal = new VoidPointable();
            VoidPointable isLastVal = new VoidPointable();
            FrameTupleAppender appender;
            FrameTupleReference tuple;
            Set<Integer> partitionSet;
            OrderedListBuilder orderedListBuilder;
            AMutableFloat aFloat;
            ArrayBackedValueStorage listStorage;
            ByteArrayAccessibleOutputStream embBytes;
            DataOutput embBytesOutput;

            @Override
            public void open() throws HyracksDataException {

                System.err.println(" Merge Centroids Activity Opened partition " + partition + " nPartitions " + nPartitions);

                writer.open();
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
                listAccessorConstant = new ListAccessor();
                accumulator = new ArrayList<>();
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
                fta = new FrameTupleAccessor(centroidDesc);
                tuple = new FrameTupleReference();
                eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                partEval = part.createScalarEvaluator(new EvaluatorContext(ctx));
                isLastEval = isLast.createScalarEvaluator(new EvaluatorContext(ctx));
                partitionSet = new HashSet<>();
                orderedListBuilder = new OrderedListBuilder();
                aFloat = new AMutableFloat(0);
                listStorage = new ArrayBackedValueStorage();
                embBytes = new ByteArrayAccessibleOutputStream();
                embBytesOutput = new DataOutputStream(embBytes);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // merge

                try {
//                    fta.reset(buffer,"Debugging frame for tuple");
                    fta.reset(buffer);
                    for(int tupleIndex = 0; tupleIndex < fta.getTupleCount(); tupleIndex++) {
                        tuple.reset(fta,tupleIndex);
                        eval.evaluate(tuple,inputVal);
                        partEval.evaluate(tuple,partVal);
                        isLastEval.evaluate(tuple,isLastVal);

                        // Extract boolean from VoidPointable
                        boolean isLast = isLastVal.getByteArray()[isLastVal.getStartOffset() + 1] != 0;

                        // Extract int32 from VoidPointable
                        int part = AInt32SerializerDeserializer.getInt(partVal.getByteArray(), partVal.getStartOffset() + 1);
                        if(isLast){
                            partitionSet.add(part);
                        }
                        ListAccessor listAccessorConstant = new ListAccessor();
                        if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                            continue;
                        }
                        listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                        float[] point = createPrimitveList(listAccessorConstant);
                        accumulator.add(point);
                    }

                    if(totalPartitions == partitionSet.size()) {
                        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1); // 1 field: the record
                        for (int i = 0; i < accumulator.size(); i++) {
                            float[] arr = accumulator.get(i);
                            orderedListBuilder.reset(new AOrderedListType(AFLOAT, "embedding"));
                            for (float value : arr) {
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
//                                FrameUtils.appendToWriter(writer,appender, tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                                // Frame is full, flush and reset
                                FrameUtils.flushFrame(appender.getBuffer(), writer);
                                appender.reset(new VSizeFrame(ctx), true);
                                appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                            }
                        }
                        FrameUtils.flushFrame(appender.getBuffer(), writer);
                        accumulator.clear();
                    }

                } catch (Exception e) {
                    writer.fail();
                    System.err.println("failed to merge frame " + e.getMessage());
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
                    writer.close();
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
