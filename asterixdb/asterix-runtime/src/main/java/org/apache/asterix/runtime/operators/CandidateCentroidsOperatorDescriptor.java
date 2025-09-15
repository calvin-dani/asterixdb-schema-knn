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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
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

    public CandidateCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            UUID sampleUUID, UUID centroidsUUID, UUID permitUUID, IScalarEvaluatorFactory args,
            RecordDescriptor embeddingDesc) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.embeddingDesc = embeddingDesc;
        this.sampleUUID = sampleUUID;
        this.centroidsUUID = centroidsUUID;
        this.args = args;
        this.permitUUID = permitUUID;
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

                @Override
                public void open() throws HyracksDataException {

                    System.err.println(" Cand Centroids Activity Opened partition " + partition + " nPartitions " + nPartitions);

                    state = new CentroidsState(ctx.getJobletContext().getJobId(), centroidsUUID);
                    eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                    inputVal = new VoidPointable();
                    fta = new FrameTupleAccessor(outRecDescs[0]);
                    tuple = new FrameTupleReference();
//                    writer.open();

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    // save centroid
//                    fta.reset(buffer, "saving centroid from init centroid " + partition);
                    fta.reset(buffer);
                    tuple.reset(fta, 0);
                    eval.evaluate(tuple, inputVal);
                    ListAccessor listAccessorConstant = new ListAccessor();
                    if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
//                        continue;
                        //TODO: handle error
                    }
                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                    try {
                        float[] point = createPrimitveList(listAccessorConstant);
                        state.addCentroid(point);

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
//                    ctx.setStateObject(state);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (state != null) {
                        ctx.setStateObject(state);
                    }
//                    writer.close();
                }

                @Override
                public void fail() throws HyracksDataException {
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

                protected float extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws HyracksDataException {
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
// FrameDebugUtils.prettyPrint(fta,new RecordDescriptor( new ISerializerDeserializer[] {     new AOrderedListSerializerDeserializer() AInt64SerializerDeserializer.INSTANCE     }))
                    ARecordType recType = new ARecordType(null, new String[]{"embedding", "count"},
                            new IAType[]{new AOrderedListType(AFLOAT, "embedding"), BuiltinType.AINT64}, false);
                    ByteArrayAccessibleOutputStream embBytes = new ByteArrayAccessibleOutputStream();
                    DataOutput embBytesOutput = new DataOutputStream(embBytes);
                    ByteArrayAccessibleOutputStream partitionBytes = new ByteArrayAccessibleOutputStream();
                    DataOutput partitionBytesOutput = new DataOutputStream(partitionBytes);
                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader in = sampleState.creatReader();

                    try {

                        AInt32 aInt32 = new AInt32(partition);
                        partitionBytesOutput.writeByte(ATypeTag.INTEGER.serialize());
                        AInt32SerializerDeserializer.INSTANCE.serialize(aInt32, partitionBytesOutput);

                        ByteArrayAccessibleOutputStream booleanBytes = new ByteArrayAccessibleOutputStream();
                        DataOutput booleanBytesOutput = new DataOutputStream(booleanBytes);
                        booleanBytesOutput.writeByte(ATypeTag.BOOLEAN.serialize());
                        ABooleanSerializerDeserializer.INSTANCE.serialize(ABoolean.FALSE, booleanBytesOutput);

                        AMutableFloat aFloat = new AMutableFloat(0);


                        OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
                        ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                        orderedListBuilder.reset(new AOrderedListType(AFLOAT, "embedding"));
//                    orderedListBuilder.reset(null);

                        UnorderedListBuilder unorderedListBuilder = new UnorderedListBuilder();
                        ArrayBackedValueStorage unorderedListStorage = new ArrayBackedValueStorage();
                        unorderedListBuilder.reset(null);


                        FrameTupleAccessor fta;
                        FrameTupleReference tuple;
                        IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                        IPointable inputVal = new VoidPointable();
                        fta = new FrameTupleAccessor(outRecDescs[0]);
                        tuple = new FrameTupleReference();
                        Semaphore proceed = new Semaphore(1);
                        ctx.setStateObject(new IterationPermitState(ctx.getJobletContext().getJobId(),
                                new PartitionedUUID(permitUUID, partition), proceed));




                        VSizeFrame vSizeFrame = new VSizeFrame(ctx);

                        PartitionedUUID centroidsKey = new PartitionedUUID(centroidsUUID, partition);
                        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));


                        writer.open();
                        for (int step = 0; step < 5; step++) {
                            proceed.acquire();
                            CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsUUID);

                            // First pass: update preCosts
                            int totalTupleCount = 8; // You need to provide this

                            List<Float> preCosts = new ArrayList<>();

                            int globalTupleIndex = 0;
                            // TODO CALVINDANI: CHECK reader reset in original code
                            vSizeFrame.reset();
                            in.open();
                            while (in.nextFrame(vSizeFrame)) {
//                                fta.reset(vSizeFrame.getBuffer(), "cost step " + step + " partition " + partition);
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
                                    float[] point = createPrimitveList(listAccessorConstant);

                                    float minCost = Float.POSITIVE_INFINITY;
                                    for (float[] center : currentCentroids.getCentroids()) {
//                                        float cost =  VectorDistanceArrCalculation.euclidean_squared(point, center);
                                        float cost = euclideanDistance(point, center);
                                        if (cost < minCost) minCost = cost;
                                    }
                                    preCosts.add(minCost);
                                    // Accumulate minCost for each tuple
                                }
                            }
                            in.close();
                            int k = 2; // number of clusters
                            double l = k * 2; // expected number of candidates per round

//                            System.err.println("Precosts size: " + preCosts.size());
                            // Compute sumCosts
                            float sumCosts = 0f;
                            for (float c : preCosts) sumCosts += c;


                            // Second pass: probabilistic selection
                            List<float[]> chosen = new ArrayList<>();
                            globalTupleIndex = 0;
                            in.seek(0); // or reopen reader
                            vSizeFrame.reset();
                            Random rand = new Random();
                            in.open();
                            while (in.nextFrame(vSizeFrame)) {
//                                fta.reset(vSizeFrame.getBuffer(), "choice selection step " + step + " partition " + partition);
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
                                    float[] point = createPrimitveList(listAccessorConstant);
//                                    float prob = 2.0f * 2 * preCosts[globalTupleIndex] / sumCosts;
                                    double prob = preCosts.get(globalTupleIndex) / sumCosts;
                                    if (rand.nextDouble() < prob) {
                                        chosen.add(point);
                                    }
                                }
                            }
                            in.close();

                            for (float[] c : chosen) {
                                currentCentroids.addCentroid(c);
                            }


                            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3); // 1 field: the record
                            for (int i = 0; i < currentCentroids.getCentroids().size(); i++) {
                                float[] arr = currentCentroids.getCentroids().get(i);
                                boolean isLast = (i == currentCentroids.getCentroids().size() - 1);
                                if (isLast) {
                                    booleanBytes.reset();
                                    booleanBytesOutput.writeByte(ATypeTag.BOOLEAN.serialize());
                                    ABooleanSerializerDeserializer.INSTANCE.serialize(ABoolean.TRUE, booleanBytesOutput);
                                }
                                else{
                                    booleanBytes.reset();
                                    booleanBytesOutput.writeByte(ATypeTag.BOOLEAN.serialize());
                                    ABooleanSerializerDeserializer.INSTANCE.serialize(ABoolean.FALSE, booleanBytesOutput);
                                }
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
                                tupleBuilder.addField(partitionBytes.getByteArray(), 0, partitionBytes.getLength());
                                tupleBuilder.addField(booleanBytes.getByteArray(), 0, booleanBytes.getLength());
//                                FrameUtils.appendToWriter(writer,appender, tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                                if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                                    // Frame is full, flush and reset
                                    FrameUtils.flushFrame(appender.getBuffer(), writer);
                                    appender.reset(new VSizeFrame(ctx), true);
                                    appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                                }
                            }
                            FrameUtils.flushFrame(appender.getBuffer(), writer);
                        }

                    } catch (Throwable e) {
                        writer.fail();
                        throw new RuntimeException(e);
                    }finally {
                        in.close();
                        writer.close();
                    }
                }


                private float euclideanDistance(float[] point, float[] center) {
                    float sum = 0;
                    for (int i = 0; i < point.length; i++) {
                        float diff = point[i] - center[i];
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
