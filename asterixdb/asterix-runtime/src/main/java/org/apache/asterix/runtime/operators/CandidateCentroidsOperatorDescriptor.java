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

import java.io.ByteArrayOutputStream;
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
    private FrameTupleAccessor fta;
    private FrameTupleReference tuple;
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
                    fta.reset(buffer, "saving centroid from init centroid " + partition);
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
                    System.err.println("HERE Store Centroid Activty");
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
                    IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                    IPointable inputVal = new VoidPointable();
                    fta = new FrameTupleAccessor(outRecDescs[0]);
                    tuple = new FrameTupleReference();
                    Semaphore proceed = new Semaphore(1);
                    ctx.setStateObject(new IterationPermitState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(permitUUID, partition), proceed));
                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader in = sampleState.creatReader();
                    VSizeFrame vSizeFrame = new VSizeFrame(ctx);
                    PartitionedUUID centroidsKey = new PartitionedUUID(centroidsUUID, partition);
                    FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));

                    in.open();
                    try {
                        writer.open();
                        for (int step = 0; step < 5; step++) {
                            proceed.acquire();
                            in.seek(0);
                            System.err.println("Acqiored permit for step " + step + " partition " + partition);
                            CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsUUID);

                            // First pass: update preCosts
                            int totalTupleCount = 8; // You need to provide this
//                            float[] preCosts = new float[totalTupleCount];
//                            Arrays.fill(preCosts, Float.POSITIVE_INFINITY);

                            List<Float> preCosts = new ArrayList<>();

                            int globalTupleIndex = 0;
                            // TODO CALVINDANI: CHECK reader reset in original code
                            vSizeFrame.reset();
                            while (in.nextFrame(vSizeFrame)) {
                                fta.reset(vSizeFrame.getBuffer(),"cost step "+ step + " partition " + partition);
                                int tupleCount = fta.getTupleCount();
                                System.err.println("cost step " + step + " partition " + partition + " tuple count " + tupleCount);
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
                                        float cost = euclideanDistance(point, center);
                                        if (cost < minCost) minCost = cost;
                                    }
                                    preCosts.add(minCost); // Accumulate minCost for each tuple
                                }
                            }

                            System.err.println("Precosts size: " + preCosts.size());

                            // Compute sumCosts
                            float sumCosts = 0f;
                            for (float c : preCosts) sumCosts += c;

                            // Second pass: probabilistic selection
                            List<float[]> chosen = new ArrayList<>();
                            globalTupleIndex = 0;
                            in.seek(0); // or reopen reader
                            vSizeFrame.reset();
                            java.util.Random rand = new java.util.Random();

                            while (in.nextFrame(vSizeFrame)) {
                                fta.reset(vSizeFrame.getBuffer(),"choice selection step "+ step + " partition " + partition);
                                int tupleCount = fta.getTupleCount();
                                System.err.println("choice selection step " + step + " partition " + partition + " tuple count " + tupleCount);
                                for (int tupleIndex = 0; tupleIndex < tupleCount; tupleIndex++, globalTupleIndex++) {
                                    tuple.reset(fta, tupleIndex);
                                    eval.evaluate(tuple, inputVal);
                                    ListAccessor listAccessorConstant = new ListAccessor();
                                    System.err.println("tuple index "+ tupleIndex + "tuple count " + tupleCount + " global index " + globalTupleIndex);
                                    if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                        continue;
                                    }
                                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    float[] point = createPrimitveList(listAccessorConstant);
//                                    float prob = 2.0f * 2 * preCosts[globalTupleIndex] / sumCosts;
                                    System.err.println("precost size: " + preCosts.size() + " global index " + globalTupleIndex );
                                    float prob = 2.0f * currentCentroids.getCentroids().size() * preCosts.get(globalTupleIndex).floatValue() / sumCosts;
                                    if (rand.nextFloat() < prob) {
                                        chosen.add(point);
                                    }
                                }
                            }
                            in.seek(0);
                            System.err.println("iteration " + step + " partition " + partition + " precost centroids " + preCosts.size() + " chosen centroids: " + chosen.size());
                            // Add chosen to centroids or send to next step
                            for (float[] c : chosen) {
                                currentCentroids.addCentroid(c);
                            }

                            byte[] serialized = serializeFloatList(chosen);

// 2. Build tuple with one field
                            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
                            tupleBuilder.addField(serialized, 0, serialized.length);

// 3. Write tuple to frame
                            FrameUtils.appendToWriter(writer, appender, tupleBuilder.getFieldEndOffsets(),
                                    tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                            // TODO CALVIN DANI: check the API and correct design?
                            appender.write(writer, true);
                            System.err.println("iteration " + step + " partition " + partition + " centroids: " + currentCentroids.getCentroids().size());

                        }

                    } catch (Throwable e) {
                        writer.fail();
                        throw new RuntimeException(e);
                    } finally {
                        in.close();
                        writer.close();
                    }
                }


                public static byte[] serializeFloatList(List<float[]> floatList) throws IOException {
                    // TODO CALVIN DANI: to chnage
                    // 1 FRAME PER LOOP or 1 TUPLE PER LOOP?
                    // KEEP TRACK OF ALL PARTITON END SIGNAL
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    dos.writeInt(floatList.size());
                    for (float[] arr : floatList) {
                        dos.writeInt(arr.length);
                        for (float f : arr) {
                            dos.writeFloat(f);
                        }
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
