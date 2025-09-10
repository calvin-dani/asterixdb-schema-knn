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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public final class MergeCentroids2OperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final UUID centroidsUUID;
    private final RecordDescriptor centroidDesc;
    private FrameTupleAccessor fta;
    private FrameTupleReference ftr;
    private IScalarEvaluatorFactory args;
    private ListAccessor listAccessorConstant;
//    private List<float[]> accumulator;
    private ArrayTupleBuilder tupleBuilder;
    private FrameTupleAppender appender;

    public MergeCentroids2OperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            RecordDescriptor centroidDesc, IScalarEvaluatorFactory args, UUID centroidsUUID) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.centroidDesc = centroidDesc;
        this.args = args;
        this.centroidsUUID = centroidsUUID;
//        accumulator = new ArrayList<>();
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        MergeCentroidsActivity sa = new MergeCentroidsActivity(new ActivityId(odId, 0));
        SendCandidatesCentroidsActivity ca = new SendCandidatesCentroidsActivity(new ActivityId(odId, 1));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ca);
        builder.addTargetEdge(0, ca, 0);

        builder.addBlockingEdge(sa, ca);
        builder.addTargetEdge(0, ca, 0);
    }

    protected class MergeCentroidsActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected MergeCentroidsActivity(ActivityId id) {
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
                                        state = (CentroidsState) ctx.getStateObject(centroidsUUID);
                    //                    eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                    //                    inputVal = new VoidPointable();
                    //                    fta = new FrameTupleAccessor(outRecDescs[0]);
                    //                    tuple = new FrameTupleReference();
                    //                    writer.open();

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    try {
                        System.err.println("Merging 2 frame ");

                        List<float[]> floats = deserializeFloatList(buffer.array());
                        System.err.println("to merge centroids size " + floats.size());
                        state.getCentroids().addAll(floats);
//                        accumulator.addAll(floats);
                        System.err.println("merged centroids size " + state.getCentroids().size());
//                                            writer.nextFrame(buffer);
                        System.err.println("Merged 2 frame ");

                    } catch (Exception e) {
                        System.err.println("failed to merge frame " + e.getMessage());
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    if (state != null) {
                        ctx.setStateObject(state);
                    }
                    System.err.println("CLOSED frame ");
                }

                @Override
                public void fail() throws HyracksDataException {
                    System.err.println("FAILED frame ");
                }


                // Deserialization
                public static List<float[]> deserializeFloatList(byte[] bytes) throws IOException {
                    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes, 9, bytes.length - 9));
                    int numArrays = dis.readInt();
                    List<float[]> result = new ArrayList<>(numArrays);
                    for (int i = 0; i < numArrays; i++) {
                        int arrLen = dis.readInt();
                        float[] arr = new float[arrLen];
                        for (int j = 0; j < arrLen; j++) {
                            arr[j] = dis.readFloat();
                        }
                        result.add(arr);
                    }
                    return result;
                }
            };
        }
    }

    protected class SendCandidatesCentroidsActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        protected SendCandidatesCentroidsActivity(ActivityId id) {
            super(id);
        }

        @Override
        @SuppressWarnings("squid:S1188")
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    CentroidsState state = (CentroidsState) ctx.getStateObject(centroidsUUID);
                    try {
                        writer.open();
                        System.err.println("sending merged centroids " + state.getCentroids().size());
                        byte[] serialized = serializeFloatList(state.getCentroids());
                        tupleBuilder.addField(serialized, 0, serialized.length);

                        // 3. Write tuple to frame
                        FrameUtils.appendToWriter(writer, appender, tupleBuilder.getFieldEndOffsets(),
                                tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                        appender.write(writer, true);
                        System.err.println("sent merged centroids " + state.getCentroids().size());

                    } catch (Throwable e) {
                        writer.fail();
                        throw new RuntimeException(e);
                    } finally {
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

            };
        }
    }

}
