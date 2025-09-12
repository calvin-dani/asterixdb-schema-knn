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
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public final class MergeCentroidsOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final RecordDescriptor centroidDesc;
    private IScalarEvaluatorFactory args;
    private ListAccessor listAccessorConstant;
    private List<float[]> accumulator;

    public MergeCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            RecordDescriptor centroidDesc, IScalarEvaluatorFactory args) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.centroidDesc = centroidDesc;
        this.args = args;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            int i = 4;
            IScalarEvaluator eval;
            VoidPointable inputVal = new VoidPointable();
            FrameTupleAppender appender;

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
                listAccessorConstant = new ListAccessor();
                accumulator = new ArrayList<>();
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // merge

                try {
                    System.err.println("Merging frame ");
                    List<float[]> floats = deserializeFloatList(buffer.array());
                    //                    System.err.println("to merge centroids size " + floats.size());
                    accumulator.addAll(floats);
                    //                    System.err.println("merged centroids size " + accumulator.size());
                    byte[] serialized = serializeFloatList(accumulator);
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
                    tupleBuilder.addField(serialized, 0, serialized.length);
                    FrameUtils.appendToWriter(writer, appender, tupleBuilder.getFieldEndOffsets(),
                            tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
//                    // TODO CALVIN DANI: check the API and correct design?
                    appender.write(writer, true);
                    System.err.println("Merged frame ");

                } catch (Exception e) {
                    System.err.println("failed to merge frame " + e.getMessage());
                }

            }

            public static byte[] serializeFloatList(List<float[]> floatList) throws IOException {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                StringBuilder sb = new StringBuilder();
                sb.append("merged serializing centroids ");
                dos.writeInt(floatList.size());
                sb.append("numArrays ");
                sb.append(floatList.size());
                sb.append(" | ");
                for (float[] arr : floatList) {
                    sb.append(Arrays.toString(arr));
                    dos.writeInt(arr.length);
                    for (float f : arr) {
                        dos.writeFloat(f);
                    }
                }
                System.err.println(sb.toString());
                dos.flush();
                return baos.toByteArray();
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
                try {
                    //                    System.err.println("sending merged centroids " + accumulator.toString());
                    byte[] serialized = serializeFloatList(accumulator);
                    ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
                    tupleBuilder.addField(serialized, 0, serialized.length);

                    // 3. Write tuple to frame
                    FrameUtils.appendToWriter(writer, appender, tupleBuilder.getFieldEndOffsets(),
                            tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
                    appender.write(writer, true);
                    System.err.println("sent merged centroids " + accumulator.size());

                } catch (Exception e) {
                } finally {
                    writer.close();
                }
            }
        };
    }
}
