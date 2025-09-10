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
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

public final class StoreMergedCentroidsOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final UUID centroidsUUID;
    private final UUID permitUUID;

    public StoreMergedCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
            UUID centroidsUUID, UUID permitUUID) {
        super(spec, 1, 1);
        this.centroidsUUID = centroidsUUID;
        this.permitUUID = permitUUID;
        outRecDescs[0] = rDesc;

    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            @Override
            public void open() throws HyracksDataException {
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                try {
                    System.err.println("Storing merged centroids");
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

                IterationPermitState permitState =
                        (IterationPermitState) ctx.getStateObject(new PartitionedUUID(permitUUID, partition));

                // store the merged centroids
                ctx.setStateObject(new CentroidsState(ctx.getJobletContext().getJobId(), centroidsUUID));

                permitState.getPermit().release(1);
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
                // compute weights and send to next writer
                writer.close();
            }

            // Deserialization
            public static List<float[]> deserializeFloatList(byte[] bytes) throws IOException {
                // TODO CALVIN DANI: can pass iteration data in buffer , enforcedframewriter
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