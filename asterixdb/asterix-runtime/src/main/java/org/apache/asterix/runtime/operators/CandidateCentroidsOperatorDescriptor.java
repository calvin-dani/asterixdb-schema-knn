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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

public final class CandidateCentroidsOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final UUID sampleUUID;
    private final UUID centroidsUUID;
    private final UUID permitUUID;
    private FrameTupleAccessor fta;
    private FrameTupleReference tuple;
    public CandidateCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc,
                                                UUID sampleUUID, UUID centroidsUUID, UUID permitUUID) {
        super(spec, 1, 1);
        outRecDescs[0] = rDesc;
        this.sampleUUID = sampleUUID;
        this.centroidsUUID = centroidsUUID;
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

                @Override
                public void open() throws HyracksDataException {
//                    fta = new FrameTupleAccessor(outRecDescs[0]);
//                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    // save centroid
                    System.err.println("HERE Store Centroid Activty");
                }

                @Override
                public void close() throws HyracksDataException {
                    CentroidsState state = new CentroidsState(ctx.getJobletContext().getJobId(), centroidsUUID);
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
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
                    // find candidates
//                    IScalarEvaluator eval = new ColumnAccessEvalFactory(1).createScalarEvaluator((IEvaluatorContext) ctx);
                fta = new FrameTupleAccessor(outRecDescs[0]);
                    FrameTupleAppender partialAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                    tuple = new FrameTupleReference();
                    Semaphore proceed = new Semaphore(1);
                    ctx.setStateObject(new IterationPermitState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(permitUUID, partition), proceed));
                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader in = sampleState.creatReader();
                    VSizeFrame vSizeFrame = new VSizeFrame(ctx);
                    PartitionedUUID centroidsKey = new PartitionedUUID(centroidsUUID, partition);
                    in.open();
                    try {
                        writer.open();
//                        for (int i = 0; i < 5; i++  ) {
                            proceed.acquire();
                            // pick up the new merged centroids
                            CentroidsState currentCentroids = (CentroidsState) ctx.getStateObject(centroidsKey);
                            vSizeFrame.reset();
//                            writer.nextFrame(vSizeFrame.getBuffer());
//                        in.nextFrame(vSizeFrame);
                                                        while (in.nextFrame(vSizeFrame)) {
                                                            fta.reset(vSizeFrame.getBuffer());
                                                            for (int tupleIndex = 0; tupleIndex < fta.getTupleCount(); tupleIndex++) {
                                                                tuple.reset(fta, tupleIndex);
                                                                tuple.getFieldData(0);
//                                                                // get array from tuple
//                                                                for (double[] center : currentCentroids) {
//                                                                double d = distanceFunction.distance(array form data, center);
//                                                                if (d < costs[i]) costs[i] = d;
//                                                                }
//                                                                double sumCosts = Arrays.stream(costs).sum();
//                                                                List<double[]> chosen = new ArrayList<>();
//                                                                for (int j = 0; j < n; j++) {
//                                                                    double prob = 2.0 * k * costs[j] / sumCosts;
//                                                                    if (rand.nextDouble() < prob) {
//                                                                        chosen.add(data.get(j));
//                                                                    }
//                                                                }
//                                                                newCenters = chosen;
//                                                                centers.addAll(chosen);
                                                            }

//                                                            vSizeFrame.getBuffer();
//                                                            writer.nextFrame(vSizeFrame.getBuffer());
//                                                            break;
                                                        }
//                        in.close();
                        FrameUtils.flushFrame(vSizeFrame.getBuffer(), writer);
//                        vSizeFrame.reset();


                            in.seek(0);
//                        }
                    } catch (Throwable e) {
                        writer.fail();
                        throw new RuntimeException(e);
                    } finally {

//                        in.close();
                        writer.close();
                    }
                }



            };
        }
    }
}