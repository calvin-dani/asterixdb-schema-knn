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
 import java.util.UUID;

 import org.apache.hyracks.api.comm.VSizeFrame;
 import org.apache.hyracks.api.context.IHyracksTaskContext;
 import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
 import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
 import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
 import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
 import org.apache.hyracks.api.exceptions.HyracksDataException;
 import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
 import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
 import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
 import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
 import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
 import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
 import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
 import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

 public final class InitCentroidOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

     private static final long serialVersionUID = 1L;
     private FrameTupleAccessor fta;
     private final UUID sampleUUID;

     public InitCentroidOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, UUID sampleUUID) {
         super(spec, 1, 1);
         outRecDescs[0] = rDesc;
         this.sampleUUID = sampleUUID;
     }

     @Override
     public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                                                    IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

         return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

             private MaterializerTaskState materializedSample;
             private boolean first = true;

             @Override
             public void open() throws HyracksDataException {
                 materializedSample = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                         new PartitionedUUID(sampleUUID, partition));
                 fta = new FrameTupleAccessor(outRecDescs[0]);
                 materializedSample.open(ctx);
                 writer.open();
             }

             @Override
             public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                 // first centroid
                 fta.reset(buffer);
                 FrameTupleAppender partialAppender = new FrameTupleAppender(new VSizeFrame(ctx));

                 if (partition == 0 && first) {
                     // broadcast the first centroid to all partitions from partition 0

                     first = false;
                     FrameUtils.appendToWriter(writer, partialAppender, fta, 0);
                     // pick the first tuple
//                     writer.nextFrame(null);
                 }
                 materializedSample.appendFrame(buffer);
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
                 if (materializedSample != null) {
                     materializedSample.close();
                     ctx.setStateObject(materializedSample);
                 }
                 writer.close();
             }
         };
     }
 }