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

 import org.apache.hyracks.api.context.IHyracksTaskContext;
 import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
 import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
 import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
 import org.apache.hyracks.api.exceptions.HyracksDataException;
 import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
 import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
 import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

 public final class MergeCentroidsOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

     private static final long serialVersionUID = 1L;

     public MergeCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc) {
         super(spec, 1, 1);
         outRecDescs[0] = rDesc;
     }

     @Override
     public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                                                    IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

         return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

             int i = 4;

             @Override
             public void open() throws HyracksDataException {
                 writer.open();
             }

             @Override
             public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                 // merge

                 // broadcast when received from all partitions
                 System.err.println(i + " partitions remaining");
                 i--;
                 if (i <= 0) {
                     writer.nextFrame(buffer);
                     i = 4;
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
         };
     }
 }