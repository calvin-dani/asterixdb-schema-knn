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

import java.util.UUID;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;

/**
 * Utility class for writing individual scalar values (integers, floats, doubles)
 * from embeddings to a run file. Similar to how MaterializerTaskState stores
 * tuples, but writes individual scalar values as separate entries.
 */
public class ScalarValueRunFileWriter {
    private MaterializerTaskState materializedScalars;
    private FrameTupleAppender appender;
    private VSizeFrame frame;
    private UUID scalarValuesUUID;
    private IHyracksTaskContext ctx;

    /**
     * Initialize the run file writer for scalar values
     * 
     * @param ctx The Hyracks task context
     * @param partition The partition ID
     * @param scalarValuesUUID Unique UUID for this scalar values run file
     * @throws HyracksDataException if initialization fails
     */
    public void initialize(IHyracksTaskContext ctx, int partition, UUID scalarValuesUUID) throws HyracksDataException {
        this.ctx = ctx;
        this.scalarValuesUUID = scalarValuesUUID;

        // Create MaterializerTaskState for storing scalar values
        materializedScalars = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                new PartitionedUUID(scalarValuesUUID, partition));
        materializedScalars.open(ctx);

        // Create frame and appender
        frame = new VSizeFrame(ctx);
        appender = new FrameTupleAppender(frame);
    }

    /**
     * Write a single integer value to the run file
     * 
     * @param value The integer value to write
     * @throws HyracksDataException if writing fails
     */
    public void writeInteger(int value) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
        tupleBuilder.reset();

        // Add integer field
        tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, value);

        // Append to frame (handles overflow automatically)
        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize())) {
            // Frame is full, flush to run file
            materializedScalars.appendFrame(appender.getBuffer());
            appender.reset(new VSizeFrame(ctx), true);
            appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
        }
    }

    /**
     * Write a single double value to the run file
     * 
     * @param value The double value to write
     * @throws HyracksDataException if writing fails
     */
    public void writeDouble(double value) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
        tupleBuilder.reset();

        // Add double field
        tupleBuilder.addField(DoubleSerializerDeserializer.INSTANCE, value);

        // Append to frame
        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize())) {
            // Frame is full, flush to run file
            materializedScalars.appendFrame(appender.getBuffer());
            appender.reset(new VSizeFrame(ctx), true);
            appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
        }
    }

    /**
     * Write a single float value to the run file
     * 
     * @param value The float value to write
     * @throws HyracksDataException if writing fails
     */
    public void writeFloat(float value) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
        tupleBuilder.reset();

        // Add float field
        tupleBuilder.addField(FloatSerializerDeserializer.INSTANCE, value);

        // Append to frame
        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                tupleBuilder.getSize())) {
            // Frame is full, flush to run file
            materializedScalars.appendFrame(appender.getBuffer());
            appender.reset(new VSizeFrame(ctx), true);
            appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
        }
    }

    /**
     * Write all values from a double array (embedding) as individual entries
     * 
     * @param embedding The double array to write
     * @throws HyracksDataException if writing fails
     */
    public void writeEmbeddingAsScalars(double[] embedding) throws HyracksDataException {
        if (embedding == null) {
            return;
        }
        for (double value : embedding) {
            writeDouble(value);
        }
    }

    /**
     * Write all values from a float array (embedding) as individual entries
     * 
     * @param embedding The float array to write
     * @throws HyracksDataException if writing fails
     */
    public void writeEmbeddingAsScalars(float[] embedding) throws HyracksDataException {
        if (embedding == null) {
            return;
        }
        for (float value : embedding) {
            writeFloat(value);
        }
    }

    /**
     * Close and save the run file state for downstream operators
     * 
     * @param ctx The Hyracks task context
     * @throws HyracksDataException if closing fails
     */
    public void close(IHyracksTaskContext ctx) throws HyracksDataException {
        // Flush any remaining data in the frame
        if (appender.getTupleCount() > 0) {
            materializedScalars.appendFrame(appender.getBuffer());
        }

        // Close the materializer
        materializedScalars.close();

        // Save state for downstream operators
        ctx.setStateObject(materializedScalars);
    }
}
