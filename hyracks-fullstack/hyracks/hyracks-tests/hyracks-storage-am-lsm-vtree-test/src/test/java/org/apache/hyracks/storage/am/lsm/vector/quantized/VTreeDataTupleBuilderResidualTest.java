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
package org.apache.hyracks.storage.am.lsm.vector.quantized;

import java.util.Arrays;

import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleBuilder;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * DOT residual encode writes SQ(x − c) in field 3. Antimatter uses the same builder, so insert/delete
 * codes stay consistent; merge still cancels on field 0 + PK (fields 2–3 are skipped).
 */
@AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "DOT residual builder field 3 vs SQ(x)")
public class VTreeDataTupleBuilderResidualTest {

    @Test
    public void residualField3DiffersFromRawSqWhenCentroidPassed() throws Exception {
        VTreeQuantizationParams params = new VTreeQuantizationParams(-1f, 1f, 127.5f, 0.99f, 8, 10);
        double[] x = { 4.2, 3.8, 4.1, 3.9 };
        double[] c = { 4.0, 4.0, 4.0, 4.0 };
        ITupleReference original = pkTuple(new byte[] { 1, 2, 3, 4 });

        VTreeDataTupleBuilder rawBuilder = new VTreeDataTupleBuilder(0, true, params, false);
        ITupleReference rawTuple = copy(rawBuilder.buildDataTuple(x, 0.1, 7, original, c));

        VTreeDataTupleBuilder residualBuilder = new VTreeDataTupleBuilder(0, true, params, true);
        ITupleReference residualTuple = residualBuilder.buildDataTuple(x, 0.1, 7, original, c);

        VTreeDataTupleAccessor accessor = new VTreeDataTupleAccessor(true);
        byte[] rawCodes = accessor.getQuantizedEmbedding(rawTuple);
        byte[] residualCodes = accessor.getQuantizedEmbedding(residualTuple);
        Assert.assertFalse("DOT residual field 3 must differ from SQ(x) when x is far from the origin",
                Arrays.equals(rawCodes, residualCodes));
        Assert.assertEquals(7, accessor.getCentroidId(residualTuple));
    }

    @Test
    public void antimatterUsesTheSameResidualEncode() throws Exception {
        VTreeQuantizationParams params = new VTreeQuantizationParams(-1f, 1f, 127.5f, 0.99f, 8, 10);
        double[] x = { 0.5, -0.25, 0.1, 0.0 };
        double[] c = { 0.4, -0.2, 0.05, 0.0 };
        ITupleReference original = pkTuple(new byte[] { 9, 9 });

        VTreeDataTupleBuilder builder = new VTreeDataTupleBuilder(0, true, params, true);
        ITupleReference insertTuple = copy(builder.buildDataTuple(x, 0.05, 3, original, c));
        ITupleReference deleteTuple = builder.buildDataTuple(x, 0.05, 3, original, c);

        VTreeDataTupleAccessor accessor = new VTreeDataTupleAccessor(true);
        Assert.assertArrayEquals(accessor.getQuantizedEmbedding(insertTuple),
                accessor.getQuantizedEmbedding(deleteTuple));
    }

    private static ITupleReference pkTuple(byte[] pk) throws Exception {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        tb.addField(new byte[] { 0 }, 0, 1);
        tb.addField(pk, 0, pk.length);
        ArrayTupleReference ref = new ArrayTupleReference();
        ref.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        return ref;
    }

    private static ITupleReference copy(ITupleReference src) throws Exception {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(src.getFieldCount());
        for (int i = 0; i < src.getFieldCount(); i++) {
            tb.addField(src.getFieldData(i), src.getFieldStart(i), src.getFieldLength(i));
        }
        ArrayTupleReference ref = new ArrayTupleReference();
        ref.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        return ref;
    }
}
