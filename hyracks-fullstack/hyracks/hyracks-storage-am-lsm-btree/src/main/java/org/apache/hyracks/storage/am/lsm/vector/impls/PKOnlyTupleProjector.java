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
package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.projection.ITupleProjector;

/**
 * Tuple projector that extracts only primary key fields from vector index search results.
 *
 * Vector index tuples contain: <distance, cosine, embedding, pk1, pk2, ...>
 * This projector skips the secondary key fields (distance, cosine, embedding) and
 * writes only the primary key fields to the output.
 *
 * This optimization avoids transferring large embedding vectors (4KB-16KB) when only
 * primary keys are needed for subsequent primary index lookup.
 */
public class PKOnlyTupleProjector implements ITupleProjector {

    private final int numSecondaryKeys;  // Number of fields to skip
    private final int numPrimaryKeys;    // Number of PK fields to write

    public PKOnlyTupleProjector(int numSecondaryKeys, int numPrimaryKeys) {
        this.numSecondaryKeys = numSecondaryKeys;
        this.numPrimaryKeys = numPrimaryKeys;
    }

    @Override
    public ITupleReference project(ITupleReference tuple, DataOutput dos, ArrayTupleBuilder tb) throws IOException {
        // Skip secondary key fields, write only primary key fields
        // Example: tuple = <distance, cosine, embedding[1024], pk0>
        //          numSecondaryKeys = 3 (distance, cosine, embedding)
        //          numPrimaryKeys = 1 (pk0)
        //          Result: write only pk0 to output

        int totalFields = tuple.getFieldCount();
        int startField = numSecondaryKeys;
        int endField = numSecondaryKeys + numPrimaryKeys;

        // Validate field bounds
        if (endField > totalFields) {
            throw new IOException("Invalid field range: trying to extract fields [" + startField + ", " + endField
                    + ") from tuple with " + totalFields + " fields");
        }

        // Write only PK fields
        for (int i = startField; i < endField; i++) {
            dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
            tb.addFieldEndOffset();
        }

        return tuple;
    }
}
