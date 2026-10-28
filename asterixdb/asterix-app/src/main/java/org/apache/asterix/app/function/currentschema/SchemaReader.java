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

package org.apache.asterix.app.function.currentschema;

import java.io.IOException;

import org.apache.asterix.app.function.FunctionReader;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.JsonSchemaBuilderVisitor;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SchemaReader extends FunctionReader {
    private final FlushColumnMetadata colMetaData;
    private final CharArrayRecord record;
    private boolean hasNext = true;
    private final String jsonString;

    SchemaReader(FlushColumnMetadata colMetaData, ARecordType itemType) throws HyracksDataException {
        if (colMetaData == null) {
            throw new HyracksDataException("Column metadata is null");
        }
        this.colMetaData = colMetaData;
        record = new CharArrayRecord();
        jsonString = new JsonSchemaBuilderVisitor(colMetaData.getFieldNamesDictionary()).build(colMetaData.getRoot(),
                itemType);
    }

    @Override
    public boolean hasNext() throws IOException {
        return hasNext;
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        record.reset();

        hasNext = false;
        String result = "{\"Schema\":" + this.jsonString + "}";

        record.append(result.toCharArray());
        record.endRecord();

        return record;
    }

}
