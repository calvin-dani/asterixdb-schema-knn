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

package org.apache.asterix.app.function.collectionschema;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.asterix.app.function.FunctionReader;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.runtime.schemainferrence.ObjectRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.RowMetadata;
import org.apache.asterix.runtime.schemainferrence.RowSchemaTransformer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CollectionSchemaReader extends FunctionReader {
    private static int instanceCount = 0;
    private final RowMetadata rowMetaData;
    private final CharArrayRecord record;
    private boolean hasNext = true;

    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream dis = new DataInputStream(bbis);

    CollectionSchemaReader(RowMetadata rowMetaData) throws HyracksDataException {

        this.rowMetaData = rowMetaData;
        record = new CharArrayRecord();
        instanceCount++;
        //        if (lastPartition) {
        //            hasNext = true;
        //        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return hasNext;
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        //TODO CALVIN DANI: RETURN NULL skip reading if only count equals last;

        record.reset();
        RowSchemaTransformer schemaTransformer = new RowSchemaTransformer(rowMetaData, rowMetaData.getRoot());
        ObjectRowSchemaNode root = schemaTransformer.getRoot();
        String res = rowMetaData.printRootSchema(root, rowMetaData.getFieldNamesDictionary());

        //        SchemaStringBuilderVisitor schemaBuilderJson = new SchemaStringBuilderVisitor(rowMetaData.getFieldNamesDictionary());
        ////        SchemaJSONBuilderVisitor schemaBuilder = new SchemaJSONBuilderVisitor(rowMetaData.getFieldNamesDictionary());
        //        String recordSchema = LogRedactionUtil.userData(schemaBuilderJson.build(rowMetaData.getRoot()));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(res);
        String jsonString = objectMapper.writeValueAsString(jsonNode);
        hasNext = false;
        String result = "{\"Schema\":" + jsonString + "}";
        record.append(result.toCharArray());
        record.endRecord();
        hasNext = false;
        return record;
    }

    public static int getInstanceCount() {
        return instanceCount;
    }

}
