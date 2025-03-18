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

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.asterix.app.function.FunctionReader;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.SchemaStringBuilderVisitor;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SchemaReader extends FunctionReader {
    private static final Logger LOGGER = LogManager.getLogger();
    private static int instanceCount = 0;
    private final FlushColumnMetadata rowMetaData;
    private final CharArrayRecord record;
    private boolean hasNext = true;
    String jsonString = "";
    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream dis = new DataInputStream(bbis);

    SchemaReader(FlushColumnMetadata rowMetaData) throws HyracksDataException, JsonProcessingException {
        if (rowMetaData == null) {
            throw new HyracksDataException("Row metadata is null");
        }
        this.rowMetaData = rowMetaData;
        record = new CharArrayRecord();
        instanceCount++;
        //        LOGGER.log(Level.INFO, "INIT SCHEMA READER  Last partition and rowmetadata: {} {}", lastPartition,
        //                this.rowMetaData.getRoot().getChildren().toString());

        SchemaStringBuilderVisitor schemaBuilderJson =
                new SchemaStringBuilderVisitor(rowMetaData.getFieldNamesDictionary());
        LOGGER.log(Level.INFO, "IN INiT CREATING SCHEMA BULDER JSON: {}",
                this.rowMetaData.getRoot().getChildren().toString());

        //        SchemaJSONBuilderVisitor schemaBuilder = new SchemaJSONBuilderVisitor(rowMetaData.getFieldNamesDictionary());
        String recordSchema = LogRedactionUtil.userData(schemaBuilderJson.build(rowMetaData.getRoot()));
        LOGGER.log(Level.INFO, "IN INiT  CREATING SCHEMA JSON: {}",
                this.rowMetaData.getRoot().getChildren().toString());

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(recordSchema);
        this.jsonString = objectMapper.writeValueAsString(jsonNode);

        LOGGER.log(Level.INFO, "IN INiT  SCHEMA: {}", jsonString);

    }

    @Override
    public boolean hasNext() throws IOException {
        return hasNext;
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        //TODO CALVIN DANI: RETURN NULL skip reading if only count equals last;

        record.reset();
        //        LOGGER.log(Level.INFO, "STARTING RECORD READER: {}", this.rowMetaData.getRoot().getChildren().toString());
        //
        //        SchemaStringBuilderVisitor schemaBuilderJson =
        //                new SchemaStringBuilderVisitor(rowMetaData.getFieldNamesDictionary());
        //        LOGGER.log(Level.INFO, "CREATING SCHEMA BULDER JSON: {}", this.rowMetaData.getRoot().getChildren().toString());
        //
        //        //        SchemaJSONBuilderVisitor schemaBuilder = new SchemaJSONBuilderVisitor(rowMetaData.getFieldNamesDictionary());
        //        String recordSchema = LogRedactionUtil.userData(schemaBuilderJson.build(rowMetaData.getRoot()));
        //        LOGGER.log(Level.INFO, "CREATING SCHEMA JSON: {}", this.rowMetaData.getRoot().getChildren().toString());
        //
        //        ObjectMapper objectMapper = new ObjectMapper();
        //        JsonNode jsonNode = objectMapper.readTree(recordSchema);
        //        String jsonString = objectMapper.writeValueAsString(jsonNode);
        //        LOGGER.log(Level.INFO, "JACKSON MAGIC: {}", this.rowMetaData.getRoot().getChildren().toString());

        hasNext = false;
        String result = "{\"Schema\":" + this.jsonString + "}";
        //        LOGGER.log(Level.INFO, "MAKING RECORD: {}", this.rowMetaData.getRoot().getChildren().toString());

        record.append(result.toCharArray());
        record.endRecord();
        //        hasNext = false;
        //        LOGGER.log(Level.INFO, "FIN: {}", this.rowMetaData.getRoot().getChildren().toString());

        return record;
    }

    public static int getInstanceCount() {
        return instanceCount;
    }

}
