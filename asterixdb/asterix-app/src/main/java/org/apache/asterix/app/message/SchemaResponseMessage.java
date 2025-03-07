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
package org.apache.asterix.app.message;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.ColumnSchemaTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.util.SchemaStringBuilderVisitor;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.ICCMessageBroker.ResponseState;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INcResponse;
import org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.Level;

public class SchemaResponseMessage implements ICcAddressedMessage, INcResponse {

    private static final long serialVersionUID = 1L;
    private final long reqId;
    SerializableArrayBackedValueStorage serializedColumnMetaData;
    private final Throwable failure;
//    protected ColumnSchemaTransformer schemaTransformer;
//    FlushColumnMetadata columnMetadata;
    private static AtomicInteger count = new AtomicInteger(0);

    protected static final int WRITERS_POINTER = 0;
    protected static final int FIELD_NAMES_POINTER = WRITERS_POINTER + Integer.BYTES;
    protected static final int SCHEMA_POINTER = FIELD_NAMES_POINTER + Integer.BYTES;
    protected static final int META_SCHEMA_POINTER = SCHEMA_POINTER + Integer.BYTES;
    protected static final int PATH_INFO_POINTER = META_SCHEMA_POINTER + Integer.BYTES;
    protected static final int OFFSETS_SIZE = PATH_INFO_POINTER + Integer.BYTES;

    public SchemaResponseMessage(long reqId, SerializableArrayBackedValueStorage columnMetaData, Throwable failure)
            throws HyracksDataException {
        this.reqId = reqId;
        this.serializedColumnMetaData = columnMetaData;
//        this.columnMetadata = deserialiseColumnMetadata(columnMetaData);
        this.failure = failure;

    }

    @Override
    public void handle(ICcApplicationContext appCtx) {
        ICCMessageBroker broker = (ICCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        broker.respond(reqId, this);
    }

    @Override
    public void setResult(MutablePair<ResponseState, Object> result) {
        if (failure != null) {
            result.setLeft(ResponseState.FAILURE);
            result.setRight(failure);
            return;
        }
        try {
            if(result == null) {
//                result = new MutablePair<>(ResponseState.UNINITIALIZED, null);
                System.out.println("Result is null");
            }
            count.incrementAndGet();
            System.out.println("Result is not null" + result.getValue().toString() + " Count: " + count);

            setResponse(result);
            } catch (Exception e) {
            System.out.println(e);
        }
    }

    private void setResponse(MutablePair<ResponseState, Object> result) throws IOException {
        switch (result.getKey()) {
            case SUCCESS:
                System.out.println("Result is not null Resp -1" + result.getValue().toString() + " Count: " + count);
                SerializableArrayBackedValueStorage currentSerColumnMetaData = (SerializableArrayBackedValueStorage)result.getValue();
                System.out.println("Result is not null Resp 0" + result.getValue().toString() + " Count: " + count);
                FlushColumnMetadata newColumnMetadata =  deserializeColumnMetadata(serializedColumnMetaData);
                System.out.println("Result is not null Resp 1" + result.getValue().toString() + " Count: " + count);
                ColumnSchemaTransformer schemaTransformer = initSchemaTransformer(deserializeColumnMetadata(currentSerColumnMetaData));
                System.out.println("Result is not null Resp 2" + result.getValue().toString() + " Count: " + count);

                System.out.println("----------------------------------------------");
                SchemaStringBuilderVisitor schemaBuilderJson =
                        new SchemaStringBuilderVisitor(newColumnMetadata.getFieldNamesDictionary());
                SchemaStringBuilderVisitor curschemaBuilderJson =
                        new SchemaStringBuilderVisitor(schemaTransformer.getRowMetadata().getFieldNamesDictionary());
                String recordSchema = LogRedactionUtil.userData(schemaBuilderJson.build(newColumnMetadata.getRoot()));
                String currecordSchema = LogRedactionUtil.userData(curschemaBuilderJson.build(schemaTransformer.getRowMetadata().getRoot()));
                System.out.println("New Schema: " + recordSchema);
                System.out.println("CurrentSchema: " + currecordSchema);
                System.out.println("----------------------------------------------");

                schemaTransformer.setToMergeFieldNamesDictionary(newColumnMetadata.getFieldNamesDictionary());
                schemaTransformer.transform(newColumnMetadata.getRoot());
                System.out.println("Result is not null Resp 3" + result.getValue().toString() + " Count: " + count);
                result.setValue(new SerializableArrayBackedValueStorage((ArrayBackedValueStorage)schemaTransformer.getRowMetadata().serializeColumnsMetadata()));
                System.out.println("Result is not null Resp 4" + result.getValue().toString() + " Count: " + count);
                break;
            case UNINITIALIZED:
                result.setLeft(ResponseState.SUCCESS);
                System.out.println("Result is not null Resp" + result.getValue().toString() + " Count: " + count);
                result.setValue(serializedColumnMetaData);
                break;
            default:
                break;
        }
    }

    public FlushColumnMetadata deserializeColumnMetadata(SerializableArrayBackedValueStorage serColumnMetaData) {
        try{
        ArrayBackedValueStorage storage = serColumnMetaData.getStorage();
        int offset = storage.getStartOffset();
        int length = storage.getLength();
        int fieldNamesStart = offset + IntegerPointable
                .getInteger(storage.getByteArray(), offset + FIELD_NAMES_POINTER);
        int metaRootStart = IntegerPointable.getInteger(storage.getByteArray(),
                offset + META_SCHEMA_POINTER);
        int metaRootSize = metaRootStart < 0 ? 0
                : IntegerPointable.getInteger(storage.getByteArray(),
                offset + PATH_INFO_POINTER) - metaRootStart;
        DataInput input =
                new DataInputStream(new ByteArrayInputStream(storage.getByteArray(),
                        fieldNamesStart, length));
        IFieldNamesDictionary fieldNamesDictionary = AbstractFieldNamesDictionary.deserialize(input);
        Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels = new HashMap<>();
        ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory factory = new ColumnValuesWriterFactory(multiPageOpRef);
        FlushColumnMetadata rowMetaData = new FlushColumnMetadata(multiPageOpRef, root, definitionLevels,fieldNamesDictionary,factory);
        return rowMetaData;
        }
        catch (IOException e) {
            System.out.println("Error in deserialiseColumnMetadata: " + e.getMessage());
            return null;
        }

    }

    public ColumnSchemaTransformer initSchemaTransformer(FlushColumnMetadata rowMetaData){
        System.out.println("Result is not null Resp 2 a");
        ColumnSchemaTransformer schemaTransformer = new ColumnSchemaTransformer(rowMetaData, rowMetaData.getRoot());
        System.out.println("Result is not null Resp 2 b");
        schemaTransformer.setToMergeFieldNamesDictionary(rowMetaData.getFieldNamesDictionary());
        System.out.println("Result is not null Resp 2 c");
        return schemaTransformer;
    }



    @Override
    public boolean isWhispered() {
        return true;
    }
}

