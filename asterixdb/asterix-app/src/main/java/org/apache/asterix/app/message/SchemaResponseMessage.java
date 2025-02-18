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

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.ColumnSchemaTransformer;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.ICCMessageBroker.ResponseState;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INcResponse;
import org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;

public class SchemaResponseMessage implements ICcAddressedMessage, INcResponse {

    private static final long serialVersionUID = 1L;
    private final long reqId;
    SerializableArrayBackedValueStorage serializedColumnMetaData;
    private final Throwable failure;
    protected ColumnSchemaTransformer schemaTransformer;

    public SchemaResponseMessage(long reqId, SerializableArrayBackedValueStorage columnMetaData, Throwable failure)
            throws HyracksDataException {
        this.reqId = reqId;
        this.serializedColumnMetaData = columnMetaData;
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
            setResponse(result);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private void setResponse(MutablePair<ResponseState, Object> result) throws IOException, ClassNotFoundException {
        //TODO CALVIN DANI: Move deserial logic.
        try {

            DataInput input =
                    new DataInputStream(new ByteArrayInputStream(serializedColumnMetaData.getStorage().getByteArray(),
                            serializedColumnMetaData.getStorage().getStartOffset(),
                            serializedColumnMetaData.getStorage().getLength()));
            //Skip offsets
            input.skipBytes(134);

            //ColumnWriter
            //            int numberOfWriters = input.readInt();
            ////        List<IColumnValuesWriter> writers = new ArrayList<>();
            //            input.skipBytes(10 *  numberOfWriters);
            //        deserializeWriters(input, writers, columnWriterFactory);

            //FieldNames RowFieldNamesDictionary.deserialize(input);
            IFieldNamesDictionary fieldNamesDictionary = AbstractFieldNamesDictionary.deserialize(input);
            //        RowFieldNamesDictionary fieldNamesDictionary = RowFieldNamesDictionary.deserialize(input);
            //Schema
            Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels = new HashMap<>();
            ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
            ObjectSchemaNode metaRoot = null;

            //        if (metaType != null) {
            //            metaRoot = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
            //        }

            ArrayBackedValueStorage schemaStorage =
                    new ArrayBackedValueStorage(serializedColumnMetaData.getStorage().getLength());
            schemaStorage.append(serializedColumnMetaData.getStorage());
            //        logSchema(root, metaRoot, fieldNamesDictionary);

            //
            //        //        IValueReference serColMetadata = (ArrayBackedValueStorage) JavaSerializationUtils.deserialize(serializedColumnMetaData.getStorage().getByteArray());
            //        int fieldNamesStart = UnsafeUtil.getInt(serializedColumnMetaData.getStorage().getByteArray(),  0);
            //        int metaRootStart = UnsafeUtil.getInt(serializedColumnMetaData.getStorage().getByteArray(),  12);
            //        int length = UnsafeUtil.getInt(serializedColumnMetaData.getStorage().getByteArray(),   2);
            //        int metaRootSize = metaRootStart < 0 ? 0 : UnsafeUtil.getInt(serializedColumnMetaData.getStorage().getByteArray(), 16) - metaRootStart;
            //        DataInput input = new DataInputStream(new ByteArrayInputStream(serializedColumnMetaData.getStorage().getByteArray(), fieldNamesStart, length));

            //FieldNames
            //            RowFieldNamesDictionary fieldNamesDictionary = RowFieldNamesDictionary.deserialize(input);
            schemaTransformer.setToMergeFieldNamesDictionary(fieldNamesDictionary);
            //            //Schema
            //            ObjectRowSchemaNode root = (ObjectRowSchemaNode) AbstractRowSchemaNode.deserialize(input);
            schemaTransformer.transform(root);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        switch (result.getKey()) {
            case SUCCESS:
                result.setValue(serializedColumnMetaData);
                break;
            case UNINITIALIZED:

                long size = 0;
                result.setLeft(ResponseState.SUCCESS);
                result.setValue(serializedColumnMetaData);
                break;
            default:
                break;
        }
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
