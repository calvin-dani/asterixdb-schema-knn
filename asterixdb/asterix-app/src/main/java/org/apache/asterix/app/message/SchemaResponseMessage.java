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

import org.apache.asterix.column.operation.lsm.flush.ColumnSchemaTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.ICCMessageBroker.ResponseState;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.messaging.api.INcResponse;
import org.apache.asterix.utils.SchemaUtil;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;

/**
 * One node's schema for a columnar dataset; responses are merged as they arrive.
 */
public class SchemaResponseMessage implements ICcAddressedMessage, INcResponse {

    private static final long serialVersionUID = 1L;
    private final long reqId;
    // Null when the node has no flushed data for the dataset
    private final SerializableArrayBackedValueStorage serializedColumnMetaData;
    private final Throwable failure;

    public SchemaResponseMessage(long reqId, SerializableArrayBackedValueStorage columnMetaData, Throwable failure) {
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
        if (result.getKey() == ResponseState.UNINITIALIZED) {
            result.setLeft(ResponseState.SUCCESS);
            result.setRight(serializedColumnMetaData);
            return;
        }
        SerializableArrayBackedValueStorage current = (SerializableArrayBackedValueStorage) result.getRight();
        if (serializedColumnMetaData == null) {
            return;
        }
        if (current == null) {
            result.setRight(serializedColumnMetaData);
            return;
        }
        try {
            result.setRight(merge(current, serializedColumnMetaData));
        } catch (HyracksDataException e) {
            result.setLeft(ResponseState.FAILURE);
            result.setRight(e);
        }
    }

    private static SerializableArrayBackedValueStorage merge(SerializableArrayBackedValueStorage target,
            SerializableArrayBackedValueStorage toMerge) throws HyracksDataException {
        FlushColumnMetadata targetMetadata = SchemaUtil.deserializeColumnMetadata(target.getStorage());
        FlushColumnMetadata toMergeMetadata = SchemaUtil.deserializeColumnMetadata(toMerge.getStorage());
        ColumnSchemaTransformer schemaTransformer =
                new ColumnSchemaTransformer(targetMetadata, targetMetadata.getRoot());
        schemaTransformer.setToMergeFieldNamesDictionary(toMergeMetadata.getFieldNamesDictionary());
        schemaTransformer.transform(toMergeMetadata.getRoot());
        return new SerializableArrayBackedValueStorage(
                (ArrayBackedValueStorage) schemaTransformer.getColMetadata().serializeColumnsMetadata());
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
