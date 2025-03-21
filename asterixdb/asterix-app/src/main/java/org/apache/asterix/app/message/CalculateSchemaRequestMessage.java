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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.utils.SchemaUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CalculateSchemaRequestMessage implements ICcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private static final SerializableArrayBackedValueStorage FAILED_CALCULATED_SCHEMA =
            new SerializableArrayBackedValueStorage(new ArrayBackedValueStorage());
    private final String nodeId;
    private final long reqId;
    private final String database;
    private final DataverseName dataverse;
    private final String collection;
    private final String index;
    private final boolean toFlush;
    private final IFileSplitProvider splitProvider;
    private static AtomicInteger count = new AtomicInteger(0);

    public CalculateSchemaRequestMessage(String nodeId, long reqId, String database, DataverseName dataverse,
            String collection, String index, IFileSplitProvider splitProvider,boolean toFlush) {

        this.nodeId = nodeId;
        this.reqId = reqId;
        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
        this.splitProvider = splitProvider;
        this.toFlush = toFlush;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException {

        CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();

        try {

            SerializableArrayBackedValueStorage serColumnMetaData =
                    SchemaUtil.getDatasetInfo(appCtx, database, dataverse, collection, index, splitProvider, toFlush);
            count.incrementAndGet();
            LOGGER.info("Count: {}", count);
            CalculateSchemaResponseMessage response =
                    new CalculateSchemaResponseMessage(this.reqId, serColumnMetaData, null);
            messageBroker.sendApplicationMessageToNC(response, nodeId);
        } catch (Exception ex) {
            LOGGER.info("Failed to process request", ex);
            try {
                CalculateSchemaResponseMessage response =
                        new CalculateSchemaResponseMessage(this.reqId, FAILED_CALCULATED_SCHEMA, ex);
                messageBroker.sendApplicationMessageToNC(response, nodeId);
            } catch (Exception ex2) {
                LOGGER.info("Failed to process request", ex2);
                throw HyracksDataException.create(ex2);
            }
        }
    }

    //    @Override
    //    public void setResult(MutablePair<ICCMessageBroker.ResponseState, Object> result) {
    //
    //    }
}
