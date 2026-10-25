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

import static org.apache.asterix.app.message.ExecuteStatementRequestMessage.DEFAULT_NC_TIMEOUT_MILLIS;

import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.CalculateSchemaRequestMessage;
import org.apache.asterix.app.message.CalculateSchemaResponseMessage;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.asterix.utils.SchemaUtil;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaFunction extends AbstractDatasourceFunction {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final String database;
    private final DataverseName dataverse;
    private final String collection;
    private final String index;
    private final IFileSplitProvider splitProvider;
    private final IndexDataflowHelperFactory indexDataflowHelperFactory;
    private final int[][] partition;

    SchemaFunction(AlgebricksAbsolutePartitionConstraint locations, String database, DataverseName dataverse,
            String collection, String index, IFileSplitProvider splitProvider,
            IndexDataflowHelperFactory indexDataflowHelperFactory, int[][] partition) {
        super(locations);

        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
        this.splitProvider = splitProvider;
        this.indexDataflowHelperFactory = indexDataflowHelperFactory;
        this.partition = partition;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        INCMessageBroker messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
        MessageFuture messageFuture = messageBroker.registerMessageFuture();
        long futureId = messageFuture.getFutureId();
        CalculateSchemaRequestMessage request = new CalculateSchemaRequestMessage(serviceCtx.getNodeId(), futureId,
                database, dataverse, collection, index, splitProvider);
        try {
            messageBroker.sendMessageToPrimaryCC(request);
            CalculateSchemaResponseMessage response = (CalculateSchemaResponseMessage) messageFuture
                    .get(DEFAULT_NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (response.getFailure() != null) {
                throw HyracksDataException.create(response.getFailure());
            }

            FlushColumnMetadata columnMetadata =
                    SchemaUtil.deserializeColumnMetadata(response.getSerSchema().getStorage());
            return new SchemaReader(columnMetadata);
        } catch (Exception e) {
            LOGGER.info("Could not fetch schema", e);
            throw HyracksDataException.create(e);
        } finally {
            messageBroker.deregisterMessageFuture(futureId);
        }
    }

}
