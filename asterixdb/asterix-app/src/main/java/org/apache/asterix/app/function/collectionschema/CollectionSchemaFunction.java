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

import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.schemainferrence.RowMetadata;
import org.apache.asterix.runtime.schemainferrence.RowTransformer;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CollectionSchemaFunction extends AbstractDatasourceFunction {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final String database;
    private final DataverseName dataverse;
    private final String collection;
    private final String index;
    private final IAType type;
    private final int[][] partition;

    CollectionSchemaFunction(AlgebricksAbsolutePartitionConstraint locations, String database, DataverseName dataverse,
            String collection, String index, int[][] partition, IAType type) {
        super(locations);

        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
        this.partition = partition;
        this.type = type;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        int[] part = this.partition[partition];
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        INCMessageBroker messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
        MessageFuture messageFuture = messageBroker.registerMessageFuture();
        long futureId = messageFuture.getFutureId();
        try {

            RowMetadata rowMetaData = new RowMetadata();
            RowTransformer transformer = new RowTransformer(rowMetaData, rowMetaData.getRoot());
            transformer.transform((ARecordType) type);

            return new CollectionSchemaReader(rowMetaData);
        } catch (Exception e) {
            LOGGER.info("Could not calculate schema", e);
            throw HyracksDataException.create(e);
        } finally {
            messageBroker.deregisterMessageFuture(futureId);
        }
    }
}
