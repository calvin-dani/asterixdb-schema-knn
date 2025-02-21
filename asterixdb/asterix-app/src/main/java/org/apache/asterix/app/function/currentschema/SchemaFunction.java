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
import static org.apache.hyracks.api.messages.IMessage.logMessage;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.app.message.CalculateSchemaRequestMessage;
import org.apache.asterix.app.message.CalculateSchemaResponseMessage;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.ColumnSchemaTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.logging.log4j.Level;
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
    private static AtomicInteger count = new AtomicInteger(0);
    protected static final int WRITERS_POINTER = 0;
    protected static final int FIELD_NAMES_POINTER = WRITERS_POINTER + Integer.BYTES;
    protected static final int SCHEMA_POINTER = FIELD_NAMES_POINTER + Integer.BYTES;
    protected static final int META_SCHEMA_POINTER = SCHEMA_POINTER + Integer.BYTES;
    protected static final int PATH_INFO_POINTER = META_SCHEMA_POINTER + Integer.BYTES;
    protected static final int OFFSETS_SIZE = PATH_INFO_POINTER + Integer.BYTES;

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
        count = new AtomicInteger(0);
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {

        int[] part = this.partition[partition];
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        INCMessageBroker messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
        MessageFuture messageFuture = messageBroker.registerMessageFuture();
        long futureId = messageFuture.getFutureId();
        //        int[] partitions = partitionsMap[partition];
        final IIndexDataflowHelper[] indexDataflowHelpers = new IIndexDataflowHelper[part.length];
        for (int i = 0; i < part.length; i++) {
            indexDataflowHelpers[i] = indexDataflowHelperFactory.create(serviceCtx, part[i]);
            indexDataflowHelpers[i].open();
        }
        CalculateSchemaRequestMessage request = new CalculateSchemaRequestMessage(serviceCtx.getNodeId(), futureId,
                database, dataverse, collection, index);

        try {
            messageBroker.sendMessageToPrimaryCC(request);
            CalculateSchemaResponseMessage response = (CalculateSchemaResponseMessage) messageFuture
                    .get(DEFAULT_NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (response.getFailure() != null) {
                throw HyracksDataException.create(response.getFailure());
            }

            count.incrementAndGet();
            int offset = response.getSerSchema().getStorage().getStartOffset();
            int length = response.getSerSchema().getStorage().getLength();
            int fieldNamesStart = offset + IntegerPointable
                    .getInteger(response.getSerSchema().getStorage().getByteArray(), offset + FIELD_NAMES_POINTER);
            int metaRootStart = IntegerPointable.getInteger(response.getSerSchema().getStorage().getByteArray(),
                    offset + META_SCHEMA_POINTER);
            int metaRootSize = metaRootStart < 0 ? 0
                    : IntegerPointable.getInteger(response.getSerSchema().getStorage().getByteArray(),
                            offset + PATH_INFO_POINTER) - metaRootStart;
            DataInput input =
                    new DataInputStream(new ByteArrayInputStream(response.getSerSchema().getStorage().getByteArray(),
                            fieldNamesStart, length));
            //FieldNames
            IFieldNamesDictionary fieldNamesDictionary = AbstractFieldNamesDictionary.deserialize(input);

            //Schema
            Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels = new HashMap<>();
            ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);

            //ColumnMetadata //TODO CALVIN DANI define multipageref
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
            //TODO CALVIN DANI to extend it and overwrite and remove flush functionalities or modify.
            FlushColumnMetadata rowMetaData = new FlushColumnMetadata(multiPageOpRef, root, definitionLevels);
            LOGGER.log(Level.INFO, "ROOT CHILDREN: {}", rowMetaData.getRoot().getChildren().toString());
            ColumnSchemaTransformer schemaTransformer = new ColumnSchemaTransformer(rowMetaData, rowMetaData.getRoot());

            schemaTransformer.setToMergeFieldNamesDictionary(fieldNamesDictionary);
            schemaTransformer.transform(root);
            LOGGER.log(Level.INFO, "COUNT AND PARTITION LENGTH: {} {}", count.get(), this.partition.length);
            return new SchemaReader(rowMetaData, count.get() == this.partition.length);
        } catch (Exception e) {
            LOGGER.info("Could not calculate collection size", e);
            throw HyracksDataException.create(e);
        } finally {
            messageBroker.deregisterMessageFuture(futureId);
        }
    }
}
