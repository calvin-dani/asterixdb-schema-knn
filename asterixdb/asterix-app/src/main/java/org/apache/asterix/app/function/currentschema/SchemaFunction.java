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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.app.message.CalculateSchemaRequestMessage;
import org.apache.asterix.app.message.CalculateSchemaResponseMessage;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
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
    private final boolean toFlush;
    private static AtomicInteger count = new AtomicInteger(0);
    protected static final int WRITERS_POINTER = 0;
    protected static final int FIELD_NAMES_POINTER = WRITERS_POINTER + Integer.BYTES;
    protected static final int SCHEMA_POINTER = FIELD_NAMES_POINTER + Integer.BYTES;
    protected static final int META_SCHEMA_POINTER = SCHEMA_POINTER + Integer.BYTES;
    protected static final int PATH_INFO_POINTER = META_SCHEMA_POINTER + Integer.BYTES;
    protected static final int OFFSETS_SIZE = PATH_INFO_POINTER + Integer.BYTES;

    SchemaFunction(AlgebricksAbsolutePartitionConstraint locations, String database, DataverseName dataverse,
            String collection, String index, IFileSplitProvider splitProvider,
            IndexDataflowHelperFactory indexDataflowHelperFactory, int[][] partition, boolean toFlush) {
        super(locations);

        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
        this.splitProvider = splitProvider;
        this.indexDataflowHelperFactory = indexDataflowHelperFactory;
        this.partition = partition;
        this.toFlush = toFlush;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition) // 0 , 1, 2
            throws HyracksDataException {
        LOGGER.log(Level.INFO, "START OF CREATE RECORD");
        LOGGER.log(Level.INFO, "PARITION IN CREATE RECORD : {} {}", this.partition, partition);
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        INCMessageBroker messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
        MessageFuture messageFuture = messageBroker.registerMessageFuture();
        long futureId = messageFuture.getFutureId();
        LOGGER.log(Level.INFO, "AFTER CREATE RECORD INDEXDATAFLOWHELPER");
        CalculateSchemaRequestMessage request = new CalculateSchemaRequestMessage(serviceCtx.getNodeId(), futureId,
                database, dataverse, collection, index, splitProvider,toFlush);
        LOGGER.log(Level.INFO, "AFTER CREATE RECORD CALCSCHEMAREQUESTMESSAGE REQUEST");
        try {
            LOGGER.log(Level.INFO, "BEFORE SEND MESSAGE TO PRIMARY CC");
            messageBroker.sendMessageToPrimaryCC(request);
            CalculateSchemaResponseMessage response = (CalculateSchemaResponseMessage) messageFuture
                    .get(DEFAULT_NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (response.getFailure() != null) {
                throw HyracksDataException.create(response.getFailure());
            }
            LOGGER.log(Level.INFO, "what is schema function response : {} partiton {}", response, partition);

            FlushColumnMetadata columnMetadata = deserializeColumnMetadata(response.getSerSchema());
            return new SchemaReader(columnMetadata);
        } catch (Exception e) {
            LOGGER.info("Could not calculate collection size", e);
            throw HyracksDataException.create(e);
        } finally {
            messageBroker.deregisterMessageFuture(futureId);
        }
    }

    public FlushColumnMetadata deserializeColumnMetadata(SerializableArrayBackedValueStorage serColumnMetaData) {
        try {
            ArrayBackedValueStorage storage = serColumnMetaData.getStorage();
            int offset = storage.getStartOffset();
            int length = storage.getLength();
            int fieldNamesStart =
                    offset + IntegerPointable.getInteger(storage.getByteArray(), offset + FIELD_NAMES_POINTER);
            int metaRootStart = IntegerPointable.getInteger(storage.getByteArray(), offset + META_SCHEMA_POINTER);
            int metaRootSize = metaRootStart < 0 ? 0
                    : IntegerPointable.getInteger(storage.getByteArray(), offset + PATH_INFO_POINTER) - metaRootStart;
            DataInput input =
                    new DataInputStream(new ByteArrayInputStream(storage.getByteArray(), fieldNamesStart, length));
            IFieldNamesDictionary fieldNamesDictionary = AbstractFieldNamesDictionary.deserialize(input);
            Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels = new HashMap<>();
            ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
            IColumnValuesWriterFactory factory = new ColumnValuesWriterFactory(multiPageOpRef);
            LOGGER.log(Level.INFO, "COLUMN WRITEER FACTORY IS NULL ? {}", factory == null);
            FlushColumnMetadata rowMetaData =
                    new FlushColumnMetadata(multiPageOpRef, root, definitionLevels, fieldNamesDictionary, factory);
            return rowMetaData;
        } catch (IOException e) {
            System.out.println("Error in deserialiseColumnMetadata: " + e.getMessage());
            return null;
        }

    }
}
