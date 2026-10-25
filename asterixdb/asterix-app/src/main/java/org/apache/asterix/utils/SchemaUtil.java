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
package org.apache.asterix.utils;

import static org.apache.asterix.common.api.IClusterManagementWork.ClusterState.ACTIVE;
import static org.apache.asterix.common.api.IClusterManagementWork.ClusterState.REBALANCE_REQUIRED;
import static org.apache.asterix.common.exceptions.ErrorCode.REJECT_BAD_CLUSTER_STATE;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.SchemaRequestMessage;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.common.api.IClusterManagementWork;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

public class SchemaUtil {
    public static final String CURRENT_SCHEMA = "current-schema";

    // Offset of the field-names pointer in the serialized column metadata header (see FlushColumnMetadata)
    private static final int FIELD_NAMES_POINTER = Integer.BYTES;
    private static final long SCHEMA_REQUEST_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);

    private SchemaUtil() {
    }

    /**
     * Collects the schema of a columnar dataset from every node and merges it.
     *
     * @return the merged column metadata, serialized
     * @throws RuntimeDataException with {@link ErrorCode#NO_FLUSHED_SCHEMA} if no node has flushed any data
     */
    public static SerializableArrayBackedValueStorage getDatasetInfo(ICcApplicationContext appCtx, String database,
            DataverseName dataverse, String collection, IFileSplitProvider splitProvider) throws Exception {
        IClusterManagementWork.ClusterState state = appCtx.getClusterStateManager().getState();
        if (!(state == ACTIVE || state == REBALANCE_REQUIRED)) {
            throw new RuntimeDataException(REJECT_BAD_CLUSTER_STATE, state);
        }

        if (!appCtx.getNamespaceResolver().isUsingDatabase()) {
            database = MetadataConstants.DEFAULT_DATABASE;
        }

        IMetadataLockManager lockManager = appCtx.getMetadataLockManager();
        MetadataProvider metadataProvider = MetadataProvider.createWithDefaultNamespace(appCtx);
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        metadataProvider.setMetadataTxnContext(mdTxnCtx);
        try {
            lockManager.acquireDatabaseReadLock(metadataProvider.getLocks(), database);
            lockManager.acquireDataverseReadLock(metadataProvider.getLocks(), database, dataverse);
            lockManager.acquireDatasetReadLock(metadataProvider.getLocks(), database, dataverse, collection);
            Dataset dataset = metadataProvider.findDataset(database, dataverse, collection);
            if (dataset == null) {
                throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, collection,
                        MetadataUtil.dataverseName(database, dataverse, metadataProvider.isUsingDatabase()));
            }
            ensureColumnar(dataset);

            List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
            CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();
            long reqId = messageBroker.newRequestId();
            List<SchemaRequestMessage> requests = new ArrayList<>();
            for (int i = 0; i < ncs.size(); i++) {
                requests.add(new SchemaRequestMessage(reqId, dataset, splitProvider));
            }
            SerializableArrayBackedValueStorage merged = (SerializableArrayBackedValueStorage) messageBroker
                    .sendSyncRequestToNCs(reqId, ncs, requests, SCHEMA_REQUEST_TIMEOUT_MILLIS, true);
            if (merged == null) {
                throw new RuntimeDataException(ErrorCode.NO_FLUSHED_SCHEMA, collection);
            }
            return merged;
        } finally {
            InvokeUtil.tryWithCleanups(() -> MetadataManager.INSTANCE.commitTransaction(mdTxnCtx),
                    () -> metadataProvider.getLocks().unlock());
        }
    }

    public static void ensureColumnar(Dataset dataset) throws CompilationException {
        if (dataset.getDatasetType() != DatasetConfig.DatasetType.INTERNAL
                || dataset.getDatasetFormatInfo().getFormat() != DatasetConfig.DatasetFormat.COLUMN) {
            throw new CompilationException(ErrorCode.COLUMN_FORMAT_REQUIRED, CURRENT_SCHEMA, dataset.getDatasetName());
        }
    }

    /**
     * Rebuilds a standalone {@link FlushColumnMetadata} from serialized column metadata. The result shares no state
     * with any index, so it can be merged into freely.
     */
    public static FlushColumnMetadata deserializeColumnMetadata(IValueReference serialized)
            throws HyracksDataException {
        byte[] bytes = serialized.getByteArray();
        int start = serialized.getStartOffset();
        int fieldNamesStart = start + IntegerPointable.getInteger(bytes, start + FIELD_NAMES_POINTER);
        int length = serialized.getLength() - (fieldNamesStart - start);
        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, fieldNamesStart, length));
        try {
            IFieldNamesDictionary fieldNamesDictionary = AbstractFieldNamesDictionary.deserialize(input);
            Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels = new HashMap<>();
            ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
            Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
            return new FlushColumnMetadata(multiPageOpRef, root, definitionLevels, fieldNamesDictionary,
                    new ColumnValuesWriterFactory(multiPageOpRef));
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
