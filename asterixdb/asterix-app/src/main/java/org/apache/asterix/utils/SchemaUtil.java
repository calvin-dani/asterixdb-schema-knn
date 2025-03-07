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
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.app.message.SchemaRequestMessage;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.ColumnSchemaTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.util.SchemaStringBuilderVisitor;
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
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.Level;

public class SchemaUtil {

    protected static final int WRITERS_POINTER = 0;
    protected static final int FIELD_NAMES_POINTER = WRITERS_POINTER + Integer.BYTES;
    protected static final int SCHEMA_POINTER = FIELD_NAMES_POINTER + Integer.BYTES;
    protected static final int META_SCHEMA_POINTER = SCHEMA_POINTER + Integer.BYTES;
    protected static final int PATH_INFO_POINTER = META_SCHEMA_POINTER + Integer.BYTES;
    protected static final int OFFSETS_SIZE = PATH_INFO_POINTER + Integer.BYTES;

    public static SerializableArrayBackedValueStorage getDatasetInfo(ICcApplicationContext appCtx, String database,
                                                                     DataverseName dataverse, String collection, String index, IFileSplitProvider splitProvider) throws IOException,Exception {
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
            List<Index> listIndex = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataset.getDatabaseName(),
                    dataset.getDataverseName(), dataset.getDatasetName());
            index = listIndex.get(0).getIndexName();
            if (dataset.getDatasetType() != DatasetConfig.DatasetType.INTERNAL) {
                throw new CompilationException(ErrorCode.STORAGE_SIZE_NOT_APPLICABLE_TO_TYPE, dataset.getDatasetType());
            }

            if (index != null) {
                Index idx = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), database,
                        dataverse, collection, index);
                metadataProvider.getPartitioningProperties(dataset, idx.getIndexName());
                if (idx == null) {
                    throw new CompilationException(ErrorCode.UNKNOWN_INDEX, index);
                }
            }

            final List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
            CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();

            long reqId = messageBroker.newRequestId();
            List<SchemaRequestMessage> requests = new ArrayList<>();
            for (int i = 0; i < ncs.size(); i++) {
                requests.add(new SchemaRequestMessage(reqId, database, dataverse.getCanonicalForm(), collection, index,
                        dataset,splitProvider));
            }
            return (SerializableArrayBackedValueStorage) messageBroker.sendSyncRequestToNCs(reqId, ncs, requests,
                    TimeUnit.SECONDS.toMillis(60000), true);
        }
        catch (IOException e) {
            throw e;
        } finally {
            InvokeUtil.tryWithCleanups(() -> MetadataManager.INSTANCE.commitTransaction(mdTxnCtx),
                    () -> metadataProvider.getLocks().unlock());
        }
    }

}
