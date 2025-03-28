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

import java.util.Set;
import java.util.function.Predicate;

import org.apache.asterix.column.operation.lsm.flush.ColumnSchemaTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.messaging.CcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaRequestMessage extends CcIdentifiedMessage implements INcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final long reqId;
    private final String database;
    private final String dataverse;
    private final String collection;
    private final String index;
    private final Dataset dataset;
    private final boolean toFlush;
    FlushColumnMetadata columnMetadata;
    ArrayBackedValueStorage serializedColumnMetadata;
    private final IFileSplitProvider splitProvider;

    public SchemaRequestMessage(long reqId, String database, String dataverse, String collection, String index,
            Dataset dataset, IFileSplitProvider splitProvider, boolean toFlush) {

        this.reqId = reqId;
        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
        this.dataset = dataset;
        this.splitProvider = splitProvider;
        this.toFlush = toFlush;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException {

        // TODO: Remove the isOpen check and let it fail if flush is requested for a dataset that is closed
        try {
            Set<Integer> partSet =
                    appCtx.getMetadataProperties().getNodePartitions(appCtx.getServiceContext().getNodeId());
            int[] part = partSet.stream().mapToInt(Integer::intValue).toArray();
            IndexDataflowHelperFactory indexDataflowHelperFactory = new IndexDataflowHelperFactory(
                    appCtx.getStorageComponentProvider().getStorageManager(), splitProvider);
            final IIndexDataflowHelper[] indexDataflowHelpers = new IIndexDataflowHelper[part.length];
            for (int i = 0; i < part.length; i++) {
                indexDataflowHelpers[i] = indexDataflowHelperFactory.create(appCtx.getServiceContext(), part[i]);
                indexDataflowHelpers[i].open();
            }

            DatasetInfo dsInfo = appCtx.getDatasetLifecycleManager().getDatasetInfo(this.dataset.getDatasetId());
            if (dsInfo == null) {
                throw HyracksDataException.create(new Exception("Dataset does not exist on this node"));
            }
            if (dsInfo.isOpen() && toFlush) {
                appCtx.getDatasetLifecycleManager().flushDataset(this.dataset.getDatasetId(), false);
            }

            final ColumnSchemaTransformer[] columnSchemaTransformer = { null };

            dsInfo.getIndexes().forEach((indexId, indexInfo) -> {
                if (dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.COLUMN) {
                    if (indexInfo.getIndex() instanceof LSMColumnBTree) {
                        LSMColumnBTree lsmColumnBTree = (LSMColumnBTree) indexInfo.getIndex();
                        this.columnMetadata = (FlushColumnMetadata) lsmColumnBTree.getPublicColumnMetadata();
                        if (columnSchemaTransformer[0] == null) {
                            columnSchemaTransformer[0] =
                                    new ColumnSchemaTransformer(this.columnMetadata, this.columnMetadata.getRoot());
                            columnSchemaTransformer[0]
                                    .setToMergeFieldNamesDictionary(this.columnMetadata.getFieldNamesDictionary());
                        } else {
                            try {
                                columnSchemaTransformer[0]
                                        .setToMergeFieldNamesDictionary(this.columnMetadata.getFieldNamesDictionary());
                                columnSchemaTransformer[0].transform(this.columnMetadata.getRoot());
                            } catch (HyracksDataException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
            });

            this.serializedColumnMetadata =
                    (ArrayBackedValueStorage) columnSchemaTransformer[0].getColMetadata().serializeColumnsMetadata();
            SchemaResponseMessage response = new SchemaResponseMessage(reqId,
                    new SerializableArrayBackedValueStorage(serializedColumnMetadata), null);
            respond(appCtx, response);

        } catch (Exception e) {
            LOGGER.info("failed to get schema", e);
            SchemaResponseMessage response = new SchemaResponseMessage(reqId, null, e);
            respond(appCtx, response);
        }
    }

    private Predicate<String> getPredicate() {
        return path -> {
            ResourceReference resourceReference = ResourceReference.of(path);
            if (resourceReference.getDatabase().equals(database)
                    && resourceReference.getDataverse().getCanonicalForm().equals(dataverse)
                    && resourceReference.getDataset().equals(collection)) {
                if (index != null) {
                    return resourceReference.getIndex().equals(index);
                }
                return true;
            }
            return false;
        };
    }

    private void respond(INcApplicationContext appCtx, SchemaResponseMessage response) throws HyracksDataException {

        NCMessageBroker messageBroker = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            messageBroker.sendMessageToPrimaryCC(response);
        } catch (Exception e) {
            LOGGER.info("failed to send schema to cc", e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
