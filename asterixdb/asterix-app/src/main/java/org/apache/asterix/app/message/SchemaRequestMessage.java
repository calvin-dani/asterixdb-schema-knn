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

import java.util.List;

import org.apache.asterix.column.operation.lsm.flush.ColumnSchemaTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.CcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.utils.Partitions;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.utils.SchemaUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.SerializableArrayBackedValueStorage;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntIterator;

/**
 * Asks one node for the merged schema of its partitions of a columnar dataset.
 */
public class SchemaRequestMessage extends CcIdentifiedMessage implements INcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final long reqId;
    private final Dataset dataset;
    private final IFileSplitProvider splitProvider;

    public SchemaRequestMessage(long reqId, Dataset dataset, IFileSplitProvider splitProvider) {
        this.reqId = reqId;
        this.dataset = dataset;
        this.splitProvider = splitProvider;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException {
        SchemaResponseMessage response;
        try {
            response = new SchemaResponseMessage(reqId, collectSchema(appCtx), null);
        } catch (Exception e) {
            LOGGER.info("failed to get schema of {}", dataset.getDatasetName(), e);
            response = new SchemaResponseMessage(reqId, null, e);
        }
        respond(appCtx, response);
    }

    /**
     * Merges the schema stored with each partition's newest flushed component. The running index's own metadata is
     * never touched: even serializing it changes state that its next flush relies on. Records not yet flushed are
     * therefore not reflected.
     *
     * @return the merged schema, or null if no partition on this node has flushed any data
     */
    private SerializableArrayBackedValueStorage collectSchema(INcApplicationContext appCtx)
            throws HyracksDataException {
        Partitions nodePartitions =
                appCtx.getMetadataProperties().getNodePartitions(appCtx.getServiceContext().getNodeId());
        IndexDataflowHelperFactory helperFactory =
                new IndexDataflowHelperFactory(appCtx.getStorageComponentProvider().getStorageManager(), splitProvider);
        IIndexDataflowHelper[] helpers = new IIndexDataflowHelper[nodePartitions.size()];
        ColumnSchemaTransformer merged = null;
        SerializableArrayBackedValueStorage result = null;
        Throwable failure = null;
        try {
            IntIterator partitions = nodePartitions.iterator();
            for (int i = 0; i < helpers.length; i++) {
                helpers[i] = helperFactory.create(appCtx.getServiceContext(), partitions.nextInt());
                helpers[i].open();
                IValueReference stored = getNewestFlushedSchema(helpers[i]);
                if (stored == null) {
                    continue;
                }
                FlushColumnMetadata partitionSchema = SchemaUtil.deserializeColumnMetadata(stored);
                if (merged == null) {
                    merged = new ColumnSchemaTransformer(partitionSchema, partitionSchema.getRoot());
                } else {
                    merged.setToMergeFieldNamesDictionary(partitionSchema.getFieldNamesDictionary());
                    merged.transform(partitionSchema.getRoot());
                }
            }
            if (merged != null) {
                result = new SerializableArrayBackedValueStorage(
                        (ArrayBackedValueStorage) merged.getColMetadata().serializeColumnsMetadata());
            }
        } catch (Throwable th) {
            failure = th;
        } finally {
            for (IIndexDataflowHelper helper : helpers) {
                if (helper != null) {
                    failure = ResourceReleaseUtils.close(helper, failure);
                }
            }
        }
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
        return result;
    }

    private static IValueReference getNewestFlushedSchema(IIndexDataflowHelper helper) throws HyracksDataException {
        if (!(helper.getIndexInstance() instanceof LSMColumnBTree)) {
            return null;
        }
        // Disk components are ordered newest first, and the newest carries the most recent schema
        List<ILSMDiskComponent> diskComponents = ((LSMColumnBTree) helper.getIndexInstance()).getDiskComponents();
        if (diskComponents.isEmpty()) {
            return null;
        }
        return ColumnUtil.getColumnMetadataCopy(diskComponents.get(0).getMetadata());
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
