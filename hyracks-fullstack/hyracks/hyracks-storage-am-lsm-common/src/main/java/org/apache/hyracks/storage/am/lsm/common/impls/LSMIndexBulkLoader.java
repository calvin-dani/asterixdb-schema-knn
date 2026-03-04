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
package org.apache.hyracks.storage.am.lsm.common.impls;

import static org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata.MAX_LEAF_TUPLE_COUNT_KEY;
import static org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata.THETA_INSERT_DELETE_SKETCH_KEY;

import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.ISampler;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class LSMIndexBulkLoader implements IChainedComponentBulkLoader {
    protected final IComponentMetadata componentMetadata;
    protected final IIndexBulkLoader bulkLoader;
    protected final ISampler sampler;

    public LSMIndexBulkLoader(IIndexBulkLoader bulkLoader, IComponentMetadata componentMetadata, ISampler sampler) {
        this.bulkLoader = bulkLoader;
        this.componentMetadata = componentMetadata;
        this.sampler = sampler;
    }

    @Override
    public ITupleReference delete(ITupleReference tuple) throws HyracksDataException {
        ILSMTreeTupleWriter tupleWriter =
                (ILSMTreeTupleWriter) ((AbstractTreeIndexBulkLoader) bulkLoader).getLeafFrame().getTupleWriter();
        tupleWriter.setAntimatter(true);
        try {
            bulkLoader.add(tuple);
        } finally {
            tupleWriter.setAntimatter(false);
        }
        return tuple;
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        //Noop
    }

    @Override
    public ITupleReference add(ITupleReference tuple) throws HyracksDataException {
        bulkLoader.add(tuple);
        return tuple;
    }

    @Override
    public void end() throws HyracksDataException {
        try {
            IValueReference reference = sampler.serializeSamplingMetadata();
            if (reference != null) {
                componentMetadata.put(THETA_INSERT_DELETE_SKETCH_KEY, reference);
            }
            // Store the max leaf tuple count for Olken rejection sampling correction
            if (bulkLoader instanceof org.apache.hyracks.storage.am.btree.impls.BTreeNSMBulkLoader) {
                int maxLeafTupleCount = ((org.apache.hyracks.storage.am.btree.impls.BTreeNSMBulkLoader) bulkLoader)
                        .getMaxLeafTupleCountOfLastPage();
                if (maxLeafTupleCount > 0) {
                    ArrayBackedValueStorage maxTupleCountStorage = new ArrayBackedValueStorage(Integer.BYTES);
                    maxTupleCountStorage.getDataOutput().writeInt(maxLeafTupleCount);
                    componentMetadata.put(MAX_LEAF_TUPLE_COUNT_KEY, maxTupleCountStorage);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        bulkLoader.end();
    }

    @Override
    public void abort() throws HyracksDataException {
        bulkLoader.abort();
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFailed() {
        return bulkLoader.hasFailed();
    }

    @Override
    public Throwable getFailure() {
        return bulkLoader.getFailure();
    }

    @Override
    public void force() throws HyracksDataException {
        bulkLoader.force();
    }
}
