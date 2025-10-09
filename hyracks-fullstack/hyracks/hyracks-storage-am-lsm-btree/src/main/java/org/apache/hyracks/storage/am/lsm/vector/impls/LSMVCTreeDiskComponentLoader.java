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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.MergeOperation;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTreeFlushLoader;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class LSMVCTreeDiskComponentLoader implements ILSMDiskComponentBulkLoader {
    private static final int CHECK_CYCLE = 1000;
    private VectorClusteringTreeFlushLoader flushLoader;
    private final ILSMIOOperation operation;
    private final ILSMDiskComponent diskComponent;
    private final boolean cleanupEmptyComponent;
    private boolean isEmptyComponent = true;
    private boolean cleanedUpArtifacts = false;
    private int tupleCounter = 0;

    public LSMVCTreeDiskComponentLoader(ILSMIOOperation operation, ILSMDiskComponent diskComponent,
            boolean cleanupEmptyComponent) {
        this.operation = operation;
        this.diskComponent = diskComponent;
        this.cleanupEmptyComponent = cleanupEmptyComponent;
    }

    public void setFlushLoader(VectorClusteringTreeFlushLoader flushLoader) {
        this.flushLoader = flushLoader;
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    public void copyPage(ICachedPage page) throws HyracksDataException {
        try {
            flushLoader.copyPage(page);
            checkOperation();
        } catch (Throwable e) {
            operation.setFailure(e);
            cleanupArtifacts();
            throw e;
        }
        if (isEmptyComponent) {
            isEmptyComponent = false;
        }
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            cleanedUpArtifacts = true;
            diskComponent.deactivateAndDestroy();
        }
    }

    @Override
    public void end() throws HyracksDataException {
        if (!cleanedUpArtifacts) {
            flushLoader.end();
            if (isEmptyComponent && cleanupEmptyComponent) {
                cleanupArtifacts();
            }
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        operation.setStatus(ILSMIOOperation.LSMIOOperationStatus.FAILURE);
        HyracksDataException failure = null;
        try {
            flushLoader.abort();
        } catch (HyracksDataException e) {
            failure = e;
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public ILSMIOOperation getOperation() {
        return operation;
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFailed() {
        if (flushLoader.hasFailed()) {
            return true;
        }
        return false;
    }

    @Override
    public Throwable getFailure() {

        if (flushLoader.hasFailed()) {
            return flushLoader.getFailure();
        }

        return null;
    }

    @Override
    public void force() throws HyracksDataException {
        flushLoader.force();
    }

    private void checkOperation() throws HyracksDataException {
        if (operation.getIOOperationType() == ILSMIOOperation.LSMIOOperationType.MERGE
                && ++tupleCounter % CHECK_CYCLE == 0) {
            tupleCounter = 0;
            ((MergeOperation) operation).waitIfPaused();
        }
    }
}
