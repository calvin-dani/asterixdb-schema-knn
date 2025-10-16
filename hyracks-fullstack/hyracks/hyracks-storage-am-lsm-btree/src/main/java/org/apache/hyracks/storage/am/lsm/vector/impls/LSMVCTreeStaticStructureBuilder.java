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
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;

import java.util.List;

public class LSMVCTreeStaticStructureBuilder{
    private final LSMVCTree lsmvcTree;
    private final IIndexBulkLoader staticStructureBuilder;
    private final ILSMIndexOperationContext opCtx;
    private boolean failed = false;

    public LSMVCTreeStaticStructureBuilder(NCConfig storageConfig,  LSMVCTree lsmvcTree,  ILSMIndexOperationContext opCtx,
            int numLevels, List<Integer> clustersPerLevel, List<List<Integer>> centroidsPerCluster, int maxEntriesPerPage,
            NoOpPageWriteCallback instance)
            throws HyracksDataException {
        this.lsmvcTree = lsmvcTree;
        this.opCtx = opCtx;
        this.staticStructureBuilder = ((LSMVCTreeDiskComponent)opCtx.getIoOperation().getNewComponent()).
                createStaticStructureBuilder(storageConfig, numLevels, clustersPerLevel, centroidsPerCluster,
                maxEntriesPerPage, instance);
    }

    public ILSMDiskComponent getComponent() {
        return opCtx.getIoOperation().getNewComponent();
    }

    public void add(ITupleReference tuple) throws HyracksDataException {
        try {
            staticStructureBuilder.add(tuple);
        } catch (Throwable th) {
            fail(th);
            throw th;
        }
    }

    public void end() throws HyracksDataException {
        try {
            presistComponentToDisk();
        } catch (Throwable th) { // NOSONAR must cleanup in case of any failure
            fail(th);
            throw th;
        } finally {
            lsmvcTree.getIOOperationCallback().completed(opCtx.getIoOperation());
        }
    }

    public void abort() throws HyracksDataException {
        opCtx.getIoOperation().setStatus(ILSMIOOperation.LSMIOOperationStatus.FAILURE);
        fail(null);
        try {
            try {
                staticStructureBuilder.abort();
            } finally {
                lsmvcTree.getIOOperationCallback().afterFinalize(opCtx.getIoOperation());
            }
        } finally {
            lsmvcTree.getIOOperationCallback().completed(opCtx.getIoOperation());
        }
    }

    public boolean hasFailed() {
        return opCtx.getIoOperation().hasFailed();
    }

    public Throwable getFailure() {
        return opCtx.getIoOperation().getFailure();
    }

    private void presistComponentToDisk() throws HyracksDataException {
        try {
            lsmvcTree.getIOOperationCallback().afterOperation(opCtx.getIoOperation());
            staticStructureBuilder.end();
        } catch (Throwable th) { // NOSONAR Must not call afterFinalize without setting failure
            fail(th);
            staticStructureBuilder.abort();
            throw th;
        } finally {
            lsmvcTree.getIOOperationCallback().afterFinalize(opCtx.getIoOperation());
        }
        if (opCtx.getIoOperation().getStatus() == ILSMIOOperation.LSMIOOperationStatus.SUCCESS
                && opCtx.getIoOperation().getNewComponent().getComponentSize() > 0) {
            lsmvcTree.getHarness().addBulkLoadedComponent(opCtx.getIoOperation());
        }
    }

    private void fail(Throwable th) {
        if (!failed) {
            failed = true;
            final ILSMIOOperation loadOp = opCtx.getIoOperation();
            loadOp.setFailure(th);
            loadOp.cleanup(lsmvcTree.getBufferCache());
        }
    }

    public void force() throws HyracksDataException {
        staticStructureBuilder.force();
    }

}
