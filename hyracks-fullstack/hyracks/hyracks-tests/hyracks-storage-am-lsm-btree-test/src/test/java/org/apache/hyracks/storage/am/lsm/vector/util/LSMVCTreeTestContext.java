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

package org.apache.hyracks.storage.am.lsm.vector.util;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVCTree;
import org.apache.hyracks.storage.am.lsm.vector.utils.LSMVCTreeUtils;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Test context for LSM Vector Clustering Tree tests.
 * This class provides the infrastructure needed to test LSMVCTree operations,
 * following the established patterns of other LSM tree test contexts.
 */
@SuppressWarnings("rawtypes")
public final class LSMVCTreeTestContext extends AbstractVectorTreeTestContext {

    public LSMVCTreeTestContext(ISerializerDeserializer[] fieldSerdes, LSMVCTree lsmVCTree, int vectorDimensions)
            throws HyracksDataException {
        super(fieldSerdes, lsmVCTree, false, vectorDimensions);
    }

    @Override
    public int getKeyFieldCount() {
        LSMVCTree lsmTree = (LSMVCTree) index;
        return lsmTree.getComparatorFactories().length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        LSMVCTree lsmTree = (LSMVCTree) index;
        return lsmTree.getComparatorFactories();
    }

    /**
     * Create a new LSMVCTreeTestContext with the specified parameters.
     * This factory method follows the pattern established by other LSM test contexts.
     */
    public static LSMVCTreeTestContext create(NCConfig storageConfig, IIOManager ioManager,
            List<IVirtualBufferCache> virtualBufferCaches, FileReference file, IBufferCache diskBufferCache,
            ISerializerDeserializer[] fieldSerdes, int numVectorFields, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory) throws Exception {

        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);

        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            if (fieldSerdes[i] instanceof UTF8StringSerializerDeserializer) {
                cmpFactories[i] = UTF8StringBinaryComparatorFactory.INSTANCE;
            } else {
                // For vector field and other numeric fields, use integer comparator for now
                cmpFactories[i] = IntegerBinaryComparatorFactory.INSTANCE;
            }
        }

        // Create the LSM Vector Clustering Tree
        LSMVCTree lsmVCTree =
                LSMVCTreeUtils.createLSMTree(storageConfig, ioManager, virtualBufferCaches, file, diskBufferCache,
                        typeTraits, cmpFactories, null, -1, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                        pageWriteCallbackFactory, false, numVectorFields, null, null, true, metadataPageManagerFactory);

        LSMVCTreeTestContext testCtx = new LSMVCTreeTestContext(fieldSerdes, lsmVCTree, numVectorFields);
        return testCtx;
    }
}
