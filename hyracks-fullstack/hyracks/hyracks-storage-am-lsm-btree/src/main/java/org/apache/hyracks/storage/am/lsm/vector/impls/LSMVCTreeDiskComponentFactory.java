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
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;

/**
 * Factory for creating LSM Vector Clustering Tree disk components.
 * 
 * This factory creates disk components for LSM Vector Clustering Trees,
 * which contain materialized VectorClusteringTree instances that have been
 * flushed from memory to persistent storage.
 */
public class LSMVCTreeDiskComponentFactory implements ILSMDiskComponentFactory {

    protected final TreeIndexFactory<VectorClusteringTree> vcTreeFactory;
    protected final IComponentFilterHelper filterHelper;

    public LSMVCTreeDiskComponentFactory(TreeIndexFactory<VectorClusteringTree> vcTreeFactory,
            IComponentFilterHelper filterHelper) {
        this.vcTreeFactory = vcTreeFactory;
        this.filterHelper = filterHelper;
    }

    @Override
    public LSMVCTreeDiskComponent createComponent(AbstractLSMIndex lsmIndex, LSMComponentFileReferences cfr)
            throws HyracksDataException {
        return new LSMVCTreeDiskComponent(lsmIndex,
                vcTreeFactory.createIndexInstance(cfr.getInsertIndexFileReference()),
                filterHelper == null ? null : filterHelper.createFilter());
    }

    public LSMVCTreeDiskComponent createComponent(LSMVCTreeMemoryComponent flushingComponent, AbstractLSMIndex lsmIndex)
            throws HyracksDataException {
        return new LSMVCTreeDiskComponent(lsmIndex, flushingComponent.getIndex(),
                filterHelper == null ? null : filterHelper.createFilter());
    }
}
