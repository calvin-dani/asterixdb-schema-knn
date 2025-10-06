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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Factory for creating VectorClusteringTree instances in LSM context.
 * 
 * This factory creates VectorClusteringTree instances that can be used
 * as disk components in LSM Vector Clustering Trees.
 */
public class VectorClusteringTreeFactory extends TreeIndexFactory<VectorClusteringTree> {

    private final ITreeIndexFrameFactory metadataFrameFactory;
    private final ITreeIndexFrameFactory dataFrameFactory;
    private final int vectorDimensions;

    public VectorClusteringTreeFactory(IIOManager ioManager, IBufferCache bufferCache,
            IPageManagerFactory freePageManagerFactory, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, ITreeIndexFrameFactory metadataFrameFactory,
            ITreeIndexFrameFactory dataFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            int vectorDimensions) {
        super(ioManager, bufferCache, freePageManagerFactory, interiorFrameFactory, leafFrameFactory, cmpFactories,
                fieldCount);
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
        this.vectorDimensions = vectorDimensions;
    }

    @Override
    public VectorClusteringTree createIndexInstance(FileReference file) {
        return new VectorClusteringTree(bufferCache, freePageManagerFactory.createPageManager(bufferCache),
                interiorFrameFactory, leafFrameFactory, metadataFrameFactory, dataFrameFactory, cmpFactories,
                vectorDimensions, 0, file);
    }
}
