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

package org.apache.hyracks.storage.am.vector.util;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.SerdeUtils;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeUtils;
import org.apache.hyracks.storage.am.vector.frames.VectorTreeFrameType;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Test context for VectorClusteringTree tests, using actual VectorClusteringTree implementation.
 */
@SuppressWarnings("rawtypes")
public class VectorTreeTestContext extends AbstractVectorTreeTestContext {

    public VectorTreeTestContext(ISerializerDeserializer[] fieldSerdes, VectorClusteringTree index, boolean filtered,
            int vectorDimensions) throws HyracksDataException {
        super(fieldSerdes, index, filtered, vectorDimensions);
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        // Create comparator factories for vector tree fields
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            if (fieldSerdes[i] instanceof UTF8StringSerializerDeserializer) {
                cmpFactories[i] = UTF8StringBinaryComparatorFactory.INSTANCE;
            } else {
                // For vector field and other numeric fields, use integer comparator for now
                cmpFactories[i] = IntegerBinaryComparatorFactory.INSTANCE;
            }
        }
        return cmpFactories;
    }

    @Override
    public int getKeyFieldCount() {
        // For vectors, typically 1 key field (the vector itself)
        return 1;
    }

    public static VectorTreeTestContext create(IIOManager ioManager, IBufferCache virtualBufferCache,
            FileReference fileRef, IBufferCache diskBufferCache, ISerializerDeserializer[] fieldSerdes, int numKeys,
            VectorTreeFrameType frameType, int vectorDimensions) throws Exception {

        // Create type traits for the fields
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);

        // Create comparator factories
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[fieldSerdes.length];
        for (int i = 0; i < fieldSerdes.length; i++) {
            if (fieldSerdes[i] instanceof UTF8StringSerializerDeserializer) {
                cmpFactories[i] = UTF8StringBinaryComparatorFactory.INSTANCE;
            } else {
                // For vector field and other numeric fields, use integer comparator for now
                cmpFactories[i] = IntegerBinaryComparatorFactory.INSTANCE;
            }
        }

        // Create page manager using AppendOnlyLinkedMetadataPageManagerFactory
        AppendOnlyLinkedMetadataPageManagerFactory pageManagerFactory =
                AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE;
        IPageManager pageManager = pageManagerFactory.createPageManager(diskBufferCache);

        // Create VectorClusteringTree using VectorTreeUtils
        VectorClusteringTree index = VectorTreeUtils.createVectorClusteringTree(diskBufferCache, typeTraits,
                cmpFactories, vectorDimensions, fileRef, pageManager);

        return new VectorTreeTestContext(fieldSerdes, index, false, vectorDimensions);
    }
}
