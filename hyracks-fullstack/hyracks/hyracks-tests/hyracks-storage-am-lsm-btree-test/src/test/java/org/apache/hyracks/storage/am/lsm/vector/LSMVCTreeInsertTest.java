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

package org.apache.hyracks.storage.am.lsm.vector;

import static org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestHarness.LEAF_FRAMES_TO_TEST;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestHarness;
import org.apache.hyracks.storage.am.vector.AbstractVectorClusteringTreeInsertTest;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.frames.VectorTreeFrameType;

@SuppressWarnings("rawtypes")
public class LSMVCTreeInsertTest extends AbstractVectorClusteringTreeInsertTest {

    private final LSMVCTreeTestHarness harness = new LSMVCTreeTestHarness();

    public LSMVCTreeInsertTest() {
        super(LEAF_FRAMES_TO_TEST);
    }

    @Override
    protected AbstractVectorTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            VectorTreeFrameType frameType, boolean filtered) throws Exception {
        return null;
    }

    @Override
    protected Random getRandom() {
        return null;
    }
}
