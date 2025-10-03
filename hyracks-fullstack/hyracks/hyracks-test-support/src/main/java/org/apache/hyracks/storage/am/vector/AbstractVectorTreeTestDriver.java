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

package org.apache.hyracks.storage.am.vector;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.vector.frames.VectorTreeFrameType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public abstract class AbstractVectorTreeTestDriver {
    protected final Logger LOGGER = LogManager.getLogger();

    protected static final int numTuplesToInsert = AccessMethodTestsConfig.BTREE_NUM_TUPLES_TO_INSERT;

    protected abstract AbstractVectorTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes,
            int numKeys, VectorTreeFrameType frameType, boolean filtered) throws Exception;

    protected abstract Random getRandom();

    protected abstract void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, VectorTreeFrameType frameType,
            ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey,
            ITupleReference prefixHighKey) throws Exception;

    protected abstract String getTestOpName();

    protected final VectorTreeFrameType[] frameTypesToTest;

    public AbstractVectorTreeTestDriver(VectorTreeFrameType[] frameTypesToTest) {
        this.frameTypesToTest = frameTypesToTest;
    }

    @Test
    public void oneVectorKeyAndValue() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorTree " + getTestOpName() + " Test With One Vector Key And Value.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        for (VectorTreeFrameType frameType : frameTypesToTest) {
            runTest(fieldSerdes, 1, frameType, null, null, null, null);
        }
    }

    @Test
    public void twoVectorKeys() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorTree " + getTestOpName() + " Test With Two Vector Keys.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, FloatArraySerializerDeserializer.INSTANCE };

        for (VectorTreeFrameType frameType : frameTypesToTest) {
            runTest(fieldSerdes, 2, frameType, null, null, null, null);
        }
    }

    @Test
    public void twoVectorKeysAndValues() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorTree " + getTestOpName() + " Test With Two Vector Keys And Values.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { FloatArraySerializerDeserializer.INSTANCE, FloatArraySerializerDeserializer.INSTANCE,
                        new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };

        for (VectorTreeFrameType frameType : frameTypesToTest) {
            runTest(fieldSerdes, 2, frameType, null, null, null, null);
        }
    }

    @Test
    public void oneStringKeyAndValue() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorTree " + getTestOpName() + " Test With One String Key And Vector Value.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), FloatArraySerializerDeserializer.INSTANCE };

        for (VectorTreeFrameType frameType : frameTypesToTest) {
            runTest(fieldSerdes, 1, frameType, null, null, null, null);
        }
    }

    @Test
    public void twoStringKeys() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorTree " + getTestOpName() + " Test With Two String Keys And Vector Value.");
        }

        ISerializerDeserializer[] fieldSerdes = { new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), FloatArraySerializerDeserializer.INSTANCE };

        for (VectorTreeFrameType frameType : frameTypesToTest) {
            runTest(fieldSerdes, 2, frameType, null, null, null, null);
        }
    }

    @Test
    public void twoStringKeysAndValues() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("VectorTree " + getTestOpName() + " Test With Two String Keys And Mixed Values.");
        }

        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                        FloatArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        for (VectorTreeFrameType frameType : frameTypesToTest) {
            runTest(fieldSerdes, 2, frameType, null, null, null, null);
        }
    }
}
