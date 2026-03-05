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
package org.apache.hyracks.storage.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class NoOpStatsAccumulator implements IComponentStatsAccumulator {

    public static final NoOpStatsAccumulator INSTANCE = new NoOpStatsAccumulator();

    @Override
    public IValueReference serializeComponentStatsMetadata() throws HyracksDataException {
        // since the bulkloader method is present at IIndex level, which is higher than what we need
        // for eg: in case of ITreeIndex, this method is not supported
        throw new UnsupportedOperationException("Not supported by" + this.getClass().getName());
    }

    @Override
    public void account(ITupleReference tuple) {

    }
}
