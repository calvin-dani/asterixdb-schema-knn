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

package org.apache.hyracks.storage.am.vector.impls;

import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleCreator;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleCreatorFactory;

/**
 * Standard factory for creating {@link VTreeDataTupleCreator} instances.
 *
 * Carries include field context so the creator knows the exact input tuple layout:
 * Input:  [vector, include_fields(numIncludeFields), pk]
 * Output: <distance, centroidId, pk, include_fields...>
 */
public class VTreeDataTupleCreatorFactory implements IVTreeDataTupleCreatorFactory {

    private static final long serialVersionUID = 1L;

    private final int numIncludeFields;

    public VTreeDataTupleCreatorFactory(int numIncludeFields) {
        this.numIncludeFields = numIncludeFields;
    }

    @Override
    public IVTreeDataTupleCreator createDataTupleCreator() {
        return new VTreeDataTupleCreator(numIncludeFields);
    }
}
