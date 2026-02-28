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

package org.apache.asterix.app.function.sampleschema;

import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;

public class SampleSchemaFunction extends AbstractDatasourceFunction {

    private static final long serialVersionUID = 2L;
    private final IndexDataflowHelperFactory indexDataflowHelperFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final int[][] partitionsMap;
    private final int numPrimaryKeys;
    private final ARecordType itemType;

    SampleSchemaFunction(AlgebricksAbsolutePartitionConstraint locations,
            IndexDataflowHelperFactory indexDataflowHelperFactory, IBinaryComparatorFactory[] comparatorFactories,
            int[][] partitionsMap, int numPrimaryKeys, ARecordType itemType) {
        super(locations);
        this.indexDataflowHelperFactory = indexDataflowHelperFactory;
        this.comparatorFactories = comparatorFactories;
        this.partitionsMap = partitionsMap;
        this.numPrimaryKeys = numPrimaryKeys;
        this.itemType = itemType;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        int[] partitions = partitionsMap[partition];
        final IIndexDataflowHelper[] indexDataflowHelpers = new IIndexDataflowHelper[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            indexDataflowHelpers[i] = indexDataflowHelperFactory.create(serviceCtx, partitions[i]);
        }
        return new SampleSchemaReader(indexDataflowHelpers, comparatorFactories, numPrimaryKeys, itemType);
    }
}
