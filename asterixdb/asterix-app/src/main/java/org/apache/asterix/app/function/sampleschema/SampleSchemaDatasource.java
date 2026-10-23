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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;

public class SampleSchemaDatasource extends FunctionDataSource {

    private final IndexDataflowHelperFactory indexDataflowHelperFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final AlgebricksAbsolutePartitionConstraint constraint;
    private final int[][] partitionsMap;
    private final int numPrimaryKeys;
    private final ARecordType itemType;

    SampleSchemaDatasource(INodeDomain domain, IndexDataflowHelperFactory indexDataflowHelperFactory,
            IBinaryComparatorFactory[] comparatorFactories, AlgebricksAbsolutePartitionConstraint constraint,
            int[][] partitionsMap, int numPrimaryKeys, ARecordType itemType) throws AlgebricksException {
        super(createDataSourceId(SampleSchemaRewriter.SAMPLE_SCHEMA), SampleSchemaRewriter.SAMPLE_SCHEMA, domain);
        this.indexDataflowHelperFactory = indexDataflowHelperFactory;
        this.comparatorFactories = comparatorFactories;
        this.constraint = constraint;
        this.partitionsMap = partitionsMap;
        this.numPrimaryKeys = numPrimaryKeys;
        this.itemType = itemType;
    }

    @Override
    protected AlgebricksAbsolutePartitionConstraint getLocations(IClusterStateManager csm, MetadataProvider md) {
        return constraint;
    }

    @Override
    protected IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations) {
        String[] allLocations = locations.getLocations();
        String chosenNc = allLocations[ThreadLocalRandom.current().nextInt(allLocations.length)];

        List<Integer> mergedStoragePartitions = new ArrayList<>();
        for (int i = 0; i < allLocations.length; i++) {
            if (allLocations[i].equals(chosenNc)) {
                for (int sp : partitionsMap[i]) {
                    mergedStoragePartitions.add(sp);
                }
            }
        }
        int[] storagePartitions = mergedStoragePartitions.stream().mapToInt(Integer::intValue).toArray();
        int[][] singlePartitionsMap = new int[][] { storagePartitions };

        return new SampleSchemaFunction(new AlgebricksAbsolutePartitionConstraint(new String[] { chosenNc }),
                indexDataflowHelperFactory, comparatorFactories, singlePartitionsMap, numPrimaryKeys, itemType);
    }

    @Override
    protected boolean sameFunctionDatasource(FunctionDataSource other) {
        return false;
    }
}
