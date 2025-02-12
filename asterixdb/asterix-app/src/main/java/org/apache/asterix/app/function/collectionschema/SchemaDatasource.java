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

package org.apache.asterix.app.function.collectionschema;

import java.util.Objects;

import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.api.IDatasourceFunction;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;

public class SchemaDatasource extends FunctionDataSource {

    private static final DataSourceId STORAGE_SIZE_DATASOURCE_ID = new DataSourceId(
            SchemaRewriter.INDEX_SCHEMA.getDatabase(), FunctionSignature.getDataverseName(SchemaRewriter.INDEX_SCHEMA),
            SchemaRewriter.INDEX_SCHEMA.getName());
    private final String database;
    private final DataverseName dataverse;
    private final String collection;
    private final String index;
    private final IFileSplitProvider splitProvider;
    private final IndexDataflowHelperFactory indexDataflowHelperFactory;
    private final int[][] partition;
    private final AlgebricksAbsolutePartitionConstraint constraint;

    SchemaDatasource(INodeDomain domain, String database, DataverseName dataverse, String collection, String index,
            IFileSplitProvider splitProvider, IndexDataflowHelperFactory indexDataflowHelperFactory, int[][] partition,
            AlgebricksAbsolutePartitionConstraint constraint) throws AlgebricksException {
        super(STORAGE_SIZE_DATASOURCE_ID, SchemaRewriter.INDEX_SCHEMA, domain);
        System.out.println("FOLLOW THE LETTERS : J2");
        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
        this.splitProvider = splitProvider;
        this.indexDataflowHelperFactory = indexDataflowHelperFactory;
        this.partition = partition;
        this.constraint = constraint;
    }

    public String getDatabase() {
        return database;
    }

    public DataverseName getDataverse() {
        return dataverse;
    }

    public String getCollection() {
        return collection;
    }

    public String getIndex() {
        return index;
    }

    @Override
    protected AlgebricksAbsolutePartitionConstraint getLocations(IClusterStateManager csm, MetadataProvider md) {
        return constraint;
    }

    @Override
    protected IDatasourceFunction createFunction(MetadataProvider metadataProvider,
            AlgebricksAbsolutePartitionConstraint locations) {
        System.out.println("FOLLOW THE LETTERS : K");
        return new SchemaFunction(locations, database, dataverse, collection, index, splitProvider,
                indexDataflowHelperFactory, partition);
    }

    @Override
    protected boolean sameFunctionDatasource(FunctionDataSource other) {
        System.out.println("FOLLOW THE LETTERS : L");
        if (!Objects.equals(this.functionId, other.getFunctionId())) {
            return false;
        }
        SchemaDatasource that = (SchemaDatasource) other;
        return Objects.equals(this.database, that.getDatabase()) && Objects.equals(this.dataverse, that.getDataverse())
                && Objects.equals(this.collection, that.getCollection()) && Objects.equals(this.index, that.getIndex());
    }
}
