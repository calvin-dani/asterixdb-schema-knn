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
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;

public class CollectionSchemaDatasource extends FunctionDataSource {

    private static final DataSourceId STORAGE_SIZE_DATASOURCE_ID =
            new DataSourceId(CollectionSchemaRewriter.COLLECTION_SCHEMA.getDatabase(),
                    FunctionSignature.getDataverseName(CollectionSchemaRewriter.COLLECTION_SCHEMA),
                    CollectionSchemaRewriter.COLLECTION_SCHEMA.getName());
    private final String database;
    private final DataverseName dataverse;
    private final String collection;
    private final String index;
    private final IAType type;

    private final int[][] partition;
    private final AlgebricksAbsolutePartitionConstraint constraint;

    CollectionSchemaDatasource(INodeDomain domain, String database, DataverseName dataverse, String collection,
            String index, IAType type, int[][] partition, AlgebricksAbsolutePartitionConstraint constraint)
            throws AlgebricksException {
        super(STORAGE_SIZE_DATASOURCE_ID, CollectionSchemaRewriter.COLLECTION_SCHEMA, domain);
        System.out.println("FOLLOW THE LETTERS : J2");
        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
        this.type = type;
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

        return new CollectionSchemaFunction(
                AlgebricksAbsolutePartitionConstraint.randomLocation(locations.getLocations()), database, dataverse,
                collection, index, partition, type);
    }

    @Override
    protected boolean sameFunctionDatasource(FunctionDataSource other) {

        if (!Objects.equals(this.functionId, other.getFunctionId())) {
            return false;
        }
        CollectionSchemaDatasource that = (CollectionSchemaDatasource) other;
        return Objects.equals(this.database, that.getDatabase()) && Objects.equals(this.dataverse, that.getDataverse())
                && Objects.equals(this.collection, that.getCollection()) && Objects.equals(this.index, that.getIndex());
    }
}
