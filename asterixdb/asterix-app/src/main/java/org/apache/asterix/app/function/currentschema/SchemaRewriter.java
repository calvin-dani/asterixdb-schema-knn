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

package org.apache.asterix.app.function.currentschema;

import static org.apache.asterix.common.exceptions.ErrorCode.TYPE_MISMATCH_FUNCTION;

import java.util.List;

import org.apache.asterix.app.function.FunctionRewriter;
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This function takes a collection's fully qualified name (database.scope.collection) and returns the collection's size
 */

public class SchemaRewriter extends FunctionRewriter {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final FunctionIdentifier INDEX_SCHEMA =
            FunctionConstants.newAsterix("current-schema", FunctionIdentifier.VARARGS);
    public static final SchemaRewriter INSTANCE = new SchemaRewriter(INDEX_SCHEMA);

    private SchemaRewriter(FunctionIdentifier functionId) {
        super(functionId);

    }

    @Override
    protected FunctionDataSource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression function)
            throws AlgebricksException {

        if (function.getArguments().size() != 4) {
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, INDEX_SCHEMA.getName());
        }

        verifyArgs(function.getArguments());
        ILogicalExpression databaseExpr = function.getArguments().get(0).getValue();
        ILogicalExpression scopeExpr = function.getArguments().get(1).getValue();
        ILogicalExpression collectionExpr = function.getArguments().get(2).getValue();
        ILogicalExpression toFlushExpr = function.getArguments().get(3).getValue();
        ILogicalExpression indexExpr = null;
        if (function.getArguments().size() == 4) {
            indexExpr = function.getArguments().get(3).getValue();
        }

        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        String database = ConstantExpressionUtil.getStringConstant(databaseExpr);
        DataverseName dataverse =
                DataverseName.createSinglePartName(ConstantExpressionUtil.getStringConstant(scopeExpr));
        String collection = ConstantExpressionUtil.getStringConstant(collectionExpr);
        Dataset dataset = metadataProvider.findDataset(database, dataverse, collection);
        boolean toFlush = Boolean.TRUE.equals(ConstantExpressionUtil.getBooleanConstant(toFlushExpr));
        if (dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.ROW) {
            throw new CompilationException(ErrorCode.CONFIGURATION_PARAMETER_INVALID_TYPE, dataset.getDatasetName(),
                    "DATASET should be in storage format : COLUMNAR");
        }
        Index primaryIndex =
                MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataset.getDatabaseName(),
                        dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName());
        PartitioningProperties partitioningProperties =
                metadataProvider.getPartitioningProperties(dataset, primaryIndex.getIndexName());
        IndexDataflowHelperFactory indexDataflowHelperFactory =
                new IndexDataflowHelperFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
                        partitioningProperties.getSplitsProvider());
        int[][] partitionMap = partitioningProperties.getComputeStorageMap();
        String index = indexExpr != null ? ConstantExpressionUtil.getStringConstant(indexExpr) : null;
        AlgebricksAbsolutePartitionConstraint secondaryPartitionConstraint =
                (AlgebricksAbsolutePartitionConstraint) partitioningProperties.getConstraints();

        return new SchemaDatasource(context.getComputationNodeDomain(), database, dataverse, collection, index,
                partitioningProperties.getSplitsProvider(), indexDataflowHelperFactory, partitionMap,
                secondaryPartitionConstraint, toFlush);
    }

    private void verifyArgs(List<Mutable<ILogicalExpression>> args) throws CompilationException {
        for (int i = 0; i < args.size(); i++) {
            ConstantExpression expr = (ConstantExpression) args.get(i).getValue();
            AsterixConstantValue value = (AsterixConstantValue) expr.getValue();
            ATypeTag type = value.getObject().getType().getTypeTag();
            if (type != ATypeTag.STRING && i != 3) {
                throw new CompilationException(TYPE_MISMATCH_FUNCTION, INDEX_SCHEMA.getName(),
                        ExceptionUtil.indexToPosition(i), ATypeTag.STRING, type);
            } else {
                if (i == 3 && type != ATypeTag.BOOLEAN) {
                    throw new CompilationException(TYPE_MISMATCH_FUNCTION, INDEX_SCHEMA.getName(),
                            ExceptionUtil.indexToPosition(i), ATypeTag.BOOLEAN, type);
                }
            }
        }
    }
}
