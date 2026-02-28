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

import static org.apache.asterix.common.exceptions.ErrorCode.TYPE_MISMATCH_FUNCTION;

import java.util.List;

import org.apache.asterix.app.function.FunctionRewriter;
import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ISecondaryIndexOperationsHelper;
import org.apache.asterix.metadata.utils.SecondaryIndexOperationsHelper;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;

public class SampleSchemaRewriter extends FunctionRewriter {

    public static final FunctionIdentifier SAMPLE_SCHEMA =
            FunctionConstants.newAsterix("sample-schema", FunctionIdentifier.VARARGS);
    public static final SampleSchemaRewriter INSTANCE = new SampleSchemaRewriter(SAMPLE_SCHEMA);

    private SampleSchemaRewriter(FunctionIdentifier functionId) {
        super(functionId);
    }

    @Override
    protected FunctionDataSource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression function)
            throws AlgebricksException {

        if (function.getArguments().size() != 3) {
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, SAMPLE_SCHEMA.getName());
        }

        verifyArgs(function.getArguments());
        ILogicalExpression databaseExpr = function.getArguments().get(0).getValue();
        ILogicalExpression scopeExpr = function.getArguments().get(1).getValue();
        ILogicalExpression collectionExpr = function.getArguments().get(2).getValue();

        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        String database = ConstantExpressionUtil.getStringConstant(databaseExpr);
        DataverseName dataverse =
                DataverseName.createSinglePartName(ConstantExpressionUtil.getStringConstant(scopeExpr));
        String datasetName = ConstantExpressionUtil.getStringConstant(collectionExpr);

        Dataset dataset = metadataProvider.findDataset(database, dataverse, datasetName);
        if (dataset == null) {
            throw new CompilationException(ErrorCode.UNKNOWN_DATASET_IN_DATAVERSE, datasetName,
                    MetadataUtil.dataverseName(database, dataverse, metadataProvider.isUsingDatabase()));
        }

        Index sampleIndex = metadataProvider.findSampleIndex(database, dataverse, datasetName);
        if (sampleIndex == null) {
            throw new CompilationException(ErrorCode.NO_SAMPLE_INDEX, datasetName);
        }

        SourceLocation loc = function.getSourceLocation();
        IAType type = metadataProvider.findType(dataset.getItemTypeDatabaseName(), dataset.getItemTypeDataverseName(),
                dataset.getItemTypeName());

        ISecondaryIndexOperationsHelper sampleHelper =
                SecondaryIndexOperationsHelper.createIndexOperationsHelper(dataset, sampleIndex, metadataProvider, loc);
        PartitioningProperties partitioningProperties =
                metadataProvider.getPartitioningProperties(dataset, sampleIndex.getIndexName());
        IndexDataflowHelperFactory indexDataflowHelperFactory =
                new IndexDataflowHelperFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
                        partitioningProperties.getSplitsProvider());
        AlgebricksAbsolutePartitionConstraint partitionConstraint =
                (AlgebricksAbsolutePartitionConstraint) partitioningProperties.getConstraints();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();

        return new SampleSchemaDatasource(context.getComputationNodeDomain(), indexDataflowHelperFactory,
                sampleHelper.getSecondaryComparatorFactories(), partitionConstraint,
                partitioningProperties.getComputeStorageMap(), numPrimaryKeys, (ARecordType) type);
    }

    private void verifyArgs(List<Mutable<ILogicalExpression>> args) throws CompilationException {
        for (int i = 0; i < args.size(); i++) {
            ConstantExpression expr = (ConstantExpression) args.get(i).getValue();
            AsterixConstantValue value = (AsterixConstantValue) expr.getValue();
            ATypeTag type = value.getObject().getType().getTypeTag();
            if (type != ATypeTag.STRING) {
                throw new CompilationException(TYPE_MISMATCH_FUNCTION, SAMPLE_SCHEMA.getName(),
                        ExceptionUtil.indexToPosition(i), ATypeTag.STRING, type);
            }
        }
    }
}
