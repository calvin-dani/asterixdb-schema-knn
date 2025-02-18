///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.asterix.app.function.collectionschema;
//
//import org.apache.asterix.app.function.FunctionRewriter;
//import org.apache.asterix.common.cluster.PartitioningProperties;
//import org.apache.asterix.common.config.DatasetConfig;
//import org.apache.asterix.common.exceptions.CompilationException;
//import org.apache.asterix.common.exceptions.ErrorCode;
//import org.apache.asterix.common.functions.FunctionConstants;
//import org.apache.asterix.common.metadata.DataverseName;
//import org.apache.asterix.metadata.MetadataManager;
//import org.apache.asterix.metadata.declared.FunctionDataSource;
//import org.apache.asterix.metadata.declared.MetadataProvider;
//import org.apache.asterix.metadata.entities.Dataset;
//import org.apache.asterix.metadata.entities.Index;
//import org.apache.asterix.om.api.IRowWriteMultiPageOp;
//import org.apache.asterix.om.constants.AsterixConstantValue;
//import org.apache.asterix.om.exceptions.ExceptionUtil;
//import org.apache.asterix.om.types.ARecordType;
//import org.apache.asterix.om.types.ATypeTag;
//import org.apache.asterix.om.types.IAType;
//import org.apache.asterix.om.utils.ConstantExpressionUtil;
//import org.apache.asterix.runtime.schemainferrence.ObjectRowSchemaNode;
//import org.apache.asterix.runtime.schemainferrence.RowMetadata;
//import org.apache.asterix.runtime.schemainferrence.RowSchemaTransformer;
//import org.apache.asterix.runtime.schemainferrence.RowTransformer;
//import org.apache.commons.lang3.mutable.Mutable;
//import org.apache.commons.lang3.mutable.MutableObject;
//import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
//import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
//import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
//import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
//import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
//import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
//import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
//import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
//
//import java.util.List;
//
//import static CollectionSchemaRewriter.METADATA_SCHEMA;
//import static org.apache.asterix.common.exceptions.ErrorCode.TYPE_MISMATCH_FUNCTION;
//
///**
// * This function takes a collection's fully qualified name (database.scope.collection) and returns the collection's size
// */
//
//public class CollectionSchemaRewriter extends FunctionRewriter {
//
//    public static final FunctionIdentifier METADATA_SCHEMA =
//            FunctionConstants.newAsterix("collection-schema", FunctionIdentifier.VARARGS);
//    public static final CollectionSchemaRewriter INSTANCE = new CollectionSchemaRewriter(METADATA_SCHEMA);
//
//    private CollectionSchemaRewriter(FunctionIdentifier functionId) {
//        super(functionId);
//        
//
//    }
//
//    @Override
//    protected FunctionDataSource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression function)
//            throws AlgebricksException {
//        
//        //TODO : CALVIN DANI change to 3 args only
//        if (function.getArguments().size() != 3) {
//            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, METADATA_SCHEMA.getName());
//        }
//
//        verifyArgs(function.getArguments());
//        ILogicalExpression databaseExpr = function.getArguments().get(0).getValue();
//        ILogicalExpression scopeExpr = function.getArguments().get(1).getValue();
//        ILogicalExpression collectionExpr = function.getArguments().get(2).getValue();
//        ILogicalExpression indexExpr = null;
//        if (function.getArguments().size() == 4) {
//            indexExpr = function.getArguments().get(3).getValue();
//        }
//
//        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
//        String database = ConstantExpressionUtil.getStringConstant(databaseExpr);
//        DataverseName dataverse =
//                DataverseName.createSinglePartName(ConstantExpressionUtil.getStringConstant(scopeExpr));
//        String collection = ConstantExpressionUtil.getStringConstant(collectionExpr);
//        Dataset ds = metadataProvider.findDataset(database, dataverse, collection);
////        if (dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.ROW) {
////            throw new CompilationException(ErrorCode.CONFIGURATION_PARAMETER_INVALID_TYPE, dataset.getDatasetName(), "DATASET should be in storage format : COLUMNAR");
////        }
////        Index primaryIndex =
////                MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataset.getDatabaseName(),
////                        dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName());
////        PartitioningProperties partitioningProperties =
////                metadataProvider.getPartitioningProperties(ds, primaryIndex.getIndexName());
//
//
//        IAType type = metadataProvider.findType(ds.getItemTypeDatabaseName(), ds.getItemTypeDataverseName(),
//                ds.getItemTypeName());
//
//
////        IndexDataflowHelperFactory indexDataflowHelperFactory =
////                new IndexDataflowHelperFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
////                        partitioningProperties.getSplitsProvider());
////        int[][] partitionMap = partitioningProperties.getComputeStorageMap();
//            String index = indexExpr != null ? ConstantExpressionUtil.getStringConstant(indexExpr) : null;
////        AlgebricksAbsolutePartitionConstraint secondaryPartitionConstraint =
////                (AlgebricksAbsolutePartitionConstraint) partitioningProperties.getConstraints();
//            return new CollectionSchemaDatasource(context.getComputationNodeDomain(), IAType type);
//        }
//    }
//
//    private void verifyArgs(List<Mutable<ILogicalExpression>> args) throws CompilationException {
//        for (int i = 0; i < args.size(); i++) {
//            ConstantExpression expr = (ConstantExpression) args.get(i).getValue();
//            AsterixConstantValue value = (AsterixConstantValue) expr.getValue();
//            ATypeTag type = value.getObject().getType().getTypeTag();
//            if (type != ATypeTag.STRING) {
//                throw new CompilationException(TYPE_MISMATCH_FUNCTION, METADATA_SCHEMA.getName(),
//                        ExceptionUtil.indexToPosition(i), ATypeTag.STRING, type);
//            }
//        }
//    }
//}

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

import static org.apache.asterix.common.exceptions.ErrorCode.TYPE_MISMATCH_FUNCTION;

import java.util.List;

import org.apache.asterix.app.function.FunctionRewriter;
import org.apache.asterix.common.cluster.PartitioningProperties;
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
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;

/**
 * This function takes a collection's fully qualified name (database.scope.collection) and returns the collection's size
 */

public class CollectionSchemaRewriter extends FunctionRewriter {

    public static final FunctionIdentifier COLLECTION_SCHEMA =
            FunctionConstants.newAsterix("collection-schema", FunctionIdentifier.VARARGS);
    public static final CollectionSchemaRewriter INSTANCE = new CollectionSchemaRewriter(COLLECTION_SCHEMA);

    private CollectionSchemaRewriter(FunctionIdentifier functionId) {
        super(functionId);

    }

    @Override
    protected FunctionDataSource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression function)
            throws AlgebricksException {

        //TODO : CALVIN DANI change to 3 args only
        if (function.getArguments().size() != 3) {
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, COLLECTION_SCHEMA.getName());
        }

        verifyArgs(function.getArguments());
        ILogicalExpression databaseExpr = function.getArguments().get(0).getValue();
        ILogicalExpression scopeExpr = function.getArguments().get(1).getValue();
        ILogicalExpression collectionExpr = function.getArguments().get(2).getValue();
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

        IAType type = metadataProvider.findType(dataset.getItemTypeDatabaseName(), dataset.getItemTypeDataverseName(),
                dataset.getItemTypeName());
        //        if (dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.ROW) {
        //            throw new CompilationException(ErrorCode.CONFIGURATION_PARAMETER_INVALID_TYPE, dataset.getDatasetName(), "DATASET should be in storage format : COLUMNAR");
        //        }
        //        Index primaryIndex =
        //                MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataset.getDatabaseName(),
        //                        dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName());
        //        PartitioningProperties partitioningProperties =
        //                metadataProvider.getPartitioningProperties(dataset, primaryIndex.getIndexName());
        //        IndexDataflowHelperFactory indexDataflowHelperFactory =
        //                new IndexDataflowHelperFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
        //                        partitioningProperties.getSplitsProvider());
        //        int[][] partitionMap = partitioningProperties.getComputeStorageMap();
        String index = indexExpr != null ? ConstantExpressionUtil.getStringConstant(indexExpr) : null;
        //        AlgebricksAbsolutePartitionConstraint secondaryPartitionConstraint =
        //                (AlgebricksAbsolutePartitionConstraint) partitioningProperties.getConstraints();
        Index primaryIndex =
                MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(), dataset.getDatabaseName(),
                        dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName());

        PartitioningProperties partitioningProperties =
                metadataProvider.getPartitioningProperties(dataset, primaryIndex.getIndexName());
        IndexDataflowHelperFactory indexDataflowHelperFactory =
                new IndexDataflowHelperFactory(metadataProvider.getStorageComponentProvider().getStorageManager(),
                        partitioningProperties.getSplitsProvider());
        int[][] partitionMap = partitioningProperties.getComputeStorageMap();
        //        String index = indexExpr != null ? ConstantExpressionUtil.getStringConstant(indexExpr) : null;
        AlgebricksAbsolutePartitionConstraint secondaryPartitionConstraint =
                (AlgebricksAbsolutePartitionConstraint) partitioningProperties.getConstraints();
        return new CollectionSchemaDatasource(context.getComputationNodeDomain(), database, dataverse, collection,
                index, type, partitionMap, secondaryPartitionConstraint);
    }

    private void verifyArgs(List<Mutable<ILogicalExpression>> args) throws CompilationException {
        for (int i = 0; i < args.size(); i++) {
            ConstantExpression expr = (ConstantExpression) args.get(i).getValue();
            AsterixConstantValue value = (AsterixConstantValue) expr.getValue();
            ATypeTag type = value.getObject().getType().getTypeTag();
            if (type != ATypeTag.STRING) {
                throw new CompilationException(TYPE_MISMATCH_FUNCTION, COLLECTION_SCHEMA.getName(),
                        ExceptionUtil.indexToPosition(i), ATypeTag.STRING, type);
            }
        }
    }
}
