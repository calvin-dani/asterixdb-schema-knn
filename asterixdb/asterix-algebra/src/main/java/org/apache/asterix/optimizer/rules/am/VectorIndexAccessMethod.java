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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.annotations.AbstractExpressionAnnotationWithIndexNames;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;

/**
 * Access method for vector indexes.
 *
 * This access method is designed specifically for ORDER BY ANN_DISTANCE() LIMIT k queries.
 * It does NOT handle SELECT-based optimizations (WHERE clauses with ANN_DISTANCE).
 *
 * Example query pattern:
 * <pre>
 * SELECT id, title
 * FROM movie
 * WHERE year > 2000  -- Handled by BTreeAccessMethod
 * ORDER BY ANN_DISTANCE(reviewEmbedding, [1.0, 2.0, ...], "Euclidean")  -- Handled by VectorIndexAccessMethod
 * LIMIT 10
 * </pre>
 *
 * - If vector index EXISTS: Optimizer transforms plan to use UNNEST-MAP(vector_index_search)
 *   → Returns candidate tuples (approximate ANN search)
 *   → ORDER BY ANN_DISTANCE computes distances on candidates only
 *   → Faster but approximate results
 *
 * - If vector index DOES NOT EXIST: Optimizer leaves plan unchanged (DATASOURCE_SCAN)
 *   → Returns ALL tuples (exhaustive scan)
 *   → ORDER BY ANN_DISTANCE computes distances on all tuples
 *   → Falls back to exact KNN search (slower but exact results)
 *
 * The same ORDER BY ANN_DISTANCE operator works in both cases - the optimizer just swaps the data source.
 *
 * This is used by IntroduceTopKAccessMethodRule to validate and analyze ANN_DISTANCE function calls
 * in ORDER BY clauses.
 */
public class VectorIndexAccessMethod implements IAccessMethod {

    public static final VectorIndexAccessMethod INSTANCE = new VectorIndexAccessMethod();

    // ANN_DISTANCE function - used in ORDER BY clauses for top-k queries
    // The second boolean (true) indicates that approximate search may need verification
    private static final List<Pair<FunctionIdentifier, Boolean>> FUNC_IDENTIFIERS =
            Collections.unmodifiableList(Arrays.asList(new Pair<>(BuiltinFunctions.ANN_DISTANCE, true)));

    @Override
    public List<Pair<FunctionIdentifier, Boolean>> getOptimizableFunctions() {
        return FUNC_IDENTIFIERS;
    }

    @Override
    public boolean analyzeFuncExprArgsAndUpdateAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {

        // Validate: ANN_DISTANCE(vectorField, queryVector, distanceMetric)
        // arg0: field reference to vector field (e.g., reviewEmbedding)
        // arg1: query vector (constant array or parameter)
        // arg2: distance metric (string constant: "Euclidean", "Cosine", etc.)

        if (funcExpr.getArguments().size() != 3) {
            return false;
        }

        // Use utility method to validate:
        // - arg0 is a variable/field reference
        // - arg1 is a constant (query vector)
        // This populates analysisCtx with necessary information for index matching
        boolean matches = AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(funcExpr,
                analysisCtx, context, typeEnvironment, false);

        return matches;
    }

    @Override
    public boolean matchIndexType(IndexType indexType) {
        return indexType == IndexType.VECTOR;
    }

    @Override
    public boolean matchAllIndexExprs(Index index) {
        // Vector indexes only have one field, so this is not applicable
        return false;
    }

    @Override
    public boolean matchPrefixIndexExprs(Index index) {
        // Vector indexes don't support prefix matching like composite BTree indexes
        return false;
    }

    @Override
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> afterSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException {
        // NOT IMPLEMENTED: Vector indexes are not used for SELECT-based optimizations
        // If we wanted to support: WHERE ANN_DISTANCE(...) < threshold
        // we would implement this method. For now, only ORDER BY + LIMIT is supported.
        return false;
    }

    @Override
    public ILogicalOperator createIndexSearchPlan(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTheOpRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, IOptimizationContext context,
            LogicalVariable newMissingNullPlaceHolderForLOJ, IAlgebricksConstantValue leftOuterMissingValue)
            throws AlgebricksException {
        // NOT IMPLEMENTED: Used for SELECT-based plan transformation
        // Vector index top-k search plan is created in IntroduceTopKAccessMethodRule
        return null;
    }

    @Override
    public boolean applyJoinPlanTransformation(List<Mutable<ILogicalOperator>> afterJoinRefs,
            Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree leftSubTree,
            OptimizableOperatorSubTree rightSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, boolean isLeftOuterJoin, boolean isLeftOuterJoinWithSpecialGroupBy,
            IAlgebricksConstantValue leftOuterMissingValue) throws AlgebricksException {
        // NOT IMPLEMENTED: Vector indexes are not used for join optimization
        return false;
    }

    /**
     * Creates vector index search plan for ORDER BY ANN_DISTANCE() queries.
     *
     * This method creates a two-stage index search plan:
     * 1. Vector index search: Returns (vector_embedding, pk) for candidate tuples
     * 2. Primary index lookup: Uses PKs to fetch full records
     *
     * Transformation:
     *   LIMIT k → ORDER BY ANN_DISTANCE(vectorField, qvec, metric) → ... → DATASOURCE_SCAN
     * Into:
     *   LIMIT k → ORDER BY ANN_DISTANCE(vectorField, qvec, metric) → PRIMARY_INDEX_UNNEST(pk) → VECTOR_INDEX_UNNEST
     *
     * Data flow:
     * - VECTOR_INDEX_UNNEST: Returns top-k candidates from index (vector_embedding + pk)
     * - PRIMARY_INDEX_UNNEST: Uses PK to fetch full record with all fields
     * - ORDER BY: Computes exact distances on full records
     * - LIMIT: Extracts final top-k results
     *
     * TODO: Add index-only plan optimization (skip primary lookup when only PK is needed)
     *
     * @param limitRef Reference to LIMIT operator
     * @param orderRef Reference to ORDER operator
     * @param annDistanceExpr The ANN_DISTANCE function expression from ORDER BY
     * @param subTree The subtree containing the data source
     * @param chosenIndex The vector index to use
     * @param analysisCtx Analysis context with index information
     * @param context Optimization context
     * @return The transformed plan with vector index search + primary lookup, or null if transformation fails
     */
    public ILogicalOperator createIndexSearchPlan(Mutable<ILogicalOperator> limitRef,
            Mutable<ILogicalOperator> orderRef, AbstractFunctionCallExpression annDistanceExpr,
            OptimizableOperatorSubTree subTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context) throws AlgebricksException {

        System.err.println("=== VectorIndexAccessMethod.createIndexSearchPlan CALLED ===");
        System.err.println("Dataset: " + subTree.getDataset().getDatasetName());
        System.err.println("Vector Index: " + chosenIndex.getIndexName());

        // Get dataset metadata
        Dataset dataset = subTree.getDataset();
        ARecordType recordType = subTree.getRecordType();
        ARecordType metaRecordType = subTree.getMetaRecordType();
        AbstractDataSourceOperator dataSourceOp = (AbstractDataSourceOperator) subTree.getDataSourceRef().getValue();

        // Extract parameters from ANN_DISTANCE(vectorField, queryVector, metric)
        // arg0 = vectorField (variable reference)
        // arg1 = queryVector (constant or variable)
        // arg2 = distanceMetric (string constant)
        ILogicalExpression queryVectorExpr = annDistanceExpr.getArguments().get(1).getValue();
        ILogicalExpression distanceMetricExpr = annDistanceExpr.getArguments().get(2).getValue();

        // Extract k value from LIMIT operator
        LimitOperator limitOp = (LimitOperator) limitRef.getValue();
        ILogicalExpression kValueExpr = limitOp.getMaxObjects().getValue();

        // Create variables to hold query parameters: [query_vector, k_value, distance_metric]
        ArrayList<LogicalVariable> queryVarList = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> queryExprList = new ArrayList<>();

        // Add query vector variable
        LogicalVariable queryVectorVar = context.newVar();
        queryVarList.add(queryVectorVar);
        queryExprList.add(new MutableObject<>(queryVectorExpr.cloneExpression()));

        // Add k value variable
        LogicalVariable kValueVar = context.newVar();
        queryVarList.add(kValueVar);
        queryExprList.add(new MutableObject<>(kValueExpr.cloneExpression()));

        // Add distance metric variable
        LogicalVariable distanceMetricVar = context.newVar();
        queryVarList.add(distanceMetricVar);
        queryExprList.add(new MutableObject<>(distanceMetricExpr.cloneExpression()));

        // Create ASSIGN operator to hold query parameters
        AssignOperator assignSearchKeys = new AssignOperator(queryVarList, queryExprList);
        assignSearchKeys.setSourceLocation(dataSourceOp.getSourceLocation());
        assignSearchKeys.getInputs().add(
                new MutableObject<>(OperatorManipulationUtil.deepCopy(dataSourceOp.getInputs().get(0).getValue())));
        assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
        context.computeAndSetTypeEnvironmentForOperator(assignSearchKeys);

        // Create VectorJobGenParams to pass parameters to Hyracks runtime
        VectorJobGenParams jobGenParams = new VectorJobGenParams(chosenIndex.getIndexName(), IndexType.VECTOR,
                chosenIndex.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName(), false, // retainInput - not needed for simple case
                false // requiresBroadcast
        );
        jobGenParams.setQueryVarList(queryVarList);

        // TODO: Add index-only plan optimization check here (like B+Tree does)
        // For now, always do primary index lookup to get full record
        boolean isIndexOnlyPlan = false;

        // Create UNNEST-MAP operator for vector index search
        // This returns: <vector_field, pk> from the index
        ILogicalOperator secondaryIndexUnnestOp =
                AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType, metaRecordType, chosenIndex,
                        assignSearchKeys, jobGenParams, context, false, false, isIndexOnlyPlan, null);

        // Update type environment to register variables produced by vector index search
        // (e.g., $32 = primary keys). This is critical so downstream operators can use these variables.
        context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);

        // Add primary index lookup to get full record
        // This uses the PKs returned from vector index to fetch complete records
        ILogicalOperator primaryIndexUnnestOp = AccessMethodUtils.createRestOfIndexSearchPlan(null, // afterTopOpRefs - not needed for ORDER BY case
                null, // topOpRef - not needed for ORDER BY case
                null, // conditionRef - no WHERE condition to push down
                null, // assignsBeforeTopOpRef - query params already in assignSearchKeys
                dataSourceOp, dataset, recordType, metaRecordType, secondaryIndexUnnestOp, // inputOp: vector index search results
                context, true, // sortPrimaryKeys
                false, // retainInput
                false, // retainMissing
                false, // requiresBroadcast
                chosenIndex, // secondaryIndex
                analysisCtx, subTree, // indexSubTree
                null, // probeSubTree - not a join
                null, // newMissingPlaceHolderForLOJ
                null, // leftOuterMissingValue
                false // anyRealTypeConvertedToIntegerType
        );

        System.err.println("=== Vector index search plan created successfully ===");
        return primaryIndexUnnestOp;
    }

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr, boolean checkApplicableOnly)
            throws AlgebricksException {
        // Check if this ANN_DISTANCE expression can use the given vector index

        if (index.getIndexType() != IndexType.VECTOR) {
            return false;
        }

        // Get the field name from the ANN_DISTANCE function (arg0)
        List<String> fieldName = optFuncExpr.getFieldName(0);
        if (fieldName == null || fieldName.isEmpty()) {
            return false;
        }

        // Check if the vector index is on this field
        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        List<List<String>> indexKeyFieldNames = vectorDetails.getKeyFieldNames();

        // Vector indexes should have exactly one field
        if (indexKeyFieldNames.size() != 1) {
            return false;
        }

        // Match field name
        return indexKeyFieldNames.get(0).equals(fieldName);
    }

    @Override
    public AbstractExpressionAnnotationWithIndexNames getSecondaryIndexAnnotation(IOptimizableFuncExpr optFuncExpr) {
        // Not used for vector indexes
        return null;
    }

    @Override
    public String getName() {
        return "VECTOR_INDEX";
    }

    @Override
    public boolean acceptsFunction(AbstractFunctionCallExpression functionExpr, Index index, IAType indexedFieldType,
            boolean defaultNull, boolean finalStep) throws AlgebricksException {
        // Check if this function can be optimized with this index type

        // Vector fields should be arrays of numbers
        ATypeTag typeTag = indexedFieldType.getTypeTag();
        return typeTag == ATypeTag.ARRAY || typeTag == ATypeTag.MULTISET;
    }

    @Override
    public int compareTo(IAccessMethod o) {
        return this.getName().compareTo(o.getName());
    }
}