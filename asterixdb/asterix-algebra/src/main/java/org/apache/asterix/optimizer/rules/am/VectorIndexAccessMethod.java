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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.annotations.AbstractExpressionAnnotationWithIndexNames;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
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
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.rewriter.rules.InlineVariablesRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final Logger LOGGER = LogManager.getLogger();

    public static final VectorIndexAccessMethod INSTANCE = new VectorIndexAccessMethod();

    // Optimizable functions for vector index:
    // - ANN_DISTANCE: approximate nearest neighbor search (probes nprobe clusters)
    // - VECTOR_DISTANCE_ARRAY: index-driven KNN when 4th arg is true (scans all clusters)
    private static final List<Pair<FunctionIdentifier, Boolean>> FUNC_IDENTIFIERS =
            Collections.unmodifiableList(Arrays.asList(new Pair<>(BuiltinFunctions.ANN_DISTANCE, true),
                    new Pair<>(BuiltinFunctions.VECTOR_DISTANCE_ARRAY, true)));

    @Override
    public List<Pair<FunctionIdentifier, Boolean>> getOptimizableFunctions() {
        return FUNC_IDENTIFIERS;
    }

    @Override
    public boolean analyzeFuncExprArgsAndUpdateAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {

        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        if (fid.equals(BuiltinFunctions.VECTOR_DISTANCE_ARRAY)) {
            // vector_distance(vectorField, queryVector, metric, true) - need >= 4 args for index hint
            if (funcExpr.getArguments().size() < 4) {
                return false;
            }
        } else {
            // ann_distance(vectorField, queryVector, metric [, min_probe_fraction, k_multiplier])
            if (funcExpr.getArguments().size() < 3 || funcExpr.getArguments().size() > 5) {
                return false;
            }
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
        return indexType == IndexType.VTREE;
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
            IOptimizationContext context, SelectOperator selectOp) throws AlgebricksException {

        LOGGER.trace("createIndexSearchPlan called");
        LOGGER.trace("Dataset: {}", subTree.getDataset().getDatasetName());
        LOGGER.trace("Vector Index: {}", chosenIndex.getIndexName());
        LOGGER.trace("SelectOp present: {}", selectOp != null);

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

        // Determine if this is VECTOR_DISTANCE_ARRAY (index-driven KNN) vs ANN_DISTANCE
        boolean isVectorDistance =
                annDistanceExpr.getFunctionIdentifier().equals(BuiltinFunctions.VECTOR_DISTANCE_ARRAY);

        // Add min_probe_fraction variable (arg 3 for ANN_DISTANCE, default 0.1)
        // Fraction of leaf clusters to probe (0.0-1.0). 0 means use default (0.1).
        // Converted to nprobe = max(1, floor(totalLeafClusters * fraction)) at runtime.
        LogicalVariable minProbeFractionVar = context.newVar();
        queryVarList.add(minProbeFractionVar);
        if (!isVectorDistance && annDistanceExpr.getArguments().size() > 3) {
            ILogicalExpression probeFracExpr = annDistanceExpr.getArguments().get(3).getValue().cloneExpression();
            queryExprList.add(new MutableObject<>(ensureDoubleConstant(probeFracExpr)));
        } else {
            queryExprList.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new ADouble(0.1)))));
        }

        // Add k_multiplier variable (arg 4 for ANN_DISTANCE, default 1)
        // Multiplier for candidate limit: K * kMultiplier candidates collected for reranking.
        LogicalVariable kMultiplierVar = context.newVar();
        queryVarList.add(kMultiplierVar);
        if (!isVectorDistance && annDistanceExpr.getArguments().size() > 4) {
            ILogicalExpression kMultExpr = annDistanceExpr.getArguments().get(4).getValue().cloneExpression();
            queryExprList.add(new MutableObject<>(ensureInt32Constant(kMultExpr)));
        } else {
            queryExprList.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(1)))));
        }

        // Add search_approach variable (always 0 for ann_distance, 4 for vector_distance)
        // Cursor selection is controlled by SET compiler.vector.prunedsearch, not by query args
        LogicalVariable searchApproachVar = context.newVar();
        queryVarList.add(searchApproachVar);
        if (isVectorDistance) {
            // Index-driven KNN: sequential scan of all clusters with bidirectional pruning
            queryExprList.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(4)))));
        } else {
            queryExprList.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(0)))));
        }

        // Create ASSIGN operator to hold query parameters
        AssignOperator assignSearchKeys = new AssignOperator(queryVarList, queryExprList);
        assignSearchKeys.setSourceLocation(dataSourceOp.getSourceLocation());
        assignSearchKeys.getInputs().add(
                new MutableObject<>(OperatorManipulationUtil.deepCopy(dataSourceOp.getInputs().get(0).getValue())));
        assignSearchKeys.setExecutionMode(dataSourceOp.getExecutionMode());
        context.computeAndSetTypeEnvironmentForOperator(assignSearchKeys);

        // Create VectorJobGenParams to pass parameters to Hyracks runtime
        VectorJobGenParams jobGenParams = new VectorJobGenParams(chosenIndex.getIndexName(), IndexType.VTREE,
                chosenIndex.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName(), false, // retainInput - not needed for simple case
                false // requiresBroadcast
        );
        jobGenParams.setQueryVarList(queryVarList);

        // TODO: Add index-only plan optimization check here (like B+Tree does)
        // For now, always do primary index lookup to get full record
        boolean isIndexOnlyPlan = false;

        // Create UNNEST-MAP operator for vector index search
        // This returns: <pk> from the index (vector embeddings skipped to save memory)
        ILogicalOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                metaRecordType, chosenIndex, assignSearchKeys, jobGenParams, context, false, false, null);

        // Handle filter pushdown for INCLUDE fields
        // If there's a SELECT operator with filter on INCLUDE fields, we:
        // 1. Add INCLUDE field variables to the UnnestMapOperator output
        // 2. Rewrite the selectCondition to use these new variables
        // 3. Set the selectCondition on the UnnestMapOperator
        // This allows ITupleFilterFactory to be created in VectorSearchPOperator
        boolean filterPushdownApplied = false;
        if (selectOp != null && secondaryIndexUnnestOp instanceof UnnestMapOperator) {
            UnnestMapOperator vectorUnnestMap = (UnnestMapOperator) secondaryIndexUnnestOp;
            filterPushdownApplied = addIncludeFieldsAndSetSelectCondition(vectorUnnestMap, selectOp, chosenIndex,
                    recordType, subTree, context);
        }

        // Update type environment to register variables produced by vector index search
        // Only do this if filter pushdown wasn't applied (which already handles type env)
        if (!filterPushdownApplied) {
            context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);
        }

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

        LOGGER.trace("Vector index search plan created successfully");
        return primaryIndexUnnestOp;
    }

    @Override
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr, boolean checkApplicableOnly)
            throws AlgebricksException {
        return exprIsOptimizable(index, optFuncExpr, checkApplicableOnly, null);
    }

    /**
     * Checks if this ANN_DISTANCE expression can use the given vector index.
     * Validates both field name and distance metric compatibility.
     * 
     * @param index The vector index to check
     * @param optFuncExpr The optimizable function expression
     * @param checkApplicableOnly Whether to only check applicability
     * @param queryDistanceMetric The distance metric from the query (optional, for metric-aware matching)
     * @return true if the index can be used, false otherwise
     */
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr, boolean checkApplicableOnly,
            String queryDistanceMetric) throws AlgebricksException {
        // Check if this ANN_DISTANCE expression can use the given vector index

        if (index.getIndexType() != IndexType.VTREE) {
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
        if (!indexKeyFieldNames.get(0).equals(fieldName)) {
            return false;
        }

        // If query distance metric is provided, check metric compatibility
        if (queryDistanceMetric != null && !queryDistanceMetric.isEmpty()) {
            String indexMetric = getIndexDistanceMetric(index);
            String normalizedQueryMetric = normalizeDistanceMetric(queryDistanceMetric);
            if (!normalizedQueryMetric.equals(indexMetric)) {
                // Field matches but distance metric doesn't - reject this index
                return false;
            }
        }

        // Index-driven KNN (VECTOR_DISTANCE_ARRAY) requires quantized vector index
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier().equals(BuiltinFunctions.VECTOR_DISTANCE_ARRAY)) {
            Index.VectorIndexDetails vectorDetails2 = (Index.VectorIndexDetails) index.getIndexDetails();
            AdmObjectNode withNode = vectorDetails2.getWithObjectNode();
            String quantization = (withNode != null) ? withNode.getOptionalString("quantization", null) : null;
            if (quantization == null) {
                return false; // Non-quantized index can't do index-driven KNN
            }
        }

        // Field name matches (and metric matches if provided)
        return true;
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

    /**
     * Normalizes a distance metric string to its canonical form.
     * Handles aliases and case-insensitive matching.
     * 
     * Supported metrics and aliases:
     * - "euclidean", "l2" → "euclidean"
     * - "euclidean_squared", "l2_squared" → "euclidean_squared"
     * - "manhattan", "manhattan distance", "l1" → "manhattan"
     * - "cosine", "cosine similarity" → "cosine"
     * - "dot" → "dot"
     * 
     * @param metric The distance metric string (may be null or empty)
     * @return Normalized canonical metric name, or "euclidean" as default
     */
    public static String normalizeDistanceMetric(String metric) {
        if (metric == null || metric.trim().isEmpty()) {
            return "euclidean"; // Default metric
        }

        String normalized = metric.toLowerCase().trim();

        // Map aliases to canonical names
        switch (normalized) {
            case "l2":
                return "euclidean";
            case "l2_squared":
                return "euclidean_squared";
            case "l1":
            case "manhattan_distance":
                return "manhattan";
            case "cosine similarity":
                return "cosine";
            case "euclidean":
            case "euclidean_squared":
            case "manhattan":
            case "cosine":
            case "dot":
                return normalized; // Already canonical
            default:
                // Unknown metric, return as-is (will be handled by distance function)
                return normalized;
        }
    }

    /**
     * Extracts and normalizes the distance metric from a vector index.
     * 
     * @param index The vector index
     * @return Normalized distance metric string, or "euclidean" as default
     */
    public static String getIndexDistanceMetric(Index index) {
        if (index.getIndexType() != IndexType.VTREE) {
            return ""; // Default for non-vector indexes
        }

        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        AdmObjectNode withObjectNode = vectorDetails.getWithObjectNode();

        String indexMetric = (withObjectNode != null) ? withObjectNode.getOptionalString("similarity", "") : "";

        return normalizeDistanceMetric(indexMetric);
    }

    /**
     * Checks if two distance metrics are compatible (same canonical form).
     *
     * @param metric1 First distance metric
     * @param metric2 Second distance metric
     * @return true if metrics are compatible, false otherwise
     */
    public static boolean areDistanceMetricsCompatible(String metric1, String metric2) {
        String normalized1 = normalizeDistanceMetric(metric1);
        String normalized2 = normalizeDistanceMetric(metric2);
        return normalized1.equals(normalized2);
    }

    /**
     * Ensures an integer constant expression uses AInt32 instead of AInt64.
     *
     * SQL++ parser creates AInt64 (8 bytes) for all integer literals, but the runtime
     * uses IntegerPointable.getInteger() which reads only 4 bytes. Reading the first
     * 4 bytes of an 8-byte big-endian AInt64 for small values yields 0.
     *
     * This method converts AInt64 constants to AInt32 at compile time so the runtime
     * can correctly read them with IntegerPointable.
     *
     * @param expr The expression to check and potentially convert
     * @return The original expression if not an AInt64 constant, or a new AInt32 constant expression
     */
    private static ILogicalExpression ensureDoubleConstant(ILogicalExpression expr) throws AlgebricksException {
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) expr;
            IAlgebricksConstantValue constVal = constExpr.getValue();
            if (constVal instanceof AsterixConstantValue) {
                IAObject obj = ((AsterixConstantValue) constVal).getObject();
                double doubleValue;
                if (obj instanceof ADouble) {
                    doubleValue = ((ADouble) obj).getDoubleValue();
                } else if (obj instanceof AInt32) {
                    doubleValue = ((AInt32) obj).getIntegerValue();
                } else if (obj instanceof AInt64) {
                    doubleValue = ((AInt64) obj).getLongValue();
                } else if (obj instanceof AFloat) {
                    doubleValue = ((AFloat) obj).getFloatValue();
                } else {
                    throw new AlgebricksException(
                            "min_probe_fraction (4th argument of ann_distance) must be a number, got: " + obj);
                }
                if (doubleValue < 0.0 || doubleValue > 1.0) {
                    throw new AlgebricksException(
                            "min_probe_fraction (4th argument of ann_distance) must be between 0.0 and 1.0, got: "
                                    + doubleValue);
                }
                if (!(obj instanceof ADouble)) {
                    return new ConstantExpression(new AsterixConstantValue(new ADouble(doubleValue)));
                }
            }
        }
        return expr;
    }

    private static ILogicalExpression ensureInt32Constant(ILogicalExpression expr) throws AlgebricksException {
        if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) expr;
            IAlgebricksConstantValue constVal = constExpr.getValue();
            if (constVal instanceof AsterixConstantValue) {
                IAObject obj = ((AsterixConstantValue) constVal).getObject();
                if (obj instanceof AInt32) {
                    return expr;
                } else if (obj instanceof AInt64) {
                    int intValue = (int) ((AInt64) obj).getLongValue();
                    return new ConstantExpression(new AsterixConstantValue(new AInt32(intValue)));
                } else if (obj instanceof ADouble || obj instanceof AFloat) {
                    throw new AlgebricksException(
                            "k_multiplier (5th argument of ann_distance) must be a positive integer, got: " + obj);
                }
            }
        }
        return expr;
    }

    /**
     * Adds INCLUDE field variables to the vector index UnnestMapOperator and sets up
     * the selectCondition for filter pushdown.
     *
     * This method follows the PushLimitIntoPrimarySearchRule pattern:
     * 1. Clones the selectCondition from SelectOperator
     * 2. Uses InlineVariablesVisitor to inline ASSIGN variables into the expression
     * 3. After inlining, the expression uses variables already in the type environment
     * 4. Adds INCLUDE field variables to UnnestMapOperator output for physical layer mapping
     * 5. Sets the INLINED selectCondition on the UnnestMapOperator
     *
     * The key insight is that we DON'T create new variable references in the selectCondition.
     * Instead, we inline the expression so it uses the record variable from DataSourceScan,
     * which IS in the input type environment and passes type checking.
     *
     * At physical layer, VectorIndexFilterSchema maps field-access expressions to the
     * correct physical tuple positions.
     *
     * @param vectorUnnestMap The vector index UnnestMapOperator
     * @param selectOp The SELECT operator containing the filter condition
     * @param chosenIndex The vector index with INCLUDE fields
     * @param recordType The record type of the dataset
     * @param subTree The subtree for resolving variable references
     * @param context The optimization context
     * @return true if filter pushdown was applied, false otherwise
     */
    private boolean addIncludeFieldsAndSetSelectCondition(UnnestMapOperator vectorUnnestMap, SelectOperator selectOp,
            Index chosenIndex, ARecordType recordType, OptimizableOperatorSubTree subTree, IOptimizationContext context)
            throws AlgebricksException {

        // Get INCLUDE field names from vector index
        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) chosenIndex.getIndexDetails();
        List<List<String>> includeFieldNames = vectorDetails.getIncludeFieldNames();

        if (includeFieldNames == null || includeFieldNames.isEmpty()) {
            LOGGER.trace("No INCLUDE fields in vector index, skipping filter pushdown");
            return false;
        }

        LOGGER.trace("Vector index has INCLUDE fields: {}", includeFieldNames);

        // Clone the selectCondition
        ILogicalExpression selectCondition = selectOp.getCondition().getValue();
        MutableObject<ILogicalExpression> selectConditionRef = new MutableObject<>(selectCondition.cloneExpression());

        // Get variables used in the select condition
        Set<LogicalVariable> selectedVariables = new HashSet<>();
        selectConditionRef.getValue().getUsedVariables(selectedVariables);

        LOGGER.trace("Select condition uses variables: {}", selectedVariables);

        // Following PushLimitIntoPrimarySearchRule pattern: inline variables from ASSIGN operators
        // This replaces variable references with their field-access expressions
        ILogicalOperator child = selectOp.getInputs().get(0).getValue();
        InlineVariablesRule.InlineVariablesVisitor inlineVisitor = null;
        Map<LogicalVariable, ILogicalExpression> varAssignRhs = null;

        for (; child.getOperatorTag() == LogicalOperatorTag.ASSIGN; child = child.getInputs().get(0).getValue()) {
            if (varAssignRhs == null) {
                varAssignRhs = new HashMap<>();
            } else {
                varAssignRhs.clear();
            }
            AssignOperator assignOp = (AssignOperator) child;
            extractInlinableVariablesFromAssign(assignOp, selectedVariables, varAssignRhs);

            if (!varAssignRhs.isEmpty()) {
                if (inlineVisitor == null) {
                    inlineVisitor = new InlineVariablesRule.InlineVariablesVisitor(varAssignRhs, null);
                    inlineVisitor.setContext(context);
                    inlineVisitor.setOperator(selectOp);
                }
                if (!inlineVisitor.transform(selectConditionRef)) {
                    break;
                }
                selectedVariables.clear();
                selectConditionRef.getValue().getUsedVariables(selectedVariables);
                LOGGER.trace("After inlining, condition uses variables: {}", selectedVariables);
            }
        }

        LOGGER.trace("Inlined selectCondition: {}", selectConditionRef.getValue());

        // Check that all filter fields are in the INCLUDE list
        // Build mapping: field name -> include field index
        Map<String, Integer> fieldNameToIncludeIndex = new HashMap<>();
        for (int i = 0; i < includeFieldNames.size(); i++) {
            List<String> fieldPath = includeFieldNames.get(i);
            String fieldName = fieldPath.get(fieldPath.size() - 1);
            fieldNameToIncludeIndex.put(fieldName, i);
        }

        // Extract field names used in the filter condition
        Set<String> filterFieldNames = new HashSet<>();
        extractFieldNamesFromExpression(selectConditionRef.getValue(), recordType, filterFieldNames);
        LOGGER.trace("Filter uses fields: {}", filterFieldNames);

        // Verify all filter fields are in the INCLUDE list
        for (String fieldName : filterFieldNames) {
            if (!fieldNameToIncludeIndex.containsKey(fieldName)) {
                LOGGER.trace("Field {} is not in INCLUDE list, skipping filter pushdown", fieldName);
                return false;
            }
        }

        // FILTER PUSHDOWN DISABLED
        // We cannot set selectCondition on VECTOR_INDEX_UNNEST because:
        // 1. The inlined expression references $row (record variable from DataSourceScan)
        // 2. $row is produced by PRIMARY_INDEX_UNNEST which is ABOVE VECTOR_INDEX_UNNEST
        // 3. computeInputTypeEnvironment() only looks at CHILDREN (below), not PARENTS (above)
        // 4. Therefore $row is not in VECTOR_INDEX_UNNEST's input type environment
        // 5. SetClosedRecordConstructorsRule fails with "Could not infer type for variable '$row'"
        //
        // See VECTOR_INDEX_FILTER_PUSHDOWN_TYPE_INFERENCE_ISSUE.md for detailed analysis.
        //
        // Future solution: Bypass the Algebricks type system by passing filter info
        // through VectorJobGenParams and creating TupleFilter directly at physical layer.
        LOGGER.trace("Filter pushdown DISABLED due to type inference issue");
        LOGGER.trace("All filter fields are in INCLUDE list, but cannot set selectCondition");
        return false;
    }

    /**
     * Extracts inlinable variables from an AssignOperator.
     * Following PushLimitIntoPrimarySearchRule pattern.
     */
    private void extractInlinableVariablesFromAssign(AssignOperator assignOp, Set<LogicalVariable> includeVariables,
            Map<LogicalVariable, ILogicalExpression> outVarExprs) {
        List<LogicalVariable> vars = assignOp.getVariables();
        List<Mutable<ILogicalExpression>> exprs = assignOp.getExpressions();
        for (int i = 0, ln = vars.size(); i < ln; i++) {
            LogicalVariable var = vars.get(i);
            if (includeVariables.contains(var)) {
                outVarExprs.put(var, exprs.get(i).getValue());
            }
        }
    }

    /**
     * Extracts field names from field-access expressions in the given expression.
     */
    private void extractFieldNamesFromExpression(ILogicalExpression expr, ARecordType recordType,
            Set<String> fieldNames) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

            // Check for field-access-by-name
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                if (funcExpr.getArguments().size() >= 2) {
                    ILogicalExpression fieldNameExpr = funcExpr.getArguments().get(1).getValue();
                    String fieldName = AccessMethodUtils.getStringConstant(new MutableObject<>(fieldNameExpr));
                    if (fieldName != null) {
                        fieldNames.add(fieldName);
                    }
                }
            }
            // Check for field-access-by-index
            else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
                if (funcExpr.getArguments().size() >= 2) {
                    ILogicalExpression fieldIndexExpr = funcExpr.getArguments().get(1).getValue();
                    Integer fieldIndex = AccessMethodUtils.getInt32Constant(new MutableObject<>(fieldIndexExpr));
                    if (fieldIndex != null && recordType != null) {
                        String[] fieldNamesArray = recordType.getFieldNames();
                        if (fieldIndex >= 0 && fieldIndex < fieldNamesArray.length) {
                            fieldNames.add(fieldNamesArray[fieldIndex]);
                        }
                    }
                }
            }

            // Recursively process arguments
            for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                extractFieldNamesFromExpression(arg.getValue(), recordType, fieldNames);
            }
        }
    }

    /**
     * Collects mappings from field names to the variables that represent them in the filter.
     * Traces through ASSIGN operators to find field name -> variable mappings.
     */
    private void collectFieldVariableMappings(ILogicalExpression expr, OptimizableOperatorSubTree subTree,
            Map<String, LogicalVariable> fieldNameToVar) {

        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            // Variable reference - trace back to find field name
            VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
            LogicalVariable var = varRef.getVariableReference();
            String fieldName = findFieldNameForVariable(var, subTree);
            if (fieldName != null) {
                fieldNameToVar.put(fieldName, var);
            }
        } else if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            // Recurse into function arguments
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
                collectFieldVariableMappings(argRef.getValue(), subTree, fieldNameToVar);
            }
        }
    }

    /**
     * Finds the field name that a variable represents by tracing through ASSIGN operators.
     */
    private String findFieldNameForVariable(LogicalVariable var, OptimizableOperatorSubTree subTree) {
        // Search assigns for the variable definition
        for (AbstractLogicalOperator op : subTree.getAssignsAndUnnests()) {
            if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) op;
                List<LogicalVariable> assignVars = assignOp.getVariables();
                List<Mutable<ILogicalExpression>> assignExprs = assignOp.getExpressions();

                for (int i = 0; i < assignVars.size(); i++) {
                    if (assignVars.get(i).equals(var)) {
                        // Found the assignment - extract field name
                        ILogicalExpression assignExpr = assignExprs.get(i).getValue();
                        return extractFieldNameFromExpression(assignExpr, subTree);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Extracts field name from a field access expression.
     * Handles both field-access-by-name and field-access-by-index.
     */
    private String extractFieldNameFromExpression(ILogicalExpression expr, OptimizableOperatorSubTree subTree) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

            // Check for field-access-by-name
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                // Second argument is the field name
                if (funcExpr.getArguments().size() >= 2) {
                    ILogicalExpression fieldNameExpr = funcExpr.getArguments().get(1).getValue();
                    return AccessMethodUtils.getStringConstant(new MutableObject<>(fieldNameExpr));
                }
            }

            // Check for field-access-by-index
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
                // Second argument is the field index
                if (funcExpr.getArguments().size() >= 2) {
                    ILogicalExpression fieldIndexExpr = funcExpr.getArguments().get(1).getValue();
                    Integer fieldIndex = AccessMethodUtils.getInt32Constant(new MutableObject<>(fieldIndexExpr));
                    if (fieldIndex != null && subTree != null && subTree.getRecordType() != null) {
                        String[] fieldNames = subTree.getRecordType().getFieldNames();
                        if (fieldIndex >= 0 && fieldIndex < fieldNames.length) {
                            String fieldName = fieldNames[fieldIndex];
                            LOGGER.trace("Resolved field-access-by-index in filter: index {} -> field '{}'", fieldIndex,
                                    fieldName);
                            return fieldName;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Rewrites an expression by replacing old variables with new variables.
     */
    private ILogicalExpression rewriteExpressionWithNewVariables(ILogicalExpression expr,
            Map<LogicalVariable, LogicalVariable> oldToNewVarMapping) {

        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
            LogicalVariable oldVar = varRef.getVariableReference();

            if (oldToNewVarMapping.containsKey(oldVar)) {
                // Replace with new variable
                LogicalVariable newVar = oldToNewVarMapping.get(oldVar);
                VariableReferenceExpression newVarRef = new VariableReferenceExpression(newVar);
                newVarRef.setSourceLocation(varRef.getSourceLocation());
                return newVarRef;
            }
            return expr.cloneExpression();

        } else if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
            List<Mutable<ILogicalExpression>> newArgs = new ArrayList<>();

            for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
                ILogicalExpression newArg = rewriteExpressionWithNewVariables(argRef.getValue(), oldToNewVarMapping);
                newArgs.add(new MutableObject<>(newArg));
            }

            ScalarFunctionCallExpression newFuncExpr =
                    new ScalarFunctionCallExpression(funcExpr.getFunctionInfo(), newArgs);
            newFuncExpr.setSourceLocation(funcExpr.getSourceLocation());
            return newFuncExpr;

        } else {
            // For other expression types (constants, etc.), just clone
            return expr.cloneExpression();
        }
    }
}
