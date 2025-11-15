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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * Optimization rule for introducing vector index access for top-k ANN queries.
 *
 * Pattern: LIMIT k → ORDER BY ANN_DISTANCE(vectorField, queryVector, metric) → ... → DATASOURCE_SCAN
 *
 * Transformation:
 * - If vector index exists: Replaces DATASOURCE_SCAN with UNNEST-MAP(vector_index_search)
 *   → Returns candidate tuples for approximate ANN search
 * - If no vector index: Leaves plan unchanged
 *   → Falls back to exact KNN search (exhaustive distance computation on all tuples)
 *
 * The ORDER BY ANN_DISTANCE operator handles distance computation in both cases.
 */
public class IntroduceTopKAccessMethodRule extends AbstractIntroduceAccessMethodRule {

    // Operators representing the pattern to be matched
    protected Mutable<ILogicalOperator> limitRef = null;
    protected LimitOperator limitOp = null;
    protected Mutable<ILogicalOperator> orderRef = null;
    protected OrderOperator orderOp = null;
    protected AbstractFunctionCallExpression annDistanceExpr = null;
    protected IVariableTypeEnvironment typeEnvironment = null;
    protected final OptimizableOperatorSubTree subTree = new OptimizableOperatorSubTree();
    protected String queryDistanceMetric = null; // Distance metric from the query (e.g., "euclidean", "cosine")

    // Register vector index access method
    protected static Map<FunctionIdentifier, List<IAccessMethod>> accessMethods = new HashMap<>();

    static {
        registerAccessMethod(VectorIndexAccessMethod.INSTANCE, accessMethods);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        clear();
        setMetadataDeclarations(context);

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Already checked?
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        // Start from root operators
        if (op.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT
                && op.getOperatorTag() != LogicalOperatorTag.SINK
                && op.getOperatorTag() != LogicalOperatorTag.DELEGATE_OPERATOR) {
            return false;
        }

        // Recursively find pattern: LIMIT → ORDER BY ANN_DISTANCE → ... → DATASOURCE_SCAN
        boolean planTransformed = checkAndApplyTopKTransformation(opRef, context);

        if (planTransformed) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }

        return planTransformed;
    }

    /**
     * Recursively checks the plan for LIMIT → ORDER BY ANN_DISTANCE pattern
     * and applies vector index optimization if applicable.
     */
    protected boolean checkAndApplyTopKTransformation(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // Check if current operator is LIMIT
        if (op.getOperatorTag() == LogicalOperatorTag.LIMIT) {
            limitRef = opRef;
            limitOp = (LimitOperator) op;

            System.err.println("=== Found LIMIT operator ===");

            // Check if already processed
            if (context.checkIfInDontApplySet(this, limitOp)) {
                System.err.println("Already in don't apply set");
                return false;
            }

            // Find ORDER operator by skipping intermediate operators (ASSIGN, EXCHANGE, etc.)
            Pair<Mutable<ILogicalOperator>, OrderOperator> orderPair = findOrderOperator(limitOp);

            if (orderPair != null) {
                orderRef = orderPair.first;
                orderOp = orderPair.second;

                System.err.println("=== Found ORDER operator ===");
                System.err.println("ORDER expressions count: " + orderOp.getOrderExpressions().size());

                // Check if ORDER BY uses ANN_DISTANCE function
                if (matchesAnnDistancePattern()) {
                    System.err.println("=== Pattern matched! Calling analyzeAndTransform ===");
                    // Try to apply transformation
                    return analyzeAndTransform(context);
                } else {
                    System.err.println("Pattern did not match (not single ORDER BY expression)");
                }
            } else {
                System.err.println("LIMIT child: " + (limitOp.getInputs().isEmpty() ? "NONE"
                        : ((AbstractLogicalOperator) limitOp.getInputs().get(0).getValue()).getOperatorTag()));
            }
        }

        // Recursively check children
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            boolean transformed = checkAndApplyTopKTransformation(inputOpRef, context);
            if (transformed) {
                return true;
            }
        }

        return false;
    }

    /**
     * Finds the ORDER operator by traversing through intermediate operators.
     * Skips ASSIGN, EXCHANGE, and nested LIMIT operators that are added for
     * distributed execution optimization.
     *
     * The optimizer often adds intermediate operators between LIMIT and ORDER:
     * - ASSIGN: For result projection
     * - EXCHANGE: For data redistribution in distributed execution
     * - LIMIT: For distributed top-k optimization
     *
     * This method traverses through these intermediate operators until it finds
     * the ORDER operator or encounters an operator it doesn't recognize.
     *
     * @param limitOp The LIMIT operator to start from
     * @return Pair of (orderRef, orderOp) if found, null otherwise
     */
    protected Pair<Mutable<ILogicalOperator>, OrderOperator> findOrderOperator(LimitOperator limitOp) {
        if (limitOp.getInputs().isEmpty()) {
            return null;
        }

        Mutable<ILogicalOperator> currentRef = limitOp.getInputs().get(0);
        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) currentRef.getValue();

        // Traverse through intermediate operators until we find ORDER or fail
        // The loop will terminate when:
        // 1. We find ORDER operator (success)
        // 2. We hit an empty input list (no more children)
        // 3. We hit an operator we don't know how to skip through
        while (true) {
            // Check if we found the ORDER operator
            if (currentOp.getOperatorTag() == LogicalOperatorTag.ORDER) {
                return new Pair<>(currentRef, (OrderOperator) currentOp);
            }

            // Skip through known intermediate operators
            if (currentOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    || currentOp.getOperatorTag() == LogicalOperatorTag.EXCHANGE
                    || currentOp.getOperatorTag() == LogicalOperatorTag.LIMIT) {

                if (currentOp.getInputs().isEmpty()) {
                    // No more children to traverse
                    return null;
                }
                currentRef = currentOp.getInputs().get(0);
                currentOp = (AbstractLogicalOperator) currentRef.getValue();
            } else {
                // Hit an operator we don't know how to skip through
                // This means LIMIT is not directly above ORDER BY ANN_DISTANCE
                return null;
            }
        }
    }

    /**
     * Checks if ORDER BY pattern exists (has at least one ordering expression).
     * We don't resolve the actual ANN_DISTANCE function here - that happens later
     * after subtree initialization in analyzeAnnDistanceFunction().
     */
    protected boolean matchesAnnDistancePattern() {
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExprs = orderOp.getOrderExpressions();

        // Just check that ORDER BY has exactly one expression
        // We'll verify it's ANN_DISTANCE later after subtree init
        return orderExprs.size() == 1;
    }

    /**
     * Analyzes the pattern and attempts to apply vector index transformation.
     */
    protected boolean analyzeAndTransform(IOptimizationContext context) throws AlgebricksException {
        // 1. Initialize subtree from ORDER down to DATASOURCE_SCAN
        if (!initializeSubTree()) {
            return false;
        }

        // 2. Check if there are filter predicates (SELECT operators) between ORDER and DATASOURCE_SCAN
        // If yes, skip vector index optimization to avoid returning too few results
        // Reason: Vector index partitions by similarity, not by other fields.
        // Filtering top-k results by other predicates may yield insufficient results.
        if (hasSelectOperatorInSubTree()) {
            System.err.println("Query has WHERE clause (SELECT operator) - skipping vector index optimization");
            System.err.println("Reason: Vector index + filters may return too few results");
            context.addToDontApplySet(this, limitOp);
            return false;
        }

        // 3. Get type environment for type checking
        typeEnvironment = context.getOutputTypeEnvironment(orderOp);

        // 3. Load dataset metadata (including vector indexes)
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        if (!subTree.setDatasetAndTypeMetadata(metadataProvider)) {
            return false;
        }

        // 4. Analyze ANN_DISTANCE function arguments
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();
        if (!analyzeAnnDistanceFunction(analyzedAMs, context)) {
            return false;
        }

        // 5. Find applicable vector indexes on the dataset
        fillSubTreeIndexExprs(subTree, analyzedAMs, context, false);

        // 6. Choose best vector index
        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        chooseVectorIndex(analyzedAMs, chosenIndexes);

        if (chosenIndexes.isEmpty()) {
            // No vector index available - fall back to data scan + sort
            context.addToDontApplySet(this, limitOp);
            return false;
        }

        // 7. Apply plan transformation
        Index vectorIndex = chosenIndexes.get(0).second;
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(VectorIndexAccessMethod.INSTANCE);

        boolean transformed = applyTopKPlanTransformation(vectorIndex, analysisCtx, context);

        // Always mark as processed to avoid re-attempting optimization on this operator
        context.addToDontApplySet(this, limitOp);

        return transformed;
    }

    /**
     * Checks if there are any SELECT operators between ORDER and DATASOURCE_SCAN.
     * SELECT operators represent WHERE clause predicates that filter rows.
     *
     * We skip vector index optimization when filters exist because:
     * - Vector index returns top-k results partitioned by vector similarity
     * - Applying additional filters (e.g., year > 2000) may yield too few results
     * - Better to scan all data and apply filters, then sort by ANN distance
     */
    protected boolean hasSelectOperatorInSubTree() {
        if (orderOp.getInputs().isEmpty()) {
            return false;
        }

        // Traverse from ORDER down to DATASOURCE_SCAN
        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) orderOp.getInputs().get(0).getValue();

        while (currentOp != null) {
            if (currentOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                // Found SELECT operator - query has WHERE clause
                return true;
            }

            // Stop at DATASOURCE_SCAN
            if (currentOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                break;
            }

            // Continue to next child
            if (currentOp.getInputs().isEmpty()) {
                break;
            }
            currentOp = (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue();
        }

        return false;
    }

    /**
     * Initializes the subtree from ORDER operator down to DATASOURCE_SCAN.
     *
     * OptimizableOperatorSubTree doesn't recognize ORDER as a valid top operator,
     * so we need to start from ORDER's child (usually ASSIGN).
     */
    protected boolean initializeSubTree() throws AlgebricksException {
        System.err.println("=== initializeSubTree() called ===");
        System.err.println("orderRef: " + orderRef.getValue().getOperatorTag());

        // Get the child of ORDER operator (usually first ASSIGN)
        // OptimizableOperatorSubTree expects to start from operators like SELECT, ASSIGN, etc.
        // not from ORDER
        if (orderOp.getInputs().isEmpty()) {
            System.err.println("ORDER has no children!");
            return false;
        }

        Mutable<ILogicalOperator> subTreeRoot = orderOp.getInputs().get(0);
        System.err.println("Starting subtree from: " + subTreeRoot.getValue().getOperatorTag());

        subTree.initFromSubTree(subTreeRoot);

        boolean hasDataSource = subTree.hasDataSourceScan();
        System.err.println("hasDataSourceScan: " + hasDataSource);

        if (!hasDataSource) {
            System.err.println("Subtree operators found:");
            AbstractLogicalOperator current = (AbstractLogicalOperator) subTreeRoot.getValue();
            int depth = 0;
            while (current != null && depth < 10) {
                System.err.println("  [" + depth + "] " + current.getOperatorTag());
                if (current.getInputs().isEmpty()) {
                    break;
                }
                current = (AbstractLogicalOperator) current.getInputs().get(0).getValue();
                depth++;
            }
        }

        return hasDataSource;
    }

    /**
     * Analyzes ANN_DISTANCE function and updates analysis context.
     * This is called AFTER subtree initialization, so assigns/unnests are available
     * for resolving variable references.
     */
    protected boolean analyzeAnnDistanceFunction(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            IOptimizationContext context) throws AlgebricksException {

        // Get ORDER BY expression
        List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExprs = orderOp.getOrderExpressions();
        ILogicalExpression orderExpr = orderExprs.get(0).second.getValue();

        // Resolve to actual ANN_DISTANCE function (handle both direct and variable reference cases)
        annDistanceExpr = resolveAnnDistanceExpr(orderExpr, subTree.getAssignsAndUnnests());

        if (annDistanceExpr == null) {
            // ORDER BY expression is not ANN_DISTANCE
            return false;
        }

        // Extract distance metric from ANN_DISTANCE function (arg2)
        // ANN_DISTANCE(vectorField, queryVector, distanceMetric)
        if (annDistanceExpr.getArguments().size() >= 3) {
            try {
                ILogicalExpression distanceMetricExpr = annDistanceExpr.getArguments().get(2).getValue();
                queryDistanceMetric = AccessMethodUtils
                        .getStringConstant(new org.apache.commons.lang3.mutable.MutableObject<>(distanceMetricExpr));
                if (queryDistanceMetric != null) {
                    queryDistanceMetric = VectorIndexAccessMethod.normalizeDistanceMetric(queryDistanceMetric);
                    System.err.println("=== Extracted query distance metric: " + queryDistanceMetric + " ===");
                }
            } catch (Exception e) {
                // If we can't extract the metric, continue without metric-aware selection
                // This maintains backward compatibility
                queryDistanceMetric = null;
                System.err.println("=== Could not extract distance metric from query, using field-only matching ===");
            }
        }

        // Now analyze the ANN_DISTANCE function arguments
        AccessMethodAnalysisContext analysisCtx = new AccessMethodAnalysisContext();

        boolean matchFound = VectorIndexAccessMethod.INSTANCE.analyzeFuncExprArgsAndUpdateAnalysisCtx(annDistanceExpr,
                subTree.getAssignsAndUnnests(), analysisCtx, context, typeEnvironment);

        if (!matchFound) {
            return false;
        }

        analyzedAMs.put(VectorIndexAccessMethod.INSTANCE, analysisCtx);
        return true;
    }

    /**
     * Resolves ORDER BY expression to ANN_DISTANCE function.
     * Handles two cases:
     * 1. Direct function call: ANN_DISTANCE(...)
     * 2. Variable reference: $$var where $$var := ANN_DISTANCE(...) in an ASSIGN
     *
     * This method is called after subtree init, so assigns/unnests are available.
     */
    protected AbstractFunctionCallExpression resolveAnnDistanceExpr(ILogicalExpression orderExpr,
            List<AbstractLogicalOperator> assignsAndUnnests) {

        // Case 1: Direct function call
        if (orderExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) orderExpr;
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.ANN_DISTANCE)) {
                return funcExpr;
            }
            return null;
        }

        // Case 2: Variable reference - trace back through assigns
        if (orderExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression varRef = (VariableReferenceExpression) orderExpr;
            LogicalVariable orderVar = varRef.getVariableReference();

            // Search assigns/unnests for the variable (similar to InvertedIndexAccessMethod)
            for (AbstractLogicalOperator op : assignsAndUnnests) {
                if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    AssignOperator assignOp = (AssignOperator) op;
                    List<LogicalVariable> assignVars = assignOp.getVariables();
                    List<Mutable<ILogicalExpression>> assignExprs = assignOp.getExpressions();

                    for (int i = 0; i < assignVars.size(); i++) {
                        if (assignVars.get(i).equals(orderVar)) {
                            // Found the assignment
                            ILogicalExpression assignExpr = assignExprs.get(i).getValue();

                            if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) assignExpr;
                                if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.ANN_DISTANCE)) {
                                    return funcExpr;
                                }
                            }
                            return null;
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Chooses the best vector index from candidates.
     * Prefers indexes with matching distance metrics when query metric is available.
     * Falls back to first field match if no metric match is found.
     */
    protected void chooseVectorIndex(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            List<Pair<IAccessMethod, Index>> result) {

        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(VectorIndexAccessMethod.INSTANCE);
        if (analysisCtx == null) {
            return;
        }

        // Iterate over candidate vector indexes
        Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt =
                analysisCtx.getIteratorForIndexExprsAndVars();

        Pair<IAccessMethod, Index> exactMatch = null; // Index with matching field AND metric
        Pair<IAccessMethod, Index> fieldMatch = null; // Index with matching field only

        while (indexIt.hasNext()) {
            Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
            Index index = indexEntry.getKey();

            if (index.getIndexType() == IndexType.VECTOR) {
                // If query distance metric is available, check for metric compatibility
                if (queryDistanceMetric != null && !queryDistanceMetric.isEmpty()) {
                    String indexMetric = VectorIndexAccessMethod.getIndexDistanceMetric(index);
                    if (queryDistanceMetric.equals(indexMetric)) {
                        // Exact match: field name AND distance metric match
                        exactMatch = new Pair<>(VectorIndexAccessMethod.INSTANCE, index);
                        System.err.println("=== Found exact match: index " + index.getIndexName() + " with metric "
                                + indexMetric + " ===");
                        break; // Prefer exact match, use first one found
                    } else {
                        // Field matches but metric doesn't - store as fallback only if no exact match
                        if (fieldMatch == null) {
                            fieldMatch = new Pair<>(VectorIndexAccessMethod.INSTANCE, index);
                        }
                    }
                } else {
                    // No query metric available - use first field match (backward compatibility)
                    result.add(new Pair<>(VectorIndexAccessMethod.INSTANCE, index));
                    break;
                }
            }
        }

        // Select best match: exact match preferred, fallback to field match
        if (exactMatch != null) {
            result.add(exactMatch);
            System.err.println("=== Selected index with matching distance metric ===");
        } else if (fieldMatch != null) {
            result.add(fieldMatch);
            System.err.println("=== Selected index with matching field (metric mismatch, may affect accuracy) ===");
        }
    }

    /**
     * Applies the top-k plan transformation using the chosen vector index.
     *
     * Transforms:
     *   LIMIT k → ORDER BY ANN_DISTANCE(vectorField, qvec, metric) → ... → DATASOURCE_SCAN
     * Into:
     *   LIMIT k → ORDER BY ANN_DISTANCE(vectorField, qvec, metric) → ... → UNNEST-MAP(vector_index_search)
     *
     * Key points:
     * - **ONLY replaces DATASOURCE_SCAN** with vector index search (UNNEST-MAP)
     * - ORDER BY and LIMIT operators **remain unchanged**
     * - Vector index returns candidate tuples (may be > k from multiple partitions)
     * - ORDER BY ANN_DISTANCE computes exact distances on candidates
     * - ORDER BY + LIMIT extract the true top-k results
     * - Similar to B+Tree/R-Tree: keeps top operator (ORDER/SELECT), only replaces data scan
     */
    protected boolean applyTopKPlanTransformation(Index vectorIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context) throws AlgebricksException {

        System.err.println("=== VECTOR INDEX TOP-K OPTIMIZATION MATCHED ===");
        System.err.println("Dataset: " + subTree.getDataset().getDatasetName());
        System.err.println("Vector Index: " + vectorIndex.getIndexName());
        System.err.println("Limit: " + limitOp.getMaxObjects().getValue());

        // Call VectorIndexAccessMethod to create the index search plan
        // This creates UNNEST-MAP operator that returns candidate tuples from vector index
        ILogicalOperator indexSearchOp = VectorIndexAccessMethod.INSTANCE.createIndexSearchPlan(limitRef, orderRef,
                annDistanceExpr, subTree, vectorIndex, analysisCtx, context);

        if (indexSearchOp == null) {
            System.err.println("Plan transformation not yet implemented");
            return false;
        }

        // Replace DATASOURCE_SCAN with vector index search
        // ORDER BY ANN_DISTANCE remains unchanged (computes actual distances on candidates)
        // Pattern follows InvertedIndexAccessMethod.applySelectPlanTransformation:498
        subTree.getDataSourceRef().setValue(indexSearchOp);

        System.err.println("=== VECTOR INDEX TOP-K OPTIMIZATION APPLIED ===");
        return true;
    }

    /**
     * Clears the state for the next optimization attempt.
     */
    protected void clear() {
        limitRef = null;
        limitOp = null;
        orderRef = null;
        orderOp = null;
        annDistanceExpr = null;
        typeEnvironment = null;
        queryDistanceMetric = null;
        subTree.reset();
    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }
}
