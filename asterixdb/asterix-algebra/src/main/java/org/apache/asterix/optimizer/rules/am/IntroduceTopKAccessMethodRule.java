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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.declared.IIndexProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
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
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final Logger LOGGER = LogManager.getLogger();

    // Operators representing the pattern to be matched
    protected Mutable<ILogicalOperator> limitRef = null;
    protected LimitOperator limitOp = null;
    protected Mutable<ILogicalOperator> orderRef = null;
    protected OrderOperator orderOp = null;
    protected AbstractFunctionCallExpression annDistanceExpr = null;
    protected IVariableTypeEnvironment typeEnvironment = null;
    protected final OptimizableOperatorSubTree subTree = new OptimizableOperatorSubTree();
    protected String queryDistanceMetric = null; // Distance metric from the query (e.g., "euclidean", "cosine")

    // SELECT operator info for filter pushdown
    protected SelectOperator selectOp = null;
    protected Mutable<ILogicalOperator> selectRef = null;
    protected Set<List<String>> filterFieldNames = null; // Field names referenced in WHERE clause

    // Register vector index access method
    protected static Map<FunctionIdentifier, List<IAccessMethod>> accessMethods = new HashMap<>();

    static {
        registerAccessMethod(VectorIndexAccessMethod.INSTANCE, accessMethods);
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        clear();
        setMetadataIndexDeclarations(context, (IIndexProvider) context.getMetadataProvider());

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

            LOGGER.trace("Found LIMIT operator");

            // Check if already processed
            if (context.checkIfInDontApplySet(this, limitOp)) {
                LOGGER.trace("Already in don't apply set");
                return false;
            }

            // Find ORDER operator by skipping intermediate operators (ASSIGN, EXCHANGE, etc.)
            Pair<Mutable<ILogicalOperator>, OrderOperator> orderPair = findOrderOperator(limitOp);

            if (orderPair != null) {
                orderRef = orderPair.first;
                orderOp = orderPair.second;

                LOGGER.trace("Found ORDER operator");
                LOGGER.trace("ORDER expressions count: {}", orderOp.getOrderExpressions().size());

                // Check if ORDER BY uses ANN_DISTANCE function
                if (matchesAnnDistancePattern()) {
                    LOGGER.trace("Pattern matched, calling analyzeAndTransform");
                    // Try to apply transformation
                    return analyzeAndTransform(context);
                } else {
                    LOGGER.trace("Pattern did not match (not single ORDER BY expression)");
                }
            } else {
                LOGGER.trace("LIMIT child: {}", limitOp.getInputs().isEmpty() ? "NONE"
                        : ((AbstractLogicalOperator) limitOp.getInputs().get(0).getValue()).getOperatorTag());
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

        // 2. Get type environment for type checking
        typeEnvironment = context.getOutputTypeEnvironment(orderOp);

        // 3. Load dataset metadata (including vector indexes)
        // This MUST be done before extracting filter fields because field-access-by-index
        // needs the record type to resolve field index to field name
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        if (!subTree.setDatasetAndTypeMetadata(metadataProvider)) {
            return false;
        }

        // 4. Find SELECT operator (if any) and extract filter fields
        // This populates selectOp, selectRef, and filterFieldNames if a SELECT exists
        // Must be called AFTER setDatasetAndTypeMetadata so recordType is available
        findSelectOperatorInSubTree();

        if (selectOp != null) {
            LOGGER.trace("Query has WHERE clause (SELECT operator)");
            LOGGER.trace("Filter fields: {}", filterFieldNames);
        }

        // 5. Analyze ANN_DISTANCE function arguments
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new TreeMap<>();
        if (!analyzeAnnDistanceFunction(analyzedAMs, context)) {
            return false;
        }

        // 6. Find applicable vector indexes on the dataset
        fillSubTreeIndexExprs(subTree, analyzedAMs, context, false);

        // 7. Choose best vector index (considering INCLUDE fields if filter exists)
        List<Pair<IAccessMethod, Index>> chosenIndexes = new ArrayList<>();
        chooseVectorIndex(analyzedAMs, chosenIndexes);

        if (chosenIndexes.isEmpty()) {
            // No vector index available (either none exists, or none has required INCLUDE fields)
            // Fall back to data scan + sort
            if (selectOp != null && filterFieldNames != null && !filterFieldNames.isEmpty()) {
                LOGGER.trace("No vector index with required INCLUDE fields - falling back to data scan");
            }
            context.addToDontApplySet(this, limitOp);
            return false;
        }

        // 8. Apply plan transformation
        Index vectorIndex = chosenIndexes.get(0).second;
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(VectorIndexAccessMethod.INSTANCE);

        boolean transformed = applyTopKPlanTransformation(vectorIndex, analysisCtx, context);

        // Always mark as processed to avoid re-attempting optimization on this operator
        context.addToDontApplySet(this, limitOp);

        return transformed;
    }

    /**
     * Finds SELECT operator between ORDER and DATASOURCE_SCAN.
     * Also extracts filter field names from the SELECT condition.
     *
     * @return The SELECT operator if found, null otherwise
     */
    protected SelectOperator findSelectOperatorInSubTree() {
        if (orderOp.getInputs().isEmpty()) {
            return null;
        }

        // Traverse from ORDER down to DATASOURCE_SCAN
        Mutable<ILogicalOperator> currentRef = orderOp.getInputs().get(0);
        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) currentRef.getValue();

        while (currentOp != null) {
            if (currentOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                // Found SELECT operator - store it and extract filter fields
                selectOp = (SelectOperator) currentOp;
                selectRef = currentRef;
                filterFieldNames = extractFilterFieldsFromCondition(selectOp.getCondition().getValue());
                return selectOp;
            }

            // Stop at DATASOURCE_SCAN
            if (currentOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                break;
            }

            // Continue to next child
            if (currentOp.getInputs().isEmpty()) {
                break;
            }
            currentRef = currentOp.getInputs().get(0);
            currentOp = (AbstractLogicalOperator) currentRef.getValue();
        }

        return null;
    }

    /**
     * Extracts field names referenced in a filter condition expression.
     * Traverses function call expressions to find field access operations.
     *
     * @param condition The filter condition expression
     * @return Set of field names (as List<String> for nested field paths)
     */
    protected Set<List<String>> extractFilterFieldsFromCondition(ILogicalExpression condition) {
        Set<List<String>> fields = new HashSet<>();
        LOGGER.trace("extractFilterFieldsFromCondition");
        LOGGER.trace("Condition: {}", condition);
        LOGGER.trace("Condition tag: {}", condition.getExpressionTag());
        extractFieldsFromExpressionRecursive(condition, fields);
        LOGGER.trace("Extracted fields: {}", fields);
        return fields;
    }

    /**
     * Recursively extracts field names from an expression.
     * Handles function calls (AND, OR, comparison operators, field-access).
     */
    private void extractFieldsFromExpressionRecursive(ILogicalExpression expr, Set<List<String>> fields) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

            // Check if this is a field access function (e.g., field-access-by-name)
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)
                    || funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
                // Extract field name from field access function
                List<String> fieldPath = extractFieldPathFromFieldAccess(funcExpr);
                if (fieldPath != null && !fieldPath.isEmpty()) {
                    fields.add(fieldPath);
                }
            } else {
                // Recursively process arguments for other functions (AND, OR, GT, LT, etc.)
                for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                    extractFieldsFromExpressionRecursive(arg.getValue(), fields);
                }
            }
        } else if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            // Variable reference - need to trace back through assigns to find field access
            VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
            LogicalVariable var = varRef.getVariableReference();

            LOGGER.trace("Found VARIABLE in condition: {}", var);
            LOGGER.trace("subTree assigns count: {}", subTree.getAssignsAndUnnests().size());

            // Search assigns for the variable definition
            // First check subTree assigns
            boolean found = findFieldFromAssigns(var, subTree.getAssignsAndUnnests(), fields);
            LOGGER.trace("Found in subTree assigns: {}", found);

            // If not found in subtree, search all operators from ORDER down to DATASOURCE_SCAN
            if (!found && orderOp != null) {
                found = searchAssignsInPlan(var, orderOp, fields);
                LOGGER.trace("Found in plan search: {}", found);
            }
        }
    }

    /**
     * Searches for variable definition in a list of assigns and extracts field info.
     */
    private boolean findFieldFromAssigns(LogicalVariable var, List<AbstractLogicalOperator> assigns,
            Set<List<String>> fields) {
        for (AbstractLogicalOperator op : assigns) {
            if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) op;
                List<LogicalVariable> assignVars = assignOp.getVariables();
                List<Mutable<ILogicalExpression>> assignExprs = assignOp.getExpressions();

                for (int i = 0; i < assignVars.size(); i++) {
                    if (assignVars.get(i).equals(var)) {
                        // Found the assignment - recursively extract fields
                        ILogicalExpression assignExpr = assignExprs.get(i).getValue();
                        LOGGER.trace("Found assignment for {}: {}", var, assignExpr);
                        LOGGER.trace("Expression tag: {}", assignExpr.getExpressionTag());
                        if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) assignExpr;
                            LOGGER.trace("Function: {}", funcExpr.getFunctionIdentifier());
                        }
                        extractFieldsFromExpressionRecursive(assignExpr, fields);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Searches all operators in the plan from the given operator downward for variable definition.
     */
    private boolean searchAssignsInPlan(LogicalVariable var, ILogicalOperator startOp, Set<List<String>> fields) {
        ILogicalOperator currentOp = startOp;

        while (currentOp != null) {
            if (currentOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) currentOp;
                List<LogicalVariable> assignVars = assignOp.getVariables();
                List<Mutable<ILogicalExpression>> assignExprs = assignOp.getExpressions();

                for (int i = 0; i < assignVars.size(); i++) {
                    if (assignVars.get(i).equals(var)) {
                        // Found the assignment - recursively extract fields
                        extractFieldsFromExpressionRecursive(assignExprs.get(i).getValue(), fields);
                        return true;
                    }
                }
            }

            // Move to next operator
            if (currentOp.getInputs().isEmpty()) {
                break;
            }
            currentOp = currentOp.getInputs().get(0).getValue();
        }
        return false;
    }

    /**
     * Extracts field path from a field-access function expression.
     * For nested fields like row.nested.field, returns ["nested", "field"].
     */
    private List<String> extractFieldPathFromFieldAccess(AbstractFunctionCallExpression funcExpr) {
        List<String> fieldPath = new ArrayList<>();
        extractFieldPathRecursive(funcExpr, fieldPath);
        return fieldPath;
    }

    /**
     * Recursively builds field path from nested field access expressions.
     */
    private void extractFieldPathRecursive(ILogicalExpression expr, List<String> fieldPath) {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
            // First argument is the record, second argument is the field name
            if (funcExpr.getArguments().size() >= 2) {
                // Recursively process the record argument (for nested field access)
                extractFieldPathRecursive(funcExpr.getArguments().get(0).getValue(), fieldPath);

                // Extract field name from second argument
                ILogicalExpression fieldNameExpr = funcExpr.getArguments().get(1).getValue();
                String fieldName = AccessMethodUtils.getStringConstant(new MutableObject<>(fieldNameExpr));
                if (fieldName != null) {
                    fieldPath.add(fieldName);
                }
            }
        } else if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            // First argument is the record, second argument is the field index
            if (funcExpr.getArguments().size() >= 2) {
                // Recursively process the record argument (for nested field access)
                extractFieldPathRecursive(funcExpr.getArguments().get(0).getValue(), fieldPath);

                // Extract field index from second argument and look up field name
                ILogicalExpression fieldIndexExpr = funcExpr.getArguments().get(1).getValue();
                Integer fieldIndex = AccessMethodUtils.getInt32Constant(new MutableObject<>(fieldIndexExpr));
                if (fieldIndex != null && subTree.getRecordType() != null) {
                    String[] fieldNames = subTree.getRecordType().getFieldNames();
                    if (fieldIndex >= 0 && fieldIndex < fieldNames.length) {
                        String fieldName = fieldNames[fieldIndex];
                        fieldPath.add(fieldName);
                        LOGGER.trace("Resolved field-access-by-index: index {} -> field '{}'", fieldIndex, fieldName);
                    }
                }
            }
        }
    }

    /**
     * Checks if a vector index has all required filter fields in its INCLUDE fields.
     *
     * @param index The vector index to check
     * @param filterFields The set of field names referenced in the filter
     * @return true if all filter fields are in the index's INCLUDE fields
     */
    protected boolean indexHasIncludeFields(Index index, Set<List<String>> filterFields) {
        if (filterFields == null || filterFields.isEmpty()) {
            // No filter fields - index can be used
            return true;
        }

        if (index.getIndexType() != IndexType.VTREE) {
            return false;
        }

        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) index.getIndexDetails();
        List<List<String>> includeFieldNames = vectorDetails.getIncludeFieldNames();

        if (includeFieldNames == null || includeFieldNames.isEmpty()) {
            // Index has no INCLUDE fields - cannot support filters
            LOGGER.trace("Index {} has no INCLUDE fields, cannot support filter", index.getIndexName());
            return false;
        }

        // Check if all filter fields are in INCLUDE fields
        for (List<String> filterField : filterFields) {
            boolean found = false;
            for (List<String> includeField : includeFieldNames) {
                if (includeField.equals(filterField)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.trace("Filter field {} not in index {} INCLUDE fields: {}", filterField, index.getIndexName(),
                        includeFieldNames);
                return false;
            }
        }

        LOGGER.trace("All filter fields found in index {} INCLUDE fields", index.getIndexName());
        return true;
    }

    /**
     * Initializes the subtree from ORDER operator down to DATASOURCE_SCAN.
     *
     * OptimizableOperatorSubTree doesn't recognize ORDER as a valid top operator,
     * so we need to start from ORDER's child (usually ASSIGN).
     */
    protected boolean initializeSubTree() throws AlgebricksException {
        LOGGER.trace("initializeSubTree() called");
        LOGGER.trace("orderRef: {}", orderRef.getValue().getOperatorTag());

        // Get the child of ORDER operator (usually first ASSIGN)
        // OptimizableOperatorSubTree expects to start from operators like SELECT, ASSIGN, etc.
        // not from ORDER
        if (orderOp.getInputs().isEmpty()) {
            LOGGER.trace("ORDER has no children");
            return false;
        }

        Mutable<ILogicalOperator> subTreeRoot = orderOp.getInputs().get(0);
        LOGGER.trace("Starting subtree from: {}", subTreeRoot.getValue().getOperatorTag());

        subTree.initFromSubTree(subTreeRoot);

        boolean hasDataSource = subTree.hasDataSourceScan();
        LOGGER.trace("hasDataSourceScan: {}", hasDataSource);

        if (!hasDataSource) {
            LOGGER.trace("Subtree operators found:");
            AbstractLogicalOperator current = (AbstractLogicalOperator) subTreeRoot.getValue();
            int depth = 0;
            while (current != null && depth < 10) {
                LOGGER.trace("  [{}] {}", depth, current.getOperatorTag());
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
                    LOGGER.trace("Extracted query distance metric: {}", queryDistanceMetric);
                }
            } catch (Exception e) {
                // If we can't extract the metric, continue without metric-aware selection
                // This maintains backward compatibility
                queryDistanceMetric = null;
                LOGGER.trace("Could not extract distance metric from query, using field-only matching");
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
            if (isAnnOrVectorDistanceWithIndexHint(funcExpr)) {
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
                                if (isAnnOrVectorDistanceWithIndexHint(funcExpr)) {
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
     * Checks if a function expression is ANN_DISTANCE or VECTOR_DISTANCE_ARRAY with 4th arg = true.
     *
     * VECTOR_DISTANCE_ARRAY with a boolean `true` 4th argument signals index-driven KNN:
     * vector_distance(field, queryVec, metric, true) → scan all clusters with bidirectional pruning.
     */
    private boolean isAnnOrVectorDistanceWithIndexHint(AbstractFunctionCallExpression funcExpr) {
        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.ANN_DISTANCE)) {
            return true;
        }
        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.EUCLIDEAN_DISTANCE_ARRAY)
                || funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.EUCLIDEAN_SQ_DISTANCE_ARRAY)
                || funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.COSINE_DISTANCE_ARRAY)
                || funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.DOT_DISTANCE_ARRAY)) {
            return true;
        }
        return false;
    }

    /**
     * Checks if an expression is a boolean constant `true`.
     */
    private boolean isConstantTrue(ILogicalExpression expr) {
        if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        ConstantExpression constExpr = (ConstantExpression) expr;
        IAlgebricksConstantValue constVal = constExpr.getValue();
        if (constVal instanceof AsterixConstantValue) {
            IAObject obj = ((AsterixConstantValue) constVal).getObject();
            return obj instanceof ABoolean && ((ABoolean) obj).getBoolean();
        }
        return false;
    }

    /**
     * Chooses the best vector index from candidates.
     * Considers:
     * 1. INCLUDE fields: If query has filter (WHERE clause), index must have all filter fields in INCLUDE
     * 2. Distance metric: Prefers indexes with matching distance metrics.
     * If the query specifies a constant distance metric that does not match the index metadata, compilation fails.
     */
    protected void chooseVectorIndex(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            List<Pair<IAccessMethod, Index>> result) throws AlgebricksException {

        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(VectorIndexAccessMethod.INSTANCE);
        if (analysisCtx == null) {
            return;
        }

        // Check if query has filter predicates
        boolean hasFilter = selectOp != null && filterFieldNames != null && !filterFieldNames.isEmpty();

        // Iterate over candidate vector indexes
        Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt =
                analysisCtx.getIteratorForIndexExprsAndVars();

        Pair<IAccessMethod, Index> exactMatch = null; // Index with matching field, metric, AND INCLUDE fields
        Pair<IAccessMethod, Index> fieldMatch = null; // Index with matching field only (and INCLUDE if needed)

        while (indexIt.hasNext()) {
            Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
            Index index = indexEntry.getKey();

            if (index.getIndexType() == IndexType.VTREE) {
                // If query has filter, check if index has required INCLUDE fields
                if (hasFilter && !indexHasIncludeFields(index, filterFieldNames)) {
                    // Skip this index - it doesn't have required INCLUDE fields for the filter
                    LOGGER.trace("Skipping index {} - missing required INCLUDE fields for filter",
                            index.getIndexName());
                    continue;
                }

                // If query distance metric is available, check for metric compatibility
                if (queryDistanceMetric != null && !queryDistanceMetric.isEmpty()) {
                    String indexMetric = VectorIndexAccessMethod.getIndexDistanceMetric(index);
                    if (queryDistanceMetric.equals(indexMetric)) {
                        // Exact match: field name AND distance metric match (AND INCLUDE fields if needed)
                        exactMatch = new Pair<>(VectorIndexAccessMethod.INSTANCE, index);
                        LOGGER.trace("Found exact match: index {} with metric {}", index.getIndexName(), indexMetric);
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

        // Select best match: exact match preferred, metric mismatch falls back to full scan
        if (exactMatch != null) {
            result.add(exactMatch);
            LOGGER.trace("Selected index with matching distance metric");
        } else if (fieldMatch != null) {
            // Field matches but metric doesn't — skip the index and fall back to full scan (KNN)
            Index idx = fieldMatch.second;
            String indexMetric = VectorIndexAccessMethod.getIndexDistanceMetric(idx);
            LOGGER.warn("Distance metric mismatch: query uses '{}' but index '{}' uses '{}'. "
                    + "Falling back to full scan (KNN).", queryDistanceMetric, idx.getIndexName(), indexMetric);
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

        LOGGER.trace("VECTOR INDEX TOP-K OPTIMIZATION MATCHED");
        LOGGER.trace("Dataset: {}", subTree.getDataset().getDatasetName());
        LOGGER.trace("Vector Index: {}", vectorIndex.getIndexName());
        LOGGER.trace("Limit: {}", limitOp.getMaxObjects().getValue());

        // Call VectorIndexAccessMethod to create the index search plan
        // This creates UNNEST-MAP operator that returns candidate tuples from vector index
        // Pass selectOp so the access method can set selectCondition for filter pushdown
        ILogicalOperator indexSearchOp = VectorIndexAccessMethod.INSTANCE.createIndexSearchPlan(limitRef, orderRef,
                annDistanceExpr, subTree, vectorIndex, analysisCtx, context, selectOp);

        if (indexSearchOp == null) {
            LOGGER.trace("Plan transformation not yet implemented");
            return false;
        }

        // Replace DATASOURCE_SCAN with vector index search
        // ORDER BY ANN_DISTANCE remains unchanged (computes actual distances on candidates)
        // Pattern follows InvertedIndexAccessMethod.applySelectPlanTransformation:498
        subTree.getDataSourceRef().setValue(indexSearchOp);

        LOGGER.trace("VECTOR INDEX TOP-K OPTIMIZATION APPLIED");
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
        selectOp = null;
        selectRef = null;
        filterFieldNames = null;
        subTree.reset();
    }

    @Override
    public Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods() {
        return accessMethods;
    }
}
