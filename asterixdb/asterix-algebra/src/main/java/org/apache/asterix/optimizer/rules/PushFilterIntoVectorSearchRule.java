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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.am.AccessMethodJobGenParams;
import org.apache.asterix.optimizer.rules.am.AccessMethodUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.rules.InlineVariablesRule;

/**
 * Pushes filter conditions into vector index search when the filter uses INCLUDE fields.
 *
 * This rule MUST run in physicalRewritesTopLevel (AFTER SetClosedRecordConstructorsRule)
 * to bypass type inference issues. See VECTOR_INDEX_FILTER_PUSHDOWN_TYPE_INFERENCE_ISSUE.md.
 *
 * Pattern:
 * <pre>
 *   SELECT (condition on INCLUDE fields)
 *     └── ASSIGN* (optional)
 *           └── PRIMARY_INDEX_UNNEST
 *                 └── ...
 *                       └── VECTOR_INDEX_UNNEST [$pk]
 * </pre>
 *
 * Transforms to:
 * <pre>
 *   ASSIGN* (optional, SELECT removed)
 *     └── PRIMARY_INDEX_UNNEST
 *           └── ...
 *                 └── VECTOR_INDEX_UNNEST [$pk, $includeField1, ...]
 *                       selectCondition: (rewritten to use $includeField1, ...)
 * </pre>
 *
 * Key insight: We create new variables for INCLUDE fields that VECTOR_INDEX_UNNEST produces,
 * then rewrite the filter condition to use these variables instead of $row.getField(...).
 */
public class PushFilterIntoVectorSearchRule implements IAlgebraicRewriteRule {

    /**
     * Annotation key for filter variable to physical field index mapping.
     * Value type: Map&lt;LogicalVariable, Integer&gt;
     */
    public static final String VECTOR_FILTER_VAR_MAPPING = "VECTOR_FILTER_VAR_MAPPING";

    /**
     * Annotation key for filter variable to type mapping.
     * Value type: Map&lt;LogicalVariable, IAType&gt;
     */
    public static final String VECTOR_FILTER_VAR_TYPES = "VECTOR_FILTER_VAR_TYPES";

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();

        // Step 1: Find SELECT operator
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        // Avoid applying the rule multiple times
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        SelectOperator selectOp = (SelectOperator) op;

        // Step 2: Find VECTOR_INDEX_UNNEST in the subtree
        VectorSearchInfo searchInfo = findVectorIndexUnnest(selectOp, context);
        if (searchInfo == null) {
            return false;
        }

        System.err.println("=== PushFilterIntoVectorSearchRule: Found vector index ===");
        System.err.println("=== Index: " + searchInfo.indexName + " ===");

        // Step 3: Check for INCLUDE fields
        if (searchInfo.includeFieldNames == null || searchInfo.includeFieldNames.isEmpty()) {
            System.err.println("=== No INCLUDE fields, skipping ===");
            return false;
        }

        System.err.println("=== INCLUDE fields: " + searchInfo.includeFieldNames + " ===");

        // Step 4: Inline the filter condition to get field-access expressions
        ILogicalExpression selectCondition = selectOp.getCondition().getValue();
        MutableObject<ILogicalExpression> conditionRef = new MutableObject<>(selectCondition.cloneExpression());

        Set<LogicalVariable> usedVariables = new HashSet<>();
        conditionRef.getValue().getUsedVariables(usedVariables);

        // Inline variables from ASSIGN operators
        ILogicalOperator child = selectOp.getInputs().get(0).getValue();
        InlineVariablesRule.InlineVariablesVisitor inlineVisitor = null;
        Map<LogicalVariable, ILogicalExpression> varAssignRhs = new HashMap<>();

        for (; child.getOperatorTag() == LogicalOperatorTag.ASSIGN; child = child.getInputs().get(0).getValue()) {
            varAssignRhs.clear();
            AssignOperator assignOp = (AssignOperator) child;
            extractInlinableVariables(assignOp, usedVariables, varAssignRhs);

            if (!varAssignRhs.isEmpty()) {
                if (inlineVisitor == null) {
                    inlineVisitor = new InlineVariablesRule.InlineVariablesVisitor(varAssignRhs, null);
                    inlineVisitor.setContext(context);
                    inlineVisitor.setOperator(selectOp);
                }
                if (!inlineVisitor.transform(conditionRef)) {
                    break;
                }
                usedVariables.clear();
                conditionRef.getValue().getUsedVariables(usedVariables);
            }
        }

        System.err.println("=== Inlined condition: " + conditionRef.getValue() + " ===");

        // Step 5: Extract field names from the filter
        Set<String> filterFieldNames = new HashSet<>();
        extractFieldNames(conditionRef.getValue(), searchInfo.recordType, filterFieldNames);

        System.err.println("=== Filter uses fields: " + filterFieldNames + " ===");

        if (filterFieldNames.isEmpty()) {
            System.err.println("=== No field accesses found in filter, skipping ===");
            return false;
        }

        // Step 6: Verify all filter fields are in INCLUDE list
        Map<String, Integer> includeFieldIndex = buildIncludeFieldIndex(searchInfo.includeFieldNames);

        for (String fieldName : filterFieldNames) {
            if (!includeFieldIndex.containsKey(fieldName)) {
                System.err.println("=== Field '" + fieldName + "' not in INCLUDE list, skipping ===");
                return false;
            }
        }

        // Step 7: Create new variables for INCLUDE fields used in filter
        // These variables are ONLY used for filter evaluation, NOT added to output
        Map<String, LogicalVariable> fieldToNewVar = new HashMap<>();
        Map<LogicalVariable, Integer> filterVarToFieldIndex = new HashMap<>();
        Map<LogicalVariable, IAType> filterVarToType = new HashMap<>();

        // Physical tuple format depends on quantization:
        // Non-quantized: [distance(0), centroidId(1), pk(2), include_fields(3+)]
        // Quantized:     [distance(0), qDist(1), qEmbed(2), centroidId(3), pk(4), include_fields(5+)]
        int numSecondaryKeys = searchInfo.isQuantized ? 4 : 2;
        int includeFieldPhysicalIndex = numSecondaryKeys + 1; // +1 for single PK

        for (List<String> fieldPath : searchInfo.includeFieldNames) {
            String fieldName = fieldPath.get(fieldPath.size() - 1);

            // Only create variable if this field is used in the filter
            if (filterFieldNames.contains(fieldName)) {
                LogicalVariable newVar = context.newVar();
                fieldToNewVar.put(fieldName, newVar);

                // Store direct mapping: variable -> physical tuple field index
                filterVarToFieldIndex.put(newVar, includeFieldPhysicalIndex);

                // Store type for filter compilation
                IAType fieldType = searchInfo.recordType.getSubFieldType(fieldPath);
                filterVarToType.put(newVar, fieldType);

                System.err.println("=== Created " + newVar + " for field '" + fieldName + "' -> physical field "
                        + includeFieldPhysicalIndex + " ===");
            }

            includeFieldPhysicalIndex++;
        }

        // Step 8: Store filter variable mapping as annotation (NOT added to output vars)
        // This allows VectorSearchPOperator to create correct filter schema
        searchInfo.vectorUnnest.getAnnotations().put(VECTOR_FILTER_VAR_MAPPING, filterVarToFieldIndex);
        searchInfo.vectorUnnest.getAnnotations().put(VECTOR_FILTER_VAR_TYPES, filterVarToType);

        // Step 9: Rewrite filter to use new variables
        ILogicalExpression rewrittenCondition =
                rewriteFieldAccess(conditionRef.getValue(), fieldToNewVar, searchInfo.recordType);

        System.err.println("=== Rewritten condition: " + rewrittenCondition + " ===");

        // Step 10: Set selectCondition on VECTOR_INDEX_UNNEST
        searchInfo.vectorUnnest.setSelectCondition(new MutableObject<>(rewrittenCondition));

        // Step 11: Remove SELECT operator
        opRef.setValue(selectOp.getInputs().get(0).getValue());

        // Mark as applied
        context.addToDontApplySet(this, op);

        // Recompute types
        OperatorPropertiesUtil.typeOpRec(opRef, context);

        System.err.println("=== Successfully pushed filter into vector search ===");
        return true;
    }

    /**
     * Information about a vector index search found in the plan.
     */
    private static class VectorSearchInfo {
        UnnestMapOperator vectorUnnest;
        String indexName;
        List<List<String>> includeFieldNames;
        ARecordType recordType;
        boolean isQuantized;
    }

    /**
     * Finds VECTOR_INDEX_UNNEST below the SELECT operator.
     */
    private VectorSearchInfo findVectorIndexUnnest(SelectOperator selectOp, IOptimizationContext context)
            throws AlgebricksException {

        // Walk down through operators to find VECTOR_INDEX_UNNEST
        ILogicalOperator current = selectOp.getInputs().get(0).getValue();

        // Skip ASSIGN operators
        while (current.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            current = current.getInputs().get(0).getValue();
        }

        return searchForVectorUnnest(current, context);
    }

    /**
     * Recursively searches for VECTOR_INDEX_UNNEST.
     */
    private VectorSearchInfo searchForVectorUnnest(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {

        if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnest = (UnnestMapOperator) op;
            ILogicalExpression expr = unnest.getExpressionRef().getValue();

            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;

                if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.INDEX_SEARCH)) {
                    AccessMethodJobGenParams params = new AccessMethodJobGenParams();
                    params.readFromFuncArgs(funcExpr.getArguments());

                    if (params.getIndexType() == IndexType.VECTOR) {
                        // Found vector index! Check if it already has selectCondition
                        if (unnest.getSelectCondition() != null) {
                            System.err.println("=== Vector index already has selectCondition ===");
                            return null;
                        }

                        return buildSearchInfo(unnest, params, context);
                    }
                }
            }
        }

        // Continue searching in inputs
        for (Mutable<ILogicalOperator> inputRef : op.getInputs()) {
            VectorSearchInfo result = searchForVectorUnnest(inputRef.getValue(), context);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    /**
     * Builds VectorSearchInfo from the found vector index.
     */
    private VectorSearchInfo buildSearchInfo(UnnestMapOperator unnest, AccessMethodJobGenParams params,
            IOptimizationContext context) throws AlgebricksException {

        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();

        Dataset dataset = mp.findDataset(params.getDatabaseName(), params.getDataverseName(), params.getDatasetName());
        if (dataset == null) {
            return null;
        }

        Index index = mp.getIndex(params.getDatabaseName(), params.getDataverseName(), params.getDatasetName(),
                params.getIndexName());
        if (index == null || index.getIndexType() != IndexType.VECTOR) {
            return null;
        }

        Index.VectorIndexDetails details = (Index.VectorIndexDetails) index.getIndexDetails();

        // Determine quantization from WITH clause description field
        AdmObjectNode withObjectNode = details.getWithObjectNode();
        String description = (withObjectNode != null) ? withObjectNode.getOptionalString("description", null) : null;

        VectorSearchInfo info = new VectorSearchInfo();
        info.vectorUnnest = unnest;
        info.indexName = params.getIndexName();
        info.includeFieldNames = details.getIncludeFieldNames();
        info.isQuantized = (description != null);
        info.recordType = (ARecordType) mp.findType(dataset.getItemTypeDatabaseName(),
                dataset.getItemTypeDataverseName(), dataset.getItemTypeName());

        return info;
    }

    /**
     * Extracts variables from ASSIGN that can be inlined.
     */
    private void extractInlinableVariables(AssignOperator assignOp, Set<LogicalVariable> targetVars,
            Map<LogicalVariable, ILogicalExpression> outMap) {
        List<LogicalVariable> vars = assignOp.getVariables();
        List<Mutable<ILogicalExpression>> exprs = assignOp.getExpressions();

        for (int i = 0; i < vars.size(); i++) {
            LogicalVariable var = vars.get(i);
            if (targetVars.contains(var)) {
                ILogicalExpression expr = exprs.get(i).getValue();
                if (expr.isFunctional()) {
                    outMap.put(var, expr);
                }
            }
        }
    }

    /**
     * Builds a map from field name to its index in the INCLUDE list.
     */
    private Map<String, Integer> buildIncludeFieldIndex(List<List<String>> includeFieldNames) {
        Map<String, Integer> result = new HashMap<>();
        for (int i = 0; i < includeFieldNames.size(); i++) {
            List<String> path = includeFieldNames.get(i);
            String fieldName = path.get(path.size() - 1);
            result.put(fieldName, i);
        }
        return result;
    }

    /**
     * Extracts field names from field-access expressions in the filter.
     */
    private void extractFieldNames(ILogicalExpression expr, ARecordType recordType, Set<String> fieldNames) {
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        if (fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
            if (funcExpr.getArguments().size() >= 2) {
                String fieldName = AccessMethodUtils.getStringConstant(funcExpr.getArguments().get(1));
                if (fieldName != null) {
                    fieldNames.add(fieldName);
                }
            }
        } else if (fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            if (funcExpr.getArguments().size() >= 2 && recordType != null) {
                Integer idx = AccessMethodUtils.getInt32Constant(funcExpr.getArguments().get(1));
                if (idx != null) {
                    String[] names = recordType.getFieldNames();
                    if (idx >= 0 && idx < names.length) {
                        fieldNames.add(names[idx]);
                    }
                }
            }
        }

        // Recurse into arguments
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            extractFieldNames(arg.getValue(), recordType, fieldNames);
        }
    }

    /**
     * Rewrites field-access expressions to use new INCLUDE field variables.
     * Example: gt($row.getField(2), 2000) -> gt($year, 2000)
     */
    private ILogicalExpression rewriteFieldAccess(ILogicalExpression expr, Map<String, LogicalVariable> fieldToVar,
            ARecordType recordType) {

        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return expr;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();

        // Check if this is a field access to replace
        String fieldName = null;

        if (fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
            if (funcExpr.getArguments().size() >= 2) {
                fieldName = AccessMethodUtils.getStringConstant(funcExpr.getArguments().get(1));
            }
        } else if (fid.equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            if (funcExpr.getArguments().size() >= 2 && recordType != null) {
                Integer idx = AccessMethodUtils.getInt32Constant(funcExpr.getArguments().get(1));
                if (idx != null) {
                    String[] names = recordType.getFieldNames();
                    if (idx >= 0 && idx < names.length) {
                        fieldName = names[idx];
                    }
                }
            }
        }

        // Replace field access with variable reference
        if (fieldName != null && fieldToVar.containsKey(fieldName)) {
            LogicalVariable newVar = fieldToVar.get(fieldName);
            VariableReferenceExpression varRef = new VariableReferenceExpression(newVar);
            varRef.setSourceLocation(funcExpr.getSourceLocation());
            return varRef;
        }

        // Recurse into arguments
        List<Mutable<ILogicalExpression>> newArgs = new ArrayList<>();
        boolean changed = false;

        for (Mutable<ILogicalExpression> argRef : funcExpr.getArguments()) {
            ILogicalExpression newArg = rewriteFieldAccess(argRef.getValue(), fieldToVar, recordType);
            newArgs.add(new MutableObject<>(newArg));
            if (newArg != argRef.getValue()) {
                changed = true;
            }
        }

        if (changed) {
            ScalarFunctionCallExpression newFunc =
                    new ScalarFunctionCallExpression(funcExpr.getFunctionInfo(), newArgs);
            newFunc.setSourceLocation(funcExpr.getSourceLocation());
            return newFunc;
        }

        return expr;
    }
}
