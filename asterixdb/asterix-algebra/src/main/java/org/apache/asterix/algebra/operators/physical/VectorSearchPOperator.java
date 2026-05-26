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
package org.apache.asterix.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.PushFilterIntoVectorSearchRule;
import org.apache.asterix.optimizer.rules.am.VectorJobGenParams;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;

/**
 * Contributes the runtime operator for an unnest-map representing a vector index search.
 */
public class VectorSearchPOperator extends IndexSearchPOperator {

    public VectorSearchPOperator(IDataSourceIndex<String, DataSourceId> idx, INodeDomain domain,
            boolean requiresBroadcast) {
        super(idx, domain, requiresBroadcast);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.VECTOR_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AbstractUnnestMapOperator unnestMap = (AbstractUnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException("Vector search unnest expression must be a function call");
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (!funcIdent.equals(BuiltinFunctions.INDEX_SEARCH)) {
            return;
        }

        VectorJobGenParams jobGenParams = new VectorJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());

        // queryVarList layout: [query_vector, k, metric, min_probe_fraction, k_multiplier, searchApproach]
        int[] queryIndexes = getKeyIndexes(jobGenParams.getQueryVarList(), inputSchemas);

        MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
        Dataset dataset = mp.findDataset(jobGenParams.getDatabaseName(), jobGenParams.getDataverseName(),
                jobGenParams.getDatasetName());
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(unnestMap);

        List<LogicalVariable> outputVars = unnestMap.getVariables();
        if (jobGenParams.getRetainInput()) {
            outputVars = new ArrayList<>();
            VariableUtilities.getLiveVariables(unnestMap, outputVars);
        }

        // Determine quantization from vector index metadata
        Index vectorIndex = mp.getIndex(jobGenParams.getDatabaseName(), jobGenParams.getDataverseName(),
                jobGenParams.getDatasetName(), jobGenParams.getIndexName());
        Index.VectorIndexDetails vectorDetails = (Index.VectorIndexDetails) vectorIndex.getIndexDetails();
        AdmObjectNode withObjectNode = vectorDetails.getWithObjectNode();
        String quantization = (withObjectNode != null) ? withObjectNode.getOptionalString("quantization", null) : null;
        boolean isQuantized = (quantization != null);
        int numSecondaryKeys = isQuantized ? 4 : 2;

        // Create tuple filter factory if selectCondition is present (for INCLUDE field filtering)
        // The opSchema only has [pk] because INCLUDE fields are only used for filtering.
        // Filter variables are mapped directly to physical field indexes via annotation.
        ITupleFilterFactory tupleFilterFactory = null;
        if (unnestMap instanceof UnnestMapOperator) {
            UnnestMapOperator unnestMapOp = (UnnestMapOperator) unnestMap;
            if (unnestMapOp.getSelectCondition() != null) {
                // Get filter variable to physical field index mapping from annotation
                @SuppressWarnings("unchecked")
                Map<LogicalVariable, Integer> filterVarToFieldIndex = (Map<LogicalVariable, Integer>) unnestMapOp
                        .getAnnotations().get(PushFilterIntoVectorSearchRule.VECTOR_FILTER_VAR_MAPPING);

                // Get filter variable types from annotation
                @SuppressWarnings("unchecked")
                Map<LogicalVariable, IAType> filterVarTypes = (Map<LogicalVariable, IAType>) unnestMapOp
                        .getAnnotations().get(PushFilterIntoVectorSearchRule.VECTOR_FILTER_VAR_TYPES);

                // Create filter schema with direct mapping for filter-only variables
                // numSecondaryKeys: offset from physical tuple start to PK field
                IOperatorSchema filterSchema =
                        new VectorIndexFilterSchema(opSchema, filterVarToFieldIndex, numSecondaryKeys);

                // Create type environment with filter variable types
                // Pass context so function expressions can use this wrapper for recursive type lookups
                IVariableTypeEnvironment filterTypeEnv =
                        new VectorIndexFilterTypeEnvironment(typeEnv, filterVarTypes, context);

                tupleFilterFactory = mp.createTupleFilterFactory(new IOperatorSchema[] { filterSchema }, filterTypeEnv,
                        unnestMapOp.getSelectCondition().getValue(), context);
            }
        }

        // Extract searchApproach compile-time constant from the upstream ASSIGN. Level-wise epsilon for
        // ANN is sourced from the vector index WITH metadata in MetadataProvider.getVectorSearchRuntime,
        // not from the query tuple.
        int searchApproach = 0; // default: naive
        List<LogicalVariable> queryVarList = jobGenParams.getQueryVarList();
        if (queryVarList.size() > 5) {
            LogicalVariable searchApproachVar = queryVarList.get(5);
            searchApproach = extractIntConstantForVariable(unnestMap, searchApproachVar, 0);
        }

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> vectorSearch = mp.getVectorSearchRuntime(
                builder.getJobSpec(), outputVars, opSchema, typeEnv, context, jobGenParams.getRetainInput(), dataset,
                jobGenParams.getIndexName(), queryIndexes, tupleFilterFactory, searchApproach);

        IOperatorDescriptor opDesc = vectorSearch.first;
        opDesc.setSourceLocation(unnestMap.getSourceLocation());

        builder.contributeHyracksOperator(unnestMap, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, vectorSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }

    /**
     * Extracts an integer constant value for a logical variable by tracing through
     * the ASSIGN operators in the input chain of the given operator.
     *
     * @param op The operator whose inputs to search
     * @param var The variable to find
     * @param defaultValue Default value if extraction fails
     * @return The integer constant value, or defaultValue if not found
     */
    private int extractIntConstantForVariable(ILogicalOperator op, LogicalVariable var, int defaultValue) {
        ILogicalOperator current = op;
        while (current != null && !current.getInputs().isEmpty()) {
            current = current.getInputs().get(0).getValue();
            if (current.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assign = (AssignOperator) current;
                int idx = assign.getVariables().indexOf(var);
                if (idx >= 0) {
                    ILogicalExpression expr = assign.getExpressions().get(idx).getValue();
                    if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                        ConstantExpression constExpr = (ConstantExpression) expr;
                        IAlgebricksConstantValue constVal = constExpr.getValue();
                        if (constVal instanceof AsterixConstantValue) {
                            IAObject obj = ((AsterixConstantValue) constVal).getObject();
                            if (obj instanceof AInt32) {
                                return ((AInt32) obj).getIntegerValue();
                            }
                        }
                    }
                    return defaultValue;
                }
            }
        }
        return defaultValue;
    }
}