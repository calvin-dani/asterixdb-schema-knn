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

import java.util.List;
import java.util.Map;

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;

/**
 * A wrapper type environment for vector index filter compilation.
 *
 * Filter-only variables (INCLUDE fields) are not in the operator's output variables,
 * so the regular type environment doesn't know their types. This wrapper provides
 * types for filter-only variables from the annotation map while delegating other
 * variable lookups to the original type environment.
 */
public class VectorIndexFilterTypeEnvironment implements IVariableTypeEnvironment {

    private final IVariableTypeEnvironment delegate;
    private final Map<LogicalVariable, IAType> filterVarTypes;

    /**
     * Creates a wrapper type environment with filter variable types.
     *
     * @param delegate The original type environment
     * @param filterVarTypes Map from filter-only variables to their types
     */
    public VectorIndexFilterTypeEnvironment(IVariableTypeEnvironment delegate,
            Map<LogicalVariable, IAType> filterVarTypes) {
        this.delegate = delegate;
        this.filterVarTypes = filterVarTypes;
    }

    @Override
    public Object getVarType(LogicalVariable var) throws AlgebricksException {
        // First check if this is a filter-only variable
        if (filterVarTypes != null && filterVarTypes.containsKey(var)) {
            return filterVarTypes.get(var);
        }
        // Fall back to delegate
        return delegate.getVarType(var);
    }

    @Override
    public Object getVarType(LogicalVariable var, List<LogicalVariable> nonMissableVariables,
            List<List<LogicalVariable>> correlatedMissableVariableLists, List<LogicalVariable> nonNullableVariables,
            List<List<LogicalVariable>> correlatedNullableVariableLists) throws AlgebricksException {
        // First check if this is a filter-only variable
        if (filterVarTypes != null && filterVarTypes.containsKey(var)) {
            return filterVarTypes.get(var);
        }
        // Fall back to delegate
        return delegate.getVarType(var, nonMissableVariables, correlatedMissableVariableLists, nonNullableVariables,
                correlatedNullableVariableLists);
    }

    @Override
    public void setVarType(LogicalVariable var, Object type) {
        delegate.setVarType(var, type);
    }

    @Override
    public Object getType(ILogicalExpression expr) throws AlgebricksException {
        // Check if expression is a variable reference to a filter-only variable
        if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
            LogicalVariable var = varRef.getVariableReference();
            if (filterVarTypes != null && filterVarTypes.containsKey(var)) {
                return filterVarTypes.get(var);
            }
        }
        // Fall back to delegate
        return delegate.getType(expr);
    }

    @Override
    public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) throws AlgebricksException {
        return delegate.substituteProducedVariable(v1, v2);
    }
}
