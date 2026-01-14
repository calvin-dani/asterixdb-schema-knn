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

import java.util.Iterator;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;

/**
 * A wrapper schema for vector index filter evaluation that maps variables to
 * their correct physical tuple positions.
 *
 * Vector index physical tuple format: [distance, centroidId, pk, include_fields...]
 * The original opSchema only contains [pk] because:
 * - distance and centroidId are internal to the index
 * - INCLUDE fields are only used for filtering, not output
 *
 * This wrapper supports two mapping modes:
 * 1. Variables in opSchema (like pk): position + 2 offset
 * 2. Filter-only variables (INCLUDE fields): direct mapping via filterVarToFieldIndex
 *
 * This allows the TupleFilter to correctly access INCLUDE fields during vector index search
 * without exposing them to downstream operators (which only need pk for primary index lookup).
 */
public class VectorIndexFilterSchema implements IOperatorSchema {

    // Number of internal fields before pk in vector index tuple (distance, centroidId)
    private static final int VECTOR_INDEX_FIELD_OFFSET = 2;

    private final IOperatorSchema delegate;

    // Direct mapping for filter-only variables (INCLUDE fields not in output schema)
    // Maps LogicalVariable -> physical tuple field index
    private final Map<LogicalVariable, Integer> filterVarToFieldIndex;

    /**
     * Constructor without direct mapping (backward compatibility).
     * All variables must be in the delegate schema.
     */
    public VectorIndexFilterSchema(IOperatorSchema delegate) {
        this(delegate, null);
    }

    /**
     * Constructor with direct mapping for filter-only variables.
     *
     * @param delegate The operator output schema (contains pk)
     * @param filterVarToFieldIndex Direct mapping for INCLUDE field variables to physical field indexes
     */
    public VectorIndexFilterSchema(IOperatorSchema delegate, Map<LogicalVariable, Integer> filterVarToFieldIndex) {
        this.delegate = delegate;
        this.filterVarToFieldIndex = filterVarToFieldIndex;
    }

    @Override
    public int findVariable(LogicalVariable var) {
        // First check direct mapping for filter-only variables
        if (filterVarToFieldIndex != null && filterVarToFieldIndex.containsKey(var)) {
            return filterVarToFieldIndex.get(var);
        }

        // Fall back to delegate schema with offset
        int originalPos = delegate.findVariable(var);
        if (originalPos < 0) {
            // Variable not found
            return originalPos;
        }
        // Add offset to account for distance and centroidId fields
        return originalPos + VECTOR_INDEX_FIELD_OFFSET;
    }

    @Override
    public LogicalVariable getVariable(int index) {
        // Adjust index back when retrieving variable
        if (index < VECTOR_INDEX_FIELD_OFFSET) {
            return null; // distance and centroidId have no logical variables
        }
        return delegate.getVariable(index - VECTOR_INDEX_FIELD_OFFSET);
    }

    @Override
    public int getSize() {
        // Include the offset fields in the size
        return delegate.getSize() + VECTOR_INDEX_FIELD_OFFSET;
    }

    // The following methods delegate to the original schema
    // They are not used during filter creation but must be implemented

    @Override
    public void addAllVariables(IOperatorSchema source) {
        delegate.addAllVariables(source);
    }

    @Override
    public void addAllNewVariables(IOperatorSchema source) {
        delegate.addAllNewVariables(source);
    }

    @Override
    public int addVariable(LogicalVariable var) {
        return delegate.addVariable(var) + VECTOR_INDEX_FIELD_OFFSET;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Iterator<LogicalVariable> iterator() {
        return delegate.iterator();
    }
}