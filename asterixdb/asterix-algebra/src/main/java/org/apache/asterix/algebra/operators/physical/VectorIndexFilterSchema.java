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

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;

/**
 * A wrapper schema for vector index filter evaluation that maps variables to
 * their correct physical tuple positions.
 *
 * Vector index physical tuple format: [distance, centroidId, pk, include_fields...]
 * The original opSchema only contains [pk, include_vars...] because distance and
 * centroidId are internal to the index and not exposed to the logical plan.
 *
 * This wrapper adds an offset of 2 to all variable positions so that:
 * - pk variable (position 0 in opSchema) maps to field 2 in physical tuple
 * - include field variables (position 1+ in opSchema) map to field 3+ in physical tuple
 *
 * This allows the TupleFilter to correctly access INCLUDE fields during vector index search
 * without changing the logical output schema (which still only outputs pk to downstream operators).
 */
public class VectorIndexFilterSchema implements IOperatorSchema {

    // Number of internal fields before pk in vector index tuple (distance, centroidId)
    private static final int VECTOR_INDEX_FIELD_OFFSET = 2;

    private final IOperatorSchema delegate;

    public VectorIndexFilterSchema(IOperatorSchema delegate) {
        this.delegate = delegate;
    }

    @Override
    public int findVariable(LogicalVariable var) {
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