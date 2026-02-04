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

package org.apache.hyracks.storage.am.vector;

import org.apache.hyracks.storage.am.common.CheckTuple;

/**
 * CheckTuple for VCTree (Vector Clustering Tree) indexes.
 *
 * VCTree tuple format in index data pages:
 * [distance_to_centroid, centroidId, primary_key_fields...]
 *
 * Key design notes:
 * - Vector embedding is NOT stored in VCTree index (lives in primary dataset)
 * - Only metadata (distance to centroid, cluster ID) and primary keys are stored
 * - distance_to_centroid is the distance from the vector to its assigned cluster centroid
 * - This is different from distance to query vector (which is computed at SQL++ layer)
 *
 * Equality semantics:
 * - equals() and hashCode() compare ONLY primary key fields
 * - Distance and centroidId are tracked for validation but not used for equality
 *
 * Example tuple with schema Movie(id: int64):
 * - Primary key: id
 * - Tuple format: [distance: double, centroidId: int, id: long]
 * - Field indices: [0: distance, 1: centroidId, 2: id (PK)]
 * - equals() compares only field 2 (id)
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class VCTreeCheckTuple extends CheckTuple {

    // Number of primary key fields (used for equals/hashCode)
    private final int numPrimaryKeyFields;

    // Field index constants
    private static final int DISTANCE_FIELD_INDEX = 0;
    private static final int CENTROID_ID_FIELD_INDEX = 1;
    private static final int FIRST_PK_FIELD_INDEX = 2;

    /**
     * Create a VCTreeCheckTuple.
     *
     * @param numFields Total number of fields in the tuple:
     *                  1 (distance) + 1 (centroidId) + numPrimaryKeyFields
     * @param numPrimaryKeyFields Number of primary key fields (used for comparison)
     */
    public VCTreeCheckTuple(int numFields, int numPrimaryKeyFields) {
        super(numFields, numPrimaryKeyFields);
        this.numPrimaryKeyFields = numPrimaryKeyFields;
    }

    /**
     * Get distance to centroid (field 0).
     * Useful for validation and debugging.
     */
    public double getDistanceToCentroid() {
        if (fields[DISTANCE_FIELD_INDEX] instanceof Double) {
            return (Double) fields[DISTANCE_FIELD_INDEX];
        }
        throw new IllegalStateException("Distance field is not a Double");
    }

    /**
     * Set distance to centroid (field 0).
     * Called after insertion to track the computed distance.
     */
    public void setDistanceToCentroid(double distance) {
        fields[DISTANCE_FIELD_INDEX] = distance;
    }

    /**
     * Get centroid ID (field 1).
     * Useful for validation and debugging.
     */
    public int getCentroidId() {
        if (fields[CENTROID_ID_FIELD_INDEX] instanceof Integer) {
            return (Integer) fields[CENTROID_ID_FIELD_INDEX];
        }
        throw new IllegalStateException("Centroid ID field is not an Integer");
    }

    /**
     * Set centroid ID (field 1).
     * Called after insertion to track which cluster the tuple was assigned to.
     */
    public void setCentroidId(int centroidId) {
        fields[CENTROID_ID_FIELD_INDEX] = centroidId;
    }

    /**
     * Compare two VCTreeCheckTuples for equality.
     * Only primary key fields are compared (fields [2, 2+numPrimaryKeyFields)).
     * Distance and centroidId are ignored.
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof VCTreeCheckTuple) {
            VCTreeCheckTuple other = (VCTreeCheckTuple) o;

            // Compare only primary key fields
            for (int i = 0; i < numPrimaryKeyFields; i++) {
                int fieldIndex = FIRST_PK_FIELD_INDEX + i;
                int cmp = fields[fieldIndex].compareTo(other.getField(fieldIndex));
                if (cmp != 0) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Compute hash code based on primary key fields only.
     */
    @Override
    public int hashCode() {
        int result = 17;
        for (int i = 0; i < numPrimaryKeyFields; i++) {
            int fieldIndex = FIRST_PK_FIELD_INDEX + i;
            result = 31 * result + (fields[fieldIndex] != null ? fields[fieldIndex].hashCode() : 0);
        }
        return result;
    }

    /**
     * String representation showing all fields.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VCTreeCheckTuple[");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            // Label fields for clarity
            if (i == DISTANCE_FIELD_INDEX) {
                sb.append("dist=");
            } else if (i == CENTROID_ID_FIELD_INDEX) {
                sb.append("cid=");
            } else {
                sb.append("pk").append(i - FIRST_PK_FIELD_INDEX).append("=");
            }
            sb.append(fields[i]);
        }
        sb.append("]");
        return sb.toString();
    }
}
