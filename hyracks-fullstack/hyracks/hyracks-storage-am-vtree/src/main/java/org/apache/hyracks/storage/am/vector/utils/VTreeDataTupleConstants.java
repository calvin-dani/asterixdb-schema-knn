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
package org.apache.hyracks.storage.am.vector.utils;

/**
 * Constants for VTree data tuple field layout.
 *
 * Two formats are supported, determined by the presence of a "description" field
 * in the DDL WITH clause at index creation time:
 *
 * Non-quantized format (description == null):
 *   | distance(0) | centroidId(1) | PK...(2+) | includes...(N+) |
 *
 * Quantized format (description != null):
 *   | distance(0) | centroidId(1) | quantized_distance(2) | quantized_embedding(3) | PK...(4+) | includes...(N+) |
 */
public final class VTreeDataTupleConstants {

    private VTreeDataTupleConstants() {
    }

    // --- Page-chain sentinels ---

    /** Value stored in a frame's next-page pointer to mark the end of a page chain. */
    public static final int NO_NEXT_PAGE = -1;

    /** Value stored in a per-cluster directory-page slot before a real page id is assigned. */
    public static final int UNASSIGNED_DIR_PAGE = -1;

    // --- Non-quantized format field offsets (description == null) ---

    /** Distance field index (same in both formats). */
    public static final int DISTANCE_FIELD = 0;

    /** Centroid ID field index in non-quantized format. */
    public static final int NQ_CENTROID_ID_FIELD = 1;

    /** First primary key field index in non-quantized format. */
    public static final int NQ_PK_START_FIELD = 2;

    /** Number of secondary (non-PK, non-include) fields in non-quantized format. */
    public static final int NQ_NUM_SECONDARY_FIELDS = 2;

    // --- Quantized format field offsets (description != null) ---
    // Layout: [distance(0), centroidId(1), quantized_distance(2), quantized_embedding(3), PK...(4+)]
    // This matches createTransformedTuple() in VTreeBulkLoaderAndGroupingOperatorDescriptor.

    /** Centroid ID field index in quantized format. */
    public static final int Q_CENTROID_ID_FIELD = 1;

    /** Quantized distance field index in quantized format. */
    public static final int Q_QUANTIZED_DISTANCE_FIELD = 2;

    /** Quantized embedding field index in quantized format. */
    public static final int Q_QUANTIZED_EMBEDDING_FIELD = 3;

    /** First primary key field index in quantized format. */
    public static final int Q_PK_START_FIELD = 4;

    /** Number of secondary (non-PK, non-include) fields in quantized format. */
    public static final int Q_NUM_SECONDARY_FIELDS = 4;

    /**
     * Returns the PK start field index based on whether quantization is enabled.
     *
     * @param isQuantized true if the index was created with a description field
     * @return the field index where primary keys start
     */
    public static int getPkStartField(boolean isQuantized) {
        return isQuantized ? Q_PK_START_FIELD : NQ_PK_START_FIELD;
    }

    /**
     * Returns the number of secondary fields based on whether quantization is enabled.
     *
     * @param isQuantized true if the index was created with a description field
     * @return the number of secondary fields (before PKs and includes)
     */
    public static int getNumSecondaryFields(boolean isQuantized) {
        return isQuantized ? Q_NUM_SECONDARY_FIELDS : NQ_NUM_SECONDARY_FIELDS;
    }

    /**
     * Returns the centroid ID field index based on whether quantization is enabled.
     *
     * @param isQuantized true if the index was created with a description field
     * @return the field index of the centroid ID
     */
    public static int getCentroidIdField(boolean isQuantized) {
        return isQuantized ? Q_CENTROID_ID_FIELD : NQ_CENTROID_ID_FIELD;
    }
}
