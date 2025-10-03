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

package org.apache.hyracks.storage.am.vector.frames;

/**
 * Enumeration of frame types for VectorClusteringTree structures.
 * This replaces the inappropriate use of BTreeLeafFrameType in vector tests.
 */
public enum VectorTreeFrameType {

    /**
     * Standard NSM (N-ary Storage Model) frame for vector clustering leaf pages.
     * Contains cluster entries with format: <cid, centroid, metadata_page_ptr>
     */
    REGULAR_NSM,

    /**
     * Columnar storage frame for vector clustering leaf pages.
     * Optimized for vector operations with column-oriented storage.
     */
    COLUMNAR,

    /**
     * Compressed frame for space-efficient storage of vector data.
     * Uses compression techniques specific to vector clustering.
     */
    COMPRESSED,

    /**
     * Hybrid frame combining NSM and columnar approaches.
     * Adapts storage format based on data characteristics.
     */
    HYBRID
}
