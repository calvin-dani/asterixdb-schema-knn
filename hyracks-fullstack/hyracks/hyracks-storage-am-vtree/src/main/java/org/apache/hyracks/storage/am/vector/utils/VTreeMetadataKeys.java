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
 * Keys for the VTree component metadata frame. Written by the static-structure builder,
 * the bulk loader, and the flush loader; read back by the bulk loader and {@link
 * org.apache.hyracks.storage.am.vector.impls.VTree} when a component is opened. The
 * writer and reader sides must agree, so the key strings live here as a single source
 * of truth.
 */
public final class VTreeMetadataKeys {

    private VTreeMetadataKeys() {
    }

    /** Number of leaf centroids in the component. */
    public static final String NUM_LEAF_CENTROIDS = "num_leaf_centroids";

    /** Centroid id of the first leaf centroid (BFS-from-root numbering). */
    public static final String FIRST_LEAF_CENTROID_ID = "first_leaf_centroid_id";
}
