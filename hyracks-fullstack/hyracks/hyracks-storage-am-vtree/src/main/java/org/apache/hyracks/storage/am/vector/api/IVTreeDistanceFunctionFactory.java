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
package org.apache.hyracks.storage.am.vector.api;

import java.io.Serializable;

import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Factory for {@link IVTreeDistanceFunction} instances. Supplied to the storage layer at
 * construction time (persisted on the local resource so it survives NC restart) and, for
 * query-time overrides, through the index-access-parameters map (key {@link #IAP_KEY}).
 * <p>
 * Lets the {@code asterix-runtime} layer plug in distance implementations without the
 * {@code hyracks-storage-am-vtree} module depending on AsterixDB types. Extends
 * {@link IJsonSerializable} so the concrete factory can be persisted on the local resource.
 */
public interface IVTreeDistanceFunctionFactory extends Serializable, IJsonSerializable {

    /** Index-access-parameters key under which a factory instance is passed to the storage layer. */
    String IAP_KEY = "VD_FUN_FACTORY";

    /**
     * Build the distance function for the metric this factory was created with. The metric is fixed at
     * index creation (it is determined by the embedding model) and baked into the concrete factory, so
     * Hyracks never passes or is aware of a metric string. Must not return {@code null}.
     */
    IVTreeDistanceFunction createDistanceFunction();

    /**
     * Distance used to compare a vector against a stored centroid (tree walk, k-means assignment,
     * insert grouping). Defaults to {@link #createDistanceFunction()}. DOT overrides this with
     * {@code -(v · c) / ||c||} so high-norm means do not steal partitions; leaf scoring stays on
     * {@link #createDistanceFunction()} ({@code -dot}).
     */
    @AiProvenance(agent = AiProvenance.Agent.GROK_4_6, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Centroid vs leaf distance split for DOT spherical routing")
    default IVTreeDistanceFunction createCentroidDistanceFunction() {
        return createDistanceFunction();
    }
}
