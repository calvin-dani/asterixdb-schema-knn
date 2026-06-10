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

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Factory for {@link IVTreeDistanceFunction} instances, supplied to the storage layer through
 * the index-access-parameters map (key {@link #IAP_KEY}).
 * <p>
 * Lets the {@code asterix-runtime} layer plug in distance implementations that wrap its own
 * vector-math utilities, without the {@code hyracks-storage-am-vtree} module needing to know
 * about AsterixDB types. Replaces the previous reflective lookup against a {@code Serializable}
 * factory object.
 */
public interface IVTreeDistanceFunctionFactory extends Serializable {

    /** Index-access-parameters key under which a factory instance is passed to the storage layer. */
    String IAP_KEY = "VECTOR_DISTANCE_FUNCTION_FACTORY";

    /**
     * Build a distance function for the given metric. Implementations should treat unknown
     * metrics as a programming error or fall back to a sensible default, but must not return
     * {@code null}.
     *
     * @param distanceMetric e.g. {@code "euclidean"}, {@code "euclidean_squared"}, {@code "cosine"}, {@code "dot"}
     */
    IVTreeDistanceFunction createDistanceFunction(String distanceMetric) throws HyracksDataException;
}
