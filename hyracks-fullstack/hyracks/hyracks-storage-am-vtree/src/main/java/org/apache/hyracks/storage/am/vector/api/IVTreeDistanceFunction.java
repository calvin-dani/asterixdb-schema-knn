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
 * software distributed under this License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.vector.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Interface for vector distance calculation functions.
 * This interface allows passing distance functions from AsterixDB to Hyracks modules
 * without creating circular dependencies.
 */
@FunctionalInterface
public interface IVTreeDistanceFunction {
    /**
     * Calculate distance between two vectors.
     *
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Distance value
     * @throws HyracksDataException if calculation fails
     */
    double apply(double[] vector1, double[] vector2) throws HyracksDataException;
}
