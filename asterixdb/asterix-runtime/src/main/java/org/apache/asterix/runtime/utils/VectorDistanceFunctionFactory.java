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
package org.apache.asterix.runtime.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Factory for creating IVectorDistanceFunction implementations that wrap VectorDistanceCalculation methods.
 * This factory allows passing VectorDistanceCalculation implementations from AsterixDB to Hyracks modules
 * without creating circular dependencies.
 * The factory is serializable and can be passed through the job pipeline.
 */
public class VectorDistanceFunctionFactory implements IVTreeDistanceFunctionFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();

    private static class EuclideanDistanceFunction implements IVTreeDistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceCalculation.euclidean(a, b);
        }
    }

    private static class EuclideanSquaredDistanceFunction implements IVTreeDistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceCalculation.euclideanSquared(a, b);
        }
    }

    private static class CosineDistanceFunction implements IVTreeDistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceCalculation.cosineDistance(a, b);
        }
    }

    private static class DotProductDistanceFunction implements IVTreeDistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        /** Returns -dot(a,b) so that minimizing "distance" equals maximizing dot product (MIPS). */
        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return -VectorDistanceCalculation.dotProduct(a, b);
        }
    }

    // Metric-string -> index distance function, keyed by the same lowercase hash used at lookup. The
    // alias set is owned by VectorSimilarityMetric, shared with ANNDistanceDescriptor, so the two can
    // no longer drift (a metric resolving in one but not the other would NPE on the index search path).
    private static final Map<Integer, IVTreeDistanceFunction> DISTANCE_MAP = buildDistanceMap();

    private static Map<Integer, IVTreeDistanceFunction> buildDistanceMap() {
        Map<Integer, IVTreeDistanceFunction> map = new HashMap<>();
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            IVTreeDistanceFunction function = functionFor(metric);
            for (String alias : metric.aliases()) {
                map.put(UTF8StringPointable.generateUTF8Pointable(alias).hash(), function);
            }
        }
        return map;
    }

    private static IVTreeDistanceFunction functionFor(VectorSimilarityMetric metric) {
        switch (metric) {
            case EUCLIDEAN:
                return new EuclideanDistanceFunction();
            case EUCLIDEAN_SQUARED:
                return new EuclideanSquaredDistanceFunction();
            case COSINE:
                return new CosineDistanceFunction();
            case DOT:
                return new DotProductDistanceFunction();
            default:
                throw new IllegalStateException("Unhandled vector similarity metric: " + metric);
        }
    }

    /**
     * Convert distance metric string to IVectorDistanceFunction implementation.
     * Uses VectorDistanceCalculation methods wrapped in IVectorDistanceFunction.
     * @param distanceMetric Distance metric string (e.g., "euclidean", "cosine similarity", etc.)
     * @return IVTreeDistanceFunction implementation wrapping VectorDistanceCalculation, or Euclidean as default
     */
    @Override
    public IVTreeDistanceFunction createDistanceFunction(String distanceMetric) {

        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(distanceMetric.toLowerCase());
        IVTreeDistanceFunction func = DISTANCE_MAP
                .get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(), formatPointable.getStartOffset()));

        if (func == null) {
            LOGGER.warn("Unsupported distance metric '{}', defaulting to euclidean. Ensure DISTANCE_MAP "
                    + "stays in sync with ANNDistanceDescriptor.DISTANCE_MAP.", distanceMetric);
            return new EuclideanDistanceFunction();
        }

        return func;
    }

    // Stateless factory: the persisted form is just the class identifier.
    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("unused")
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return new VectorDistanceFunctionFactory();
    }
}
