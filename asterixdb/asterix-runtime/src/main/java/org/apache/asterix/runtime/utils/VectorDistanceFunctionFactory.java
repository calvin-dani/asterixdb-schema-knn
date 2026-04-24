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

import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunctionDouble;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * Factory for creating IVectorDistanceFunction implementations that wrap VectorDistanceArrCalculation methods.
 * This factory allows passing VectorDistanceArrCalculation implementations from AsterixDB to Hyracks modules
 * without creating circular dependencies.
 * The factory is serializable and can be passed through the job pipeline.
 */
public class VectorDistanceFunctionFactory implements IVTreeDistanceFunctionFactory {
    private static final long serialVersionUID = 1L;

    // Distance function constants (same as VTreeBulkLoaderAndGroupingOperatorDescriptor)
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2 = UTF8StringPointable.generateUTF8Pointable("l2");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
            UTF8StringPointable.generateUTF8Pointable("euclidean");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("l2_squared");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("euclidean_squared");
    private static final UTF8StringPointable COSINE_FORMAT = UTF8StringPointable.generateUTF8Pointable("cosine");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot");

    private static class EuclideanDistanceFunction implements DistanceFunctionDouble, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclidean(a, b);
        }
    }

    private static class EuclideanSquaredDistanceFunction implements DistanceFunctionDouble, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclideanSquared(a, b);
        }
    }

    private static class CosineDistanceFunction implements DistanceFunctionDouble, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.cosineDistance(a, b);
        }
    }

    private static class DotProductDistanceFunction implements DistanceFunctionDouble, Serializable {
        private static final long serialVersionUID = 1L;

        /** Returns -dot(a,b) so that minimizing "distance" equals maximizing dot product (MIPS). */
        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return -VectorDistanceArrCalculation.dot(a, b);
        }
    }

    // Distance function hash map (same as VTreeBulkLoaderAndGroupingOperatorDescriptor)
    private static final java.util.Map<Integer, DistanceFunctionDouble> DISTANCE_MAP = java.util.Map.of(
            EUCLIDEAN_DISTANCE.hash(), new EuclideanDistanceFunction(), EUCLIDEAN_DISTANCE_L2.hash(),
            new EuclideanDistanceFunction(), EUCLIDEAN_DISTANCE_SQUARED.hash(), new EuclideanSquaredDistanceFunction(),
            EUCLIDEAN_DISTANCE_L2_SQUARED.hash(), new EuclideanSquaredDistanceFunction(), COSINE_FORMAT.hash(),
            new CosineDistanceFunction(), DOT_PRODUCT_FORMAT.hash(), new DotProductDistanceFunction());

    /**
     * Convert distance metric string to IVectorDistanceFunction implementation.
     * Uses VectorDistanceArrCalculation methods wrapped in IVectorDistanceFunction.
     * @param distanceMetric Distance metric string (e.g., "euclidean", "cosine similarity", etc.)
     * @return IVTreeDistanceFunction implementation wrapping VectorDistanceArrCalculation, or Euclidean as default
     */
    @Override
    public IVTreeDistanceFunction createDistanceFunction(String distanceMetric) {

        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(distanceMetric.toLowerCase());
        DistanceFunctionDouble func = DISTANCE_MAP
                .get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(), formatPointable.getStartOffset()));

        return wrapDistanceFunction(func);
    }

    /**
     * Convert DistanceFunctionDouble to IVectorDistanceFunction for use in Hyracks modules.
     * @param distanceFunction AsterixDB DistanceFunctionDouble
     * @return IVectorDistanceFunction wrapper
     */
    private static IVTreeDistanceFunction wrapDistanceFunction(DistanceFunctionDouble distanceFunction) {
        return distanceFunction::apply;
    }
}
