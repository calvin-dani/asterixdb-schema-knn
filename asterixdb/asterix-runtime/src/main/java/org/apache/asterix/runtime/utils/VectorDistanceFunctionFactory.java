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

import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.storage.am.vector.api.IVectorDistanceFunction;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * Factory for creating IVectorDistanceFunction implementations that wrap VectorDistanceArrCalculation methods.
 * This factory allows passing VectorDistanceArrCalculation implementations from AsterixDB to Hyracks modules
 * without creating circular dependencies.
 * 
 * The factory is serializable and can be passed through the job pipeline.
 */
public class VectorDistanceFunctionFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    // Distance function constants (same as VCTreeBulkLoaderAndGroupingOperatorDescriptor)
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2 = UTF8StringPointable.generateUTF8Pointable("l2");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
            UTF8StringPointable.generateUTF8Pointable("euclidean");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("l2_squared");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("euclidean_squared");
    private static final UTF8StringPointable MANHATTAN_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("manhattan distance");
    private static final UTF8StringPointable COSINE_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("cosine similarity");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot");

    // Serializable distance function implementations that wrap VectorDistanceArrCalculation
    private static class ManhattanDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.manhattan(a, b);
        }
    }

    private static class EuclideanDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclidean(a, b);
        }
    }

    private static class EuclideanSquaredDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclidean_squared(a, b);
        }
    }

    private static class CosineDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.cosineDistance(a, b);
        }
    }

    private static class DotProductDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.dot(a, b);
        }
    }

    // Distance function hash map (same as VCTreeBulkLoaderAndGroupingOperatorDescriptor)
    private static final java.util.Map<Integer, DistanceFunction> DISTANCE_MAP =
            java.util.Map.of(MANHATTAN_FORMAT.hash(), new ManhattanDistanceFunction(), EUCLIDEAN_DISTANCE.hash(),
                    new EuclideanDistanceFunction(), EUCLIDEAN_DISTANCE_L2.hash(), new EuclideanDistanceFunction(),
                    EUCLIDEAN_DISTANCE_SQUARED.hash(), new EuclideanSquaredDistanceFunction(),
                    EUCLIDEAN_DISTANCE_L2_SQUARED.hash(), new EuclideanSquaredDistanceFunction(), COSINE_FORMAT.hash(),
                    new CosineDistanceFunction(), DOT_PRODUCT_FORMAT.hash(), new DotProductDistanceFunction());

    /**
     * Convert distance metric string to IVectorDistanceFunction implementation.
     * Uses VectorDistanceArrCalculation methods wrapped in IVectorDistanceFunction.
     * 
     * @param distanceMetric Distance metric string (e.g., "euclidean", "cosine similarity", etc.)
     * @return IVectorDistanceFunction implementation wrapping VectorDistanceArrCalculation, or Euclidean as default
     */
    public IVectorDistanceFunction createDistanceFunction(String distanceMetric) {
        if (distanceMetric == null || distanceMetric.trim().isEmpty()) {
            return wrapDistanceFunction(new EuclideanDistanceFunction());
        }

        UTF8StringPointable formatPointable = UTF8StringPointable.generateUTF8Pointable(distanceMetric.toLowerCase());
        DistanceFunction func = DISTANCE_MAP
                .get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(), formatPointable.getStartOffset()));

        if (func == null) {
            // Default to Euclidean if not found
            System.err
                    .println("WARNING: Unsupported distance function: " + distanceMetric + ", defaulting to euclidean");
            return wrapDistanceFunction(new EuclideanDistanceFunction());
        }

        return wrapDistanceFunction(func);
    }

    /**
     * Convert DistanceFunction to IVectorDistanceFunction for use in Hyracks modules.
     * 
     * @param distanceFunction AsterixDB DistanceFunction
     * @return IVectorDistanceFunction wrapper
     */
    private static IVectorDistanceFunction wrapDistanceFunction(DistanceFunction distanceFunction) {
        return distanceFunction::apply;
    }
}
