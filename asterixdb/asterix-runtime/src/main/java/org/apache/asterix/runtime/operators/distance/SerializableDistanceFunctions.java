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

package org.apache.asterix.runtime.operators.distance;

import java.io.Serializable;

import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunction;
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Serializable distance function implementations for use in distributed environments.
 * These classes implement the DistanceFunction interface and are serializable,
 * making them suitable for use in operators that need to be serialized.
 */
public class SerializableDistanceFunctions {

    /**
     * Manhattan distance function implementation
     */
    public static class ManhattanDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.manhattan(a, b);
        }
    }

    /**
     * Euclidean distance function implementation
     */
    public static class EuclideanDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclidean(a, b);
        }
    }

    /**
     * Euclidean squared distance function implementation
     */
    public static class EuclideanSquaredDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.euclidean_squared(a, b);
        }
    }

    /**
     * Cosine similarity function implementation
     */
    public static class CosineDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.cosine(a, b);
        }
    }

    /**
     * Dot product function implementation
     */
    public static class DotProductDistanceFunction implements DistanceFunction, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public double apply(double[] a, double[] b) throws HyracksDataException {
            return VectorDistanceArrCalculation.dot(a, b);
        }
    }
}
