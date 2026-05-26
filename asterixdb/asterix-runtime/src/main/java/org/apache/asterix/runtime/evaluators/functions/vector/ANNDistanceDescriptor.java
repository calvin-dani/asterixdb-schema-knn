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

package org.apache.asterix.runtime.evaluators.functions.vector;

import java.util.HashMap;
import java.util.Map;
import java.util.function.ToDoubleBiFunction;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;
import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.asterix.runtime.utils.VectorSimilarityMetric;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * Descriptor for {@code ann_distance(vector1, vector2, metric)}.
 * The metric argument must be a constant string; supported values are
 * {@code euclidean} / {@code l2}, {@code euclidean_squared} / {@code l2_squared},
 * {@code cosine} / {@code "cosine similarity"}, and {@code dot} (returns {@code -dot(a,b)}
 * so that smaller "distance" means more similar, matching the vector index convention).
 */
@MissingNullInOutFunction
public class ANNDistanceDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    // Metric-string -> distance function, keyed by the same lowercase hash used at lookup. The alias
    // set is owned by VectorSimilarityMetric so it cannot drift from VectorDistanceFunctionFactory.
    private static final Map<Integer, ToDoubleBiFunction<double[], double[]>> DISTANCE_MAP = buildDistanceMap();

    private static Map<Integer, ToDoubleBiFunction<double[], double[]>> buildDistanceMap() {
        Map<Integer, ToDoubleBiFunction<double[], double[]>> map = new HashMap<>();
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            ToDoubleBiFunction<double[], double[]> function = doubleFunctionFor(metric);
            for (String alias : metric.aliases()) {
                map.put(UTF8StringPointable.generateUTF8Pointable(alias).hash(), function);
            }
        }
        return map;
    }

    private static ToDoubleBiFunction<double[], double[]> doubleFunctionFor(VectorSimilarityMetric metric) {
        switch (metric) {
            case EUCLIDEAN:
                return VectorDistanceCalculation::euclidean;
            case EUCLIDEAN_SQUARED:
                return VectorDistanceCalculation::euclideanSquared;
            case COSINE:
                return VectorDistanceCalculation::cosineDistance;
            case DOT:
                // -dot(a,b): minimizing "distance" == maximizing dot product (MIPS convention).
                return (a, b) -> -VectorDistanceCalculation.dotProduct(a, b);
            default:
                throw new IllegalStateException("Unhandled vector similarity metric: " + metric);
        }
    }

    public final static IFunctionDescriptorFactory FACTORY =
            DescriptorFactoryUtil.createFactory(ANNDistanceDescriptor::new, FunctionTypeInferers.SET_ARGUMENTS_TYPE);

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ANN_DISTANCE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                ToDoubleBiFunction<double[], double[]> distanceFunction = resolveDistanceFunctionDouble(ctx, args);
                // ann_distance only operates on the double[] variant.
                return new VectorDistanceScalarEvaluator(ctx, args, getIdentifier(), distanceFunction, sourceLoc);
            }

        };
    }

    private ToDoubleBiFunction<double[], double[]> resolveDistanceFunctionDouble(IEvaluatorContext ctx,
            IScalarEvaluatorFactory[] args) throws HyracksDataException {
        if (args.length < 3 || args.length > 5) {
            throw new RuntimeDataException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, sourceLoc,
                    getIdentifier().getName());
        }
        if (!(args[2] instanceof ConstantEvalFactory)) {
            throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    String.format("The third argument (metric) of function %s must be a constant string.",
                            getIdentifier().getName()));
        }

        IScalarEvaluator metricEvaluator = args[2].createScalarEvaluator(ctx);
        IPointable metricPointable = new VoidPointable();
        metricEvaluator.evaluate(null, metricPointable);

        byte metricTypeTag = metricPointable.getByteArray()[metricPointable.getStartOffset()];
        if (metricTypeTag != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    String.format("The third argument (metric) of function %s must be a constant string.",
                            getIdentifier().getName()));
        }

        UTF8StringPointable metricString = new UTF8StringPointable();
        metricString.set(metricPointable.getByteArray(), metricPointable.getStartOffset() + 1,
                metricPointable.getLength() - 1);
        int metricHash = UTF8StringUtil.lowerCaseHash(metricString.getByteArray(), metricString.getStartOffset());
        ToDoubleBiFunction<double[], double[]> distanceFunction = DISTANCE_MAP.get(metricHash);
        if (distanceFunction == null) {
            throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    String.format(
                            "Illegal distance function: '%s'. Supported metrics: euclidean, l2, euclidean_squared, l2_squared, cosine, dot",
                            metricString.toString()));
        }
        return distanceFunction;
    }

}
