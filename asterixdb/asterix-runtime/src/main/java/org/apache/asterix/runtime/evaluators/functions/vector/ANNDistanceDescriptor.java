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

import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorDistanceArrScalarEvaluator.DistanceFunctionDouble;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
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
public class ANNDistanceDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
            UTF8StringPointable.generateUTF8Pointable("euclidean");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2 = UTF8StringPointable.generateUTF8Pointable("l2");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("euclidean_squared");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("l2_squared");
    private static final UTF8StringPointable COSINE_DISTANCE = UTF8StringPointable.generateUTF8Pointable("cosine");
    private static final UTF8StringPointable COSINE_SIMILARITY =
            UTF8StringPointable.generateUTF8Pointable("cosine similarity");
    private static final UTF8StringPointable DOT_DISTANCE = UTF8StringPointable.generateUTF8Pointable("dot");
    private static final Map<Integer, DistanceFunctionDouble> DISTANCE_MAP =
            Map.of(EUCLIDEAN_DISTANCE.hash(), VectorDistanceArrCalculation::euclidean, EUCLIDEAN_DISTANCE_L2.hash(),
                    VectorDistanceArrCalculation::euclidean, EUCLIDEAN_DISTANCE_SQUARED.hash(),
                    VectorDistanceArrCalculation::euclideanSquared, EUCLIDEAN_DISTANCE_L2_SQUARED.hash(),
                    VectorDistanceArrCalculation::euclideanSquared, COSINE_DISTANCE.hash(),
                    VectorDistanceArrCalculation::cosineDistance, COSINE_SIMILARITY.hash(),
                    VectorDistanceArrCalculation::cosineDistance, DOT_DISTANCE.hash(),
                    (a, b) -> -VectorDistanceArrCalculation.dot(a, b));

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
                DistanceFunctionDouble distanceFunction = resolveDistanceFunctionDouble(ctx, args);
                // ann_distance only operates on the Double variant; the typed variants
                // (Int8/Int16/Int32/Int64/Float) are unused for this descriptor.
                return new VectorDistanceArrScalarEvaluator(ctx, args, getIdentifier(), null, null, null, null, null,
                        distanceFunction, sourceLoc);
            }

        };
    }

    private DistanceFunctionDouble resolveDistanceFunctionDouble(IEvaluatorContext ctx, IScalarEvaluatorFactory[] args)
            throws HyracksDataException {
        if (args.length < 3) {
            throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    String.format(
                            "Invalid number of arguments for function %s. Expected at least 3 arguments: (vector1, vector2, metric)",
                            getIdentifier().getName()));
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
        DistanceFunctionDouble distanceFunction = DISTANCE_MAP.get(metricHash);
        if (distanceFunction == null) {
            throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    String.format(
                            "Illegal distance function: '%s'. Supported metrics: euclidean, l2, euclidean_squared, l2_squared, cosine, dot",
                            metricString.toString()));
        }
        return distanceFunction;
    }

}
