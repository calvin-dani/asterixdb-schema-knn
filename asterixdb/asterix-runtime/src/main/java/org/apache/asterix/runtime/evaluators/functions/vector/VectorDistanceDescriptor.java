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

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;
import static org.apache.asterix.runtime.utils.VectorDistanceCalculation.cosine;
import static org.apache.asterix.runtime.utils.VectorDistanceCalculation.dot;
import static org.apache.asterix.runtime.utils.VectorDistanceCalculation.euclidean;
import static org.apache.asterix.runtime.utils.VectorDistanceCalculation.manhattan;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.evaluators.functions.binary.AbstractBinaryScalarEvaluator;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;


@MissingNullInOutFunction
public class VectorDistanceDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
            UTF8StringPointable.generateUTF8Pointable("euclidean distance");
    private static final UTF8StringPointable MANHATTAN_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("manhattan distance");
    private static final UTF8StringPointable COSINE_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("cosine similarity");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("dot product");
    public final static IFunctionDescriptorFactory FACTORY = DescriptorFactoryUtil.createFactory(VectorDistanceDescriptor::new, FunctionTypeInferers.SET_ARGUMENTS_TYPE);
    public final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.VECTOR_DISTANCE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractBinaryScalarEvaluator(ctx, args, getIdentifier(), sourceLoc) {
                    private final ListAccessor listAccessor1 = new ListAccessor();
                    private final ListAccessor listAccessor2 = new ListAccessor();

                    private final UTF8StringPointable formatPointable = new UTF8StringPointable();
                    private final byte[][] listBytes = new byte[2][];
                    private final int[] listOffset = new int[2];
                    protected final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                    private final IPointable tempVal = new VoidPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evaluators[0].evaluate(tuple, pointables[0]);
                        evaluators[1].evaluate(tuple, pointables[1]);
                        evaluators[2].evaluate(tuple, pointables[2]);
                        boolean vectorCalSuccess = true;


                        listBytes[0] = pointables[0].getByteArray();
                        listOffset[0] = pointables[0].getStartOffset();
                        listBytes[1] = pointables[1].getByteArray();
                        listOffset[1] = pointables[1].getStartOffset();

                        if (!ATYPETAGDESERIALIZER.deserialize(listBytes[0][listOffset[0]]).isListType() ||
                                !ATYPETAGDESERIALIZER.deserialize(listBytes[1][listOffset[1]]).isListType()) {
                            PointableHelper.setNull(result);
                            return;
                        }
                        listAccessor1.reset( pointables[0].getByteArray(), pointables[0].getStartOffset());
                        int numItems1 = listAccessor1.size();
                        listAccessor2.reset( pointables[1].getByteArray(), pointables[1].getStartOffset());
                        int numItems2 = listAccessor2.size();
                        if (numItems1 != numItems2 || numItems1 == 0 || numItems2 == 0) {
                            PointableHelper.setNull(result);
                            return;
                        }


                        if (PointableHelper.checkAndSetMissingOrNull(result, pointables[0], pointables[1],
                                pointables[2])) {
                            PointableHelper.setNull(result);
                            return;
                        }
                        double distanceCal = Float.MAX_VALUE;
                        formatPointable.set(pointables[2].getByteArray(), pointables[2].getStartOffset() + 1,
                                pointables[2].getLength());
                        try {
                            if (MANHATTAN_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                distanceCal = manhattan(listAccessor1, listAccessor2);
                            } else if (EUCLIDEAN_DISTANCE.ignoreCaseCompareTo(formatPointable) == 0) {
                                distanceCal = euclidean(listAccessor1, listAccessor2);
                            } else if (COSINE_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                distanceCal = cosine(listAccessor1, listAccessor2);
                            } else if (DOT_PRODUCT_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                distanceCal = dot(listAccessor1, listAccessor2);
                            } else {
                                throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
                                        formatPointable.toString());
                            }
                        }
                        catch (HyracksDataException e) {
                            PointableHelper.setNull(result);
                            return;
                        }
                        try {
                            if(vectorCalSuccess) {
                                writeResult(distanceCal, dataOutput, vectorCalSuccess);
                            }
                            else{
                                PointableHelper.setNull(result);
                                return;
                            }
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };

    }

    protected void writeResult(double distance, DataOutput dataOutput, boolean vectorCalSuccess) throws IOException {
            AMutableDouble aDouble = new AMutableDouble(-1);
            aDouble.setValue(distance);
            doubleSerde.serialize(aDouble, dataOutput);


    }



}
