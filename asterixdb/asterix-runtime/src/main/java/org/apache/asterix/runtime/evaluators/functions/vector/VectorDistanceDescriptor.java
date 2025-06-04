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

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.*;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.ArrayIntersectDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.evaluators.functions.TypeCaster;
import org.apache.asterix.runtime.evaluators.functions.binary.AbstractBinaryScalarEvaluator;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.*;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.bytes.Base64Parser;
import org.apache.hyracks.util.bytes.Base64Printer;
import org.apache.hyracks.util.bytes.HexParser;
import org.apache.hyracks.util.bytes.HexPrinter;
import org.apache.hyracks.util.string.UTF8StringWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import smile.math.MathEx;
import smile.math.distance.EuclideanDistance;
import smile.math.distance.ManhattanDistance;

import static org.apache.asterix.om.types.ATypeTag.*;
import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

@MissingNullInOutFunction
public class VectorDistanceDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private static final UTF8StringPointable EUCLIDIAN_DISTANCE = UTF8StringPointable.generateUTF8Pointable("euclidian distance");
    private static final UTF8StringPointable MANHATTAN_FORMAT = UTF8StringPointable.generateUTF8Pointable("manhattan distance");
    private static final UTF8StringPointable COSINE_FORMAT = UTF8StringPointable.generateUTF8Pointable("cosine similarity");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot product");
    public final static IFunctionDescriptorFactory FACTORY = VectorDistanceDescriptor::new;
    public static final ATypeTag[] EXPECTED_INPUT_TAGS = { ATypeTag.ARRAY, ATypeTag.ARRAY, ATypeTag.STRING };

    private IAsterixListBuilder orderedListBuilder;
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

                    private StringBuilder stringBuilder = new StringBuilder();
                    private final ByteArrayPointable byteArrayPtr = new ByteArrayPointable();
                    private final UTF8StringPointable formatPointable = new UTF8StringPointable();


                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evaluators[0].evaluate(tuple, pointables[0]);
                        evaluators[1].evaluate(tuple, pointables[1]);
                        evaluators[2].evaluate(tuple, pointables[2]);
                        ATypeTag valueTag;
                        valueTag =
                                ATYPETAGDESERIALIZER.deserialize(pointables[0].getByteArray()[pointables[0].getStartOffset()]);
                        if (valueTag == ATypeTag.MISSING) {
                            PointableHelper.setMissing(result);
                            return;
                        }


//                        if (!acceptNullValues && valueTag == ATypeTag.NULL) {
//                            returnNull = true;
//                        }
//                        if (returnNull) {
//                            PointableHelper.setNull(result);
//                            return;
//                        }
                        if (PointableHelper.checkAndSetMissingOrNull(result, pointables[0], pointables[1],pointables[2])) {
                            return;
                        }

                        try {
                            ATypeTag arg0Tag = ATypeTag.VALUE_TYPE_MAPPING[pointables[0].getByteArray()[pointables[0]
                                    .getStartOffset()]];
                            ATypeTag arg1Tag = ATypeTag.VALUE_TYPE_MAPPING[pointables[1].getByteArray()[pointables[1]
                                    .getStartOffset()]];
                            ATypeTag arg2Tag = ATypeTag.VALUE_TYPE_MAPPING[pointables[2].getByteArray()[pointables[2]
                                    .getStartOffset()]];
                            checkTypeMachingThrowsIfNot(EXPECTED_INPUT_TAGS, arg0Tag, arg1Tag, arg2Tag);

                            formatPointable.set(pointables[2].getByteArray(), pointables[2].getStartOffset() + 1,
                                    pointables[2].getLength());
                            if (checkDimensionType(pointables[0], pointables[1])) {
                                double[] vector1 = extractNumericVector(pointables[0]);
                                double[] vector2 = extractNumericVector(pointables[1]);
                                if (MANHATTAN_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                    ManhattanDistance distance = new ManhattanDistance();
                                    double distanceCal = distance.d(vector1, vector2);
                                    dataOutput.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                    dataOutput.writeDouble(distanceCal);
                                } else if (EUCLIDIAN_DISTANCE.ignoreCaseCompareTo(formatPointable) == 0) {
                                    EuclideanDistance distance = new EuclideanDistance();
                                    double distanceCal = distance.d(vector1, vector2);
                                    dataOutput.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                    dataOutput.writeDouble(distanceCal);
                                } else if (COSINE_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                    double dotCal =  MathEx.dot(vector1,vector2);
                                    double distanceCal = dotCal / (MathEx.norm2(vector1) * MathEx.norm2(vector2));
                                    dataOutput.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                    dataOutput.writeDouble(distanceCal);
                                }
                                else if (DOT_PRODUCT_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                    double dotCal =  MathEx.dot(vector1,vector2);
                                    dataOutput.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                    dataOutput.writeDouble(dotCal);
                                }
                                else {
                                    throw new UnsupportedItemTypeException(sourceLoc, getIdentifier(), arg1Tag.serialize());
                                }
                            }
                            else {
                                System.out.println("Vector dimension mismatch");
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
    public boolean checkDimensionType(IPointable pointable0,IPointable pointable1) throws HyracksDataException {
        ListAccessor listAccessor = new ListAccessor();
        listAccessor.reset(pointable0.getByteArray(), pointable0.getStartOffset());
        if (!listAccessor.getItemType().isNumericType()) {
            return false; // Return false if the item type is not numeric
        }
        int size0 =  listAccessor.size();
        // Return true if the list has at least one item
        listAccessor.reset(pointable1.getByteArray(), pointable1.getStartOffset());
        if (!listAccessor.getItemType().isNumericType()) {
            return false; // Return false if the item type is not numeric
        }
        int size1 =  listAccessor.size();
        return size0 == size1;
    }
    /**
     * Extracts a numeric vector from the given pointable.
     * Assumes that the pointable contains an array of numeric values.
     *
     * @param pointable The pointable containing the array.
     * @return A double array representing the numeric vector.
     * @throws HyracksDataException If there is an error during extraction.
     */

    public double[] extractNumericVector(IPointable pointable) throws HyracksDataException {
        ListAccessor listAccessor = new ListAccessor();;
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        if (!listAccessor.getItemType().isNumericType()) {
            return new double[0]; // Return an empty array if the item type is not numeric
        }
        double[] vector = new double[listAccessor.size()];
        for (int i = 0; i < vector.length; i++) {
            int itemOffset = listAccessor.getItemOffset(i);
            ATypeTag itemType = listAccessor.getItemType(itemOffset);
            switch (itemType) {
                case TINYINT:
                    vector[i] = AInt8SerializerDeserializer.getByte(listAccessor.getByteArray(), itemOffset);
                    break;
                case SMALLINT:
                    vector[i] = AInt16SerializerDeserializer.getShort(listAccessor.getByteArray(), itemOffset);
                    break;
                case INTEGER:
                    vector[i] = AInt32SerializerDeserializer.getInt(listAccessor.getByteArray(), itemOffset);
                    break;
                case BIGINT:
                    vector[i] = AInt64SerializerDeserializer.getLong(listAccessor.getByteArray(), itemOffset);
                    break;
                case FLOAT:
                    vector[i] = AFloatSerializerDeserializer.getFloat(listAccessor.getByteArray(), itemOffset);
                    break;
                case DOUBLE:
                    vector[i] = ADoubleSerializerDeserializer.getDouble(listAccessor.getByteArray(), itemOffset);
                    break;
                default:
                    throw new HyracksDataException("Unsupported numeric type: " + itemType);
            }
        }
        return vector;
    }

    }
