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
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.*;
import org.apache.asterix.runtime.aggregates.base.SingleFieldFrameTupleReference;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.ArrayIntersectDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.evaluators.functions.TypeCaster;
import org.apache.asterix.runtime.evaluators.functions.binary.AbstractBinaryScalarEvaluator;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
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

import java.io.DataOutput;
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
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE = UTF8StringPointable.generateUTF8Pointable("euclidean distance");
    private static final UTF8StringPointable MANHATTAN_FORMAT = UTF8StringPointable.generateUTF8Pointable("manhattan distance");
    private static final UTF8StringPointable COSINE_FORMAT = UTF8StringPointable.generateUTF8Pointable("cosine similarity");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot product");
    public final static IFunctionDescriptorFactory FACTORY = VectorDistanceDescriptor::new;
//    public static final ATypeTag[] EXPECTED_INPUT_TAGS = { ARRAY, ARRAY, STRING };

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

                    final ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory scanCollectionFactory1 =
                            new ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory(args[0], sourceLoc, getIdentifier());
                    final ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory scanCollectionFactory2 =
                            new ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory(args[1], sourceLoc, getIdentifier());
                    private final IUnnestingEvaluator scanCollection1 = scanCollectionFactory1.createUnnestingEvaluator(ctx);
                    private final IUnnestingEvaluator scanCollection2 = scanCollectionFactory2.createUnnestingEvaluator(ctx);
                    private final UTF8StringPointable formatPointable = new UTF8StringPointable();
                    private final SingleFieldFrameTupleReference itemTuple = new SingleFieldFrameTupleReference();
                    private final IScalarEvaluator colEval =  new ColumnAccessEvalFactory(0).createScalarEvaluator(ctx);
                    private final List<Double> vectorList0 = new ArrayList<>();
                    private final List<Double> vectorList1 = new ArrayList<>();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evaluators[0].evaluate(tuple, pointables[0]);
                        evaluators[1].evaluate(tuple, pointables[1]);
                        evaluators[2].evaluate(tuple, pointables[2]);
                        boolean vectorCalSuccess = true;
                        final IPointable listItemOut = new VoidPointable();
                        scanCollection1.init(tuple);
                        vectorList0.clear();
                        vectorList1.clear();
                        while (scanCollection1.step(listItemOut)) {
                            itemTuple.reset(listItemOut.getByteArray(), listItemOut.getStartOffset(),listItemOut.getLength());
                             if(!extractNumericVector(itemTuple, colEval,vectorList0)){
                                 vectorCalSuccess = false;
                             }
                        }
//                        List<Double> vectorList1 = new ArrayList<>();
                        scanCollection2.init(tuple);
                        while (scanCollection2.step(listItemOut)) {
                            itemTuple.reset(listItemOut.getByteArray(), listItemOut.getStartOffset(),listItemOut.getLength());
                            if(!extractNumericVector(itemTuple, colEval,vectorList1)){
                                vectorCalSuccess = false;
                            }
                        }
//                        System.out.println("Vector 1 size: " + vectorList0.size() + " Vector 2 size: " + vectorList1.size());
//                        System.out.println("Vector 1: " + vectorList0);
//                        System.out.println("Vector 2: " + vectorList1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, pointables[0], pointables[1],pointables[2])) {
                            return;
                        }
                        double distanceCal = Float.MAX_VALUE;
                        formatPointable.set(pointables[2].getByteArray(), pointables[2].getStartOffset() + 1,
                                pointables[2].getLength());
                        if (checkDimension(vectorList0, vectorList1) && vectorCalSuccess) {
                            double[] vector1 = vectorList0.stream().mapToDouble(Double::doubleValue).toArray();
                            double[] vector2 = vectorList1.stream().mapToDouble(Double::doubleValue).toArray();
                            if (MANHATTAN_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                ManhattanDistance distance = new ManhattanDistance();
                                 distanceCal = distance.d(vector1, vector2);

                            } else if (EUCLIDEAN_DISTANCE.ignoreCaseCompareTo(formatPointable) == 0) {
                                EuclideanDistance distance = new EuclideanDistance();
                                 distanceCal = distance.d(vector1, vector2);

                            } else if (COSINE_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                double dotCal =  MathEx.dot(vector1,vector2);
                                 distanceCal = dotCal / (MathEx.norm2(vector1) * MathEx.norm2(vector2));

                            }
                            else if (DOT_PRODUCT_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                distanceCal =  MathEx.dot(vector1,vector2);
                            }
                            else{
                                throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc,funcId.getName(),
                                        formatPointable.toString());
                            }
                        }
                        else {
//                            System.out.println("Vector dimension mismatch or vector type mismatch" +
//                                    vectorList0.size() +  vectorList1.size());
                            vectorCalSuccess = false;
                        }
                        try {
                            writeResult(distanceCal, dataOutput,vectorCalSuccess);
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };

    }

    protected void writeResult(double distance,DataOutput dataOutput,boolean vectorCalSuccess) throws IOException {
        if (vectorCalSuccess) {
            AMutableDouble aDouble = new AMutableDouble(-1);
            ISerializerDeserializer<ABoolean> booleanSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

            ISerializerDeserializer<ADouble> doubleSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);


            aDouble.setValue(distance);
            doubleSerde.serialize(aDouble, dataOutput);
        }
        else {
             ISerializerDeserializer<ANull> nullSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

            nullSerde.serialize(ANull.NULL,dataOutput);
        }

    }

    public boolean checkDimension(List<Double> vectorList0, List<Double> vectorList1)  {
        if (vectorList0 == null || vectorList1 == null) {
            return false; // Return false if either list is null
        }
        return vectorList0.size() == vectorList1.size(); // Check if the sizes of both lists are equal

    }
    /**
     * Extracts a numeric vector from the given pointable.
     * Assumes that the pointable contains an array of numeric values.
     *
     * @param pointable The pointable containing the array.
     * @return A double array representing the numeric vector.
     * @throws HyracksDataException If there is an error during extraction.
     */

    public boolean  extractNumericVector(IFrameTupleReference pointable,IScalarEvaluator colEval,List<Double> vector) throws HyracksDataException {
        IPointable inputVal = new VoidPointable();
        colEval.evaluate(pointable, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
        if (!typeTag.isNumericType()) {
           return false; // Return an empty array if the item type is not numeric
        }
            switch (typeTag) {
                case TINYINT:
                    vector.add((double)AInt8SerializerDeserializer.getByte(data, offset + 1));
                    break;
                case SMALLINT:
                    vector.add((double)AInt16SerializerDeserializer.getShort(data, offset + 1));
                    break;
                case INTEGER:
                    vector.add((double)AInt32SerializerDeserializer.getInt(data, offset + 1));
                    break;
                case BIGINT:
                    vector.add((double)AInt64SerializerDeserializer.getLong(data, offset + 1));
                    break;
                case FLOAT:
                    vector.add((double)AFloatSerializerDeserializer.getFloat(data, offset + 1));
                    break;
                case DOUBLE:
                    vector.add(ADoubleSerializerDeserializer.getDouble(data, offset + 1));
                    break;
                default:
                    return false; // Return an empty array if the item type is not numeric
            }
        return true; // Return an empty array if the item type is not numeric
    }

    }
