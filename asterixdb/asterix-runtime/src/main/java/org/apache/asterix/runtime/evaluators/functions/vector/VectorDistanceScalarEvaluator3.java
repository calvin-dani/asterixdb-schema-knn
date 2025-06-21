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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.utils.VectorDistanceCalculation2;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class VectorDistanceScalarEvaluator3 implements IScalarEvaluator {
    private final ListAccessor[] listAccessor = new ListAccessor[2];
    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected DataOutput dataOutput = resultStorage.getDataOutput();
    protected IPointable[] pointables;
    protected IScalarEvaluator[] evaluators;
    // Function ID, for error reporting.
    protected final FunctionIdentifier funcId;
    protected final SourceLocation sourceLoc;

    private final UTF8StringPointable formatPointable = new UTF8StringPointable();

    private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
            UTF8StringPointable.generateUTF8Pointable("euclidean distance");
    private static final UTF8StringPointable MANHATTAN_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("manhattan distance");
    private static final UTF8StringPointable COSINE_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("cosine similarity");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("dot product");

    public final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    public DistanceFunction func;

    @FunctionalInterface
    public interface DistanceFunction {
        double apply(double[] a, double[] b) throws HyracksDataException;
    }

    private static final Map<Integer, DistanceFunction> DISTANCE_MAP =
            Map.of(MANHATTAN_FORMAT.hash(), VectorDistanceCalculation2::manhattan, EUCLIDEAN_DISTANCE.hash(),
                    VectorDistanceCalculation2::euclidean, COSINE_FORMAT.hash(), VectorDistanceCalculation2::cosine,
                    DOT_PRODUCT_FORMAT.hash(), VectorDistanceCalculation2::dot);

    public final ListAccessor[] listAccessorConstant = new ListAccessor[2];
    public double[][] primitiveArrayConstant = new double[2][];
    //    private final ListAccessor listAccessorConstant2 = new ListAccessor();
    public final boolean[] isConstant = new boolean[3];

    public VectorDistanceScalarEvaluator3(IEvaluatorContext context, final IScalarEvaluatorFactory[] evaluatorFactories,
            FunctionIdentifier funcId, SourceLocation sourceLoc) throws HyracksDataException {
        pointables = new IPointable[evaluatorFactories.length];
        evaluators = new IScalarEvaluator[evaluatorFactories.length];
        for (int i = 0; i < evaluators.length; ++i) {
            pointables[i] = new VoidPointable();
            evaluators[i] = evaluatorFactories[i].createScalarEvaluator(context);
            if (evaluatorFactories[i] instanceof ConstantEvalFactory) {
                // If the evaluator is a constant, we need to evaluate it to get the value.
                // This is necessary for functions that require both arguments to be evaluated.
                evaluators[i].evaluate(null, pointables[i]);
                isConstant[i] = true;
                if (i == 2) {
                    formatPointable.set(pointables[2].getByteArray(), pointables[2].getStartOffset() + 1,
                            pointables[2].getLength());
                    func = DISTANCE_MAP.get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(),
                            formatPointable.getStartOffset()));
                    if (func == null) {
                        throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
                                formatPointable.toString());
                    }
                } else {
                    listAccessorConstant[i] = new ListAccessor();

                    if (!ATYPETAGDESERIALIZER.deserialize(pointables[i].getByteArray()[pointables[i].getStartOffset()])
                            .isListType()) {
                        throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
                                pointables[i].toString());
                    }
                    listAccessorConstant[i].reset(pointables[i].getByteArray(), pointables[i].getStartOffset());
                    try {
                        primitiveArrayConstant[0] = createPrimitveList(listAccessorConstant[i]);
                    } catch (IOException e) {
                        throw new HyracksDataException("Error creating primitive list from constant vector", e);
                    }
                }
            }
        }
        this.funcId = funcId;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        for (int i = 0; i < 2; i++) {
            if (!isConstant[i]) {
                listAccessor[i] = new ListAccessor();
                evaluators[i].evaluate(tuple, pointables[i]);

                if (!ATYPETAGDESERIALIZER.deserialize(pointables[i].getByteArray()[pointables[i].getStartOffset()])
                        .isListType()) {
                    PointableHelper.setNull(result);
                    return;
                }

                listAccessor[i].reset(pointables[i].getByteArray(), pointables[i].getStartOffset());
            }

        }

        ListAccessor listAccessor1 = isConstant[0] ? listAccessorConstant[0] : listAccessor[0];
        ListAccessor listAccessor2 = isConstant[1] ? listAccessorConstant[1] : listAccessor[1];
        double distanceCal;
        try {
            double[] primitiveArray1 = isConstant[0] ? primitiveArrayConstant[0] : createPrimitveList(listAccessor1);
            double[] primitiveArray2 = isConstant[1] ? primitiveArrayConstant[1] : createPrimitveList(listAccessor2);
            if (listAccessor1.size() != listAccessor2.size() || listAccessor1.size() == 0
                    || listAccessor2.size() == 0) {
                PointableHelper.setNull(result);
                return;
            }

            if (PointableHelper.checkAndSetMissingOrNull(result, pointables[0], pointables[1], pointables[2])) {
                PointableHelper.setNull(result);
                return;
            }
            distanceCal = func.apply(primitiveArray1, primitiveArray2);
        } catch (IOException e) {
            PointableHelper.setNull(result);
            return;
        }
        try {
            writeResult(distanceCal, dataOutput);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected void writeResult(double distance, DataOutput dataOutput) throws IOException {
        AMutableDouble aDouble = new AMutableDouble(-1);
        aDouble.setValue(distance);
        doubleSerde.serialize(aDouble, dataOutput);

    }

    protected double[] createPrimitveList(ListAccessor listAccessor) throws IOException {
        ATypeTag typeTag = listAccessor.getItemType();
        if (!typeTag.isNumericType()) {
            throw new HyracksDataException("Unsupported type tag for numeric vector extraction: " + typeTag);
        }
        double[] primitiveArray = new double[listAccessor.size()];
        IPointable tempVal = new VoidPointable();
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, tempVal, storage);
            primitiveArray[i] = extractNumericVector(tempVal, typeTag);
        }
        return primitiveArray;
    }

    protected double extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws HyracksDataException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (derivedTypeTag.isNumericType()) {
            return getValueFromTag(derivedTypeTag, data, offset);
        } else if (derivedTypeTag == ATypeTag.ANY) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            return getValueFromTag(typeTag, data, offset);
        } else {
            throw new HyracksDataException("Unsupported type tag for numeric vector extraction: " + derivedTypeTag);
        }
    }

    // TODO CALVIN DANI add a type derivation func to abstract.
    protected double getValueFromTag(ATypeTag typeTag, byte[] data, int offset) throws HyracksDataException {
        return switch (typeTag) {
            case TINYINT -> AInt8SerializerDeserializer.getByte(data, offset + 1);
            case SMALLINT -> AInt16SerializerDeserializer.getShort(data, offset + 1);
            case INTEGER -> AInt32SerializerDeserializer.getInt(data, offset + 1);
            case BIGINT -> AInt64SerializerDeserializer.getLong(data, offset + 1);
            case FLOAT -> AFloatSerializerDeserializer.getFloat(data, offset + 1);
            case DOUBLE -> ADoubleSerializerDeserializer.getDouble(data, offset + 1);
            default -> throw new HyracksDataException("Unsupported type tag: " + typeTag);
        };
    }

}
