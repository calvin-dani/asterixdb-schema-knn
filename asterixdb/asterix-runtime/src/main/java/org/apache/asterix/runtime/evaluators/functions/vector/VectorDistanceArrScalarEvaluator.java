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
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class VectorDistanceArrScalarEvaluator implements IScalarEvaluator {

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput dataOutput = resultStorage.getDataOutput();

    private final IPointable pointableLeft;
    private final IPointable pointableRight;
    private final IScalarEvaluator evaluatorLeft;
    private final IScalarEvaluator evaluatorRight;

    // Function ID, for error reporting.
    private final FunctionIdentifier funcId;
    private final SourceLocation sourceLoc;
    private final DistanceFunctionInt8 int8Func;
    private final DistanceFunctionInt16 int16Func;
    private final DistanceFunctionInt32 int32Func;
    private final DistanceFunctionInt64 int64Func;
    private final DistanceFunctionDouble doubleFunc;
    private final DistanceFunctionFloat floatFunc;

    private final AMutableDouble aDouble = new AMutableDouble(-1);
    private final IPointable tempArrVal = new VoidPointable();
    private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();

    private final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    @FunctionalInterface
    public interface DistanceFunctionDouble {
        double apply(double[] a, double[] b) throws HyracksDataException;
    }

    @FunctionalInterface
    public interface DistanceFunctionInt8 {
        double apply(byte[] a, byte[] b) throws HyracksDataException;
    }

    @FunctionalInterface
    public interface DistanceFunctionInt16 {
        double apply(short[] a, short[] b) throws HyracksDataException;
    }

    @FunctionalInterface
    public interface DistanceFunctionInt32 {
        double apply(int[] a, int[] b) throws HyracksDataException;
    }

    @FunctionalInterface
    public interface DistanceFunctionInt64 {
        double apply(long[] a, long[] b) throws HyracksDataException;
    }

    @FunctionalInterface
    public interface DistanceFunctionFloat {
        double apply(float[] a, float[] b) throws HyracksDataException;
    }

    private final ListAccessor listAccessorLeft = new ListAccessor();
    private final ListAccessor listAccessorRight = new ListAccessor();
    private final boolean isConstantLeft;
    private final boolean isConstantRight;
    private byte[] vectorArgsLeftInt8 = new byte[0];
    private byte[] vectorArgsRightInt8 = new byte[0];
    private short[] vectorArgsLeftInt16 = new short[0];
    private short[] vectorArgsRightInt16 = new short[0];
    private int[] vectorArgsLeftInt32 = new int[0];
    private int[] vectorArgsRightInt32 = new int[0];
    private long[] vectorArgsLeftInt64 = new long[0];
    private long[] vectorArgsRightInt64 = new long[0];
    private double[] vectorArgsLeftDouble = new double[0];
    private double[] vectorArgsRightDouble = new double[0];
    private float[] vectorArgsLeftFloat = new float[0];
    private float[] vectorArgsRightFloat = new float[0];
    private ATypeTag leftArgType;
    private ATypeTag rightArgType;

    public VectorDistanceArrScalarEvaluator(IEvaluatorContext context,
            final IScalarEvaluatorFactory[] evaluatorFactories, FunctionIdentifier funcId,
            DistanceFunctionInt8 int8Func, DistanceFunctionInt16 int16Func, DistanceFunctionInt32 int32Func,
            DistanceFunctionInt64 int64Func, DistanceFunctionFloat floatFunc, DistanceFunctionDouble doubleFunc,
            SourceLocation sourceLoc) throws HyracksDataException {
        this.int8Func = int8Func;
        this.int16Func = int16Func;
        this.int32Func = int32Func;
        this.int64Func = int64Func;
        this.doubleFunc = doubleFunc;
        this.floatFunc = floatFunc;

        pointableLeft = new VoidPointable();
        pointableRight = new VoidPointable();
        evaluatorLeft = evaluatorFactories[0].createScalarEvaluator(context);
        evaluatorRight = evaluatorFactories[1].createScalarEvaluator(context);

        boolean constantLeft = false;
        boolean constantRight = false;
        try {

            if (evaluatorFactories[0] instanceof ConstantEvalFactory) {
                constantLeft = true;
                evaluatorLeft.evaluate(null, pointableLeft);
                if (!checkListType(pointableLeft)) {
                    throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
                            "One or both of the arguments are not lists");
                }
                listAccessorLeft.reset(pointableLeft.getByteArray(), pointableLeft.getStartOffset());
                leftArgType = resolveArgType(listAccessorLeft);
                if (leftArgType != null && isNumericType(leftArgType)) {
                    switch (leftArgType) {
                        case TINYINT:
                            vectorArgsLeftInt8 = createByteArrayFromListAccessor(pointableLeft, listAccessorLeft);
                            break;
                        case SMALLINT:
                            vectorArgsLeftInt16 = createShortArrayFromListAccessor(pointableLeft, listAccessorLeft);
                            break;
                        case INTEGER:
                            vectorArgsLeftInt32 = createIntArrayFromListAccessor(pointableLeft, listAccessorLeft);
                            break;
                        case BIGINT:
                            vectorArgsLeftInt64 = createLongArrayFromListAccessor(pointableLeft, listAccessorLeft);
                            break;
                        case FLOAT:
                            vectorArgsLeftFloat = createFloatArrayFromListAccessor(pointableLeft, listAccessorLeft);
                            break;
                        case DOUBLE:
                            vectorArgsLeftDouble = createDoubleArrayFromListAccessor(pointableLeft, listAccessorLeft);
                            break;
                        default:
                            leftArgType = null;
                            break;
                    }
                } else {
                    leftArgType = null;
                }
            } else {
                leftArgType = null;
            }

            if (evaluatorFactories[1] instanceof ConstantEvalFactory) {
                constantRight = true;
                evaluatorRight.evaluate(null, pointableRight);
                if (!checkListType(pointableRight)) {
                    throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
                            "One or both of the arguments are not lists");
                }
                listAccessorRight.reset(pointableRight.getByteArray(), pointableRight.getStartOffset());
                rightArgType = resolveArgType(listAccessorRight);
                if (rightArgType != null && isNumericType(rightArgType)) {
                    switch (rightArgType) {
                        case TINYINT:
                            vectorArgsRightInt8 = createByteArrayFromListAccessor(pointableRight, listAccessorRight);
                            break;
                        case SMALLINT:
                            vectorArgsRightInt16 = createShortArrayFromListAccessor(pointableRight, listAccessorRight);
                            break;
                        case INTEGER:
                            vectorArgsRightInt32 = createIntArrayFromListAccessor(pointableRight, listAccessorRight);
                            break;
                        case BIGINT:
                            vectorArgsRightInt64 = createLongArrayFromListAccessor(pointableRight, listAccessorRight);
                            break;
                        case FLOAT:
                            vectorArgsRightFloat = createFloatArrayFromListAccessor(pointableRight, listAccessorRight);
                            break;
                        case DOUBLE:
                            vectorArgsRightDouble =
                                    createDoubleArrayFromListAccessor(pointableRight, listAccessorRight);
                            break;
                        default:
                            rightArgType = null;
                            break;
                    }
                } else {
                    rightArgType = null;
                }
            } else {
                rightArgType = null;
            }

        } catch (IOException e) {
            leftArgType = null;
            rightArgType = null;
            constantLeft = false;
            constantRight = false;
        }

        this.isConstantLeft = constantLeft;
        this.isConstantRight = constantRight;
        this.funcId = funcId;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();

        if (!isConstantLeft) {
            if (!initializeListAccessors(tuple, result, evaluatorLeft, listAccessorLeft, pointableLeft)) {
                PointableHelper.setNull(result);
                return;
            }
            leftArgType = resolveArgType(listAccessorLeft);
        }
        if (!isConstantRight) {
            if (!initializeListAccessors(tuple, result, evaluatorRight, listAccessorRight, pointableRight)) {
                PointableHelper.setNull(result);
                return;
            }
            rightArgType = resolveArgType(listAccessorRight);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, pointableLeft, pointableRight)) {
            return;
        }
        if (listAccessorLeft.size() != listAccessorRight.size() || listAccessorLeft.size() == 0
                || listAccessorRight.size() == 0) {
            PointableHelper.setNull(result);
            return;
        }

        if (leftArgType == null || rightArgType == null) {
            PointableHelper.setNull(result);
            return;
        }

        if (!isNumericType(leftArgType) || !isNumericType(rightArgType)) {
            PointableHelper.setNull(result);
            return;
        }
        if (leftArgType != rightArgType) {
            PointableHelper.setNull(result);
            return;
        }

        double distanceCal;
        try {
            switch (leftArgType) {
                case TINYINT:
                    vectorArgsLeftInt8 =
                            isConstantLeft ? vectorArgsLeftInt8
                                    : createByteArrayFromList(listAccessorLeft,
                                            ensureByteCapacity(vectorArgsLeftInt8, listAccessorLeft.size()),
                                            leftArgType);
                    vectorArgsRightInt8 =
                            isConstantRight ? vectorArgsRightInt8
                                    : createByteArrayFromList(listAccessorRight,
                                            ensureByteCapacity(vectorArgsRightInt8, listAccessorRight.size()),
                                            rightArgType);
                    distanceCal = int8Func.apply(vectorArgsLeftInt8, vectorArgsRightInt8);
                    break;
                case SMALLINT:
                    vectorArgsLeftInt16 =
                            isConstantLeft ? vectorArgsLeftInt16
                                    : createShortArrayFromList(listAccessorLeft,
                                            ensureShortCapacity(vectorArgsLeftInt16, listAccessorLeft.size()),
                                            leftArgType);
                    vectorArgsRightInt16 =
                            isConstantRight ? vectorArgsRightInt16
                                    : createShortArrayFromList(listAccessorRight,
                                            ensureShortCapacity(vectorArgsRightInt16, listAccessorRight.size()),
                                            rightArgType);
                    distanceCal = int16Func.apply(vectorArgsLeftInt16, vectorArgsRightInt16);
                    break;
                case INTEGER:
                    vectorArgsLeftInt32 =
                            isConstantLeft ? vectorArgsLeftInt32
                                    : createIntArrayFromList(listAccessorLeft,
                                            ensureIntCapacity(vectorArgsLeftInt32, listAccessorLeft.size()),
                                            leftArgType);
                    vectorArgsRightInt32 =
                            isConstantRight ? vectorArgsRightInt32
                                    : createIntArrayFromList(listAccessorRight,
                                            ensureIntCapacity(vectorArgsRightInt32, listAccessorRight.size()),
                                            rightArgType);
                    distanceCal = int32Func.apply(vectorArgsLeftInt32, vectorArgsRightInt32);
                    break;
                case BIGINT:
                    vectorArgsLeftInt64 =
                            isConstantLeft ? vectorArgsLeftInt64
                                    : createLongArrayFromList(listAccessorLeft,
                                            ensureLongCapacity(vectorArgsLeftInt64, listAccessorLeft.size()),
                                            leftArgType);
                    vectorArgsRightInt64 =
                            isConstantRight ? vectorArgsRightInt64
                                    : createLongArrayFromList(listAccessorRight,
                                            ensureLongCapacity(vectorArgsRightInt64, listAccessorRight.size()),
                                            rightArgType);
                    distanceCal = int64Func.apply(vectorArgsLeftInt64, vectorArgsRightInt64);
                    break;
                case FLOAT:
                    vectorArgsLeftFloat =
                            isConstantLeft ? vectorArgsLeftFloat
                                    : createFloatArrayFromList(listAccessorLeft,
                                            ensureFloatCapacity(vectorArgsLeftFloat, listAccessorLeft.size()),
                                            leftArgType);
                    vectorArgsRightFloat =
                            isConstantRight ? vectorArgsRightFloat
                                    : createFloatArrayFromList(listAccessorRight,
                                            ensureFloatCapacity(vectorArgsRightFloat, listAccessorRight.size()),
                                            rightArgType);
                    distanceCal = floatFunc.apply(vectorArgsLeftFloat, vectorArgsRightFloat);
                    break;
                case DOUBLE:
                    vectorArgsLeftDouble =
                            isConstantLeft ? vectorArgsLeftDouble
                                    : createDoubleArrayFromList(listAccessorLeft,
                                            ensureDoubleCapacity(vectorArgsLeftDouble, listAccessorLeft.size()),
                                            leftArgType);
                    vectorArgsRightDouble = isConstantRight ? vectorArgsRightDouble
                            : createDoubleArrayFromList(listAccessorRight,
                                    ensureDoubleCapacity(vectorArgsRightDouble, listAccessorRight.size()),
                                    rightArgType);
                    distanceCal = doubleFunc.apply(vectorArgsLeftDouble, vectorArgsRightDouble);
                    break;
                default:
                    PointableHelper.setNull(result);
                    return;
            }

            if (Double.isNaN(distanceCal)) {
                PointableHelper.setNull(result);
                return;
            }
            writeResult(distanceCal, dataOutput);
            result.set(resultStorage);
        } catch (IOException e) {
            PointableHelper.setNull(result);
            return;
        }
    }

    protected void writeResult(double distance, DataOutput dataOutput) throws IOException {
        aDouble.setValue(distance);
        doubleSerde.serialize(aDouble, dataOutput);

    }

    protected boolean checkListType(IPointable pointable) {
        return ATYPETAGDESERIALIZER.deserialize(pointable.getByteArray()[pointable.getStartOffset()]).isListType();
    }

    protected boolean initializeListAccessors(IFrameTupleReference tuple, IPointable result, IScalarEvaluator evaluator,
            ListAccessor listAccessor, IPointable pointable) throws HyracksDataException {
        evaluator.evaluate(tuple, pointable);
        if (!checkListType(pointable)) {
            PointableHelper.setNull(result);
            return false;
        }
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        return true;
    }

    protected byte[] createByteArrayFromListAccessor(IPointable pointable, ListAccessor listAccessor)
            throws IOException {
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        byte[] primitiveArray = new byte[listAccessor.size()];
        return createByteArrayFromList(listAccessor, primitiveArray, resolveArgType(listAccessor));
    }

    protected short[] createShortArrayFromListAccessor(IPointable pointable, ListAccessor listAccessor)
            throws IOException {
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        short[] primitiveArray = new short[listAccessor.size()];
        return createShortArrayFromList(listAccessor, primitiveArray, resolveArgType(listAccessor));
    }

    protected int[] createIntArrayFromListAccessor(IPointable pointable, ListAccessor listAccessor) throws IOException {
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        int[] primitiveArray = new int[listAccessor.size()];
        return createIntArrayFromList(listAccessor, primitiveArray, resolveArgType(listAccessor));
    }

    protected long[] createLongArrayFromListAccessor(IPointable pointable, ListAccessor listAccessor)
            throws IOException {
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        long[] primitiveArray = new long[listAccessor.size()];
        return createLongArrayFromList(listAccessor, primitiveArray, resolveArgType(listAccessor));
    }

    protected double[] createDoubleArrayFromListAccessor(IPointable pointable, ListAccessor listAccessor)
            throws IOException {
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        double[] primitiveArray = new double[listAccessor.size()];
        return createDoubleArrayFromList(listAccessor, primitiveArray, resolveArgType(listAccessor));
    }

    protected float[] createFloatArrayFromListAccessor(IPointable pointable, ListAccessor listAccessor)
            throws IOException {
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        float[] primitiveArray = new float[listAccessor.size()];
        return createFloatArrayFromList(listAccessor, primitiveArray, resolveArgType(listAccessor));
    }

    protected byte[] createByteArrayFromList(ListAccessor listAccessor, byte[] primitiveArray, ATypeTag typeTag)
            throws IOException {
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, this.tempArrVal, this.storage);
            primitiveArray[i] = extractInt8Vector(this.tempArrVal, typeTag);
        }
        return primitiveArray;
    }

    protected short[] createShortArrayFromList(ListAccessor listAccessor, short[] primitiveArray, ATypeTag typeTag)
            throws IOException {
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, this.tempArrVal, this.storage);
            primitiveArray[i] = extractInt16Vector(this.tempArrVal, typeTag);
        }
        return primitiveArray;
    }

    protected int[] createIntArrayFromList(ListAccessor listAccessor, int[] primitiveArray, ATypeTag typeTag)
            throws IOException {
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, this.tempArrVal, this.storage);
            primitiveArray[i] = extractInt32Vector(this.tempArrVal, typeTag);
        }
        return primitiveArray;
    }

    protected long[] createLongArrayFromList(ListAccessor listAccessor, long[] primitiveArray, ATypeTag typeTag)
            throws IOException {
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, this.tempArrVal, this.storage);
            primitiveArray[i] = extractInt64Vector(this.tempArrVal, typeTag);
        }
        return primitiveArray;
    }

    protected double[] createDoubleArrayFromList(ListAccessor listAccessor, double[] primitiveArray, ATypeTag typeTag)
            throws IOException {
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, this.tempArrVal, this.storage);
            primitiveArray[i] = extractDoubleVector(this.tempArrVal, typeTag);
        }
        return primitiveArray;
    }

    protected float[] createFloatArrayFromList(ListAccessor listAccessor, float[] primitiveArray, ATypeTag typeTag)
            throws IOException {
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, this.tempArrVal, this.storage);
            primitiveArray[i] = extractFloatVector(this.tempArrVal, typeTag);
        }
        return primitiveArray;
    }

    protected byte extractInt8Vector(IPointable pointable, ATypeTag derivedTypeTag) throws TypeMismatchException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (derivedTypeTag == ATypeTag.TINYINT) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            if (typeTag != ATypeTag.TINYINT) {
                throw new TypeMismatchException(sourceLoc, funcId, typeTag.serialize(), ATypeTag.TINYINT.serialize());
            }
            return AInt8SerializerDeserializer.getByte(data, offset + 1);
        }
        throw new TypeMismatchException(sourceLoc, funcId, derivedTypeTag.serialize(), ATypeTag.TINYINT.serialize());
    }

    protected short extractInt16Vector(IPointable pointable, ATypeTag derivedTypeTag) throws TypeMismatchException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (derivedTypeTag == ATypeTag.SMALLINT) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            if (typeTag != ATypeTag.SMALLINT) {
                throw new TypeMismatchException(sourceLoc, funcId, typeTag.serialize(), ATypeTag.SMALLINT.serialize());
            }
            return AInt16SerializerDeserializer.getShort(data, offset + 1);
        }
        throw new TypeMismatchException(sourceLoc, funcId, derivedTypeTag.serialize(), ATypeTag.SMALLINT.serialize());
    }

    protected int extractInt32Vector(IPointable pointable, ATypeTag derivedTypeTag) throws TypeMismatchException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (derivedTypeTag == ATypeTag.INTEGER) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            if (typeTag != ATypeTag.INTEGER) {
                throw new TypeMismatchException(sourceLoc, funcId, typeTag.serialize(), ATypeTag.INTEGER.serialize());
            }
            return AInt32SerializerDeserializer.getInt(data, offset + 1);
        }
        throw new TypeMismatchException(sourceLoc, funcId, derivedTypeTag.serialize(), ATypeTag.INTEGER.serialize());
    }

    protected long extractInt64Vector(IPointable pointable, ATypeTag derivedTypeTag) throws TypeMismatchException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (derivedTypeTag == ATypeTag.BIGINT) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            if (typeTag != ATypeTag.BIGINT) {
                throw new TypeMismatchException(sourceLoc, funcId, typeTag.serialize(), ATypeTag.BIGINT.serialize());
            }
            return AInt64SerializerDeserializer.getLong(data, offset + 1);
        }
        throw new TypeMismatchException(sourceLoc, funcId, derivedTypeTag.serialize(), ATypeTag.BIGINT.serialize());
    }

    protected double extractDoubleVector(IPointable pointable, ATypeTag derivedTypeTag) throws TypeMismatchException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (derivedTypeTag == ATypeTag.DOUBLE) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            if (typeTag != ATypeTag.DOUBLE) {
                throw new TypeMismatchException(sourceLoc, funcId, typeTag.serialize(), ATypeTag.DOUBLE.serialize());
            }
            return getValueFromTag(typeTag, data, offset);
        } else {
            throw new TypeMismatchException(sourceLoc, funcId, derivedTypeTag.serialize(), ATypeTag.DOUBLE.serialize());
        }
    }

    protected float extractFloatVector(IPointable pointable, ATypeTag derivedTypeTag) throws TypeMismatchException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (derivedTypeTag == ATypeTag.FLOAT) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            if (typeTag != ATypeTag.FLOAT) {
                throw new TypeMismatchException(sourceLoc, funcId, typeTag.serialize(), ATypeTag.FLOAT.serialize());
            }
            return getFloatValueFromTag(typeTag, data, offset);
        } else {
            throw new TypeMismatchException(sourceLoc, funcId, derivedTypeTag.serialize(), ATypeTag.FLOAT.serialize());
        }
    }

    protected ATypeTag resolveArgType(ListAccessor listAccessor) {
        ATypeTag itemType = listAccessor.getItemType();
        return itemType == ATypeTag.ANY ? resolveAnyItemType(listAccessor) : itemType;
    }

    protected ATypeTag resolveAnyItemType(ListAccessor listAccessor) {
        if (listAccessor.size() == 0) {
            return null;
        }
        try {
            listAccessor.getOrWriteItem(0, this.tempArrVal, this.storage);
            return EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(tempArrVal.getByteArray()[tempArrVal.getStartOffset()]);
        } catch (IOException e) {
            return null;
        }
    }

    protected double getValueFromTag(ATypeTag typeTag, byte[] data, int offset) {
        return switch (typeTag) {
            case TINYINT -> AInt8SerializerDeserializer.getByte(data, offset + 1);
            case SMALLINT -> AInt16SerializerDeserializer.getShort(data, offset + 1);
            case INTEGER -> AInt32SerializerDeserializer.getInt(data, offset + 1);
            case BIGINT -> AInt64SerializerDeserializer.getLong(data, offset + 1);
            case FLOAT -> AFloatSerializerDeserializer.getFloat(data, offset + 1);
            case DOUBLE -> ADoubleSerializerDeserializer.getDouble(data, offset + 1);
            default -> Double.NaN;
        };
    }

    protected boolean isNumericType(ATypeTag typeTag) {
        return ATypeHierarchy.getTypeDomain(typeTag) == ATypeHierarchy.Domain.NUMERIC;
    }

    protected byte[] ensureByteCapacity(byte[] arr, int size) {
        return arr.length == size ? arr : new byte[size];
    }

    protected short[] ensureShortCapacity(short[] arr, int size) {
        return arr.length == size ? arr : new short[size];
    }

    protected int[] ensureIntCapacity(int[] arr, int size) {
        return arr.length == size ? arr : new int[size];
    }

    protected long[] ensureLongCapacity(long[] arr, int size) {
        return arr.length == size ? arr : new long[size];
    }

    protected float getFloatValueFromTag(ATypeTag typeTag, byte[] data, int offset) {
        return switch (typeTag) {
            case TINYINT -> AInt8SerializerDeserializer.getByte(data, offset + 1);
            case SMALLINT -> AInt16SerializerDeserializer.getShort(data, offset + 1);
            case INTEGER -> AInt32SerializerDeserializer.getInt(data, offset + 1);
            case BIGINT -> AInt64SerializerDeserializer.getLong(data, offset + 1);
            case FLOAT -> AFloatSerializerDeserializer.getFloat(data, offset + 1);
            default -> Float.NaN;
        };
    }

    protected double[] ensureDoubleCapacity(double[] arr, int size) {
        return arr.length == size ? arr : new double[size];
    }

    protected float[] ensureFloatCapacity(float[] arr, int size) {
        return arr.length == size ? arr : new float[size];
    }

}
