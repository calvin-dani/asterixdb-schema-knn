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
import java.util.HashMap;
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
import org.apache.asterix.runtime.utils.VectorDistanceArrCalculation;
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

public class VectorDistanceArrScalarEvaluator implements IScalarEvaluator {
    private final ListAccessor[] listAccessor = new ListAccessor[2];
    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected DataOutput dataOutput = resultStorage.getDataOutput();
    protected IPointable[] pointables;
    protected IScalarEvaluator[] evaluators;
    // Function ID, for error reporting.
    protected final FunctionIdentifier funcId;
    protected final SourceLocation sourceLoc;
    private final UTF8StringPointable formatPointable = new UTF8StringPointable();

    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2 = UTF8StringPointable.generateUTF8Pointable("l2");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
            UTF8StringPointable.generateUTF8Pointable("euclidean");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_L2_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("l2_squared");
    private static final UTF8StringPointable EUCLIDEAN_DISTANCE_SQUARED =
            UTF8StringPointable.generateUTF8Pointable("euclidean_squared");
    private static final UTF8StringPointable MANHATTAN_FORMAT =
            UTF8StringPointable.generateUTF8Pointable("manhattan_distance");
    private static final UTF8StringPointable COSINE_FORMAT = UTF8StringPointable.generateUTF8Pointable("cosine");
    private static final UTF8StringPointable DOT_PRODUCT_FORMAT = UTF8StringPointable.generateUTF8Pointable("dot");

    public final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    public DistanceFunction func;

    @FunctionalInterface
    public interface DistanceFunction {
        double apply(double[] a, double[] b) throws HyracksDataException;
    }

    @FunctionalInterface
    protected interface TypeSpecificExtractor {
        void extractAndSet(Object array, int index, byte[] data, int offset) throws HyracksDataException;
    }

    private static final Map<Integer, DistanceFunction> DISTANCE_MAP = Map.of(EUCLIDEAN_DISTANCE.hash(), VectorDistanceArrCalculation::euclidean,
            EUCLIDEAN_DISTANCE_L2.hash(), VectorDistanceArrCalculation::euclidean, EUCLIDEAN_DISTANCE_SQUARED.hash(),
            VectorDistanceArrCalculation::euclidean_squared, EUCLIDEAN_DISTANCE_L2_SQUARED.hash(),
            VectorDistanceArrCalculation::euclidean_squared, COSINE_FORMAT.hash(),
            VectorDistanceArrCalculation::cosineDistance, DOT_PRODUCT_FORMAT.hash(),
            (a, b) -> -VectorDistanceArrCalculation.dot(a, b));

    private static final Map<ATypeTag, TypeSpecificExtractor> HOMOGENEOUS_EXTRACTORS = new HashMap<>();

    static {
        HOMOGENEOUS_EXTRACTORS.put(ATypeTag.TINYINT, (array, index, data, offset) -> {
            if (array instanceof int[]) {
                ((int[]) array)[index] = AInt8SerializerDeserializer.getByte(data, offset);
            } else if (array instanceof long[]) {
                ((long[]) array)[index] = AInt8SerializerDeserializer.getByte(data, offset);
            } else if (array instanceof float[]) {
                ((float[]) array)[index] = AInt8SerializerDeserializer.getByte(data, offset);
            } else if (array instanceof double[]) {
                ((double[]) array)[index] = AInt8SerializerDeserializer.getByte(data, offset);
            }
        });
        HOMOGENEOUS_EXTRACTORS.put(ATypeTag.SMALLINT, (array, index, data, offset) -> {
            if (array instanceof int[]) {
                ((int[]) array)[index] = AInt16SerializerDeserializer.getShort(data, offset);
            } else if (array instanceof long[]) {
                ((long[]) array)[index] = AInt16SerializerDeserializer.getShort(data, offset);
            } else if (array instanceof float[]) {
                ((float[]) array)[index] = AInt16SerializerDeserializer.getShort(data, offset);
            } else if (array instanceof double[]) {
                ((double[]) array)[index] = AInt16SerializerDeserializer.getShort(data, offset);
            }
        });
        HOMOGENEOUS_EXTRACTORS.put(ATypeTag.INTEGER, (array, index, data, offset) -> {
            if (array instanceof int[]) {
                ((int[]) array)[index] = AInt32SerializerDeserializer.getInt(data, offset);
            } else if (array instanceof long[]) {
                ((long[]) array)[index] = AInt32SerializerDeserializer.getInt(data, offset);
            } else if (array instanceof float[]) {
                ((float[]) array)[index] = AInt32SerializerDeserializer.getInt(data, offset);
            } else if (array instanceof double[]) {
                ((double[]) array)[index] = AInt32SerializerDeserializer.getInt(data, offset);
            }
        });
        HOMOGENEOUS_EXTRACTORS.put(ATypeTag.BIGINT, (array, index, data, offset) -> {
            if (array instanceof long[]) {
                ((long[]) array)[index] = AInt64SerializerDeserializer.getLong(data, offset);
            } else if (array instanceof float[]) {
                ((float[]) array)[index] = AInt64SerializerDeserializer.getLong(data, offset);
            } else if (array instanceof double[]) {
                ((double[]) array)[index] = AInt64SerializerDeserializer.getLong(data, offset);
            }
        });
        HOMOGENEOUS_EXTRACTORS.put(ATypeTag.FLOAT, (array, index, data, offset) -> {
            if (array instanceof float[]) {
                ((float[]) array)[index] = AFloatSerializerDeserializer.getFloat(data, offset);
            } else if (array instanceof double[]) {
                ((double[]) array)[index] = AFloatSerializerDeserializer.getFloat(data, offset);
            }
        });
        HOMOGENEOUS_EXTRACTORS.put(ATypeTag.DOUBLE, (array, index, data, offset) -> {
            if (array instanceof double[]) {
                ((double[]) array)[index] = ADoubleSerializerDeserializer.getDouble(data, offset);
            }
        });
    }

    public final ListAccessor[] listAccessorConstant = new ListAccessor[2];
    public double[][] primitiveArrayConstant = new double[2][];
    public final boolean[] isConstant = new boolean[3];

    public VectorDistanceArrScalarEvaluator(IEvaluatorContext context,
            final IScalarEvaluatorFactory[] evaluatorFactories, FunctionIdentifier funcId, SourceLocation sourceLoc)
            throws HyracksDataException {
        if (evaluatorFactories.length < 3 || evaluatorFactories.length > 5) {
            throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    String.format(
                            "Invalid number of arguments for function %s. Expected 3-5 arguments: (vector1, vector2, metric [, min_probe_fraction] [, k_multiplier])",
                            funcId.getName()));
        }

        // Only use first 3 args for distance computation; args 3-4 are optimizer hints
        pointables = new IPointable[3];
        evaluators = new IScalarEvaluator[3];
        for (int i = 0; i < 3; ++i) {
            pointables[i] = new VoidPointable();
            evaluators[i] = evaluatorFactories[i].createScalarEvaluator(context);
            // Only process constant optimization for the first 3 args (vector1, vector2, metric).
            // Args 3+ (nprobe, epsilon, searchApproach) are optimizer hints, not used at runtime.
            if (i == 2 && !(evaluatorFactories[i] instanceof ConstantEvalFactory)) {
                throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc, String.format(
                        "The third argument (metric) of function %s must be a constant string.", funcId.getName()));
            }
            if (i < 3 && evaluatorFactories[i] instanceof ConstantEvalFactory) {
                evaluators[i].evaluate(null, pointables[i]);
                isConstant[i] = true;
                if (i == 2) {
                    byte metricTypeTag = pointables[2].getByteArray()[pointables[2].getStartOffset()];
                    if (metricTypeTag != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                        throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                String.format("The third argument (metric) of function %s must be a constant string.",
                                        funcId.getName()));
                    }
                    formatPointable.set(pointables[2].getByteArray(), pointables[2].getStartOffset() + 1,
                            pointables[2].getLength() - 1);
                    func = DISTANCE_MAP.get(UTF8StringUtil.lowerCaseHash(formatPointable.getByteArray(),
                            formatPointable.getStartOffset()));
                    if (func == null) {
                        throw new RuntimeDataException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                                String.format(
                                        "Illegal distance function: '%s'. Supported: euclidean, l2, cosine similarity, dot product, manhattan distance",
                                        formatPointable.toString()));
                    }
                } else {
                    listAccessorConstant[i] = new ListAccessor();

                    if (!ATYPETAGDESERIALIZER.deserialize(pointables[i].getByteArray()[pointables[i].getStartOffset()])
                            .isListType()) {
                        throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
                                String.format("Argument %d is expected to be a list type for function %s.", i + 1,
                                        funcId.getName()));
                    }
                    listAccessorConstant[i].reset(pointables[i].getByteArray(), pointables[i].getStartOffset());
                    try {
                        primitiveArrayConstant[i] = createPrimitveList(listAccessorConstant[i]);
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
            if (Double.isNaN(distanceCal)) {
                PointableHelper.setNull(result);
                return;
            }
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

    protected Object createPrimitiveArray(ATypeTag itemType, int size) throws HyracksDataException {
        switch (itemType) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return new int[size];
            case BIGINT:
                return new long[size];
            case FLOAT:
                return new float[size];
            case DOUBLE:
                return new double[size];
            case ANY:
                return new double[size];
            default:
                throw new HyracksDataException("Unsupported type for primitive array: " + itemType);
        }
    }

    protected void extractValueIntoArray(int[] array, int index, IPointable item, ATypeTag typeTag)
            throws HyracksDataException {
        byte[] data = item.getByteArray();
        int offset = item.getStartOffset();
        ATypeTag actualType = typeTag;
        int valueOffset = offset + 1;

        if (typeTag == ATypeTag.ANY) {
            actualType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            valueOffset = offset + 1;
        }

        switch (actualType) {
            case TINYINT:
                array[index] = AInt8SerializerDeserializer.getByte(data, valueOffset);
                break;
            case SMALLINT:
                array[index] = AInt16SerializerDeserializer.getShort(data, valueOffset);
                break;
            case INTEGER:
                array[index] = AInt32SerializerDeserializer.getInt(data, valueOffset);
                break;
            case BIGINT:
                long longVal = AInt64SerializerDeserializer.getLong(data, valueOffset);
                if (longVal < Integer.MIN_VALUE || longVal > Integer.MAX_VALUE) {
                    throw new HyracksDataException("BIGINT value " + longVal + " out of range for int[]");
                }
                array[index] = (int) longVal;
                break;
            case FLOAT:
            case DOUBLE:
                throw new HyracksDataException("FLOAT/DOUBLE value cannot be stored in int[] array");
            default:
                throw new HyracksDataException("Unsupported type for int[]: " + actualType);
        }
    }

    protected void extractValueIntoArray(long[] array, int index, IPointable item, ATypeTag typeTag)
            throws HyracksDataException {
        byte[] data = item.getByteArray();
        int offset = item.getStartOffset();
        ATypeTag actualType = typeTag;
        int valueOffset = offset + 1;

        if (typeTag == ATypeTag.ANY) {
            actualType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            valueOffset = offset + 1;
        }

        switch (actualType) {
            case TINYINT:
                array[index] = AInt8SerializerDeserializer.getByte(data, valueOffset);
                break;
            case SMALLINT:
                array[index] = AInt16SerializerDeserializer.getShort(data, valueOffset);
                break;
            case INTEGER:
                array[index] = AInt32SerializerDeserializer.getInt(data, valueOffset);
                break;
            case BIGINT:
                array[index] = AInt64SerializerDeserializer.getLong(data, valueOffset);
                break;
            case FLOAT:
            case DOUBLE:
                throw new HyracksDataException("FLOAT/DOUBLE value cannot be stored in long[] array");
            default:
                throw new HyracksDataException("Unsupported type for long[]: " + actualType);
        }
    }

    protected void extractValueIntoArray(float[] array, int index, IPointable item, ATypeTag typeTag)
            throws HyracksDataException {
        byte[] data = item.getByteArray();
        int offset = item.getStartOffset();
        ATypeTag actualType = typeTag;
        int valueOffset = offset + 1;

        if (typeTag == ATypeTag.ANY) {
            actualType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            valueOffset = offset + 1;
        }

        switch (actualType) {
            case TINYINT:
                array[index] = AInt8SerializerDeserializer.getByte(data, valueOffset);
                break;
            case SMALLINT:
                array[index] = AInt16SerializerDeserializer.getShort(data, valueOffset);
                break;
            case INTEGER:
                array[index] = AInt32SerializerDeserializer.getInt(data, valueOffset);
                break;
            case BIGINT:
                array[index] = AInt64SerializerDeserializer.getLong(data, valueOffset);
                break;
            case FLOAT:
                array[index] = AFloatSerializerDeserializer.getFloat(data, valueOffset);
                break;
            case DOUBLE:
                double doubleVal = ADoubleSerializerDeserializer.getDouble(data, valueOffset);
                array[index] = (float) doubleVal;
                break;
            default:
                throw new HyracksDataException("Unsupported type for float[]: " + actualType);
        }
    }

    protected void extractValueIntoArray(double[] array, int index, IPointable item, ATypeTag typeTag)
            throws HyracksDataException {
        byte[] data = item.getByteArray();
        int offset = item.getStartOffset();
        ATypeTag actualType = typeTag;
        int valueOffset = offset + 1;

        if (typeTag == ATypeTag.ANY) {
            actualType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            valueOffset = offset + 1;
        }

        switch (actualType) {
            case TINYINT:
                array[index] = AInt8SerializerDeserializer.getByte(data, valueOffset);
                break;
            case SMALLINT:
                array[index] = AInt16SerializerDeserializer.getShort(data, valueOffset);
                break;
            case INTEGER:
                array[index] = AInt32SerializerDeserializer.getInt(data, valueOffset);
                break;
            case BIGINT:
                array[index] = AInt64SerializerDeserializer.getLong(data, valueOffset);
                break;
            case FLOAT:
                array[index] = AFloatSerializerDeserializer.getFloat(data, valueOffset);
                break;
            case DOUBLE:
                array[index] = ADoubleSerializerDeserializer.getDouble(data, valueOffset);
                break;
            default:
                throw new HyracksDataException("Unsupported type for double[]: " + actualType);
        }
    }

    protected void extractValueIntoArray(Object array, int index, IPointable item, ATypeTag typeTag)
            throws HyracksDataException {
        byte[] data = item.getByteArray();
        int offset = item.getStartOffset();
        ATypeTag actualType = typeTag;
        int valueOffset = offset + 1;

        if (typeTag == ATypeTag.ANY) {
            actualType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            valueOffset = offset + 1;
        }

        TypeSpecificExtractor extractor = HOMOGENEOUS_EXTRACTORS.get(actualType);
        if (extractor != null) {
            extractor.extractAndSet(array, index, data, valueOffset);
        } else {
            if (array instanceof int[]) {
                extractValueIntoArray((int[]) array, index, item, typeTag);
            } else if (array instanceof long[]) {
                extractValueIntoArray((long[]) array, index, item, typeTag);
            } else if (array instanceof float[]) {
                extractValueIntoArray((float[]) array, index, item, typeTag);
            } else if (array instanceof double[]) {
                extractValueIntoArray((double[]) array, index, item, typeTag);
            } else {
                throw new HyracksDataException("Unsupported array type: " + array.getClass());
            }
        }
    }

    protected Object createPrimitiveList(ListAccessor listAccessor) throws IOException, HyracksDataException {
        int size = listAccessor.size();
        if (size == 0) {
            return null;
        }

        ATypeTag declaredItemType = listAccessor.getItemType();
        ATypeTag detectedType = null;
        Object array = null;

        if (declaredItemType == ATypeTag.ANY) {
            IPointable firstItem = new VoidPointable();
            ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
            listAccessor.getOrWriteItem(0, firstItem, storage);

            byte[] data = firstItem.getByteArray();
            int offset = firstItem.getStartOffset();
            detectedType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);

            array = new double[size];
        } else {
            detectedType = declaredItemType;
            array = createPrimitiveArray(detectedType, size);
        }

        IPointable tempVal = new VoidPointable();
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        for (int i = 0; i < size; i++) {
            listAccessor.getOrWriteItem(i, tempVal, storage);
            extractValueIntoArray(array, i, tempVal, detectedType);
        }

        return array;
    }

    protected double[] createPrimitveList(ListAccessor listAccessor) throws IOException, HyracksDataException {
        Object result = createPrimitiveList(listAccessor);
        if (result == null) {
            return new double[0];
        }
        if (result instanceof double[]) {
            return (double[]) result;
        }
        double[] doubleArray = new double[listAccessor.size()];
        if (result instanceof int[]) {
            int[] intArray = (int[]) result;
            for (int i = 0; i < intArray.length; i++) {
                doubleArray[i] = intArray[i];
            }
        } else if (result instanceof long[]) {
            long[] longArray = (long[]) result;
            for (int i = 0; i < longArray.length; i++) {
                doubleArray[i] = longArray[i];
            }
        } else if (result instanceof float[]) {
            float[] floatArray = (float[]) result;
            for (int i = 0; i < floatArray.length; i++) {
                doubleArray[i] = floatArray[i];
            }
        }
        return doubleArray;
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

    protected double getValueFromTag(ATypeTag typeTag, byte[] data, int offset) throws HyracksDataException {
        return switch (typeTag) {
            case TINYINT -> AInt8SerializerDeserializer.getByte(data, offset + 1);
            case SMALLINT -> AInt16SerializerDeserializer.getShort(data, offset + 1);
            case INTEGER -> AInt32SerializerDeserializer.getInt(data, offset + 1);
            case BIGINT -> AInt64SerializerDeserializer.getLong(data, offset + 1);
            case FLOAT -> AFloatSerializerDeserializer.getFloat(data, offset + 1);
            case DOUBLE -> ADoubleSerializerDeserializer.getDouble(data, offset + 1);
            default -> Float.NaN;
        };
    }

}
