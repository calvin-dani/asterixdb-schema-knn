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
    private DistanceFunction func;

    public final ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    @FunctionalInterface
    public interface DistanceFunction {
        double apply(double[] a, double[] b) throws HyracksDataException;
    }

    private final ListAccessor listAccessorLeft = new ListAccessor();
    private final ListAccessor listAccessorRight = new ListAccessor();
    private final boolean isConstantLeft;
    private final boolean isConstantRight;
    private double[] vectorArgsLeft = new double[0];
    private double[] vectorArgsRight = new double[0];

    public VectorDistanceArrScalarEvaluator(IEvaluatorContext context,
            final IScalarEvaluatorFactory[] evaluatorFactories, FunctionIdentifier funcId, DistanceFunction func,
            SourceLocation sourceLoc) throws HyracksDataException {
        this.func = func;

        pointableLeft = new VoidPointable();
        pointableRight = new VoidPointable();
        evaluatorLeft = evaluatorFactories[0].createScalarEvaluator(context);
        evaluatorRight = evaluatorFactories[1].createScalarEvaluator(context);

        try {

            if (evaluatorFactories[0] instanceof ConstantEvalFactory) {
                isConstantLeft = true;
                evaluatorLeft.evaluate(null, pointableLeft);
                if (!checkListType(pointableLeft)) {
                    throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
                            "One or both of the arguments are not lists");
                }
                vectorArgsLeft = createArrayFromListAccessor(pointableLeft, listAccessorLeft);
            } else {
                isConstantLeft = false;
            }

            if (evaluatorFactories[1] instanceof ConstantEvalFactory) {
                isConstantRight = true;
                evaluatorRight.evaluate(null, pointableRight);
                if (!checkListType(pointableRight)) {
                    throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
                            "One or both of the arguments are not lists");
                }
                vectorArgsRight = createArrayFromListAccessor(pointableRight, listAccessorRight);
            } else {
                isConstantRight = false;
            }

        } catch (IOException e) {
            throw new HyracksDataException("Error creating primitive list from constant vector", e);
        }

        this.funcId = funcId;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();

        if (!isConstantLeft) {
            vectorArgsLeft = initializeListAccessors(tuple, result, evaluatorLeft, listAccessorLeft, pointableLeft);
            if (vectorArgsLeft.length == 0) {
                PointableHelper.setNull(result);
                return;
            }
        }
        if (!isConstantRight) {
            vectorArgsRight = initializeListAccessors(tuple, result, evaluatorRight, listAccessorRight, pointableRight);
            if (vectorArgsRight.length == 0) {
                PointableHelper.setNull(result);
                return;
            }
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, pointableLeft, pointableRight)) {
            PointableHelper.setNull(result);
            return;
        }

        if (listAccessorLeft.size() != listAccessorRight.size() || listAccessorLeft.size() == 0
                || listAccessorRight.size() == 0) {
            PointableHelper.setNull(result);
            return;
        }

        double distanceCal;
        try {
            vectorArgsLeft = isConstantLeft ? vectorArgsLeft : createArrayFromList(listAccessorLeft, vectorArgsLeft);
            vectorArgsRight =
                    isConstantRight ? vectorArgsRight : createArrayFromList(listAccessorRight, vectorArgsRight);

            distanceCal = func.apply(vectorArgsLeft, vectorArgsRight);

            if (Double.isNaN(distanceCal)) {
                PointableHelper.setNull(result);
                return;
            }
            writeResult(distanceCal, dataOutput);
            result.set(resultStorage);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void writeResult(double distance, DataOutput dataOutput) throws IOException {
        AMutableDouble aDouble = new AMutableDouble(-1);

        aDouble.setValue(distance);
        doubleSerde.serialize(aDouble, dataOutput);

    }

    protected boolean checkListType(IPointable pointable) {
        return ATYPETAGDESERIALIZER.deserialize(pointable.getByteArray()[pointable.getStartOffset()]).isListType();
    }

    protected double[] initializeListAccessors(IFrameTupleReference tuple, IPointable result,
            IScalarEvaluator evaluator, ListAccessor listAccessor, IPointable pointable) throws HyracksDataException {
        evaluator.evaluate(tuple, pointable);
        if (!checkListType(pointable)) {
            PointableHelper.setNull(result);
            return new double[0];
        }
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        return new double[listAccessor.size()];
    }

    protected double[] createArrayFromListAccessor(IPointable pointable, ListAccessor listAccessor) throws IOException {
        listAccessor.reset(pointable.getByteArray(), pointable.getStartOffset());
        double[] primitiveArray = new double[listAccessor.size()];
        return createArrayFromList(listAccessor, primitiveArray);
    }

    protected double[] createArrayFromList(ListAccessor listAccessor, double[] primitiveArray) throws IOException {
        ATypeTag typeTag = listAccessor.getItemType();
        IPointable tempVal = new VoidPointable();
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, tempVal, storage);
            primitiveArray[i] = extractNumericVector(tempVal, typeTag);
        }
        return primitiveArray;
    }

    protected double extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws TypeMismatchException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (ATypeHierarchy.getTypeDomain(derivedTypeTag) == ATypeHierarchy.Domain.NUMERIC) {
            return getValueFromTag(derivedTypeTag, data, offset);
        } else if (derivedTypeTag == ATypeTag.ANY) {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            return getValueFromTag(typeTag, data, offset);
        } else {
            throw new TypeMismatchException(sourceLoc, funcId, derivedTypeTag.serialize(), ATypeTag.DOUBLE.serialize());
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
            default -> Float.NaN;
        };
    }

}
