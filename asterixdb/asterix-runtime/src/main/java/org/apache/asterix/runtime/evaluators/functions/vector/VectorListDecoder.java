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

import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * Decodes AsterixDB list pointables into primitive arrays for the vector-distance evaluators. This isolates all of the
 * type-tag handling, per-element extraction, and reusable-buffer management away from the evaluator, which only needs
 * to orchestrate the decoding and feed the resulting typed arrays to the distance kernels.
 */
public final class VectorListDecoder {

    private final SourceLocation sourceLoc;
    private final IPointable tempArrVal = new VoidPointable();
    private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();

    public VectorListDecoder(SourceLocation sourceLoc, FunctionIdentifier funcId) {
        this.sourceLoc = sourceLoc;
    }

    public boolean checkListType(IPointable pointable) {
        return ATYPETAGDESERIALIZER.deserialize(pointable.getByteArray()[pointable.getStartOffset()]).isListType();
    }

    public double[] createArrayFromList(ListAccessor listAccessor, double[] primitiveArray) throws IOException {
        ATypeTag typeTag = listAccessor.getItemType();
        for (int i = 0; i < listAccessor.size(); i++) {
            listAccessor.getOrWriteItem(i, tempArrVal, storage);
            primitiveArray[i] = extractNumericVector(tempArrVal, typeTag);
        }
        return primitiveArray;
    }

    private double extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws TypeMismatchException {
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();
        if (derivedTypeTag == ATypeTag.ANY) {
            derivedTypeTag = ATYPETAGDESERIALIZER.deserialize(data[offset]);
        }
        if (isNumericType(derivedTypeTag)) {
            return getValueFromTag(derivedTypeTag, data, offset);
        } else {
            throw new TypeMismatchException(sourceLoc, derivedTypeTag.serialize(), ATypeTag.DOUBLE.serialize());
        }
    }

    private double getValueFromTag(ATypeTag typeTag, byte[] data, int offset) {
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

    public ATypeTag resolveArgType(ListAccessor listAccessor) {
        ATypeTag itemType = listAccessor.getItemType();
        return itemType == ATypeTag.ANY ? resolveAnyItemType(listAccessor) : itemType;
    }

    public ATypeTag resolveAnyItemType(ListAccessor listAccessor) {
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

    /**
     * For {@code list(any)} values, {@link #resolveArgType(ListAccessor)} derives the effective item type from the first
     * element's runtime tag. This helper checks that all items share that same runtime tag.
     *
     * @return the first mismatching runtime tag, or {@code null} if homogeneous / not {@code ANY}
     */
    public ATypeTag findFirstMismatchedAnyItemType(ListAccessor listAccessor, ATypeTag expectedItemTag)
            throws IOException {
        if (listAccessor.getItemType() != ATypeTag.ANY) {
            return null;
        }
        for (int i = 0; i < listAccessor.size(); i++) {
            ATypeTag actual = listAccessor.getItemTypeAt(i);
            if (actual != expectedItemTag) {
                return actual;
            }
        }
        return null;
    }

    public boolean isNumericType(ATypeTag typeTag) {
        return ATypeHierarchy.getTypeDomain(typeTag) == ATypeHierarchy.Domain.NUMERIC;
    }

    public double[] ensureDoubleCapacity(double[] arr, int size) {
        return arr.length == size ? arr : new double[size];
    }
}
