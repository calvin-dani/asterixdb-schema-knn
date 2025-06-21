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

package org.apache.asterix.runtime.utils;

import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import smile.math.MathEx;
import smile.math.distance.EuclideanDistance;
import smile.math.distance.ManhattanDistance;

public class VectorDistanceCalculation2 {

    //    // Euclidean Distance
    public static double euclidean(double[] a, double[] b) {
        //        checkDimensions(a, b);
        EuclideanDistance distance = new EuclideanDistance();
        return distance.d(a, b);
    }

    public static double euclidean(ListAccessor a, ListAccessor b) throws HyracksDataException {
        //        checkDimensions(a, b);
        ATypeTag listType1 = a.getItemType();
        ATypeTag listType2 = b.getItemType();
        IPointable tempVal1 = new VoidPointable();
        ArrayBackedValueStorage storage1 = new ArrayBackedValueStorage();
        IPointable tempVal2 = new VoidPointable();
        ArrayBackedValueStorage storage2 = new ArrayBackedValueStorage();
        try {
            double sum = 0.0;
            double l1 = 0.0;
            double l2 = 0.0;
            for (int i = 0; i < a.size(); i++) {
                a.getOrWriteItem(i, tempVal1, storage1);
                b.getOrWriteItem(i, tempVal2, storage2);
                l1 = extractNumericVector(tempVal1, listType1);
                l2 = extractNumericVector(tempVal2, listType2);
                sum += Math.abs(l1 - l2);
                double diff = l1 - l2;
                sum += diff * diff;
            }
            return sum;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    // Manhattan Distance
    public static double manhattan(double[] a, double[] b) {
        //        checkDimensions(a, b);
        ManhattanDistance distance = new ManhattanDistance();
        return distance.d(a, b);
    }

    public static double manhattan(ListAccessor a, ListAccessor b) throws HyracksDataException {
        //        checkDimensions(a, b);
        ATypeTag listType1 = a.getItemType();
        ATypeTag listType2 = b.getItemType();
        IPointable tempVal1 = new VoidPointable();
        ArrayBackedValueStorage storage1 = new ArrayBackedValueStorage();
        IPointable tempVal2 = new VoidPointable();
        ArrayBackedValueStorage storage2 = new ArrayBackedValueStorage();
        try {
            double sum = 0.0;
            double l1 = 0.0;
            double l2 = 0.0;
            for (int i = 0; i < a.size(); i++) {
                a.getOrWriteItem(i, tempVal1, storage1);
                b.getOrWriteItem(i, tempVal2, storage2);
                l1 = extractNumericVector(tempVal1, listType1);
                l2 = extractNumericVector(tempVal2, listType2);
                sum += Math.abs(l1 - l2);
            }
            return sum;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    // Cosine Similarity
    public static double cosine(double[] a, double[] b) {
        //        checkDimensions(a, b);
        double dotCal = MathEx.dot(a, b);
        return dotCal / (MathEx.norm2(a) * MathEx.norm2(b));

    }

    public static double cosine(ListAccessor a, ListAccessor b) throws HyracksDataException {
        //        checkDimensions(a, b);
        ATypeTag listType1 = a.getItemType();
        ATypeTag listType2 = b.getItemType();
        IPointable tempVal1 = new VoidPointable();
        ArrayBackedValueStorage storage1 = new ArrayBackedValueStorage();
        IPointable tempVal2 = new VoidPointable();
        ArrayBackedValueStorage storage2 = new ArrayBackedValueStorage();
        try {
            double dot = 0.0, normA = 0.0, normB = 0.0;
            double l1 = 0.0;
            double l2 = 0.0;
            for (int i = 0; i < a.size(); i++) {
                a.getOrWriteItem(i, tempVal1, storage1);
                b.getOrWriteItem(i, tempVal2, storage2);
                l1 = extractNumericVector(tempVal1, listType1);
                l2 = extractNumericVector(tempVal2, listType2);
                dot += l1 * l2;
                normA += l1 * l1;
                normB += l2 * l2;
            }
            if (normA == 0.0 || normB == 0.0) {
                return 0.0; // or throw exception for zero vector
            }
            return dot / (Math.sqrt(normA) * Math.sqrt(normB));
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    // Dot Product
    public static double dot(double[] a, double[] b) {
        return MathEx.dot(a, b);
    }

    public static double dot(ListAccessor a, ListAccessor b) throws HyracksDataException {
        ATypeTag listType1 = a.getItemType();
        ATypeTag listType2 = b.getItemType();
        IPointable tempVal1 = new VoidPointable();
        ArrayBackedValueStorage storage1 = new ArrayBackedValueStorage();
        IPointable tempVal2 = new VoidPointable();
        ArrayBackedValueStorage storage2 = new ArrayBackedValueStorage();
        try {
            double sum = 0.0;
            double l1 = 0.0;
            double l2 = 0.0;
            for (int i = 0; i < a.size(); i++) {
                a.getOrWriteItem(i, tempVal1, storage1);
                b.getOrWriteItem(i, tempVal2, storage2);
                l1 = extractNumericVector(tempVal1, listType1);
                l2 = extractNumericVector(tempVal2, listType2);
                sum += l1 * l2;
            }
            return sum;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    static double extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws HyracksDataException {
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
    public static double getValueFromTag(ATypeTag typeTag, byte[] data, int offset) throws HyracksDataException {
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
