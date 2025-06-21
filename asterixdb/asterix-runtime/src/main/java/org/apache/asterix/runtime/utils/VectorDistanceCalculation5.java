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

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class VectorDistanceCalculation5 {
    protected static VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    //    // Euclidean Distance
    public static double euclidean(double[] a, double[] b) {
        //        checkDimensions(a, b);
        int i = 0;
        double sumSq = 0.0;
        for (; i <= a.length - SPECIES.length(); i += SPECIES.length()) {
            var v1 = DoubleVector.fromArray(SPECIES, a, i);
            var v2 = DoubleVector.fromArray(SPECIES, b, i);
            var diff = v1.sub(v2);
            var sq = diff.mul(diff);
            sumSq += sq.reduceLanes(VectorOperators.ADD);
        }
        for (; i < a.length; i++) {
            double diff = a[i] - b[i];
            sumSq += diff * diff;
        }
        return Math.sqrt(sumSq);
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
        double sum = 0.0;
        int i = 0;
        for (; i <= a.length - SPECIES.length(); i += SPECIES.length()) {
            var v1 = DoubleVector.fromArray(SPECIES, a, i);
            var v2 = DoubleVector.fromArray(SPECIES, b, i);
            var diff = v1.sub(v2).abs();
            sum += diff.reduceLanes(VectorOperators.ADD);
        }
        // scalar fallback
        for (; i < a.length; i++) {
            sum += Math.abs(a[i] - b[i]);
        }
        return sum;
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
        double dot = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;
        int i = 0;
        for (; i <= a.length - SPECIES.length(); i += SPECIES.length()) {
            var v1 = DoubleVector.fromArray(SPECIES, a, i);
            var v2 = DoubleVector.fromArray(SPECIES, b, i);

            dot += v1.mul(v2).reduceLanes(VectorOperators.ADD);
            norm1 += v1.mul(v1).reduceLanes(VectorOperators.ADD);
            norm2 += v2.mul(v2).reduceLanes(VectorOperators.ADD);
        }

        for (; i < a.length; i++) {
            dot += a[i] * b[i];
            norm1 += a[i] * a[i];
            norm2 += b[i] * b[i];
        }

        return dot / (Math.sqrt(norm1) * Math.sqrt(norm2));
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
        int i = 0;
        double sum = 0.0;
        for (; i <= a.length - SPECIES.length(); i += SPECIES.length()) {
            var v1 = DoubleVector.fromArray(SPECIES, a, i);
            var v2 = DoubleVector.fromArray(SPECIES, b, i);
            sum += v1.mul(v2).reduceLanes(VectorOperators.ADD);
        }
        for (; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
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
