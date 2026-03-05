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
package org.apache.asterix.runtime.operators;

import static org.apache.asterix.runtime.utils.VectorDistanceArrCalculation.euclidean_squared;

import java.io.IOException;
import java.util.List;
import java.util.Random;

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
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class KMeansUtils {

    protected final IPointable tempVal;
    protected final ArrayBackedValueStorage storage;

    KMeansUtils(IPointable tempVal, ArrayBackedValueStorage storage) {
        this.tempVal = tempVal;
        this.storage = storage;

    }

    // Euclidean Distance
    protected double[] createPrimitveList(ListAccessor listAccessor) throws IOException {
        ATypeTag typeTag = listAccessor.getItemType();
        double[] primitiveArray = new double[listAccessor.size()];
        storage.reset();
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

    protected int findClosest(double[][] centers, double[] point) {
        int best = 0;
        double bestDist = euclidean_squared(centers[0], point);
        for (int i = 1; i < centers.length; i++) {
            double dist = euclidean_squared(centers[i], point);
            if (dist < bestDist) {
                bestDist = dist;
                best = i;
            }
        }
        return best;
    }

    protected int pickWeightedIndex(Random rand, List<double[]> data, int[] weights) {
        float sum = 0f;
        for (double w : weights) {
            sum += w;
        }
        double r = rand.nextDouble() * sum;
        double curWeight = 0.0;
        int i = 0;
        while (i < data.size() && curWeight < r) {
            curWeight += weights[i];
            i++;
        }
        return Math.max(0, i - 1);
    }

    protected void addWeighted(double[] sum, double[] point, double weight) {
        for (int i = 0; i < sum.length; i++) {
            sum[i] += point[i] * weight;
        }
    }

    protected void scale(double[] vec, double factor) {
        for (int i = 0; i < vec.length; i++) {
            vec[i] *= factor;
        }
    }

}
