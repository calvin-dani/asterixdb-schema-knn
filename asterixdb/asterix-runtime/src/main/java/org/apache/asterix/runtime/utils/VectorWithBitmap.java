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

public class VectorWithBitmap {
    public final double[] values;
    public final long[] bitmap;

    public VectorWithBitmap(double[] values, long[] bitmap) {
        this.values = values;
        this.bitmap = bitmap;
    }

    public VectorWithBitmap() {
        this.values = new double[1024];
        this.bitmap = new long[16];;
    }

    public interface DoublePairConsumer {
        void apply(int index, double aVal, double bVal);
    }

    public void forEachNonZeroPair(VectorWithBitmap other, DoublePairConsumer consumer) {
        int wordCount = bitmap.length;
        for (int w = 0; w < wordCount; w++) {
            long active = this.bitmap[w] & other.bitmap[w];
            while (active != 0) {
                int bit = Long.numberOfTrailingZeros(active);
                int index = w * 64 + bit;
                consumer.apply(index, this.values[index], other.values[index]);
                active &= ~(1L << bit); // clear bit
            }
        }
    }
}