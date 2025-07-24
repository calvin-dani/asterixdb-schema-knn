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

public class VectorDistanceArrCalculation {

    public static double euclidean(double[] a, double[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = euclideanSquared(a, b);
        return Math.sqrt(sum);
    }

    public static double euclidean(float[] a, float[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = euclideanSquared(a, b);
        return Math.sqrt(sum);
    }

    public static double euclidean(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = euclideanSquared(a, b);
        return Math.sqrt(sum);
    }

    public static double euclidean(short[] a, short[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = euclideanSquared(a, b);
        return Math.sqrt(sum);
    }

    public static double euclidean(int[] a, int[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = euclideanSquared(a, b);
        return Math.sqrt(sum);
    }

    public static double euclidean(long[] a, long[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = euclideanSquared(a, b);
        return Math.sqrt(sum);
    }

    public static double euclideanSquared(float[] a, float[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = (double) a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double euclideanSquared(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = (double) a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double euclideanSquared(short[] a, short[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = (double) a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double euclideanSquared(int[] a, int[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = (double) a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double euclideanSquared(long[] a, long[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = (double) a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double euclideanSquared(double[] a, double[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double manhattan(double[] a, double[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.abs(a[i] - b[i]);
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

    public static double cosine(double[] a, double[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Double.NaN; // or throw exception for zero vector
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double cosine(float[] a, float[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Double.NaN; // or throw exception for zero vector
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double cosine(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Double.NaN;
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double cosine(short[] a, short[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Double.NaN;
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double cosine(int[] a, int[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Double.NaN;
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double cosine(long[] a, long[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Double.NaN;
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double dot(double[] a, double[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

    public static double dot(float[] a, float[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

    public static double dot(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += (double) a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN;
        }
        return sum;
    }

    public static double dot(short[] a, short[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += (double) a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN;
        }
        return sum;
    }

    public static double dot(int[] a, int[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += (double) a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN;
        }
        return sum;
    }

    public static double dot(long[] a, long[] b) {
        if (a.length != b.length) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += (double) a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN;
        }
        return sum;
    }
}
