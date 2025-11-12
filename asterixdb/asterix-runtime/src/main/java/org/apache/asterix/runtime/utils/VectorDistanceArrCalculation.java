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

    // Euclidean Distance
    public static double euclidean(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return Math.sqrt(sum);
    }

    public static double euclidean(int[] a, int[] b) {
        long sum = 0L;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            sum += (long) diff * (long) diff;
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return Math.sqrt(sumDouble);
    }

    public static double euclidean(long[] a, long[] b) {
        long sum = 0L;
        for (int i = 0; i < a.length; i++) {
            long diff = a[i] - b[i];
            sum += diff * diff;
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return Math.sqrt(sumDouble);
    }

    public static double euclidean(float[] a, float[] b) {
        float sum = 0.0f;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return Math.sqrt(sumDouble);
    }

    public static double euclidean_squared(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

    public static double euclidean_squared(int[] a, int[] b) {
        long sum = 0L;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            sum += (long) diff * (long) diff;
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

    public static double euclidean_squared(long[] a, long[] b) {
        long sum = 0L;
        for (int i = 0; i < a.length; i++) {
            long diff = a[i] - b[i];
            sum += diff * diff;
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

    public static double euclidean_squared(float[] a, float[] b) {
        float sum = 0.0f;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

    public static double faissL2(float[] a, float[] b) {
        float[] kmeancentroid = FaissWrapper.trainAndGetCentroids(1, 10, a, 49);
        System.out.println("kmeancentroid: " + kmeancentroid);
        return FaissWrapper.l2sqr(a, b);
    }

    // Manhattan Distance
    public static double manhattan(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.abs(a[i] - b[i]);
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

    public static double manhattan(int[] a, int[] b) {
        long sum = 0L;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            sum += Math.abs((long) diff);
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

    public static double manhattan(long[] a, long[] b) {
        long sum = 0L;
        for (int i = 0; i < a.length; i++) {
            long diff = a[i] - b[i];
            sum += Math.abs(diff);
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

    public static double manhattan(float[] a, float[] b) {
        float sum = 0.0f;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            sum += Math.abs(diff);
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

    // Cosine Similarity
    public static double cosine(double[] a, double[] b) {
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Float.NaN; // or throw exception for zero vector
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double cosine(int[] a, int[] b) {
        long dot = 0L;
        long normA = 0L;
        long normB = 0L;
        for (int i = 0; i < a.length; i++) {
            dot += (long) a[i] * (long) b[i];
            normA += (long) a[i] * (long) a[i];
            normB += (long) b[i] * (long) b[i];
        }
        double dotDouble = (double) dot;
        double normADouble = (double) normA;
        double normBDouble = (double) normB;
        if (normADouble == 0.0 || normBDouble == 0.0 || Double.isNaN(normADouble) || Double.isNaN(normBDouble)
                || Double.isNaN(dotDouble)) {
            return Float.NaN;
        }
        return dotDouble / (Math.sqrt(normADouble) * Math.sqrt(normBDouble));
    }

    public static double cosine(long[] a, long[] b) {
        long dot = 0L;
        long normA = 0L;
        long normB = 0L;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        double dotDouble = (double) dot;
        double normADouble = (double) normA;
        double normBDouble = (double) normB;
        if (normADouble == 0.0 || normBDouble == 0.0 || Double.isNaN(normADouble) || Double.isNaN(normBDouble)
                || Double.isNaN(dotDouble)) {
            return Float.NaN;
        }
        return dotDouble / (Math.sqrt(normADouble) * Math.sqrt(normBDouble));
    }

    public static double cosine(float[] a, float[] b) {
        float dot = 0.0f;
        float normA = 0.0f;
        float normB = 0.0f;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        double dotDouble = (double) dot;
        double normADouble = (double) normA;
        double normBDouble = (double) normB;
        if (normADouble == 0.0 || normBDouble == 0.0 || Double.isNaN(normADouble) || Double.isNaN(normBDouble)
                || Double.isNaN(dotDouble)) {
            return Float.NaN;
        }
        return dotDouble / (Math.sqrt(normADouble) * Math.sqrt(normBDouble));
    }

    // Dot Product
    public static double dot(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

    public static double dot(int[] a, int[] b) {
        long sum = 0L;
        for (int i = 0; i < a.length; i++) {
            sum += (long) a[i] * (long) b[i];
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

    public static double dot(long[] a, long[] b) {
        long sum = 0L;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

    public static double dot(float[] a, float[] b) {
        float sum = 0.0f;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        double sumDouble = (double) sum;
        if (Double.isNaN(sumDouble)) {
            return Double.NaN;
        }
        return sumDouble;
    }

}
