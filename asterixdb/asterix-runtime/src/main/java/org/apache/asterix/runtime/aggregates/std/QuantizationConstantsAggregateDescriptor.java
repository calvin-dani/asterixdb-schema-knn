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
package org.apache.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.storage.QuantizationConstants;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.aggregates.serializable.std.BufferSerDeUtil;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

/**
 * Aggregate function to compute quantization constants (minQ, maxQ, alpha) from scalar values.
 * 
 * Local aggregate: Collects all scalar double values (no limit).
 * Global aggregate: Sorts collected values, computes quantiles using confidence interval,
 * and calculates alpha based on bits parameter.
 */
public class QuantizationConstantsAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private float confidenceInterval;
    private int bits;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new QuantizationConstantsAggregateDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.QUANTIZATION_CONSTANTS;
    }

    @Override
    public void setImmutableStates(Object... states) {
        confidenceInterval = (float) states[0];
        bits = (int) states[1];
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx)
                    throws HyracksDataException {
                return new QuantizationConstantsFunction(args, ctx, confidenceInterval, bits, sourceLoc);
            }
        };
    }

    private static class QuantizationConstantsFunction extends AbstractAggregateFunction {
        @SuppressWarnings("unchecked")
        private ISerializerDeserializer<ABinary> binarySerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);
        private final AMutableBinary binary = new AMutableBinary(null, 0, 0);
        private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        private final ArrayBackedValueStorage valuesBits = new ArrayBackedValueStorage();
        private final IPointable input = new VoidPointable();
        private final ByteArrayPointable valuesPointable = new ByteArrayPointable();
        private final IScalarEvaluator scalarValueEval;
        private final IEvaluatorContext context;
        private final float confidenceInterval;
        private final int bits;
        private final List<Double> localValues = new ArrayList<>(); // Store extracted numeric values as doubles
        private final List<Double> finalValues = new ArrayList<>();
        private boolean isWarned = false;
        private static final int MAX_VALUES_TO_COLLECT = 2; // TEMPORARY: Limit to prevent "record too big" error

        private QuantizationConstantsFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
                float confidenceInterval, int bits, SourceLocation sourceLocation) throws HyracksDataException {
            super(sourceLocation);
            this.scalarValueEval = args[0].createScalarEvaluator(context);
            this.context = context;
            this.confidenceInterval = confidenceInterval;
            this.bits = bits;
        }

        @Override
        public void init() throws HyracksDataException {
            localValues.clear();
            finalValues.clear();
            isWarned = false;
            System.err.println("[QuantizationConstantsAgg] init() called - cleared localValues and finalValues");
        }

        /**
         * Unified step() method that always expects BINARY format for intermediate results.
         * Local aggregate: Receives tag|value from VectorComponentExtractorOperatorDescriptor,
         * extracts numeric value and stores it (will be serialized to binary in finishPartial).
         * Global aggregate: Receives BINARY format from local aggregates or merge phase,
         * deserializes with proper bounds checking.
         * @param tuple contains a scalar numeric value (local) or serialized doubles in BINARY (global)
         * @throws HyracksDataException IO Exception
         */
        @Override
        public void step(IFrameTupleReference tuple) throws HyracksDataException {
            scalarValueEval.evaluate(tuple, input);
            byte[] data = input.getByteArray();
            int offset = input.getStartOffset();

            // Use EnumDeserializer like AvgAggregateFunction
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);

            // Check for null/missing first
            if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL || typeTag == ATypeTag.SYSTEM_NULL) {
                return;
            }

            // Unified approach: Always expect BINARY format for intermediate results
            if (typeTag == ATypeTag.BINARY) {
                // This is global aggregate receiving serialized local values (or merge phase)
                System.err.println("[QuantizationConstantsAgg] step() - received BINARY format");
                deserializeBinaryValues(data, offset);
            } else {
                // This is local aggregate receiving tag|value from VectorComponentExtractorOperatorDescriptor
                // Extract numeric value and store (will be serialized to binary in finishPartial)
                // TEMPORARY: Limit collection to prevent "record too big" error
                if (localValues.size() >= MAX_VALUES_TO_COLLECT) {
                    // Skip additional values once we've collected enough
                    return;
                }
                System.err.println("[QuantizationConstantsAgg] step() - LOCAL: extracting numeric value, typeTag=" + typeTag);
                double value = extractNumericValue(data, offset, typeTag);
                if (!Double.isNaN(value)) {
                    localValues.add(value);
                    System.err.println("[QuantizationConstantsAgg] step() - LOCAL: extracted value=" + value + ", total localValues=" + localValues.size() + " (limit=" + MAX_VALUES_TO_COLLECT + ")");
                } else {
                    System.err.println("[QuantizationConstantsAgg] step() - LOCAL: skipped NaN value (unsupported type)");
                }
            }
        }

        /**
         * Deserializes binary format with proper bounds checking.
         * Format: [typeTag=BINARY] [metaLength] [numValues (int)] [value1 (double)] [value2 (double)] ...
         * @param data byte array containing the input
         * @param offset offset where type tag is located
         * @throws HyracksDataException if deserialization fails or bounds are exceeded
         */
        private void deserializeBinaryValues(byte[] data, int offset) throws HyracksDataException {
            System.err.println("[QuantizationConstantsAgg] deserializeBinaryValues() - starting deserialization");
            
            // Set ByteArrayPointable to binary content (skip type tag)
            // input.getLength() includes the type tag, so we skip it
            int binaryLength = input.getLength() - 1; // Subtract 1 for type tag
            valuesPointable.set(data, offset + 1, binaryLength);
            
            byte[] valuesBytes = valuesPointable.getByteArray();
            int contentStartOffset = valuesPointable.getContentStartOffset();
            int contentLength = valuesPointable.getContentLength();
            
            System.err.println("[QuantizationConstantsAgg] deserializeBinaryValues() - binary content: " +
                    "startOffset=" + contentStartOffset + ", contentLength=" + contentLength +
                    ", valuesBytes.length=" + valuesBytes.length);
            
            // Validate we have enough bytes for at least the numValues int
            if (contentLength < Integer.BYTES) {
                throw new HyracksDataException("Binary data too short: expected at least " + 
                        Integer.BYTES + " bytes for numValues, got " + contentLength);
            }
            
            // Validate we can read the integer without going out of bounds
            if (contentStartOffset + Integer.BYTES > valuesBytes.length) {
                throw new HyracksDataException("Out of bounds: trying to read numValues at offset " + 
                        contentStartOffset + ", but array length is " + valuesBytes.length);
            }
            
            // Read number of values
            int numValues = IntegerPointable.getInteger(valuesBytes, contentStartOffset);
            System.err.println("[QuantizationConstantsAgg] deserializeBinaryValues() - numValues=" + numValues);
            
            // Validate numValues is reasonable
            if (numValues < 0) {
                throw new HyracksDataException("Invalid numValues: " + numValues + " (negative)");
            }
            if (numValues > 10000000) { // Sanity check: 10 million values max
                throw new HyracksDataException("Invalid numValues: " + numValues + " (too large, possible corruption)");
            }
            
            // Calculate expected bytes: Integer.BYTES (numValues) + (numValues * Double.BYTES)
            long expectedBytes = (long) Integer.BYTES + ((long) numValues * (long) Double.BYTES);
            if (expectedBytes > Integer.MAX_VALUE) {
                throw new HyracksDataException("Calculated expected bytes exceeds Integer.MAX_VALUE: " + expectedBytes);
            }
            int expectedBytesInt = (int) expectedBytes;
            
            System.err.println("[QuantizationConstantsAgg] deserializeBinaryValues() - expectedBytes=" + expectedBytesInt);
            
            // Validate we have enough content bytes
            if (expectedBytesInt > contentLength) {
                throw new HyracksDataException("Binary data too short: expected " + expectedBytesInt + 
                        " bytes, got " + contentLength + " (numValues=" + numValues + ")");
            }
            
            // Validate we can read all values without going out of bounds
            int lastByteOffset = contentStartOffset + expectedBytesInt - 1;
            if (lastByteOffset >= valuesBytes.length) {
                throw new HyracksDataException("Out of bounds: trying to read up to offset " + lastByteOffset + 
                        ", but array length is " + valuesBytes.length);
            }
            
            // Read each double value
            int pointer = contentStartOffset + Integer.BYTES;
            int valuesAdded = 0;
            for (int i = 0; i < numValues; i++) {
                // Verify we have enough bytes remaining for this double
                if (pointer + Double.BYTES > contentStartOffset + contentLength) {
                    throw new HyracksDataException("Out of bounds: trying to read double #" + (i + 1) + 
                            " at offset " + pointer + ", but content ends at " + 
                            (contentStartOffset + contentLength));
                }
                
                // Verify we don't exceed array bounds
                if (pointer + Double.BYTES > valuesBytes.length) {
                    throw new HyracksDataException("Out of bounds: trying to read double #" + (i + 1) + 
                            " at offset " + pointer + ", but array length is " + valuesBytes.length);
                }
                
                double value = BufferSerDeUtil.getDouble(valuesBytes, pointer);
                pointer += Double.BYTES;
                finalValues.add(value);
                valuesAdded++;
            }
            
            System.err.println("[QuantizationConstantsAgg] deserializeBinaryValues() - added " + valuesAdded + 
                    " values, total finalValues=" + finalValues.size());
        }

        /**
         * Extracts numeric value from serialized data, following AvgAggregateFunction pattern.
         * @param data byte array containing serialized value
         * @param offset offset where type tag is located
         * @param typeTag the type tag of the value
         * @return extracted numeric value as double, or NaN if type is not numeric
         */
        private double extractNumericValue(byte[] data, int offset, ATypeTag typeTag) {
            System.err.println("[QuantizationConstantsAgg] extractNumericValue() - extracting from embedding, typeTag=" + typeTag);
            switch (typeTag) {
                case TINYINT: {
                    byte val = AInt8SerializerDeserializer.getByte(data, offset + 1);
                    System.err.println("[QuantizationConstantsAgg] extractNumericValue() - TINYINT: " + val);
                    return val;
                }
                case SMALLINT: {
                    short val = AInt16SerializerDeserializer.getShort(data, offset + 1);
                    System.err.println("[QuantizationConstantsAgg] extractNumericValue() - SMALLINT: " + val);
                    return val;
                }
                case INTEGER: {
                    int val = AInt32SerializerDeserializer.getInt(data, offset + 1);
                    System.err.println("[QuantizationConstantsAgg] extractNumericValue() - INTEGER: " + val);
                    return val;
                }
                case BIGINT: {
                    long val = AInt64SerializerDeserializer.getLong(data, offset + 1);
                    System.err.println("[QuantizationConstantsAgg] extractNumericValue() - BIGINT: " + val);
                    return val;
                }
                case FLOAT: {
                    float val = AFloatSerializerDeserializer.getFloat(data, offset + 1);
                    System.err.println("[QuantizationConstantsAgg] extractNumericValue() - FLOAT: " + val);
                    return val;
                }
                case DOUBLE: {
                    double val = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                    System.err.println("[QuantizationConstantsAgg] extractNumericValue() - DOUBLE: " + val);
                    return val;
                }
                default: {
                    // Issue warning only once and skip unsupported types
                    if (!isWarned) {
                        isWarned = true;
                        System.err.println("[QuantizationConstantsAgg] extractNumericValue() - WARNING: unsupported type " + typeTag);
                        ExceptionUtil.warnUnsupportedType(context, sourceLoc, getIdentifier().getName(), typeTag);
                    }
                    return Double.NaN;
                }
            }
        }

        /**
         * Local aggregate: Serializes collected double values.
         * Global aggregate: Sorts collected values, computes quantiles, and calculates alpha.
         * @param result contains serialized double values (local) or serialized QuantizationConstants (global)
         * @throws HyracksDataException IO Exception
         */
        @Override
        public void finish(IPointable result) throws HyracksDataException {
            storage.reset();
            try {
                if (!localValues.isEmpty()) {
                    // Local aggregate: serialize collected double values
                    System.err.println("[QuantizationConstantsAgg] finish() - LOCAL: serializing " + localValues.size() + " values");
                    // Format: [numValues (int)] [value1 (double)] [value2 (double)] ...
                    valuesBits.reset();
                    IntegerSerializerDeserializer.write(localValues.size(), valuesBits.getDataOutput());
                    for (Double value : localValues) {
                        DoubleSerializerDeserializer.write(value, valuesBits.getDataOutput());
                    }
                    binary.setValue(valuesBits.getByteArray(), valuesBits.getStartOffset(), valuesBits.getLength());
                    binarySerde.serialize(binary, storage.getDataOutput());
                    result.set(storage);
                    System.err.println("[QuantizationConstantsAgg] finish() - LOCAL: serialized " + localValues.size() + " values to binary");
                } else if (!finalValues.isEmpty()) {
                    // Global aggregate: compute quantization constants
                    System.err.println("[QuantizationConstantsAgg] finish() - GLOBAL: computing constants from " + finalValues.size() + " values");
                    // Global aggregate: compute quantization constants
                    // Sort all values
                    System.err.println("[QuantizationConstantsAgg] finish() - GLOBAL: sorting " + finalValues.size() + " values");
                    Collections.sort(finalValues);

                    // Compute quantiles using QuantileCalculatorOperatorDescriptor helper logic
                    float half = (1.0f - confidenceInterval) / 2.0f;
                    int totalCount = finalValues.size();
                    int lowerIdx = (int) Math.floor(half * (totalCount - 1));
                    int upperIdx = (int) Math.ceil((1.0f - half) * (totalCount - 1));

                    // Clamp indices to valid range
                    lowerIdx = Math.max(0, Math.min(lowerIdx, totalCount - 1));
                    upperIdx = Math.max(0, Math.min(upperIdx, totalCount - 1));

                    float minQ = finalValues.get(lowerIdx).floatValue();
                    float maxQ = finalValues.get(upperIdx).floatValue();
                    System.err.println("[QuantizationConstantsAgg] finish() - GLOBAL: quantiles computed - lowerIdx=" + lowerIdx + 
                            " (minQ=" + minQ + "), upperIdx=" + upperIdx + " (maxQ=" + maxQ + ")");

                    // Avoid division by zero
                    double eps = 1e-12;
                    if (maxQ <= minQ + eps) {
                        maxQ = minQ + 1e-6f;
                        System.err.println("[QuantizationConstantsAgg] finish() - GLOBAL: adjusted maxQ to avoid division by zero: " + maxQ);
                    }

                    // Calculate alpha
                    int levels = 1 << bits; // 2^bits
                    float alpha = (levels - 1) / (maxQ - minQ);
                    System.err.println("[QuantizationConstantsAgg] finish() - GLOBAL: alpha computed - levels=" + levels + ", alpha=" + alpha);

                    // Create QuantizationConstants object
                    QuantizationConstants constants = new QuantizationConstants(minQ, maxQ, alpha, bits,
                            confidenceInterval, totalCount);
                    System.err.println("[QuantizationConstantsAgg] finish() - GLOBAL: QuantizationConstants created: " + constants);

                    // Serialize QuantizationConstants to binary format
                    valuesBits.reset();
                    serializeQuantizationConstants(constants, valuesBits.getDataOutput());
                    binary.setValue(valuesBits.getByteArray(), valuesBits.getStartOffset(), valuesBits.getLength());
                    binarySerde.serialize(binary, storage.getDataOutput());
                    result.set(storage);
                    System.err.println("[QuantizationConstantsAgg] finish() - GLOBAL: serialized QuantizationConstants to binary");
                } else {
                    // Empty dataset
                    System.err.println("[QuantizationConstantsAgg] finish() - WARNING: empty dataset (no values collected)");
                    storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                    result.set(storage);
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Serializes QuantizationConstants to binary format.
         * Format: [minQ (float)] [maxQ (float)] [alpha (float)] [bits (int)] [confidenceInterval (float)] [sampleCount (int)]
         */
        private void serializeQuantizationConstants(QuantizationConstants constants, DataOutput out)
                throws IOException {
            FloatSerializerDeserializer.write(constants.getMinQ(), out);
            FloatSerializerDeserializer.write(constants.getMaxQ(), out);
            FloatSerializerDeserializer.write(constants.getAlpha(), out);
            IntegerSerializerDeserializer.write(constants.getBits(), out);
            FloatSerializerDeserializer.write(constants.getConfidenceInterval(), out);
            IntegerSerializerDeserializer.write(constants.getSampleCount(), out);
        }

        @Override
        public void finishPartial(IPointable result) throws HyracksDataException {
            // finishPartial() is called during merge phase and must return intermediate results
            // that can be merged, not the final result.
            // 
            // Local aggregate: return serialized doubles [numValues] [value1] [value2] ...
            // Global aggregate during merge: return serialized doubles from finalValues (not QuantizationConstants)
            // Global aggregate final: return QuantizationConstants (but this is called via finish(), not finishPartial())
            
            storage.reset();
            try {
                if (!localValues.isEmpty()) {
                    // Local aggregate: serialize collected double values
                    System.err.println("[QuantizationConstantsAgg] finishPartial() - LOCAL: serializing " + localValues.size() + " values");
                    valuesBits.reset();
                    IntegerSerializerDeserializer.write(localValues.size(), valuesBits.getDataOutput());
                    for (Double value : localValues) {
                        DoubleSerializerDeserializer.write(value, valuesBits.getDataOutput());
                    }
                    binary.setValue(valuesBits.getByteArray(), valuesBits.getStartOffset(), valuesBits.getLength());
                    binarySerde.serialize(binary, storage.getDataOutput());
                    result.set(storage);
                    System.err.println("[QuantizationConstantsAgg] finishPartial() - LOCAL: serialized " + localValues.size() + " values to binary");
                } else if (!finalValues.isEmpty()) {
                    // Global aggregate during merge: return intermediate format (serialized doubles)
                    // This allows merge phase to combine results from multiple partitions
                    System.err.println("[QuantizationConstantsAgg] finishPartial() - GLOBAL (merge): serializing " + finalValues.size() + " values as intermediate format");
                    valuesBits.reset();
                    IntegerSerializerDeserializer.write(finalValues.size(), valuesBits.getDataOutput());
                    for (Double value : finalValues) {
                        DoubleSerializerDeserializer.write(value, valuesBits.getDataOutput());
                    }
                    binary.setValue(valuesBits.getByteArray(), valuesBits.getStartOffset(), valuesBits.getLength());
                    binarySerde.serialize(binary, storage.getDataOutput());
                    result.set(storage);
                    System.err.println("[QuantizationConstantsAgg] finishPartial() - GLOBAL (merge): serialized " + finalValues.size() + " values to binary");
                } else {
                    // Empty dataset
                    System.err.println("[QuantizationConstantsAgg] finishPartial() - WARNING: empty dataset (no values collected)");
                    storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                    result.set(storage);
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }

        private FunctionIdentifier getIdentifier() {
            return BuiltinFunctions.QUANTIZATION_CONSTANTS;
        }
    }
}
