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

package org.apache.asterix.runtime.evaluators.functions;

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
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
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.aggregates.base.SingleFieldFrameTupleReference;
import org.apache.asterix.runtime.base.ListAccessorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import smile.math.MathEx;
import smile.math.distance.EuclideanDistance;
import smile.math.distance.ManhattanDistance;

@MissingNullInOutFunction
public class VectorDistanceDescriptor2 extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType inputListType;

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new VectorDistanceDescriptor2();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.VECTOR_DISTANCE_TEMP;
    }

    @Override
    public void setImmutableStates(Object... states) {
        inputListType = (IAType) states[0];
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new VectorDistanceEval(args, ctx);

            }

        };
    }

    public class VectorDistanceEval implements IScalarEvaluator {
        protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

        protected DataOutput dataOutput = resultStorage.getDataOutput();
        private final IScalarEvaluator listEval0;
        private final IScalarEvaluator listEval1;
        private final IScalarEvaluator stringEval;
        //
        private final IPointable list0;
        private final IPointable list1;
        private final IPointable format;

        private final AbstractPointable pointable0;
        private final AbstractPointable pointable1;
        private final TaggedValuePointable formatArg;
        private final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;
        private final IObjectPool<ListAccessor, ATypeTag> listAccessorAllocator;
        private final TypeCaster caster;
        private final ArrayBackedValueStorage finalStorage;
        private ArrayBackedValueStorage storage;
        private IAsterixListBuilder orderedListBuilder;
        private IAsterixListBuilder unorderedListBuilder;
        //        private final IUnnestingEvaluator scanCollection1 = scanCollectionFactory1.createUnnestingEvaluator(ctx);
        //        private final IUnnestingEvaluator scanCollection2 = scanCollectionFactory2.createUnnestingEvaluator(ctx);
        private static final UTF8StringPointable EUCLIDEAN_DISTANCE =
                UTF8StringPointable.generateUTF8Pointable("euclidean distance");
        private static final UTF8StringPointable MANHATTAN_FORMAT =
                UTF8StringPointable.generateUTF8Pointable("manhattan distance");
        private static final UTF8StringPointable COSINE_FORMAT =
                UTF8StringPointable.generateUTF8Pointable("cosine similarity");
        private static final UTF8StringPointable DOT_PRODUCT_FORMAT =
                UTF8StringPointable.generateUTF8Pointable("dot product");
        private final UTF8StringPointable formatPointable = new UTF8StringPointable();
        private final SingleFieldFrameTupleReference itemTuple = new SingleFieldFrameTupleReference();
        private final IScalarEvaluator colEval;
        private final List<Double> vectorList0 = new ArrayList<>();
        private final List<Double> vectorList1 = new ArrayList<>();
        private boolean vectorCalSuccess = true;
        private final ISerializerDeserializer<ADouble> doubleSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
        private final AMutableDouble aDouble = new AMutableDouble(-1);

        public VectorDistanceEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
            listAccessorAllocator = new ListObjectPool<>(new ListAccessorFactory());
            finalStorage = new ArrayBackedValueStorage();
            listEval0 = args[0].createScalarEvaluator(ctx);
            listEval1 = args[1].createScalarEvaluator(ctx);
            stringEval = args[2].createScalarEvaluator(ctx);
            colEval = new ColumnAccessEvalFactory(0).createScalarEvaluator(ctx);
            list0 = new VoidPointable();
            list1 = new VoidPointable();
            format = new VoidPointable();
            pointable0 = new VoidPointable();
            pointable1 = new VoidPointable();
            caster = new TypeCaster(sourceLoc);
            formatArg = new TaggedValuePointable();
            orderedListBuilder = null;
            unorderedListBuilder = null;
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();
            // Evaluate the input arguments
            evaluateInputs(tuple, result);

            if (PointableHelper.checkAndSetMissingOrNull(result, pointable0, pointable1, formatArg)) {
                return;
            }

            ATypeTag listType0 =
                    ATYPETAGDESERIALIZER.deserialize(pointable0.getByteArray()[pointable0.getStartOffset()]);
            ATypeTag listType1 =
                    ATYPETAGDESERIALIZER.deserialize(pointable1.getByteArray()[pointable1.getStartOffset()]);

            if (!ATypeHierarchy.isCompatible(ATYPETAGDESERIALIZER.deserialize(formatArg.getTag()), ATypeTag.STRING)
                    || !listType0.isListType() || !listType1.isListType()) {
                PointableHelper.setNull(result);
                return;
            }

            try {
                caster.allocateAndCast(pointable0, inputListType, list0,
                        DefaultOpenFieldType.getDefaultOpenFieldType(listType0));
                IAsterixListBuilder listBuilder;
                if (listType0 == ATypeTag.ARRAY) {
                    if (orderedListBuilder == null) {
                        orderedListBuilder = new OrderedListBuilder();
                    }
                    listBuilder = orderedListBuilder;
                } else {
                    if (unorderedListBuilder == null) {
                        unorderedListBuilder = new UnorderedListBuilder();
                    }
                    listBuilder = unorderedListBuilder;
                }
                ListAccessor mainListAccessor = listAccessorAllocator.allocate(null);
                listBuilder.reset((AbstractCollectionType) DefaultOpenFieldType.getDefaultOpenFieldType(listType0));
                mainListAccessor.reset(list0.getByteArray(), list0.getStartOffset());
                vectorList0.clear();

                storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                process(mainListAccessor, listBuilder);

                vectorCalSuccess = true;
                final IPointable listItemOut = new VoidPointable();
                //                final ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory scanCollectionFactory1 =
                //                        new ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory(args[0], sourceLoc, getIdentifier());
                //                final ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory scanCollectionFactory2 =
                //                        new ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory(args[1], sourceLoc, getIdentifier());

                // Extract numeric vectors from the first collection
                //                scanCollection1.init(tuple);
                //                extractVectorFromCollection(scanCollection1, listItemOut, vectorList0);

                // Extract numeric vectors from the second collection
                vectorList1.clear();
                //                scanCollection2.init(tuple);
                //                extractVectorFromCollection(scanCollection2, listItemOut, vectorList1);

                // Check for missing or null values in the input

                // Calculate the distance based on the format
                double distanceCal = calculateDistance();
                writeResult(distanceCal);

            } catch (IOException e) {
                throw HyracksDataException.create(e);
            } finally {
                storageAllocator.reset();
                listAccessorAllocator.reset();
                caster.deallocatePointables();
            }

            // Write the result to the output
            result.set(resultStorage);
        }

        private void process(ListAccessor listAccessor, IAsterixListBuilder listBuilder) throws IOException {
            boolean itemInStorage;
            for (int i = 0; i < listAccessor.size(); i++) {
                itemInStorage = listAccessor.getOrWriteItem(i, pointable0, storage);
                // if item is not a list or depth is reached, write it
                if (!ATYPETAGDESERIALIZER.deserialize(pointable0.getByteArray()[pointable0.getStartOffset()])
                        .isListType()) {
                    itemTuple.reset(pointable0.getByteArray(), pointable0.getStartOffset(), pointable0.getLength());
                    if (!extractNumericVector(itemTuple, colEval, vectorList0)) {
                        vectorCalSuccess = false;
                    }
                } else {
                    // recurse on the sublist
                    ListAccessor newListAccessor = listAccessorAllocator.allocate(null);
                    newListAccessor.reset(pointable0.getByteArray(), pointable0.getStartOffset());
                    if (itemInStorage) {
                        // create a new storage since the item is using it
                        storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                        storage.reset();
                    }
                    process(newListAccessor, listBuilder);
                }
            }
        }

        protected void writeResult(double distance) throws IOException {
            if (vectorCalSuccess) {
                aDouble.setValue(distance);
                doubleSerde.serialize(aDouble, dataOutput);
            } else {
                ISerializerDeserializer<ANull> nullSerde =
                        SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

                nullSerde.serialize(ANull.NULL, dataOutput);
            }

        }

        public boolean checkDimension(List<Double> vectorList0, List<Double> vectorList1) {
            if (vectorList0 == null || vectorList1 == null || vectorList0.isEmpty() || vectorList1.isEmpty()) {
                return false; // Return false if either list is null
            }
            return vectorList0.size() == vectorList1.size(); // Check if the sizes of both lists are equal

        }

        /**
         * Evaluates the input arguments and stores the results in pointables.
         */
        private void evaluateInputs(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            listEval0.evaluate(tuple, pointable0);
            listEval1.evaluate(tuple, pointable1);
            stringEval.evaluate(tuple, formatArg);

        }

        /**
         * Extracts numeric vectors from a collection and updates the success flag.
         *
         * @param scanCollection The collection to scan.
         * @param listItemOut    The pointable for list items.
         * @param vectorList     The list to store extracted numeric values.
         */
        private void extractVectorFromCollection(IUnnestingEvaluator scanCollection, IPointable listItemOut,
                List<Double> vectorList) throws HyracksDataException {
            while (scanCollection.step(listItemOut)) {
                itemTuple.reset(listItemOut.getByteArray(), listItemOut.getStartOffset(), listItemOut.getLength());
                if (!extractNumericVector(itemTuple, colEval, vectorList)) {
                    vectorCalSuccess = false;
                }
            }

        }

        /**
         * Calculates the distance between two vectors based on the format.
         *
         * @return The calculated distance.
         * @throws HyracksDataException If the format is invalid.
         */
        private double calculateDistance() throws HyracksDataException {
            double distanceCal = Float.MAX_VALUE;

            formatPointable.set(formatArg.getByteArray(), formatArg.getStartOffset() + 1, formatArg.getLength());
            vectorList1.addAll(vectorList0);
            if (checkDimension(vectorList0, vectorList1) && vectorCalSuccess) {
                double[] vector1 = vectorList0.stream().mapToDouble(Double::doubleValue).toArray();
                //                vectorList1.addAll(vectorList1); // Ensure vectorList1 is not modified
                double[] vector2 = vectorList1.stream().mapToDouble(Double::doubleValue).toArray();
                if (MANHATTAN_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                    ManhattanDistance distance = new ManhattanDistance();
                    distanceCal = distance.d(vector1, vector2);
                } else if (EUCLIDEAN_DISTANCE.ignoreCaseCompareTo(formatPointable) == 0) {
                    EuclideanDistance distance = new EuclideanDistance();
                    distanceCal = distance.d(vector1, vector2);
                } else if (COSINE_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                    double dotCal = MathEx.dot(vector1, vector2);
                    distanceCal = dotCal / (MathEx.norm2(vector1) * MathEx.norm2(vector2));
                } else if (DOT_PRODUCT_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                    distanceCal = MathEx.dot(vector1, vector2);
                } else {
                    throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, getIdentifier().getName(),
                            formatPointable.toString());
                }
            } else {
                vectorCalSuccess = false;
            }

            return distanceCal;
        }

        /**
         * Extracts a numeric vector from the given pointable.
         * Assumes that the pointable contains an array of numeric values.
         *
         * @param pointable The pointable containing the array.
         * @return A double array representing the numeric vector.
         * @throws HyracksDataException If there is an error during extraction.
         */

        public boolean extractNumericVector(IFrameTupleReference pointable, IScalarEvaluator colEval,
                List<Double> vector) throws HyracksDataException {
            IPointable inputVal = new VoidPointable();
            colEval.evaluate(pointable, inputVal);
            byte[] data = inputVal.getByteArray();
            int offset = inputVal.getStartOffset();

            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
            if (!typeTag.isNumericType()) {
                return false; // Return an empty array if the item type is not numeric
            }
            switch (typeTag) {
                case TINYINT:
                    vector.add((double) AInt8SerializerDeserializer.getByte(data, offset + 1));
                    break;
                case SMALLINT:
                    vector.add((double) AInt16SerializerDeserializer.getShort(data, offset + 1));
                    break;
                case INTEGER:
                    vector.add((double) AInt32SerializerDeserializer.getInt(data, offset + 1));
                    break;
                case BIGINT:
                    vector.add((double) AInt64SerializerDeserializer.getLong(data, offset + 1));
                    break;
                case FLOAT:
                    vector.add((double) AFloatSerializerDeserializer.getFloat(data, offset + 1));
                    break;
                case DOUBLE:
                    vector.add(ADoubleSerializerDeserializer.getDouble(data, offset + 1));
                    break;
                default:
                    return false; // Return an empty array if the item type is not numeric
            }
            return true; // Return an empty array if the item type is not numeric
        }
    }

}
