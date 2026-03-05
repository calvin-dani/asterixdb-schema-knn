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
package org.apache.asterix.runtime.aggregates.cluster;

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.aggregates.std.AbstractAggregateFunction;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.utils.FaissWrapper;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class KmeansClusterEvalFactory implements IAggregateEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private final IScalarEvaluatorFactory[] args;
    private final boolean isLocal;
    private final SourceLocation sourceLoc;

    public KmeansClusterEvalFactory(IScalarEvaluatorFactory[] args, boolean isLocal, SourceLocation sourceLoc) {
        this.args = args;
        this.isLocal = isLocal;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
        return new AbstractAggregateFunction(sourceLoc) {

            private boolean first = true;
            // Needs to copy the bytes from inputVal to outputVal because the byte space of inputVal could be re-used
            // by consequent tuples.
            private ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
            private IPointable inputVal = new VoidPointable();
            private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
            private final byte[] nullBytes = new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG };
            private final byte[] systemNullBytes = new byte[] { ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG };
            private ISerializerDeserializer<AInt64> bigIntSerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
            public float[] primitiveArrayConstant;
            public List<Float> primitiveArrayList =  new ArrayList<>();
            public int counter = 0;
            @Override
            public void init() throws HyracksDataException {
                first = true;
            }

            @Override
            public void step(IFrameTupleReference tuple) throws HyracksDataException {

                eval.evaluate(tuple, inputVal);
                ListAccessor listAccessorConstant = new ListAccessor();
                counter++;
                if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                        .isListType()) {
                    return;
//                    throw new RuntimeDataException(ErrorCode.INVALID_FORMAT, sourceLoc, funcId.getName(),
//                            pointables[i].toString());
                }
                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                try {
                    primitiveArrayConstant = createPrimitveList(listAccessorConstant);
                } catch (Exception e) {

//                    throw new HyracksDataException("Error creating primitive list from constant vector", e);
                }


//                if (typeTagByte == ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG) {
//                    // Ignores SYSTEM_NULLs generated by local-first-element.
//                    return;
//                }
//                outputVal.assign(inputVal);
                for (float value : primitiveArrayConstant) {
                    primitiveArrayList.add(value);
                }
            }

            protected float[] createPrimitveList(ListAccessor listAccessor) throws IOException {
                ATypeTag typeTag = listAccessor.getItemType();
                float[] primitiveArray = new float[listAccessor.size()];
                IPointable tempVal = new VoidPointable();
                ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                for (int i = 0; i < listAccessor.size(); i++) {
                    listAccessor.getOrWriteItem(i, tempVal, storage);
                    primitiveArray[i] = extractNumericVector(tempVal, typeTag);
                }
                return primitiveArray;
            }

            protected float extractNumericVector(IPointable pointable, ATypeTag derivedTypeTag) throws HyracksDataException {
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

            protected float getValueFromTag(ATypeTag typeTag, byte[] data, int offset) throws HyracksDataException {
                return switch (typeTag) {
                    case TINYINT -> AInt8SerializerDeserializer.getByte(data, offset + 1);
                    case SMALLINT -> AInt16SerializerDeserializer.getShort(data, offset + 1);
                    case INTEGER -> AInt32SerializerDeserializer.getInt(data, offset + 1);
                    case BIGINT -> AInt64SerializerDeserializer.getLong(data, offset + 1);
                    case FLOAT -> AFloatSerializerDeserializer.getFloat(data, offset + 1);
//                    case DOUBLE -> ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                    default -> Float.NaN;
                };
            }



        @Override
            public void finish(IPointable result) throws HyracksDataException {
//            outputVal = anySerde.serialize(new AOrderedList(primitiveArrayList));
            outputVal.reset();
            float[] arr = new float[primitiveArrayList.size()];
            for (int i = 0; i < primitiveArrayList.size(); i++) {
                arr[i] = primitiveArrayList.get(i);
            }
            // TODO CALVIN DANI : TO check JNI
            float[] kmeancentroid = FaissWrapper.trainAndGetCentroids(3, 2, arr, counter);
            System.err.println("kmeancentroid: " + Arrays.toString(kmeancentroid));
            bigIntSerde.serialize(new AInt64(1),outputVal.getDataOutput());
            result.set(outputVal);
            }

            @Override
            public void finishPartial(IPointable result) throws HyracksDataException {
                finish(result);
            }

        };
    }

}
