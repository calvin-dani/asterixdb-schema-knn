
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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.RowMetadata;
import org.apache.asterix.om.api.IRowWriteMultiPageOp;
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.RowSchemaTransformer;
import org.apache.asterix.om.lazy.RowTransformer;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.metadata.RowFieldNamesDictionary;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.ObjectRowSchemaNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.UnsafeUtil;
import org.apache.asterix.runtime.evaluators.common.AccessibleByteArrayEval;
import org.apache.asterix.runtime.evaluators.common.ClosedRecordConstructorEvalFactory.ClosedRecordConstructorEval;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractSchemaAggregateFunction extends AbstractAggregateFunction {
    private static final int SUM_FIELD_ID = 0;
    private static final int COUNT_FIELD_ID = 1;

    private final IEvaluatorContext context;

    // Warning flag to warn only once in case of non-numeric data
    private boolean isWarned;

    private final ARecordType recType;

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    //    private IPointable inputVal = new VoidPointable();
    private RecordLazyVisitablePointable inputVal = new RecordLazyVisitablePointable(true); //TODO : CALVIN_DANI to be tagged
    //    private TypedRecordLazyVisitablePointable inputVal = new TypedRecordLazyVisitablePointable(null);

    private IScalarEvaluator eval;
    protected ATypeTag aggType;
    private double sum;
    private long count;
    private AMutableDouble aDouble = new AMutableDouble(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);

    private IPointable avgBytes = new VoidPointable();
    private ByteArrayAccessibleOutputStream sumBytes = new ByteArrayAccessibleOutputStream();
    private DataOutput sumBytesOutput = new DataOutputStream(sumBytes);
    private ByteArrayAccessibleOutputStream countBytes = new ByteArrayAccessibleOutputStream();
    private DataOutput countBytesOutput = new DataOutputStream(countBytes);
    private IScalarEvaluator evalSum = new AccessibleByteArrayEval(sumBytes);
    private IScalarEvaluator evalCount = new AccessibleByteArrayEval(countBytes);
    private ClosedRecordConstructorEval recordEval;

    private RowTransformer transformer;
    private RowSchemaTransformer schemaTransformer;
    RowMetadata rowMetaData;
    //    private int recordFieldId = 0;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ADouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> longSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractSchemaAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, IAType aggFieldState) throws HyracksDataException {
        super(sourceLoc);
        this.context = context;
        eval = args[0].createScalarEvaluator(context);
        //        recType = new ARecordType(null, new String[] { "sum", "count" },//TODO CALVIN_DANI REMOVE
        //                new IAType[] { BuiltinType.ADOUBLE, BuiltinType.AINT64 }, false);
        recType = (ARecordType) aggFieldState;
        inputVal = new TypedRecordLazyVisitablePointable(recType);
        //        recordEval = new ClosedRecordConstructorEval(recType, new IScalarEvaluator[] { evalSum, evalCount });
    }

    @Override
    public void init() throws HyracksDataException {
        aggType = ATypeTag.SYSTEM_NULL;
        sum = 0.0;
        count = 0;
        isWarned = false;

        // Schema
        Mutable<IRowWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        rowMetaData = new RowMetadata(multiPageOpRef);
        transformer = new RowTransformer(rowMetaData, rowMetaData.getRoot());
        schemaTransformer = new RowSchemaTransformer(rowMetaData, rowMetaData.getRoot());

        //TODO CALVIN_DANI ADD TYPED RECORD DESC
    }

    @Override
    public abstract void step(IFrameTupleReference tuple) throws HyracksDataException;

    @Override
    public abstract void finish(IPointable result) throws HyracksDataException;

    @Override
    public abstract void finishPartial(IPointable result) throws HyracksDataException;

    protected abstract void processNull();

    protected void processDataValues(IFrameTupleReference tuple) throws HyracksDataException {
        if (skipStep()) {
            return;
        }
        //print thread and thread id
        int recordFieldId = rowMetaData.getRecordFieldIndex();
        inputVal.set(tuple.getFieldData(recordFieldId), tuple.getFieldStart(recordFieldId),
                tuple.getFieldLength(recordFieldId));
        transformer.transform(inputVal);
        //        (FrameTupleAccessor)(tuple.getFrameTupleAccessor())
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
        ATypeTag aggTypeTag = aggType;
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull();
            return;
        } else if (aggTypeTag == ATypeTag.SYSTEM_NULL) {
            aggTypeTag = typeTag;
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggTypeTag)) {
            // Issue warning only once and treat current tuple as null
            if (!isWarned) {
                isWarned = true;
                ExceptionUtil.warnUnsupportedType(context, sourceLoc, getIdentifier().getName(), typeTag);
            }
            processNull();
            return;
        } else if (ATypeHierarchy.canPromote(aggTypeTag, typeTag)) {
            aggTypeTag = typeTag;
        }

        switch (typeTag) {
            case TINYINT: {
                byte val = AInt8SerializerDeserializer.getByte(data, offset + 1);
                sum += val;
                break;
            }
            case SMALLINT: {
                short val = AInt16SerializerDeserializer.getShort(data, offset + 1);
                sum += val;
                break;
            }
            case INTEGER: {
                int val = AInt32SerializerDeserializer.getInt(data, offset + 1);
                sum += val;
                break;
            }
            case BIGINT: {
                long val = AInt64SerializerDeserializer.getLong(data, offset + 1);
                sum += val;
                break;
            }
            case FLOAT: {
                float val = AFloatSerializerDeserializer.getFloat(data, offset + 1);
                sum += val;
                break;
            }
            case DOUBLE: {
                double val = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
                sum += val;
                break;
            }
            default: {
                // Issue warning only once and treat current tuple as null
                if (!isWarned) {
                    isWarned = true;
                    ExceptionUtil.warnUnsupportedType(context, sourceLoc, getIdentifier().getName(), typeTag);
                }
                processNull();
                return;
            }
        }
        count++;
        aggType = aggTypeTag;
    }

    protected void finishPartialResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();

        try {

            if (rowMetaData != null) {
//                System.out.println("schema at local written ");
//                System.out.println("Thread: " + Thread.currentThread().getName() + " Thread ID: "
//                        + Thread.currentThread().getId());
                IValueReference serSchema = rowMetaData.serializeColumnsMetadata();
//                System.out.println("schema at local written end ");
                result.set(serSchema);
            } // Double check that count 0 is accounted
            else if (aggType == ATypeTag.SYSTEM_NULL) {
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                result.set(resultStorage);
            } else if (aggType == ATypeTag.NULL) {
                resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                result.set(resultStorage);
            } else {
                sumBytes.reset();
                aDouble.setValue(sum);
                doubleSerde.serialize(aDouble, sumBytesOutput);
                countBytes.reset();
                aInt64.setValue(count);
                longSerde.serialize(aInt64, countBytesOutput);
                recordEval.evaluate(null, avgBytes);
                result.set(avgBytes);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void processPartialResults(IFrameTupleReference tuple) throws HyracksDataException {
        if (skipStep()) {
            return;
        }
        eval.evaluate(tuple, inputVal);

        byte[] serBytes = tuple.getFrameTupleAccessor().getBuffer().array();

        int offset = 9; // TO CHANGE
        //        int length = 732;

        int fieldNamesStart = offset + UnsafeUtil.getInt(serBytes, offset + 4);
        int metaRootStart = UnsafeUtil.getInt(serBytes, offset + 12);
        int length = UnsafeUtil.getInt(serBytes, offset + 20);
        int metaRootSize = metaRootStart < 0 ? 0 : UnsafeUtil.getInt(serBytes, offset + 16) - metaRootStart;
        DataInput input = new DataInputStream(new ByteArrayInputStream(serBytes, fieldNamesStart, length));

        try {
            //FieldNames
            RowFieldNamesDictionary fieldNamesDictionary = RowFieldNamesDictionary.deserialize(input);
            //Schema
            ObjectRowSchemaNode root = (ObjectRowSchemaNode) AbstractRowSchemaNode.deserialize(input, null);
            //            transformer.transform(root);
            String res = rowMetaData.printRootSchema(root, fieldNamesDictionary);
            schemaTransformer.transform(root);
//            System.out.println(timesCalled + " Schema at GlobalAgg: ");
//            System.out.println(
//                    "Thread: " + Thread.currentThread().getName() + " Thread ID: " + Thread.currentThread().getId());

            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serBytes[offset]);
//            ATypeTag typeTag = ATypeTag.OBJECT;
            switch (typeTag) {
                case NULL: {
                    processNull();
                    break;
                }
                case SYSTEM_NULL: {
                    // Ignore and return.
                    break;
                }
                case OBJECT: {
                    // Expected.
                    aggType = ATypeTag.DOUBLE;
                    int nullBitmapSize = 0;
                    int offset1 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, SUM_FIELD_ID,
                            nullBitmapSize, false);
                    sum += ADoubleSerializerDeserializer.getDouble(serBytes, offset1);
                    int offset2 = ARecordSerializerDeserializer.getFieldOffsetById(serBytes, offset, COUNT_FIELD_ID,
                            nullBitmapSize, false);
                    count += AInt64SerializerDeserializer.getLong(serBytes, offset2);
                    break;
                }
                default: {
                    throw new UnsupportedItemTypeException(sourceLoc, "intermediate/global-Avg", serBytes[offset]);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    protected void finishFinalResults(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            ObjectRowSchemaNode root = schemaTransformer.getRoot();
            String res = rowMetaData.printRootSchema(root, rowMetaData.getFieldNamesDictionary());
            if (root == null) {
                throw new HyracksDataException("Cannot compute Schema on empty root.");
            }
            else{
                stringSerde.serialize(new AString(res) , resultStorage.getDataOutput());
            }

//            if (count == 0 || aggType == ATypeTag.NULL) {
//                nullSerde.serialize(ANull.NULL, resultStorage.getDataOutput());
//            }
//            } else {
//                aString.setValue();
//                stringSerde.serialize(aString, resultStorage.getDataOutput());
//            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected boolean skipStep() {
        return false;
    }

    // Function identifier
    private FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.AVG;
    }
}
