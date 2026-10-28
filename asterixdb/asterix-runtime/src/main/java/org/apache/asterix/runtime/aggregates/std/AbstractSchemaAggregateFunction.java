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
import java.io.IOException;

import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.schemainference.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainference.ObjectRowSchemaNode;
import org.apache.asterix.runtime.schemainference.RowMetadata;
import org.apache.asterix.runtime.schemainference.RowSchemaTransformer;
import org.apache.asterix.runtime.schemainference.RowTransformer;
import org.apache.asterix.runtime.schemainference.lazy.metadata.RowFieldNamesDictionary;
import org.apache.asterix.runtime.schemainference.utils.JSONRowSchemaADMRecordBuilderVisitor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Infers a schema from the records its argument evaluates to. Local steps build a schema tree from records and
 * emit it serialized (see {@link RowMetadata#serializeRowMetadata()}); global steps merge those trees and emit the
 * final JSON Schema record. The strict variant yields null as soon as it meets a null or missing value; that null
 * travels from the local step to the global one in place of a partial schema.
 */
public abstract class AbstractSchemaAggregateFunction extends AbstractAggregateFunction {
    // Offset of the field-names pointer in the serialized RowMetadata header
    private static final int FIELD_NAMES_POINTER = 0;

    private final IScalarEvaluator eval;
    private final IPointable inputVal = new VoidPointable();
    // Null when the argument's static type is not a record, so there is no declared type to seed from
    private final ARecordType declaredType;
    private final RecordLazyVisitablePointable recordVal;
    private final boolean strict;

    private RowMetadata rowMetaData;
    private RowTransformer transformer;
    private RowSchemaTransformer schemaTransformer;
    private boolean sawUnknown;

    /**
     * @param strict whether a null or missing input makes the result null, as the strict_ aggregates do, rather
     *               than being skipped
     */
    public AbstractSchemaAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, IAType argType, boolean strict) throws HyracksDataException {
        super(sourceLoc);
        this.strict = strict;
        eval = args[0].createScalarEvaluator(context);
        declaredType = getDeclaredRecordType(argType);
        recordVal = declaredType != null ? new TypedRecordLazyVisitablePointable(declaredType)
                : new RecordLazyVisitablePointable(true);
    }

    @Override
    public void init() throws HyracksDataException {
        // The evaluator is reused for every group, so each group must start from an empty tree
        rowMetaData = new RowMetadata();
        transformer = new RowTransformer(rowMetaData, rowMetaData.getRoot());
        schemaTransformer = new RowSchemaTransformer(rowMetaData, rowMetaData.getRoot());
        sawUnknown = false;
        if (declaredType != null) {
            // Declared fields are what makes a field required, so every variant seeds them, split or not
            transformer.transform(declaredType, false);
        }
    }

    @Override
    public abstract void step(IFrameTupleReference tuple) throws HyracksDataException;

    @Override
    public abstract void finish(IPointable result) throws HyracksDataException;

    @Override
    public abstract void finishPartial(IPointable result) throws HyracksDataException;

    protected void processDataValues(IFrameTupleReference tuple) throws HyracksDataException {
        if (sawUnknown) {
            return;
        }
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int start = inputVal.getStartOffset();
        if (strict && (data[start] == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                || data[start] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG)) {
            sawUnknown = true;
            return;
        }
        // Null, missing and non-object values carry no fields to infer from
        if (data[start] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
            return;
        }
        recordVal.set(data, start, inputVal.getLength());
        transformer.transform(recordVal);
    }

    protected void processPartialResults(IFrameTupleReference tuple) throws HyracksDataException {
        if (sawUnknown) {
            return;
        }
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int start = inputVal.getStartOffset();
        // A partial schema is far longer than one byte, so a lone null tag can only be a strict step's null
        if (inputVal.getLength() == 1 && data[start] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
            sawUnknown = true;
            return;
        }
        int fieldNamesStart = start + IntegerPointable.getInteger(data, start + FIELD_NAMES_POINTER);
        int length = inputVal.getLength() - (fieldNamesStart - start);
        DataInput input = new DataInputStream(new ByteArrayInputStream(data, fieldNamesStart, length));
        try {
            RowFieldNamesDictionary fieldNamesDictionary = RowFieldNamesDictionary.deserialize(input);
            schemaTransformer.setToMergeFieldNamesDictionary(fieldNamesDictionary);
            ObjectRowSchemaNode root = (ObjectRowSchemaNode) AbstractRowSchemaNode.deserialize(input);
            schemaTransformer.transform(root);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void finishPartialResults(IPointable result) throws HyracksDataException {
        if (sawUnknown) {
            PointableHelper.setNull(result);
            return;
        }
        result.set(rowMetaData.serializeRowMetadata());
    }

    protected void finishFinalResults(IPointable result) throws HyracksDataException {
        if (sawUnknown) {
            PointableHelper.setNull(result);
            return;
        }
        JSONRowSchemaADMRecordBuilderVisitor schemaADMBuilder =
                new JSONRowSchemaADMRecordBuilderVisitor(rowMetaData.getFieldNamesDictionary());
        result.set(schemaADMBuilder.build(rowMetaData.getRoot()));
    }

    private static ARecordType getDeclaredRecordType(IAType argType) {
        IAType type = argType;
        if (type != null && type.getTypeTag() == ATypeTag.UNION) {
            type = ((AUnionType) type).getActualType();
        }
        return type != null && type.getTypeTag() == ATypeTag.OBJECT ? (ARecordType) type : null;
    }
}
