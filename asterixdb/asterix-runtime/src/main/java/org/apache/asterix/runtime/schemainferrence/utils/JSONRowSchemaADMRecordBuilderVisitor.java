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
package org.apache.asterix.runtime.schemainferrence.utils;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.schemainferrence.AbstractRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.IRowSchemaNodeVisitor;
import org.apache.asterix.runtime.schemainferrence.ObjectRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.UnionRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.collection.AbstractRowCollectionSchemaNode;
import org.apache.asterix.runtime.schemainferrence.lazy.metadata.RowFieldNamesDictionary;
import org.apache.asterix.runtime.schemainferrence.primitive.PrimitiveRowSchemaNode;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleDataInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

import it.unimi.dsi.fastutil.ints.IntList;

/*
Schema string builder for logging purpose
Implementation taken from column schema inference.
 */
public class JSONRowSchemaADMRecordBuilderVisitor
        implements IRowSchemaNodeVisitor<ArrayBackedValueStorage, RecordBuilder> {
    public static String RECORD_SCHEMA = "record";
    public static String META_RECORD_SCHEMA = "meta-record";
    private final StringBuilder builder;

    ArrayBackedValueStorage k;
    ArrayBackedValueStorage v;

    private final DataOutput out;
    private final List<String> fieldNames;

    private int level;
    private int indent;

    private ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

    public JSONRowSchemaADMRecordBuilderVisitor(RowFieldNamesDictionary dictionary) throws HyracksDataException {

        k = new ArrayBackedValueStorage();
        v = new ArrayBackedValueStorage();
        out = new ArrayBackedValueStorage().getDataOutput();

        builder = new StringBuilder();
        this.fieldNames = new ArrayList<>();
        AStringSerializerDeserializer stringSerDer =
                new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
        List<IValueReference> extractedFieldNames = dictionary.getFieldNames();

        //Deserialize field names
        ByteArrayAccessibleInputStream in = new ByteArrayAccessibleInputStream(new byte[0], 0, 0);
        ByteArrayAccessibleDataInputStream dataIn = new ByteArrayAccessibleDataInputStream(in);
        for (IValueReference serFieldName : extractedFieldNames) {
            in.setContent(serFieldName.getByteArray(), 0, serFieldName.getLength());
            AString fieldName = stringSerDer.deserialize(dataIn);
            this.fieldNames.add(fieldName.getStringValue());
        }
        level = 0;
        indent = 0;
    }

    public void addKeyValue(String key, String value, RecordBuilder rb) throws HyracksDataException {

        stringSerde.serialize(new AString(key), k.getDataOutput());
        stringSerde.serialize(new AString(value), v.getDataOutput());
        rb.addField(k, v);
        k.reset();
        v.reset();
    }

    public void addKeyValue(String key, ArrayBackedValueStorage value, RecordBuilder rb) throws HyracksDataException {

        stringSerde.serialize(new AString(key), k.getDataOutput());
        rb.addField(k, value);
        k.reset();
        value.reset();
    }

    public ArrayBackedValueStorage build(ObjectRowSchemaNode root) throws HyracksDataException {

        ArrayBackedValueStorage out = new ArrayBackedValueStorage();

        RecordBuilder recordBuilder = new RecordBuilder();
        recordBuilder.reset(null);
        addKeyValue("$schema", "https://json-schema.org/draft/2020-12/schema", recordBuilder);
        addKeyValue("$id", "https://json-schema.org/draft/2020-12/schema", recordBuilder);
        addKeyValue("type", "object", recordBuilder);
        visit(root, recordBuilder);
        recordBuilder.write(out.getDataOutput(), true);
        return out;
    }

    @Override
    public ArrayBackedValueStorage visit(ObjectRowSchemaNode objectNode, RecordBuilder parentRB)
            throws HyracksDataException {
        List<String> requiredFieldNames = new ArrayList<>();
        List<AbstractRowSchemaNode> children = objectNode.getChildren();
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        level++;
        indent++;
        //        recordBuilder.reset(null);
        // Prop open
        RecordBuilder recordBuilder = new RecordBuilder();
        recordBuilder.reset(null);
        ArrayBackedValueStorage curObjectOut = new ArrayBackedValueStorage();
        //        recordBuilder.addField( UTF8StringPointable.generateUTF8Pointable("properties"), UTF8StringPointable.generateUTF8Pointable("object"));
        RecordBuilder childRecordBuilder = new RecordBuilder();

        for (int i = 0; i < children.size(); i++) {
            ArrayBackedValueStorage childOut = new ArrayBackedValueStorage();
            childRecordBuilder.reset(null);
            int index = fieldNameIndexes.getInt(i);
            String fieldName = fieldNames.get(index);
            AbstractRowSchemaNode child = children.get(i);
            if (!child.isOptional()) {
                requiredFieldNames.add(fieldName);
            }
            append(fieldName, index, child, childRecordBuilder);

            child.accept(this, childRecordBuilder);

            childRecordBuilder.write(childOut.getDataOutput(), true);
            addKeyValue(fieldName, childOut, recordBuilder);
        }

        OrderedListBuilder requiredListBuilder = new OrderedListBuilder();
        requiredListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        // Prop close
        recordBuilder.write(curObjectOut.getDataOutput(), true);
        addKeyValue("properties", curObjectOut, parentRB);
        ArrayBackedValueStorage requiredOut = new ArrayBackedValueStorage();

        if (!requiredFieldNames.isEmpty()) {
            for (int i = 0; i < requiredFieldNames.size(); i++) {
                requiredOut.reset();
                stringSerde.serialize(new AString(requiredFieldNames.get(i)), requiredOut.getDataOutput());
                requiredListBuilder.addItem(requiredOut);
            }
            requiredOut.reset();
            requiredListBuilder.write(requiredOut.getDataOutput(), true);
            addKeyValue("required", requiredOut, parentRB);
        }

        //        recordBuilder.write(out,true);

        level--;
        indent--;

        return null;
    }

    @Override
    public ArrayBackedValueStorage visit(AbstractRowCollectionSchemaNode collectionNode, RecordBuilder parentRB)
            throws HyracksDataException {
        level++;
        indent++;
        AbstractRowSchemaNode itemNode = collectionNode.getItemNode();
        RecordBuilder oneOfRecordBuilder = new RecordBuilder();
        OrderedListBuilder oneOfListBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage childOut = new ArrayBackedValueStorage();

        RecordBuilder childRecordBuilder = new RecordBuilder();
        childRecordBuilder.reset(null);
        if (itemNode.getTypeTag() != ATypeTag.UNION) {
            addKeyValue("type", writeSchemaType(getNormalizedTypeTag(itemNode.getTypeTag())), childRecordBuilder);
        }

        itemNode.accept(this, childRecordBuilder);
        childRecordBuilder.write(childOut.getDataOutput(), true);

        ArrayBackedValueStorage testOut = new ArrayBackedValueStorage();
        RecordBuilder testRecordBuilder = new RecordBuilder();
        testRecordBuilder.reset(null);
        addKeyValue("innerObj", "innerVal", testRecordBuilder);
        testOut.reset();
        testRecordBuilder.write(testOut.getDataOutput(), true);
        OrderedListBuilder testListBuilder = new OrderedListBuilder();
        testListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        testListBuilder.addItem(childOut);
        testOut.reset();
        testListBuilder.write(testOut.getDataOutput(), true);
        testRecordBuilder.reset(null);
        addKeyValue("oneOf", testOut, testRecordBuilder);
        testOut.reset();
        testRecordBuilder.write(testOut.getDataOutput(), true);

        oneOfListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        oneOfListBuilder.addItem(childOut);
        //        oneOfListBuilder.addItem(childOut);
        childOut.reset();
        oneOfListBuilder.write(childOut.getDataOutput(), true);
        addKeyValue("oneOf", childOut, oneOfRecordBuilder);

        childOut.reset();
        oneOfRecordBuilder.write(childOut.getDataOutput(), true);
        //

        //        addKeyValue("oneOf", childOut, parentRB);
        addKeyValue("items", testOut, parentRB);

        level--;
        indent--;
        return null;
    }

    @Override
    public ArrayBackedValueStorage visit(UnionRowSchemaNode unionNode, RecordBuilder parentRB)
            throws HyracksDataException {
        indent++;
        ArrayBackedValueStorage out = new ArrayBackedValueStorage();
        OrderedListBuilder oneOfListBuilder = new OrderedListBuilder();
        oneOfListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        List<AbstractRowSchemaNode> unionChildren =
                new ArrayList<AbstractRowSchemaNode>(unionNode.getChildren().values());
        //        for (AbstractRowSchemaNode child : unionNode.getChildren().values()) {
        //        RecordBuilder recordBuilder = new RecordBuilder();
        RecordBuilder childRecordBuilder = new RecordBuilder();
        for (int i = 0; i < unionChildren.size(); i++) {
            childRecordBuilder.reset(null);

            AbstractRowSchemaNode child = unionChildren.get(i);
            //            append(child.getTypeTag().toString(), child);
            if (child.getTypeTag() != ATypeTag.UNION) {
                addKeyValue("type", writeSchemaType(getNormalizedTypeTag(child.getTypeTag())), childRecordBuilder);
            }
            child.accept(this, childRecordBuilder);
            out.reset();
            childRecordBuilder.write(out.getDataOutput(), true);
            oneOfListBuilder.addItem(out);
            out.reset();
        }
        out.reset();
        oneOfListBuilder.write(out.getDataOutput(), true);
        addKeyValue("oneOf", out, parentRB);
        out.reset();
        //        recordBuilder.write(out.getDataOutput(), true);
        //        addKeyValue("unionTest", out, parentRB);

        indent--;
        return null;
    }

    @Override
    public ArrayBackedValueStorage visit(PrimitiveRowSchemaNode primitiveNode, RecordBuilder arg)
            throws HyracksDataException {
        return null;
    }

    private void append(String key, int index, AbstractRowSchemaNode node, RecordBuilder childRecordBuilder)
            throws HyracksDataException {
        if (node.getTypeTag() != ATypeTag.UNION) {
            addKeyValue("type", writeSchemaType(getNormalizedTypeTag(node.getTypeTag())), childRecordBuilder);
        }
    }

    public static ATypeTag getNormalizedTypeTag(ATypeTag typeTag) {
        switch (typeTag) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return ATypeTag.BIGINT;
            case FLOAT:
                return ATypeTag.DOUBLE;
            default:
                return typeTag;
        }
    }

    private String writeSchemaType(ATypeTag normalizedTypeTag) throws HyracksDataException {
        switch (normalizedTypeTag) {
            case OBJECT:
                return "object";
            case MULTISET:
            case ARRAY:
                return "array";
            case NULL:
            case MISSING:
                return "null";
            case BOOLEAN:
                return "boolean";
            case DOUBLE:
            case BIGINT:
                return "number";
            case UUID:
            case STRING:
                return "string";
            case UNION:
                return "union";
            default:
                throw new IllegalStateException("Unsupported type " + normalizedTypeTag);

        }
    }
}
