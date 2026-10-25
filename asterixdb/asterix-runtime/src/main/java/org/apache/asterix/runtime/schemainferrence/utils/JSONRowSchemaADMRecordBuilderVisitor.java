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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.dataflow.data.nontagged.printers.adm.AObjectPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.JsonSchemaUtil;
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

/**
 * Builds the JSON Schema of a row schema tree as an ADM record. This is the only printer of row schemas: the
 * text form ({@link #buildString}) is that record printed by the ADM printer, so both forms always agree and
 * field names are escaped by the printer rather than by hand.
 */
public class JSONRowSchemaADMRecordBuilderVisitor implements IRowSchemaNodeVisitor<Void, RecordBuilder> {
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AString> stringSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
    private final ArrayBackedValueStorage keyStorage = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage valueStorage = new ArrayBackedValueStorage();
    private final List<String> fieldNames;

    public JSONRowSchemaADMRecordBuilderVisitor(RowFieldNamesDictionary dictionary) throws HyracksDataException {
        fieldNames = new ArrayList<>();
        AStringSerializerDeserializer stringSerDer =
                new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
        ByteArrayAccessibleInputStream in = new ByteArrayAccessibleInputStream(new byte[0], 0, 0);
        ByteArrayAccessibleDataInputStream dataIn = new ByteArrayAccessibleDataInputStream(in);
        for (IValueReference serFieldName : dictionary.getFieldNames()) {
            in.setContent(serFieldName.getByteArray(), 0, serFieldName.getLength());
            fieldNames.add(stringSerDer.deserialize(dataIn).getStringValue());
        }
    }

    public ArrayBackedValueStorage build(ObjectRowSchemaNode root) throws HyracksDataException {
        RecordBuilder schema = new RecordBuilder();
        schema.reset(null);
        addString("$schema", JsonSchemaUtil.SCHEMA_URI, schema);
        writeNode(root, schema);
        ArrayBackedValueStorage out = new ArrayBackedValueStorage();
        schema.write(out.getDataOutput(), true);
        return out;
    }

    public String buildString(ObjectRowSchemaNode root) throws HyracksDataException {
        ArrayBackedValueStorage adm = build(root);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            AObjectPrinterFactory.INSTANCE.createPrinter(null).print(adm.getByteArray(), adm.getStartOffset(),
                    adm.getLength(), ps);
        }
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Override
    public Void visit(ObjectRowSchemaNode objectNode, RecordBuilder schema) throws HyracksDataException {
        List<AbstractRowSchemaNode> children = objectNode.getChildren();
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        List<String> requiredFieldNames = new ArrayList<>();
        RecordBuilder properties = new RecordBuilder();
        properties.reset(null);
        for (int i = 0; i < children.size(); i++) {
            String fieldName = fieldNames.get(fieldNameIndexes.getInt(i));
            AbstractRowSchemaNode child = children.get(i);
            if (!child.isOptional()) {
                requiredFieldNames.add(fieldName);
            }
            addRecord(fieldName, nodeSchema(child), properties);
        }
        addRecord("properties", properties, schema);

        if (!requiredFieldNames.isEmpty()) {
            OrderedListBuilder required = new OrderedListBuilder();
            required.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
            ArrayBackedValueStorage item = new ArrayBackedValueStorage();
            for (String fieldName : requiredFieldNames) {
                item.reset();
                stringSerde.serialize(new AString(fieldName), item.getDataOutput());
                required.addItem(item);
            }
            item.reset();
            required.write(item.getDataOutput(), true);
            addValue("required", item, schema);
        }
        return null;
    }

    @Override
    public Void visit(AbstractRowCollectionSchemaNode collectionNode, RecordBuilder schema)
            throws HyracksDataException {
        addRecord("items", nodeSchema(collectionNode.getItemNode()), schema);
        return null;
    }

    @Override
    public Void visit(UnionRowSchemaNode unionNode, RecordBuilder schema) throws HyracksDataException {
        OrderedListBuilder oneOf = new OrderedListBuilder();
        oneOf.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        ArrayBackedValueStorage item = new ArrayBackedValueStorage();
        for (AbstractRowSchemaNode child : unionNode.getChildren().values()) {
            item.reset();
            nodeSchema(child).write(item.getDataOutput(), true);
            oneOf.addItem(item);
        }
        item.reset();
        oneOf.write(item.getDataOutput(), true);
        addValue("oneOf", item, schema);
        return null;
    }

    @Override
    public Void visit(PrimitiveRowSchemaNode primitiveNode, RecordBuilder schema) {
        return null;
    }

    private RecordBuilder nodeSchema(AbstractRowSchemaNode node) throws HyracksDataException {
        RecordBuilder schema = new RecordBuilder();
        schema.reset(null);
        writeNode(node, schema);
        return schema;
    }

    private void writeNode(AbstractRowSchemaNode node, RecordBuilder schema) throws HyracksDataException {
        ATypeTag typeTag = node.getTypeTag();
        // A union has no single type: its alternatives are listed under oneOf by the union visitor
        if (typeTag != ATypeTag.UNION) {
            addString("type", JsonSchemaUtil.getType(typeTag), schema);
            String format = JsonSchemaUtil.getFormat(typeTag);
            if (format != null) {
                addString("format", format, schema);
            }
            String asterixType = JsonSchemaUtil.getAsterixType(typeTag);
            if (asterixType != null) {
                addString(JsonSchemaUtil.ASTERIX_TYPE_KEYWORD, asterixType, schema);
            }
        }
        node.accept(this, schema);
    }

    private void addString(String key, String value, RecordBuilder schema) throws HyracksDataException {
        valueStorage.reset();
        stringSerde.serialize(new AString(value), valueStorage.getDataOutput());
        addValue(key, valueStorage, schema);
    }

    private void addRecord(String key, RecordBuilder value, RecordBuilder schema) throws HyracksDataException {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        value.write(storage.getDataOutput(), true);
        addValue(key, storage, schema);
    }

    private void addValue(String key, IValueReference value, RecordBuilder schema) throws HyracksDataException {
        keyStorage.reset();
        stringSerde.serialize(new AString(key), keyStorage.getDataOutput());
        schema.addField(keyStorage, value);
    }
}
