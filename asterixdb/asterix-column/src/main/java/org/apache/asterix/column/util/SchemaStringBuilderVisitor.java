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
package org.apache.asterix.column.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ISchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleDataInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

import it.unimi.dsi.fastutil.ints.IntList;

public class SchemaStringBuilderVisitor implements ISchemaNodeVisitor<Void, Void> {
    private final StringBuilder builder;
    private List<String> fieldNames;

    private int level;
    private int indent;

    public SchemaStringBuilderVisitor(IFieldNamesDictionary dictionary) throws HyracksDataException {
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

    public void updateFieldNames(IFieldNamesDictionary dictionary) throws HyracksDataException {
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
    }

    public String build(ObjectSchemaNode root) throws HyracksDataException {
        builder.append("{ ");
        builder.append("\"$schema\": \"https://json-schema.org/draft/2020-12/schema\",")
                .append("\"$id\": \"https://json-schema.org/draft/2020-12/schema\",");
        builder.append("\"type\":\"object\", ");
        visit(root, null);
        //        appendPostDecor();
        builder.append(" }\n ");
        return builder.toString();
    }

    @Override
    public Void visit(ObjectSchemaNode objectNode, Void arg) throws HyracksDataException {
        List<AbstractSchemaNode> children = objectNode.getChildren();
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        level++;
        indent++;
        builder.append("\"properties\": {  ");
        for (int i = 0; i < children.size(); i++) {
            int index = fieldNameIndexes.getInt(i);
            String fieldName = index < 0 ? "<empty>" : fieldNames.get(index);
            AbstractSchemaNode child = children.get(i);
            boolean wrapNullable = shouldWrapNullable(child);
            append(fieldName, index, child, wrapNullable);
            if (child.getTypeTag() != ATypeTag.UNION) {
                if (child.isNested()) {
                    builder.append(", ");
                } else {
                    builder.append(" ");
                }
            }
            child.accept(this, null);
            closePropertyValue(child, wrapNullable, i != children.size() - 1);
        }

        builder.append(" } ");

        level--;
        indent--;
        return null;
    }

    @Override
    public Void visit(AbstractCollectionSchemaNode collectionNode, Void arg) throws HyracksDataException {
        level++;
        indent++;
        AbstractSchemaNode itemNode = collectionNode.getItemNode();
        builder.append("\"items\": ");
        builder.append("{  \"oneOf\": [ ");
        if (itemNode.getTypeTag() != ATypeTag.UNION) {
            builder.append("{  \"type\":");
            writeSchemaType(getNormalizedTypeTag(itemNode.getTypeTag()));

            if (itemNode.isNested()) {
                builder.append(", ");
            } else {
                builder.append(" } ");
            }
        }

        itemNode.accept(this, null);
        if (itemNode.getTypeTag() != ATypeTag.UNION && itemNode.isNested()) {
            builder.append(" } ");
        }
        if (shouldWrapNullable(itemNode) || (itemNode.getTypeTag() == ATypeTag.UNION && itemNode.isNullable())) {
            // union visitor already appends null when nullable; only add for non-union items
            if (itemNode.getTypeTag() != ATypeTag.UNION) {
                builder.append(", { \"type\": \"null\" } ");
            }
        }
        builder.append("] ").append(" } ");
        level--;
        indent--;
        return null;
    }

    @Override
    public Void visit(UnionSchemaNode unionNode, Void arg) throws HyracksDataException {
        indent++;
        builder.append("{ \"oneOf\": [ ");
        List<AbstractSchemaNode> unionChildren = new ArrayList<AbstractSchemaNode>(unionNode.getChildren().values());
        for (int i = 0; i < unionChildren.size(); i++) {
            AbstractSchemaNode child = unionChildren.get(i);
            if (child.getTypeTag() != ATypeTag.UNION) {
                builder.append("{  \"type\":");
                writeSchemaType(getNormalizedTypeTag(child.getTypeTag()));
                if (child.isNested()) {
                    builder.append(", ");
                }
            }
            child.accept(this, null);
            if (i != unionChildren.size() - 1) {
                builder.append(" }, ");
            } else {
                builder.append(" } ");
            }
        }
        if (unionNode.isNullable()) {
            builder.append(", { \"type\": \"null\" } ");
        }
        builder.append("]  }");
        indent--;
        return null;
    }

    @Override
    public Void visit(PrimitiveSchemaNode primitiveNode, Void arg) throws HyracksDataException {
        return null;
    }

    private void append(String key, int index, AbstractSchemaNode node, boolean wrapNullable)
            throws HyracksDataException {
        builder.append("\"").append(key).append("\"").append(": ");
        if (node.getTypeTag() != ATypeTag.UNION) {
            if (wrapNullable) {
                builder.append("{ \"oneOf\": [ ");
            }
            builder.append("{  \"type\":");
            writeSchemaType(getNormalizedTypeTag(node.getTypeTag()));
        }
    }

    private void closePropertyValue(AbstractSchemaNode child, boolean wrapNullable, boolean moreSiblings) {
        if (child.getTypeTag() == ATypeTag.UNION) {
            if (moreSiblings) {
                builder.append(", ");
            } else {
                builder.append(" ");
            }
            return;
        }
        // Close the typed schema object opened in append()
        builder.append(" } ");
        if (wrapNullable) {
            builder.append(", { \"type\": \"null\" } ] } ");
        }
        if (moreSiblings) {
            builder.append(", ");
        } else {
            builder.append(" ");
        }
    }

    private static boolean shouldWrapNullable(AbstractSchemaNode node) {
        return node.isNullable() && node.getTypeTag() != ATypeTag.UNION && node.getTypeTag() != ATypeTag.NULL
                && node.getTypeTag() != ATypeTag.MISSING;
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

    private void writeSchemaType(ATypeTag normalizedTypeTag) throws HyracksDataException {
        switch (normalizedTypeTag) {
            case OBJECT:
                builder.append("\"object\"");
                return;
            case MULTISET:
            case ARRAY:
                builder.append("\"array\"");
                return;
            case NULL:
            case MISSING:
                builder.append("\"null\"");
                return;
            case BOOLEAN:
                builder.append("\"boolean\"");
                return;
            case DOUBLE:
            case BIGINT:
                builder.append("\"number\"");
                return;
            case UUID:
            case STRING:
                builder.append("\"string\"");
                return;
            case UNION:
                builder.append("\"union\"");
                return;
            default:
                throw new IllegalStateException("Unsupported type " + normalizedTypeTag);

        }

    }
}
