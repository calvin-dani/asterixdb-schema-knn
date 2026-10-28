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
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.JsonSchemaUtil;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleDataInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Prints a column schema tree as JSON Schema text, in the same form as the row schema printer. The schema is built
 * as a JSON tree and serialized, so field names are escaped by the serializer rather than by hand.
 */
public class JsonSchemaBuilderVisitor implements ISchemaNodeVisitor<Void, ObjectNode> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String EMPTY_FIELD_NAME = "<empty>";
    private final List<String> fieldNames;
    // The declared type of the node being written, or null where the dataset type declares nothing
    private IAType declaredType;

    public JsonSchemaBuilderVisitor(IFieldNamesDictionary dictionary) throws HyracksDataException {
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

    /**
     * @param root     the root of the column schema tree
     * @param itemType the dataset's declared item type; its non-optional fields are reported as required
     * @return the schema as JSON text
     */
    public String build(ObjectSchemaNode root, ARecordType itemType) throws HyracksDataException {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("$schema", JsonSchemaUtil.SCHEMA_URI);
        writeNode(root, schema, itemType);
        try {
            return MAPPER.writeValueAsString(schema);
        } catch (JsonProcessingException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Void visit(ObjectSchemaNode objectNode, ObjectNode schema) throws HyracksDataException {
        List<AbstractSchemaNode> children = objectNode.getChildren();
        IntList fieldNameIndexes = objectNode.getChildrenFieldNameIndexes();
        ARecordType recordType = declaredType instanceof ARecordType ? (ARecordType) declaredType : null;
        ObjectNode properties = schema.putObject("properties");
        List<String> requiredFieldNames = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            int index = fieldNameIndexes.getInt(i);
            // A negative index is the placeholder child of an object that has no fields yet
            String fieldName = index < 0 ? EMPTY_FIELD_NAME : fieldNames.get(index);
            IAType fieldType = recordType != null && index >= 0 ? recordType.getFieldType(fieldName) : null;
            if (fieldType != null && !NonTaggedFormatUtil.isOptional(fieldType)) {
                requiredFieldNames.add(fieldName);
            }
            writeNode(children.get(i), properties.putObject(fieldName), fieldType);
        }
        if (!requiredFieldNames.isEmpty()) {
            ArrayNode required = schema.putArray("required");
            requiredFieldNames.forEach(required::add);
        }
        return null;
    }

    @Override
    public Void visit(AbstractCollectionSchemaNode collectionNode, ObjectNode schema) throws HyracksDataException {
        IAType itemType = declaredType instanceof AbstractCollectionType
                ? ((AbstractCollectionType) declaredType).getItemType() : null;
        writeNode(collectionNode.getItemNode(), schema.putObject("items"), itemType);
        return null;
    }

    @Override
    public Void visit(UnionSchemaNode unionNode, ObjectNode schema) throws HyracksDataException {
        IAType unionDeclaredType = declaredType;
        ArrayNode oneOf = schema.putArray("oneOf");
        for (AbstractSchemaNode child : unionNode.getChildren().values()) {
            // Only the alternative of the declared kind follows the declared type; the others were never declared
            boolean declared = unionDeclaredType != null && unionDeclaredType.getTypeTag() == child.getTypeTag();
            writeNode(child, oneOf.addObject(), declared ? unionDeclaredType : null);
        }
        return null;
    }

    @Override
    public Void visit(PrimitiveSchemaNode primitiveNode, ObjectNode schema) {
        return null;
    }

    private void writeNode(AbstractSchemaNode node, ObjectNode schema, IAType nodeDeclaredType)
            throws HyracksDataException {
        ATypeTag typeTag = node.getTypeTag();
        // A union has no single type: its alternatives are listed under oneOf by the union visitor
        if (typeTag != ATypeTag.UNION) {
            schema.put("type", JsonSchemaUtil.getType(typeTag));
            String format = JsonSchemaUtil.getFormat(typeTag);
            if (format != null) {
                schema.put("format", format);
            }
            String asterixType = JsonSchemaUtil.getAsterixType(typeTag);
            if (asterixType != null) {
                schema.put(JsonSchemaUtil.ASTERIX_TYPE_KEYWORD, asterixType);
            }
        }
        IAType parentDeclaredType = declaredType;
        declaredType = nodeDeclaredType != null && nodeDeclaredType.getTypeTag() == ATypeTag.UNION
                ? ((AUnionType) nodeDeclaredType).getActualType() : nodeDeclaredType;
        node.accept(this, schema);
        declaredType = parentDeclaredType;
    }
}
