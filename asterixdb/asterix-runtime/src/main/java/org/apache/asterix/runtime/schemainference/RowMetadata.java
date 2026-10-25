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
package org.apache.asterix.runtime.schemainference;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.schemainference.collection.AbstractRowCollectionSchemaNode;
import org.apache.asterix.runtime.schemainference.collection.ArrayRowSchemaNode;
import org.apache.asterix.runtime.schemainference.collection.MultisetRowSchemaNode;
import org.apache.asterix.runtime.schemainference.lazy.metadata.RowFieldNamesDictionary;
import org.apache.asterix.runtime.schemainference.primitive.PrimitiveRowSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

/**
 * A row schema tree and its field-name dictionary. The serialized form ({@link #serializeRowMetadata()}) is what a
 * local schema aggregate passes to the global one: a field-names offset, the dictionary, then the root node.
 */
public final class RowMetadata {

    public final RowFieldNamesDictionary fieldNamesDictionary;
    private final ObjectRowSchemaNode root;

    private final ArrayBackedValueStorage serializedMetadata;
    private boolean changed;
    private int level;

    public RowMetadata() throws HyracksDataException {
        level = -1;
        fieldNamesDictionary = new RowFieldNamesDictionary();
        root = new ObjectRowSchemaNode(false);
        serializedMetadata = new ArrayBackedValueStorage();
        changed = true;
        serializeRowMetadata();
    }

    public RowFieldNamesDictionary getFieldNamesDictionary() {
        return fieldNamesDictionary;
    }

    public ObjectRowSchemaNode getRoot() {
        return root;
    }

    public IValueReference serializeRowMetadata() throws HyracksDataException {
        if (changed) {
            try {
                serializeChanges();
                changed = false;
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
        return serializedMetadata;
    }

    private void serializeChanges() throws IOException {
        serializedMetadata.reset();
        DataOutput output = serializedMetadata.getDataOutput();
        int fieldNamesOffsetPointer = reserveInt(output);
        setOffset(fieldNamesOffsetPointer);
        fieldNamesDictionary.serialize(output);
        root.serialize(output);
    }

    private int reserveInt(DataOutput output) throws IOException {
        int offset = serializedMetadata.getLength();
        output.writeInt(-1);
        return offset;
    }

    private void setOffset(int pointer) {
        int offset = serializedMetadata.getLength();
        IntegerPointable.setInteger(serializedMetadata.getByteArray(), pointer, offset);
    }

    public int getLevel() {
        return level;
    }

    public AbstractRowSchemaNode getOrCreateChild(AbstractRowSchemaNode child, ATypeTag childTypeTag, boolean optional)
            throws HyracksDataException {
        AbstractRowSchemaNode currentChild = child;
        ATypeTag normalizedTypeTag = getNormalizedTypeTag(childTypeTag);
        if (currentChild == null || normalizedTypeTag != ATypeTag.MISSING && normalizedTypeTag != ATypeTag.NULL
                && currentChild.getTypeTag() != ATypeTag.UNION && currentChild.getTypeTag() != normalizedTypeTag) {
            currentChild = createChild(child, normalizedTypeTag, optional);
            changed = true;
        }
        return currentChild;
    }

    public void enterNode(AbstractRowSchemaNode parent, AbstractRowSchemaNode node) throws HyracksDataException {
        if (node.isObjectOrCollection()) {
            level++;
        }
    }

    public void exitNode(AbstractRowSchemaNode node) {
        // A union is a logical node and does not add a nesting level
        if (node.isNested() && node.isObjectOrCollection()) {
            level--;
        }
        node.incrementCounter();
    }

    public void exitCollectionNode(AbstractRowCollectionSchemaNode collectionNode, int numberOfItems) {
        level--;
        collectionNode.incrementCounter();
    }

    private AbstractRowSchemaNode createChild(AbstractRowSchemaNode child, ATypeTag normalizedTypeTag, boolean optional)
            throws HyracksDataException {
        AbstractRowSchemaNode createdChild;
        if (child != null) {
            if (child.getTypeTag() == ATypeTag.NULL) {
                // A field seen only as null so far takes the first concrete type it is seen with
                createdChild = createChild(normalizedTypeTag, optional);
            } else {
                createdChild = new UnionRowSchemaNode(child, createChild(normalizedTypeTag, optional));
            }
        } else {
            createdChild = createChild(normalizedTypeTag, optional);
        }
        return createdChild;
    }

    private AbstractRowSchemaNode createChild(ATypeTag normalizedTypeTag, boolean optional) {
        switch (normalizedTypeTag) {
            case OBJECT:
                return new ObjectRowSchemaNode(optional);
            case ARRAY:
                return new ArrayRowSchemaNode(optional);
            case MULTISET:
                return new MultisetRowSchemaNode(optional);
            case NULL:
            case MISSING:
            case BOOLEAN:
            case DOUBLE:
            case BIGINT:
            case STRING:
            case UUID:
                return new PrimitiveRowSchemaNode(normalizedTypeTag, false, optional);
            default:
                throw new IllegalStateException("Unsupported type " + normalizedTypeTag);
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

    public void addNestedNull(AbstractRowSchemaNestedNode node) {
        node.incrementCounter();
    }
}
