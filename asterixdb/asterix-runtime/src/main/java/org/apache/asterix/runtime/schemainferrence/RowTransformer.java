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
package org.apache.asterix.runtime.schemainferrence;

import java.io.IOException;

import org.apache.asterix.om.lazy.AbstractLazyVisitablePointable;
import org.apache.asterix.om.lazy.AbstractListLazyVisitablePointable;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.lazy.ILazyVisitablePointableVisitor;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.asterix.runtime.schemainferrence.collection.AbstractRowCollectionSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class RowTransformer implements ILazyVisitablePointableVisitor<AbstractRowSchemaNode, AbstractRowSchemaNode>,
        IATypeVisitor<AbstractRowSchemaNode, AbstractRowSchemaNode> {

    private final RowMetadata rowMetadata;
    private final VoidPointable nonTaggedValue;
    private final ObjectRowSchemaNode root;
    private AbstractRowSchemaNestedNode currentParent;
    private int primaryKeysLength;

    public ObjectRowSchemaNode getRoot() {
        return root;
    }

    public RowTransformer(RowMetadata rowMetadata, ObjectRowSchemaNode root) {
        this.rowMetadata = rowMetadata;
        this.root = root;
        nonTaggedValue = new VoidPointable();
    }

    /**
     *
     * @param pointable record pointable
     * @return the estimated size (possibly overestimated) of the primary key(s) columns
     */
    public int transform(RecordLazyVisitablePointable pointable) throws HyracksDataException {
        primaryKeysLength = 0;
        pointable.accept(this, root);
        return primaryKeysLength;
    }

    public int transform(ARecordType recType) throws HyracksDataException {
        primaryKeysLength = 0;
        recType.accept(this, root);
        return primaryKeysLength;
    }

    public AbstractRowSchemaNode visit(ARecordType recType, AbstractRowSchemaNode arg) {
        try {
            rowMetadata.enterNode(currentParent, arg);

            AbstractRowSchemaNestedNode previousParent = currentParent;

            ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) arg;
            currentParent = objectNode;

            for (int i = 0; i < recType.getFieldNames().length; i++) {
                String fieldName = recType.getFieldNames()[i];

                IAType fieldType = recType.getFieldType(fieldName);
                ATypeTag childTypeTag = fieldType.getTypeTag();

                if (childTypeTag != ATypeTag.MISSING) {
                    ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                    try {
                        storage.getDataOutput().writeByte(fieldName.length());
                        storage.getDataOutput().writeBytes(fieldName);
                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                    // Only write actual field values (including NULL) but ignore MISSING fields
                    AbstractRowSchemaNode childNode = objectNode.getOrCreateChild(storage, childTypeTag, rowMetadata);
                    if (fieldType.getTypeTag() == ATypeTag.OBJECT) {
                        acceptActualNode((ARecordType) fieldType, childNode);
                    } else if (fieldType.getTypeTag() == ATypeTag.MULTISET) {
                        acceptActualNode((AUnorderedListType) fieldType, childNode);
                    } else if (fieldType.getTypeTag() == ATypeTag.ARRAY) {
                        acceptActualNode((AOrderedListType) fieldType, childNode);
                    } else {
                        acceptActualNode(fieldType, childNode);
                    }
                }
            }
            rowMetadata.exitNode(arg);
            currentParent = previousParent;
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(AbstractCollectionType collectionType, AbstractRowSchemaNode arg) {
        try {
            rowMetadata.enterNode(currentParent, arg);
            AbstractRowSchemaNestedNode previousParent = currentParent;

            AbstractRowCollectionSchemaNode collectionNode = (AbstractRowCollectionSchemaNode) arg;

            currentParent = collectionNode;
            ATypeTag childTypeTag = collectionType.getItemType().getTypeTag();
            AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata);
            acceptActualNode(collectionType.getItemType(), childNode);

            rowMetadata.exitCollectionNode(collectionNode, 1);
            currentParent = previousParent;
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }

        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(AUnionType unionType, AbstractRowSchemaNode arg) {
        return null;
    }

    @Override
    public AbstractRowSchemaNode visitFlat(IAType flatType, AbstractRowSchemaNode arg) {
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(RecordLazyVisitablePointable pointable, AbstractRowSchemaNode arg)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, arg);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) arg;
        currentParent = objectNode;
        for (int i = 0; i < pointable.getNumberOfChildren(); i++) {
            pointable.nextChild();
            IValueReference fieldName = pointable.getFieldName();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            if (childTypeTag != ATypeTag.MISSING) {
                //Only write actual field values (including NULL) but ignore MISSING fields
                AbstractRowSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, rowMetadata);
                acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            }
        }
        //        rowMetadata.printRootSchema(objectNode, rowMetadata.getFieldNamesDictionary());
        rowMetadata.exitNode(arg);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(AbstractListLazyVisitablePointable pointable, AbstractRowSchemaNode arg)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, arg);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        AbstractRowCollectionSchemaNode collectionNode = (AbstractRowCollectionSchemaNode) arg;
        currentParent = collectionNode;

        int numberOfChildren = pointable.getNumberOfChildren();
        for (int i = 0; i < numberOfChildren; i++) {
            pointable.nextChild();
            ATypeTag childTypeTag = pointable.getChildTypeTag();
            AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata);
            acceptActualNode(pointable.getChildVisitablePointable(), childNode);
        }

        rowMetadata.exitCollectionNode(collectionNode, numberOfChildren);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractRowSchemaNode visit(FlatLazyVisitablePointable pointable, AbstractRowSchemaNode arg)
            throws HyracksDataException {
        return null;
    }

    private void acceptActualNode(AbstractLazyVisitablePointable pointable, AbstractRowSchemaNode node)
            throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            rowMetadata.enterNode(currentParent, node);
            AbstractRowSchemaNestedNode previousParent = currentParent;

            UnionRowSchemaNode unionNode = (UnionRowSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = pointable.getTypeTag();
            AbstractRowSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                actualNode = unionNode.getOriginalType();
            } else {
                actualNode = unionNode.getOrCreateChild(pointable.getTypeTag(), rowMetadata);
            }
            pointable.accept(this, actualNode);

            currentParent = previousParent;
            rowMetadata.exitNode(node);
        } else if (pointable.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            rowMetadata.addNestedNull((AbstractRowSchemaNestedNode) node);
        } else {
            pointable.accept(this, node);
        }
    }

    private void acceptActualNode(ARecordType recType, AbstractRowSchemaNode node) throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            rowMetadata.enterNode(currentParent, node);
            AbstractRowSchemaNestedNode previousParent = currentParent;

            UnionRowSchemaNode unionNode = (UnionRowSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = recType.getTypeTag();
            AbstractRowSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                actualNode = unionNode.getOriginalType();
            } else {
                actualNode = unionNode.getOrCreateChild(recType.getTypeTag(), rowMetadata);
            }
            recType.accept(this, actualNode);

            currentParent = previousParent;
            rowMetadata.exitNode(node);
        } else if (recType.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            rowMetadata.addNestedNull((AbstractRowSchemaNestedNode) node);
        } else {
            recType.accept(this, node);
        }
    }

    private void acceptActualNode(AUnorderedListType recType, AbstractRowSchemaNode node) throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            rowMetadata.enterNode(currentParent, node);
            AbstractRowSchemaNestedNode previousParent = currentParent;

            UnionRowSchemaNode unionNode = (UnionRowSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = recType.getTypeTag();
            AbstractRowSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                actualNode = unionNode.getOriginalType();
            } else {
                actualNode = unionNode.getOrCreateChild(recType.getTypeTag(), rowMetadata);
            }
            recType.accept(this, actualNode);

            currentParent = previousParent;
            rowMetadata.exitNode(node);
        } else if (recType.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            rowMetadata.addNestedNull((AbstractRowSchemaNestedNode) node);
        } else {
            recType.accept(this, node);
        }
    }

    private void acceptActualNode(AOrderedListType recType, AbstractRowSchemaNode node) throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            rowMetadata.enterNode(currentParent, node);
            AbstractRowSchemaNestedNode previousParent = currentParent;

            UnionRowSchemaNode unionNode = (UnionRowSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = recType.getTypeTag();
            AbstractRowSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                actualNode = unionNode.getOriginalType();
            } else {
                actualNode = unionNode.getOrCreateChild(recType.getTypeTag(), rowMetadata);
            }
            recType.accept(this, actualNode);

            currentParent = previousParent;
            rowMetadata.exitNode(node);
        } else if (recType.getTypeTag() == ATypeTag.NULL && node.isNested()) {
            rowMetadata.addNestedNull((AbstractRowSchemaNestedNode) node);
        } else {
            recType.accept(this, node);
        }
    }

    public void acceptActualNode(IAType flatType, AbstractRowSchemaNode arg) {
    }

}
