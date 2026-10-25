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

import org.apache.asterix.om.lazy.AbstractLazyVisitablePointable;
import org.apache.asterix.om.lazy.AbstractListLazyVisitablePointable;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.lazy.ILazyVisitablePointableVisitor;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.asterix.runtime.schemainferrence.collection.AbstractRowCollectionSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class RowTransformer implements ILazyVisitablePointableVisitor<AbstractRowSchemaNode, AbstractRowSchemaNode>,
        IATypeVisitor<AbstractRowSchemaNode, AbstractRowSchemaNode> {

    private final RowMetadata rowMetadata;
    private final ObjectRowSchemaNode root;
    private AbstractRowSchemaNestedNode currentParent;
    private boolean optional;

    public ObjectRowSchemaNode getRoot() {
        return root;
    }

    public RowTransformer(RowMetadata rowMetadata, ObjectRowSchemaNode root) {
        this.rowMetadata = rowMetadata;
        this.root = root;
        optional = true;
    }

    /**
     * Seeds the tree from a declared type.
     *
     * @param optional whether the declared fields are optional; false makes every non-optional declared field
     *                 required in the inferred schema
     */
    public void transform(ARecordType recType, boolean optional) throws HyracksDataException {
        this.optional = optional;
        recType.accept(this, root);
        this.optional = true;
    }

    public void transform(RecordLazyVisitablePointable pointable) throws HyracksDataException {
        pointable.accept(this, root);
    }

    public AbstractRowSchemaNode visit(ARecordType recType, AbstractRowSchemaNode arg) {
        try {
            rowMetadata.enterNode(currentParent, arg);
            AbstractRowSchemaNestedNode previousParent = currentParent;
            ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) arg;
            currentParent = objectNode;

            String[] fieldNames = recType.getFieldNames();
            IAType[] fieldTypes = recType.getFieldTypes();
            for (int i = 0; i < fieldNames.length; i++) {
                IAType fieldType = fieldTypes[i];
                boolean declaredOptional = false;
                if (fieldType.getTypeTag() == ATypeTag.UNION) {
                    fieldType = ((AUnionType) fieldType).getActualType();
                    declaredOptional = true;
                }
                if (isDeclarable(fieldType)) {
                    AbstractRowSchemaNode childNode = objectNode.getOrCreateChild(fieldNames[i], fieldType.getTypeTag(),
                            rowMetadata, optional || declaredOptional);
                    acceptDeclaredType(fieldType, childNode);
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

            IAType itemType = collectionType.getItemType();
            if (itemType.getTypeTag() == ATypeTag.UNION) {
                itemType = ((AUnionType) itemType).getActualType();
            }
            if (isDeclarable(itemType)) {
                AbstractRowSchemaNode childNode = collectionNode.getOrCreateItem(itemType.getTypeTag(), rowMetadata);
                acceptDeclaredType(itemType, childNode);
            }

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
                AbstractRowSchemaNode childNode =
                        objectNode.getOrCreateChild(fieldName, childTypeTag, rowMetadata, true);
                acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            }
        }
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
                actualNode = unionNode.getOrCreateChild(pointable.getTypeTag(), rowMetadata, false);
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

    private void acceptDeclaredType(IAType type, AbstractRowSchemaNode node) throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            rowMetadata.enterNode(currentParent, node);
            AbstractRowSchemaNestedNode previousParent = currentParent;
            UnionRowSchemaNode unionNode = (UnionRowSchemaNode) node;
            currentParent = unionNode;
            visitDeclaredType(type, unionNode.getOrCreateChild(type.getTypeTag(), rowMetadata, optional));
            currentParent = previousParent;
            rowMetadata.exitNode(node);
        } else {
            visitDeclaredType(type, node);
        }
    }

    private void visitDeclaredType(IAType type, AbstractRowSchemaNode node) {
        switch (type.getTypeTag()) {
            case OBJECT:
            case ARRAY:
            case MULTISET:
                type.accept(this, node);
                break;
            default:
                // A primitive node is complete once created
                break;
        }
    }

    /**
     * A declared type contributes a node only if it names a concrete type: MISSING is never stored, and ANY
     * (e.g. a computed field of a query result) carries nothing to declare, so its values are inferred from data.
     */
    private static boolean isDeclarable(IAType type) {
        ATypeTag tag = type.getTypeTag();
        return tag != ATypeTag.MISSING && tag != ATypeTag.ANY;
    }

}
