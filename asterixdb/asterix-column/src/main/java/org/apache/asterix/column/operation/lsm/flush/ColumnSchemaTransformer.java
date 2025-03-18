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

package org.apache.asterix.column.operation.lsm.flush;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.IObjectSchemaNodeVisitor;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.metadata.schema.UnionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.ArraySchemaNode;
import org.apache.asterix.column.metadata.schema.collection.GenericListSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MultisetSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import it.unimi.dsi.fastutil.ints.IntList;

public class ColumnSchemaTransformer
        implements IObjectSchemaNodeVisitor<AbstractSchemaNode, AbstractSchemaNode>, Serializable {

    private final FlushColumnMetadata rowMetadata;
    private final ObjectSchemaNode root;
    private AbstractSchemaNestedNode currentParent;
    private IFieldNamesDictionary toMergeFieldNamesDictionary;
    private final ArrayBackedValueStorage serializedMetadata;

    public void setToMergeFieldNamesDictionary(IFieldNamesDictionary toMergeFieldNamesDictionary) {
        this.toMergeFieldNamesDictionary = toMergeFieldNamesDictionary;
    }

    public FlushColumnMetadata getRowMetadata() {
        return rowMetadata;
    }

    public ObjectSchemaNode getRoot() {
        return root;
    }

    public ColumnSchemaTransformer(FlushColumnMetadata rowMetadata, ObjectSchemaNode root) {
        this.rowMetadata = rowMetadata;
        this.root = root;
        this.serializedMetadata = new ArrayBackedValueStorage();
    }

    /**
     * Transform a tuple in row format into columns
     *
     * @param toMergeRoot record pointable
     * @return the estimated size (possibly overestimated) of the primary key(s) columns
     */

    public int transform(ObjectSchemaNode toMergeRoot) throws HyracksDataException {
        int primaryKeysLength = 0;
        toMergeRoot.accept(this, root);
        return primaryKeysLength;
    }

    @Override
    public AbstractSchemaNode visit(ObjectSchemaNode toMergeRoot, AbstractSchemaNode mainRoot)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, mainRoot);
        AbstractSchemaNestedNode previousParent = currentParent;

        ObjectSchemaNode objectNode = (ObjectSchemaNode) mainRoot;
        currentParent = objectNode;
        System.out.println("CHECKING TRANSFORM WHILE INIT ASTERIX: A");
        IntList fieldNameIndexes = toMergeRoot.getChildrenFieldNameIndexes();
        System.out.println("CHECKING TRANSFORM WHILE INIT ASTERIX: B");

        for (int i = 0; i < toMergeRoot.getNumberOfChildren(); i++) {
            System.out.println("CHECKING TRANSFORM WHILE INIT ASTERIX: L " + i);
            System.out.println("NUNMBER OF CHILDEREN OF MERGE ROOT " + toMergeRoot.getNumberOfChildren());
            System.out.println("CHECK Field name index " + fieldNameIndexes);
            int index = fieldNameIndexes.getInt(i);
            System.out.println("CHECK INDEX " + index + " " + toMergeFieldNamesDictionary.getFieldName(index));
            IValueReference fieldName = this.toMergeFieldNamesDictionary.getFieldName(index);
            AbstractSchemaNode child = toMergeRoot.getChild(index);
            ATypeTag childTypeTag = child.getTypeTag();
            if (childTypeTag == ATypeTag.UNION) {
                UnionSchemaNode unionChild = (UnionSchemaNode) child;
                unionChild.setFieldName(fieldName);
                unionChild.accept(this, mainRoot);
            } else if (childTypeTag != ATypeTag.MISSING) {
                //Only write actual field values (including NULL) but ignore MISSING fields
                AbstractSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, rowMetadata);
                acceptActualNode(new GenericListSchemaNode(childTypeTag, child), childNode);
            }
        }
        rowMetadata.exitNode(mainRoot);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(MultisetSchemaNode toMergeRoot, AbstractSchemaNode mainRoot)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, mainRoot);
        AbstractSchemaNestedNode previousParent = currentParent;

        AbstractCollectionSchemaNode collectionNode = (AbstractCollectionSchemaNode) mainRoot;
        //the level at which an item is missing
        currentParent = collectionNode;
        int numberOfChildren;

        if (toMergeRoot.getItemTypeTag() == ATypeTag.UNION) {
            AbstractSchemaNode unionItem = toMergeRoot.getItemNode();
            numberOfChildren = unionItem.getNumberOfChildren();
            ArrayList<AbstractSchemaNode> unionChildren = ((UnionSchemaNode) unionItem).getChildrenList();

            for (AbstractSchemaNode child : unionChildren) {
                ATypeTag childTypeTag = child.getTypeTag();
                AbstractSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata);
                acceptActualNode(new GenericListSchemaNode(childTypeTag, child), childNode);
                /*
                 * The array item may change (e.g., BIGINT --> UNION). Thus, new items would be considered as missing
                 */
            }
        } else {
            numberOfChildren = 1;
            AbstractSchemaNode primItem = toMergeRoot.getItemNode();
            ATypeTag childTypeTag = primItem.getTypeTag();
            AbstractSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata);
            acceptActualNode(new GenericListSchemaNode(childTypeTag, primItem), childNode);
        }

        rowMetadata.exitCollectionNode(collectionNode, numberOfChildren);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(ArraySchemaNode toMergeRoot, AbstractSchemaNode mainRoot)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, mainRoot);
        AbstractSchemaNestedNode previousParent = currentParent;

        AbstractCollectionSchemaNode collectionNode = (AbstractCollectionSchemaNode) mainRoot;
        //the level at which an item is missing
        currentParent = collectionNode;
        int numberOfChildren;
        if (toMergeRoot.getItemTypeTag() == ATypeTag.UNION) {
            AbstractSchemaNode unionItem = toMergeRoot.getItemNode();
            numberOfChildren = unionItem.getNumberOfChildren();
            ArrayList<AbstractSchemaNode> unionChildren = ((UnionSchemaNode) unionItem).getChildrenList();

            for (AbstractSchemaNode child : unionChildren) {
                ATypeTag childTypeTag = child.getTypeTag();
                AbstractSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata);
                acceptActualNode(new GenericListSchemaNode(childTypeTag, child), childNode);
                /*
                 * The array item may change (e.g., BIGINT --> UNION). Thus, new items would be considered as missing
                 */
            }
        } else {
            numberOfChildren = 1;
            AbstractSchemaNode primItem = toMergeRoot.getItemNode();
            ATypeTag childTypeTag = primItem.getTypeTag();
            AbstractSchemaNode childNode = collectionNode.getOrCreateItem(childTypeTag, rowMetadata);
            acceptActualNode(new GenericListSchemaNode(childTypeTag, primItem), childNode);
        }

        rowMetadata.exitCollectionNode(collectionNode, numberOfChildren);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(UnionSchemaNode toMergeRoot, AbstractSchemaNode mainRoot)
            throws HyracksDataException {
        rowMetadata.enterNode(currentParent, mainRoot);
        AbstractSchemaNestedNode previousParent = currentParent;

        ObjectSchemaNode objectNode = (ObjectSchemaNode) mainRoot;
        currentParent = objectNode;
        //
        ArrayList<AbstractSchemaNode> unionChildren = toMergeRoot.getChildrenList();

        for (AbstractSchemaNode unionChildNode : unionChildren) {
            IValueReference fieldName = toMergeRoot.getFieldName();
            ATypeTag unionChildTypeTag = unionChildNode.getTypeTag();
            if (unionChildTypeTag != ATypeTag.MISSING) {
                AbstractSchemaNode childNode = objectNode.getOrCreateChild(fieldName, unionChildTypeTag, rowMetadata);
                acceptActualNode(new GenericListSchemaNode(unionChildTypeTag, unionChildNode), childNode);
            }
        }
        rowMetadata.exitNode(mainRoot);
        currentParent = previousParent;
        return null;
    }

    @Override
    public AbstractSchemaNode visit(PrimitiveSchemaNode toMergeRoot, AbstractSchemaNode mainRoot)
            throws HyracksDataException {

        return null;
    }

    @Override
    public AbstractSchemaNode visit(AbstractCollectionSchemaNode collectionNode, AbstractSchemaNode mainRoot)
            throws HyracksDataException {
        return null;
    }

    private void acceptActualNode(AbstractSchemaNode nodeToAdd, AbstractSchemaNode node) throws HyracksDataException {
        if (node.getTypeTag() == ATypeTag.UNION) {
            rowMetadata.enterNode(currentParent, node);
            AbstractSchemaNestedNode previousParent = currentParent;

            UnionSchemaNode unionNode = (UnionSchemaNode) node;
            currentParent = unionNode;

            ATypeTag childTypeTag = nodeToAdd.getTypeTag();
            AbstractSchemaNode actualNode;
            if (childTypeTag == ATypeTag.NULL || childTypeTag == ATypeTag.MISSING) {
                actualNode = unionNode.getOriginalType();
            } else {
                actualNode = unionNode.getOrCreateChild(childTypeTag, rowMetadata);
            }
            nodeToAdd.accept(this, actualNode);

            currentParent = previousParent;
            rowMetadata.exitNode(node);
        } else {
            nodeToAdd.accept(this, node);
        }
    }

    //    public ArrayBackedValueStorage serialize() throws IOException{
    //        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
    //        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    //        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
    //            objectOutputStream.writeObject(this);
    //        }
    //        storage.reset();
    //        storage.append(byteArrayOutputStream.toByteArray(), 0, byteArrayOutputStream.size());
    //        return storage;
    //    }

    //    private void serializeChanges() throws IOException {
    //        serializedMetadata.reset();
    //        DataOutput output = serializedMetadata.getDataOutput();
    //
    //
    //        //FieldNames
    //        this.toMergeFieldNamesDictionary.serialize(output);
    //
    //        //Schema
    //        root.serialize(output, new PathInfoSerializer());
    //
    //
    //
    //    }

}
