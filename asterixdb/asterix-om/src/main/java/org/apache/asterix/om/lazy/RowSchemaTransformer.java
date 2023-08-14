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
package org.apache.asterix.om.lazy;

import java.util.ArrayList;

import org.apache.asterix.om.RowMetadata;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNestedNode;
import org.apache.asterix.om.lazy.metadata.schema.AbstractRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.ObjectRowSchemaNode;
import org.apache.asterix.om.lazy.metadata.schema.UnionRowSchemaNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class RowSchemaTransformer implements IObjectRowSchemaNodeVisitor<AbstractRowSchemaNode, AbstractRowSchemaNode> {

    private final RowMetadata columnMetadata;
    private final VoidPointable nonTaggedValue;
    private final ObjectRowSchemaNode root;
    private AbstractRowSchemaNestedNode currentParent;
    private int primaryKeysLength;

    public ObjectRowSchemaNode getRoot() {
        return root;
    }

    public RowSchemaTransformer(RowMetadata columnMetadata, ObjectRowSchemaNode root) {
        this.columnMetadata = columnMetadata;
        this.root = root;
        nonTaggedValue = new VoidPointable();
    }

    /**
     * Transform a tuple in row format into columns
     *
     * @param toMergeRoot record pointable
     * @return the estimated size (possibly overestimated) of the primary key(s) columns
     */

    public int transform(ObjectRowSchemaNode toMergeRoot) throws HyracksDataException {
        primaryKeysLength = 0;
        toMergeRoot.accept(this, root);
        return primaryKeysLength;
    }

    @Override
    public AbstractRowSchemaNode visit(ObjectRowSchemaNode toMergeRoot, AbstractRowSchemaNode mainRoot)
            throws HyracksDataException {
        columnMetadata.enterNode(currentParent, mainRoot);
        AbstractRowSchemaNestedNode previousParent = currentParent;

        ObjectRowSchemaNode objectNode = (ObjectRowSchemaNode) mainRoot;
        columnMetadata.printRootSchema(objectNode, columnMetadata.getFieldNamesDictionary());
        //        columnMetadata.printRootSchema(toMergeRoot, columnMetadata.getFieldNamesDictionary(),"MERGER SCHEMA BY SCHEMA BEFORE");
        currentParent = objectNode;
        for (int i = 0; i < toMergeRoot.getNumberOfChildren(); i++) {
            AbstractRowSchemaNode child = toMergeRoot.getChild(i);
            //            ObjectRowSchemaNode objectMergeNode = (ObjectRowSchemaNode) child;
            IValueReference fieldName = child.getFieldName();
            ATypeTag childTypeTag = child.getTypeTag();
            if (childTypeTag == ATypeTag.UNION) {
                UnionRowSchemaNode unionChild = (UnionRowSchemaNode) child;
                ArrayList<AbstractRowSchemaNode> unionChildren = unionChild.getChildrenList();
                for (AbstractRowSchemaNode unionChildNode : unionChildren) {
                    ATypeTag unionChildTypeTag = unionChildNode.getTypeTag();
                    if (unionChildTypeTag != ATypeTag.MISSING) {
                        AbstractRowSchemaNode childNode =
                                objectNode.getOrCreateChild(fieldName, unionChildTypeTag, columnMetadata);
                        // Writing into Columnar format               acceptActualNode(pointable.getChildVisitablePointable(), childNode);
                    }
                }
            } else if (childTypeTag != ATypeTag.MISSING) {
                //Only write actual field values (including NULL) but ignore MISSING fields
                AbstractRowSchemaNode childNode = objectNode.getOrCreateChild(fieldName, childTypeTag, columnMetadata);
                // Writing into Columnar format               acceptActualNode(pointable.getChildVisitablePointable(), childNode);
            }
        }
        columnMetadata.printRootSchema(objectNode, columnMetadata.getFieldNamesDictionary());
        columnMetadata.exitNode(mainRoot);
        currentParent = previousParent;
        return null;
    }

}
