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

/*
                                                  
                                                  
                                                  
                                                  /**
                                                  * A visitor for ADM values which utilizes the lazy visitable:
                                                  *
                                                  * @param <R> return type
                                                  * @param <T> argument type
                                                  * @see AbstractLazyVisitablePointable
                                                  * @see AbstractLazyNestedVisitablePointable
                                                  */

package org.apache.asterix.column.metadata.schema;

import org.apache.asterix.column.metadata.schema.collection.AbstractCollectionSchemaNode;
import org.apache.asterix.column.metadata.schema.collection.ArraySchemaNode;
import org.apache.asterix.column.metadata.schema.collection.MultisetSchemaNode;
import org.apache.asterix.column.metadata.schema.primitive.PrimitiveSchemaNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IObjectSchemaNodeVisitor<R, T> {

    R visit(ObjectSchemaNode toMergeRoot, T mainRoot) throws HyracksDataException;

    R visit(MultisetSchemaNode unionNode, T mainRoot) throws HyracksDataException;

    R visit(ArraySchemaNode toMergeRoot, T mainRoot) throws HyracksDataException;

    R visit(UnionSchemaNode unionNode, T mainRoot) throws HyracksDataException;

    R visit(PrimitiveSchemaNode primitiveNode, T mainRoot) throws HyracksDataException;

    R visit(AbstractCollectionSchemaNode collectionNode, T mainRoot) throws HyracksDataException;
}
