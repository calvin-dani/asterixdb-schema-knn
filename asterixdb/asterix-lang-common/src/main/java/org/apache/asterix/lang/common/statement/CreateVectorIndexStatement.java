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
package org.apache.asterix.lang.common.statement;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.VectorIndexDeclUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmObjectNode;

public class CreateVectorIndexStatement extends AbstractStatement {

    private final Namespace namespace;
    private final Identifier vectorIndexName;
    private final Identifier datasetName;
    private final CreateIndexStatement.IndexedElement vectorField;
    private final List<CreateIndexStatement.IndexedElement> includedFields;
    private final AdmObjectNode withObjectNode;
    private final boolean ifNotExists;

    public CreateVectorIndexStatement(Namespace namespace, Identifier datasetName, Identifier vectorIndexName,
            CreateIndexStatement.IndexedElement vectorField, List<CreateIndexStatement.IndexedElement> includedFields,
            RecordConstructor withRecord, boolean ifNotExists) throws CompilationException {
        this.namespace = namespace;
        this.vectorIndexName = vectorIndexName;
        this.datasetName = datasetName;
        this.vectorField = vectorField;
        this.includedFields = includedFields;
        this.withObjectNode = VectorIndexDeclUtil.validateAndGetWithObjectNode(withRecord);
        this.ifNotExists = ifNotExists;
    }

    public Identifier getVectorIndexName() {
        return vectorIndexName;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_VECTOR_INDEX;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }
}
