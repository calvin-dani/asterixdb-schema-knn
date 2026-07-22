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
package org.apache.asterix.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.column.metadata.schema.AbstractSchemaNestedNode;
import org.apache.asterix.column.metadata.schema.AbstractSchemaNode;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.om.dictionary.AbstractFieldNamesDictionary;
import org.apache.asterix.om.dictionary.IFieldNamesDictionary;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;

/**
 * Resolves columnar schema metadata for {@code current-schema} from the newest on-disk LSM
 * component when available (matching scan/projection behavior), falling back to the index-level
 * flush working copy when no disk components exist yet.
 */
public final class ColumnSchemaMetadataUtil {

    private static final int WRITERS_POINTER = 0;
    private static final int FIELD_NAMES_POINTER = WRITERS_POINTER + Integer.BYTES;

    private ColumnSchemaMetadataUtil() {
    }

    /**
     * Returns column schema metadata for the given index.
     * Prefers metadata from {@code diskComponents.get(0)} (newest disk component); if the index
     * has no disk components, falls back to {@link LSMColumnBTree#getPublicColumnMetadata()}.
     */
    public static FlushColumnMetadata resolveColumnMetadata(LSMColumnBTree index) throws HyracksDataException {
        List<ILSMDiskComponent> diskComponents = index.getDiskComponents();
        if (diskComponents == null || diskComponents.isEmpty()) {
            return (FlushColumnMetadata) index.getPublicColumnMetadata();
        }
        IValueReference serialized = ColumnUtil.getColumnMetadataCopy(diskComponents.get(0).getMetadata());
        try {
            return deserializeColumnMetadata(serialized);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Deserializes field-name dictionary and schema tree from serialized column metadata.
     * Writers are not restored; only the schema tree is needed for merge/print.
     */
    public static FlushColumnMetadata deserializeColumnMetadata(IValueReference serializedMetadata) throws IOException {
        int offset = serializedMetadata.getStartOffset();
        int length = serializedMetadata.getLength();
        byte[] bytes = serializedMetadata.getByteArray();
        int fieldNamesStart = offset + IntegerPointable.getInteger(bytes, offset + FIELD_NAMES_POINTER);
        int remaining = (offset + length) - fieldNamesStart;
        DataInput input = new DataInputStream(new ByteArrayInputStream(bytes, fieldNamesStart, remaining));
        IFieldNamesDictionary fieldNamesDictionary = AbstractFieldNamesDictionary.deserialize(input);
        Map<AbstractSchemaNestedNode, RunLengthIntArray> definitionLevels = new HashMap<>();
        ObjectSchemaNode root = (ObjectSchemaNode) AbstractSchemaNode.deserialize(input, definitionLevels);
        Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>();
        IColumnValuesWriterFactory factory = new ColumnValuesWriterFactory(multiPageOpRef);
        return new FlushColumnMetadata(multiPageOpRef, root, definitionLevels, fieldNamesDictionary, factory);
    }
}
