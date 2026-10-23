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
package org.apache.asterix.column.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.asterix.column.common.buffer.NoOpWriteMultiPageOp;
import org.apache.asterix.column.metadata.PathInfoSerializer;
import org.apache.asterix.column.operation.lsm.flush.ColumnTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.ColumnValuesWriterFactory;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;

public class NullableSchemaNodeTest {
    private final Mutable<IColumnWriteMultiPageOp> multiPageOpRef = new MutableObject<>(NoOpWriteMultiPageOp.INSTANCE);
    private final IColumnValuesWriterFactory writerFactory = new ColumnValuesWriterFactory(multiPageOpRef);
    private final FlushColumnMetadata columnMetadata =
            new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null, Collections.emptyList(), null,
                    writerFactory, multiPageOpRef);
    private final ColumnTransformer transformer = new ColumnTransformer(columnMetadata, columnMetadata.getRoot());

    public NullableSchemaNodeTest() throws IOException {
    }

    @Test
    public void nullForExistingFieldIsSerialized() throws IOException {
        transform("{\"id\": 1, \"name\": \"a\"}");
        columnMetadata.serializeColumnsMetadata();
        transform("{\"id\": 2, \"name\": null}");
        assertTrue(stringChild(columnMetadata.getRoot()).isNullable());

        FlushColumnMetadata restored =
                FlushColumnMetadata.createMutableMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null, 0, false,
                        writerFactory, multiPageOpRef, columnMetadata.serializeColumnsMetadata());
        assertTrue(stringChild(restored.getRoot()).isNullable());
    }

    @Test
    public void abortDiscardsNullability() throws IOException {
        transform("{\"id\": 1, \"name\": \"a\"}");
        columnMetadata.serializeColumnsMetadata();
        transform("{\"id\": 2, \"name\": null}");
        columnMetadata.abort();
        assertFalse(stringChild(columnMetadata.getRoot()).isNullable());
    }

    @Test
    public void nodeWithoutNullableBitReadsAsNonNullable() throws IOException {
        // Same bytes as a primitive node written before nullability was tracked
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes);
        output.writeByte(ATypeTag.STRING.serialize());
        output.writeInt(3);
        output.writeBoolean(false);

        DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        AbstractSchemaNode node = AbstractSchemaNode.deserialize(input, null);
        assertEquals(ATypeTag.STRING, node.getTypeTag());
        assertFalse(node.isNullable());
        assertEquals(0, input.available());
    }

    @Test
    public void nullableBitRoundTrips() throws IOException {
        transform("{\"id\": 1, \"name\": null}");
        transform("{\"id\": 2, \"name\": \"a\"}");
        AbstractSchemaNode node = stringChild(columnMetadata.getRoot());
        assertTrue(node.isNullable());

        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        node.serialize(storage.getDataOutput(), new PathInfoSerializer());
        AbstractSchemaNode copy = AbstractSchemaNode.deserialize(new DataInputStream(
                new ByteArrayInputStream(storage.getByteArray(), storage.getStartOffset(), storage.getLength())), null);
        assertEquals(ATypeTag.STRING, copy.getTypeTag());
        assertTrue(copy.isNullable());
    }

    private void transform(String json) throws IOException {
        JSONDataParser parser = new JSONDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, new JsonFactory());
        parser.reset(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        assertTrue(parser.parse(storage.getDataOutput()));
        RecordLazyVisitablePointable pointable = new RecordLazyVisitablePointable(true);
        pointable.set(storage);
        transformer.transform(pointable);
    }

    private static AbstractSchemaNode stringChild(ObjectSchemaNode root) {
        return root.getChildren().stream().filter(c -> c.getTypeTag() == ATypeTag.STRING).findFirst()
                .orElseThrow(() -> new AssertionError("no string field in " + root.getChildren()));
    }
}
