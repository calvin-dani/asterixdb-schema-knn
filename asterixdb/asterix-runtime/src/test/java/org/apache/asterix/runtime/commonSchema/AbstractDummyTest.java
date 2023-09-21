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
package org.apache.asterix.runtime.commonSchema;

import org.apache.asterix.om.RowMetadata;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.RowTransformer;
import org.apache.asterix.om.lazy.metadata.schema.ObjectRowSchemaNode;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import java.io.IOException;

public abstract class AbstractDummyTest extends TestBase {
//    private static final DummyColumnValuesWriterFactory WRITER_FACTORY = new DummyColumnValuesWriterFactory();
    protected final RowMetadata columnMetadata;
    protected final RowTransformer columnTransformer;
//    protected final BatchFinalizerVisitor finalizer;
    //Schema
    protected final ArrayBackedValueStorage storage;
    protected final RecordLazyVisitablePointable pointable;
    protected int numberOfTuples;

    protected AbstractDummyTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
        columnMetadata = new RowMetadata(new MutableObject<>());
        columnTransformer = new RowTransformer(columnMetadata, columnMetadata.getRoot());
//        finalizer = new BatchFinalizerVisitor(columnMetadata);
        storage = new ArrayBackedValueStorage();
        pointable = new RecordLazyVisitablePointable(true);
    }

    public ObjectRowSchemaNode transform() throws IOException {
        storage.reset();
//        while (parser.parse(storage.getDataOutput())) {
//            pointable.set(storage);
//            columnTransformer.transform(pointable);
//            storage.reset();
//            numberOfTuples++;
//        }
//        finalizer.finalizeBatch(NoOpColumnBatchWriter.INSTANCE, columnMetadata);
        return columnMetadata.getRoot();
    }
}
