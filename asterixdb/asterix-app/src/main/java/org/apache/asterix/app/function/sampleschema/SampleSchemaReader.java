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

package org.apache.asterix.app.function.sampleschema;

import java.io.IOException;

import org.apache.asterix.app.function.FunctionReader;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.schemainferrence.ObjectRowSchemaNode;
import org.apache.asterix.runtime.schemainferrence.RowMetadata;
import org.apache.asterix.runtime.schemainferrence.RowTransformer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SampleSchemaReader extends FunctionReader {

    private final CharArrayRecord record;
    private final IIndexDataflowHelper[] indexDataflowHelpers;
    private final IIndexAccessor[] accessors;
    private final IIndexCursor[] searchCursors;
    private final String schemaJson;
    private boolean hasNext;

    SampleSchemaReader(IIndexDataflowHelper[] indexDataflowHelpers, IBinaryComparatorFactory[] comparatorFactories,
            int numPrimaryKeys, ARecordType itemType) throws HyracksDataException {
        this.indexDataflowHelpers = indexDataflowHelpers;
        this.accessors = new IIndexAccessor[indexDataflowHelpers.length];
        this.searchCursors = new IIndexCursor[indexDataflowHelpers.length];
        this.record = new CharArrayRecord();
        this.hasNext = true;

        RowMetadata rowMetadata = new RowMetadata();
        RowTransformer transformer = new RowTransformer(rowMetadata, rowMetadata.getRoot());
        transformer.transform(itemType, false);

        TypedRecordLazyVisitablePointable pointable = new TypedRecordLazyVisitablePointable(itemType);

        MultiComparator searchMultiComparator = MultiComparator.create(comparatorFactories);
        RangePredicate rangePredicate =
                new RangePredicate(null, null, true, true, searchMultiComparator, searchMultiComparator, null, null);

        try {
            for (int i = 0; i < indexDataflowHelpers.length; i++) {
                indexDataflowHelpers[i].open();
                accessors[i] =
                        indexDataflowHelpers[i].getIndexInstance().createAccessor(NoOpIndexAccessParameters.INSTANCE);
                searchCursors[i] = accessors[i].createSearchCursor(false);
                accessors[i].search(searchCursors[i], rangePredicate);
            }

            for (int i = 0; i < searchCursors.length; i++) {
                while (searchCursors[i].hasNext()) {
                    searchCursors[i].next();
                    ITupleReference tuple = searchCursors[i].getTuple();
                    pointable.set(tuple.getFieldData(numPrimaryKeys), tuple.getFieldStart(numPrimaryKeys),
                            tuple.getFieldLength(numPrimaryKeys));
                    transformer.transform(pointable);
                }
            }

            ObjectRowSchemaNode root = rowMetadata.getRoot();
            String rawSchema = rowMetadata.printRootSchema(root, rowMetadata.getFieldNamesDictionary());
            ObjectMapper objectMapper = new ObjectMapper();
            this.schemaJson = objectMapper.writeValueAsString(objectMapper.readTree(rawSchema));
        } catch (Exception e) {
            Throwable failure = releaseResources();
            if (failure != null) {
                e.addSuppressed(failure);
            }
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        return hasNext;
    }

    @Override
    public IRawRecord<char[]> next() throws IOException {
        record.reset();
        String result = "{\"Schema\":" + schemaJson + "}";
        record.append(result.toCharArray());
        record.endRecord();
        hasNext = false;
        return record;
    }

    @Override
    public void close() throws IOException {
        Throwable failure = releaseResources();
        if (failure != null) {
            throw HyracksDataException.create(failure);
        }
    }

    private Throwable releaseResources() {
        Throwable failure = null;
        for (int i = 0; i < indexDataflowHelpers.length; i++) {
            if (searchCursors[i] != null) {
                failure = ResourceReleaseUtils.close(searchCursors[i], failure);
                failure = CleanupUtils.destroy(failure, searchCursors[i], accessors[i]);
            }
            failure = ResourceReleaseUtils.close(indexDataflowHelpers[i], failure);
        }
        return failure;
    }
}
