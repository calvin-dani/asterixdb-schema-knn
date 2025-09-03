package org.apache.hyracks.storage.am.common.api;

import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.ISearchPredicate;

public interface ILSMIndexBatchPointCursor {

    // will be used by SampleCursor, to reuse the same cursor
    void setPredicate(ISearchPredicate predicate);

    void doHasNextWithPredicate(BitSet foundRecordsIndex) throws HyracksDataException;
}
