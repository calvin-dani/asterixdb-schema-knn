package org.apache.hyracks.storage.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IComponentStatsAccumulator {
    /**
     * @return a serialized version of the column statistics metadata
     */
    IValueReference serializeComponentStatsMetadata() throws HyracksDataException;

    /*
        * Account for the given tuple
        * In some callers like BTreeNSMBulkLoader, I can't reference higher entity like ILSMTupleReference
     */
    void account(ITupleReference tuple);
}
