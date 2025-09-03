package org.apache.hyracks.storage.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class NoOpStatsAccumulator implements IComponentStatsAccumulator {

    public static final NoOpStatsAccumulator INSTANCE = new NoOpStatsAccumulator();

    @Override
    public IValueReference serializeComponentStatsMetadata() throws HyracksDataException {
        // since the bulkloader method is present at IIndex level, which is higher than what we need
        // for eg: in case of ITreeIndex, this method is not supported
        throw new UnsupportedOperationException("Not supported by" + this.getClass().getName());
    }

    @Override
    public void account(ITupleReference tuple) {

    }
}
