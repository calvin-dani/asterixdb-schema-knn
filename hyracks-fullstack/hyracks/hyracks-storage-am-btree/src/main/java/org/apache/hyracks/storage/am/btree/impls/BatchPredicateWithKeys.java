package org.apache.hyracks.storage.am.btree.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.MultiComparator;

public class BatchPredicateWithKeys extends RangePredicate {

    protected List<ITupleReference> keyTuples;
    protected ITupleReference keyTuple;
    private int keyIndex;

    public BatchPredicateWithKeys() {
        this.keyIndex = -1;
    }

    public void reset(List<ITupleReference> keyTuples) {
        this.keyTuples = keyTuples;
        this.keyIndex = -1;
    }

    private boolean isValid() {
        return keyIndex >= 0 && keyIndex < keyTuples.size();
    }

    @Override
    public ITupleReference getLowKey() {
        return isValid() ? keyTuples.get(keyIndex) : null;
    }

    @Override
    public ITupleReference getHighKey() {
        return isValid() ? keyTuples.get(keyIndex) : null;
    }

    @Override
    public ITupleReference getMinFilterTuple() {
        return null;
        //        throw new UnsupportedOperationException(
        //                "Min filter tuple not supported in " + getClass().getName() + " implementation.");
    }

    @Override
    public ITupleReference getMaxFilterTuple() {
        return null;
        //        throw new UnsupportedOperationException(
        //                "Max filter tuple not supported in " + getClass().getName() + " implementation.");
    }

    @Override
    public boolean isPointPredicate(MultiComparator originalKeyComparator) throws HyracksDataException {
        return true;
    }

    public boolean hasNext() {
        return keyIndex + 1 < keyTuples.size();
    }

    public void next() {
        keyIndex++;
        if (isValid()) {
            keyTuple = keyTuples.get(keyIndex);
        }
    }

    // use this to remove the keys from this index, as this index is present in the later components.
    public int getKeyIndex() {
        return keyIndex;
    }

    public int getNumKeys() {
        return keyTuples.size();
    }
}
