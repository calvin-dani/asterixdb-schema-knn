package org.apache.hyracks.storage.am.lsm.vector;

import static org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestHarness.LEAF_FRAMES_TO_TEST;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVCTreeTestHarness;
import org.apache.hyracks.storage.am.vector.AbstractVectorClusteringTreeInsertTest;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.frames.VectorTreeFrameType;

@SuppressWarnings("rawtypes")
public class LSMVCTreeInsertTest extends AbstractVectorClusteringTreeInsertTest {

    private final LSMVCTreeTestHarness harness = new LSMVCTreeTestHarness();

    public LSMVCTreeInsertTest() {
        super(LEAF_FRAMES_TO_TEST);
    }

    @Override
    protected AbstractVectorTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes, int numKeys,
            VectorTreeFrameType frameType, boolean filtered) throws Exception {
        return null;
    }

    @Override
    protected Random getRandom() {
        return null;
    }
}
