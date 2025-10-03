package org.apache.hyracks.storage.am.lsm.vector.impls;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;

/**
 * Helper class for creating protocol tuples used by VCTreeStaticStructureLoader
 */
public class VCTreeProtocolTuples {

    // Protocol tuple type constants
    public static final int LEVEL_START_TYPE = 0;
    public static final int CLUSTER_START_TYPE = 1;
    public static final int CENTROID_DATA_TYPE = 2;
    public static final int CLUSTER_END_TYPE = 3;
    public static final int LEVEL_END_TYPE = 4;

    // Page type constants
    public static final int LEAF_PAGE_TYPE = 0;
    public static final int INTERIOR_PAGE_TYPE = 1;
    public static final int ROOT_PAGE_TYPE = 2;
    public static final int METADATA_PAGE_TYPE = 3;

    // Serializer/Deserializer arrays for different tuple types
    private static final ISerializerDeserializer[] LEVEL_START_SERDES = {
            IntegerSerializerDeserializer.INSTANCE, // tuple type
            IntegerSerializerDeserializer.INSTANCE, // level
            IntegerSerializerDeserializer.INSTANCE  // page type
    };

    private static final ISerializerDeserializer[] PAGE_START_SERDES = {
            IntegerSerializerDeserializer.INSTANCE, // tuple type
            IntegerSerializerDeserializer.INSTANCE  // page type
    };

    private static final ISerializerDeserializer[] CENTROID_DATA_SERDES = {
            IntegerSerializerDeserializer.INSTANCE,      // tuple type
            IntegerSerializerDeserializer.INSTANCE,      // centroid ID
            FloatArraySerializerDeserializer.INSTANCE    // embedding
    };

    private static final ISerializerDeserializer[] SINGLE_INT_SERDES = {
            IntegerSerializerDeserializer.INSTANCE // tuple type only
    };

    // Reusable tuple builders for efficiency
    private static final ThreadLocal<ArrayTupleBuilder> tupleBuilder =
            ThreadLocal.withInitial(() -> new ArrayTupleBuilder(3));
    private static final ThreadLocal<ArrayTupleReference> tupleReference =
            ThreadLocal.withInitial(ArrayTupleReference::new);

    /**
     * Create LEVEL_START tuple: <TYPE:0, level:int, page_type:int>
     */
    public static ITupleReference createLevelStartTuple(int level, int pageType) throws HyracksDataException {
        return TupleUtils.createTuple(LEVEL_START_SERDES, LEVEL_START_TYPE, level, pageType);
    }

    /**
     * Create PAGE_START tuple: <TYPE:1, page_type:int>
     */
    public static ITupleReference createPageStartTuple(int pageType) throws HyracksDataException {
        return TupleUtils.createTuple(PAGE_START_SERDES, CLUSTER_START_TYPE, pageType);
    }

    /**
     * Create CENTROID_DATA tuple: <TYPE:2, cid:int, embedding:float[]>
     */
    public static ITupleReference createCentroidDataTuple(int centroidId, float[] embedding) throws HyracksDataException {
        return TupleUtils.createTuple(CENTROID_DATA_SERDES, CENTROID_DATA_TYPE, centroidId, embedding);
    }

    /**
     * Create PAGE_END tuple: <TYPE:3>
     */
    public static ITupleReference createPageEndTuple() throws HyracksDataException {
        return TupleUtils.createTuple(SINGLE_INT_SERDES, CLUSTER_END_TYPE);
    }

    /**
     * Create LEVEL_END tuple: <TYPE:4>
     */
    public static ITupleReference createLevelEndTuple() throws HyracksDataException {
        return TupleUtils.createTuple(SINGLE_INT_SERDES, LEVEL_END_TYPE);
    }

    /**
     * Parse tuple type from ITupleReference using TupleUtils
     */
    public static int parseTupleType(ITupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() < 1) {
            throw new HyracksDataException("Invalid tuple: no fields");
        }

        // Use TupleUtils to deserialize the first field only
        Object[] fields = TupleUtils.deserializeTuple(tuple, new ISerializerDeserializer[]{
                IntegerSerializerDeserializer.INSTANCE
        });

        return (Integer) fields[0];
    }

    /**
     * Parse level from LEVEL_START tuple using TupleUtils
     */
    public static int parseLevel(ITupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() < 2) {
            throw new HyracksDataException("LEVEL_START tuple must have at least 2 fields");
        }

        // Deserialize first two fields
        Object[] fields = TupleUtils.deserializeTuple(tuple, new ISerializerDeserializer[]{
                IntegerSerializerDeserializer.INSTANCE, // tuple type
                IntegerSerializerDeserializer.INSTANCE  // level
        });

        return (Integer) fields[1];
    }

    /**
     * Parse page type from LEVEL_START or PAGE_START tuple using TupleUtils
     */
    public static int parsePageType(ITupleReference tuple) throws HyracksDataException {
        int tupleType = parseTupleType(tuple);

        if (tupleType == LEVEL_START_TYPE) {
            if (tuple.getFieldCount() < 3) {
                throw new HyracksDataException("LEVEL_START tuple must have 3 fields");
            }
            // Deserialize all three fields
            Object[] fields = TupleUtils.deserializeTuple(tuple, LEVEL_START_SERDES);
            return (Integer) fields[2];

        } else if (tupleType == CLUSTER_START_TYPE) {
            if (tuple.getFieldCount() < 2) {
                throw new HyracksDataException("PAGE_START tuple must have 2 fields");
            }
            // Deserialize both fields
            Object[] fields = TupleUtils.deserializeTuple(tuple, PAGE_START_SERDES);
            return (Integer) fields[1];

        } else {
            throw new HyracksDataException("Not a LEVEL_START or PAGE_START tuple");
        }
    }

    /**
     * Parse centroid ID from CENTROID_DATA tuple using TupleUtils
     */
    public static int parseCentroidId(ITupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() < 2) {
            throw new HyracksDataException("CENTROID_DATA tuple must have at least 2 fields");
        }

        // Deserialize first two fields
        Object[] fields = TupleUtils.deserializeTuple(tuple, new ISerializerDeserializer[]{
                IntegerSerializerDeserializer.INSTANCE, // tuple type
                IntegerSerializerDeserializer.INSTANCE  // centroid ID
        });

        return (Integer) fields[1];
    }

    /**
     * Parse embedding from CENTROID_DATA tuple using TupleUtils
     */
    public static float[] parseEmbedding(ITupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() < 3) {
            throw new HyracksDataException("CENTROID_DATA tuple must have 3 fields");
        }

        // Deserialize all three fields
        Object[] fields = TupleUtils.deserializeTuple(tuple, CENTROID_DATA_SERDES);

        return (float[]) fields[2];
    }

    /**
     * Parse entire LEVEL_START tuple using TupleUtils
     */
    public static LevelStartData parseLevelStartTuple(ITupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() != 3) {
            throw new HyracksDataException("LEVEL_START tuple must have exactly 3 fields");
        }

        Object[] fields = TupleUtils.deserializeTuple(tuple, LEVEL_START_SERDES);

        int tupleType = (Integer) fields[0];
        if (tupleType != LEVEL_START_TYPE) {
            throw new HyracksDataException("Not a LEVEL_START tuple: " + tupleType);
        }

        return new LevelStartData((Integer) fields[1], (Integer) fields[2]);
    }

    /**
     * Parse entire PAGE_START tuple using TupleUtils
     */
    public static PageStartData parsePageStartTuple(ITupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() != 2) {
            throw new HyracksDataException("PAGE_START tuple must have exactly 2 fields");
        }

        Object[] fields = TupleUtils.deserializeTuple(tuple, PAGE_START_SERDES);

        int tupleType = (Integer) fields[0];
        if (tupleType != CLUSTER_START_TYPE) {
            throw new HyracksDataException("Not a PAGE_START tuple: " + tupleType);
        }

        return new PageStartData((Integer) fields[1]);
    }

    /**
     * Parse entire CENTROID_DATA tuple using TupleUtils
     */
    public static CentroidDataTuple parseCentroidDataTuple(ITupleReference tuple) throws HyracksDataException {
        if (tuple.getFieldCount() != 3) {
            throw new HyracksDataException("CENTROID_DATA tuple must have exactly 3 fields");
        }

        Object[] fields = TupleUtils.deserializeTuple(tuple, CENTROID_DATA_SERDES);

        int tupleType = (Integer) fields[0];
        if (tupleType != CENTROID_DATA_TYPE) {
            throw new HyracksDataException("Not a CENTROID_DATA tuple: " + tupleType);
        }

        return new CentroidDataTuple((Integer) fields[1], (float[]) fields[2]);
    }

    /**
     * Debug method to print tuple contents using TupleUtils
     */
    public static String printProtocolTuple(ITupleReference tuple) throws HyracksDataException {
        StringBuilder sb = new StringBuilder();
        int tupleType = parseTupleType(tuple);

        switch (tupleType) {
            case LEVEL_START_TYPE:
                LevelStartData levelStartData = parseLevelStartTuple(tuple);
                sb.append("LEVEL_START(level=").append(levelStartData.level)
                        .append(", pageType=").append(getPageTypeName(levelStartData.pageType)).append(")");
                break;

            case CLUSTER_START_TYPE:
                PageStartData pageStartData = parsePageStartTuple(tuple);
                sb.append("PAGE_START(pageType=").append(getPageTypeName(pageStartData.pageType)).append(")");
                break;

            case CENTROID_DATA_TYPE:
                CentroidDataTuple centroidData = parseCentroidDataTuple(tuple);
                sb.append("CENTROID_DATA(cid=").append(centroidData.centroidId)
                        .append(", embedding=[");
                float[] embedding = centroidData.embedding;
                for (int i = 0; i < Math.min(embedding.length, 4); i++) { // Limit output
                    sb.append(String.format("%.2f", embedding[i]));
                    if (i < Math.min(embedding.length, 4) - 1) sb.append(", ");
                }
                if (embedding.length > 4) sb.append("...");
                sb.append("])");
                break;

            case CLUSTER_END_TYPE:
                sb.append("PAGE_END()");
                break;

            case LEVEL_END_TYPE:
                sb.append("LEVEL_END()");
                break;

            default:
                sb.append("UNKNOWN_TYPE(").append(tupleType).append(")");
        }

        return sb.toString();
    }

    /**
     * Get page type name for debugging
     */
    private static String getPageTypeName(int pageType) {
        switch (pageType) {
            case LEAF_PAGE_TYPE: return "LEAF";
            case INTERIOR_PAGE_TYPE: return "INTERIOR";
            case ROOT_PAGE_TYPE: return "ROOT";
            case METADATA_PAGE_TYPE: return "METADATA";
            default: return "UNKNOWN(" + pageType + ")";
        }
    }

    // Data classes for parsed tuple content
    public static class LevelStartData {
        public final int level;
        public final int pageType;

        public LevelStartData(int level, int pageType) {
            this.level = level;
            this.pageType = pageType;
        }
    }

    public static class PageStartData {
        public final int pageType;

        public PageStartData(int pageType) {
            this.pageType = pageType;
        }
    }

    public static class CentroidDataTuple {
        public final int centroidId;
        public final float[] embedding;

        public CentroidDataTuple(int centroidId, float[] embedding) {
            this.centroidId = centroidId;
            this.embedding = embedding;
        }
    }
}