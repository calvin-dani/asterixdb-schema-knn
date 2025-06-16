package org.apache.asterix.runtime.utils;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.IOException;
import java.util.List;

public class VectorDistanceCalculation {


//    // Euclidean Distance
    public static double euclidean(double[] a, double[] b) {
//        checkDimensions(a, b);
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public static double euclidean(ListAccessor a, ListAccessor b) throws HyracksDataException {
//        checkDimensions(a, b);

        IPointable tempVal1 = new VoidPointable();
        ArrayBackedValueStorage storage1  = new ArrayBackedValueStorage();
        IPointable tempVal2 = new VoidPointable();
        ArrayBackedValueStorage storage2  = new ArrayBackedValueStorage();
        try {
            double sum = 0.0;
            double l1 = 0.0;
            double l2 = 0.0;
            for (int i = 0; i < a.size(); i++) {
                a.getOrWriteItem(i, tempVal1, storage1);
                b.getOrWriteItem(i, tempVal2, storage2);
                l1 = extractNumericVector(tempVal1);
                l2 = extractNumericVector(tempVal2);
                sum += Math.abs(l1 - l2);
                double diff = l1 - l2;
                sum += diff * diff;
            }
            return sum;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }



    // Manhattan Distance
    public static double manhattan(double[] a, double[] b) {
//        checkDimensions(a, b);
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.abs(a[i] - b[i]);
        }
        return sum;
    }

    public static double manhattan(ListAccessor a, ListAccessor b) throws HyracksDataException {
//        checkDimensions(a, b);

          IPointable tempVal1 = new VoidPointable();
          ArrayBackedValueStorage storage1  = new ArrayBackedValueStorage();
          IPointable tempVal2 = new VoidPointable();
            ArrayBackedValueStorage storage2  = new ArrayBackedValueStorage();
          try {
              double sum = 0.0;
              double l1 = 0.0;
              double l2 = 0.0;
            for (int i = 0; i < a.size(); i++) {
                a.getOrWriteItem(i, tempVal1, storage1);
                b.getOrWriteItem(i, tempVal2, storage2);
                l1 = extractNumericVector(tempVal1);
                l2 = extractNumericVector(tempVal2);
                sum += Math.abs(l1 - l2);
            }
            return sum;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    // Cosine Similarity
    public static double cosine(double[] a, double[] b) {
//        checkDimensions(a, b);
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0) {
            return 0.0; // or throw exception for zero vector
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    public static double cosine(ListAccessor a, ListAccessor b) throws HyracksDataException {
//        checkDimensions(a, b);

        IPointable tempVal1 = new VoidPointable();
        ArrayBackedValueStorage storage1  = new ArrayBackedValueStorage();
        IPointable tempVal2 = new VoidPointable();
        ArrayBackedValueStorage storage2  = new ArrayBackedValueStorage();
        try {
            double dot = 0.0, normA = 0.0, normB = 0.0;
            double l1 = 0.0;
            double l2 = 0.0;
            for (int i = 0; i < a.size(); i++) {
                a.getOrWriteItem(i, tempVal1, storage1);
                b.getOrWriteItem(i, tempVal2, storage2);
                l1 = extractNumericVector(tempVal1);
                l2 = extractNumericVector(tempVal2);
                dot += l1 * l2;
                normA +=l1 *l1;
                normB +=l2 *l2;
            }
            if (normA == 0.0 || normB == 0.0) {
                return 0.0; // or throw exception for zero vector
            }
            return dot / (Math.sqrt(normA) * Math.sqrt(normB));
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }

    // Dot Product
    public static double dot(double[] a, double[] b) {
//        checkDimensions(a, b);
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }


    public static double dot(ListAccessor a, ListAccessor b) throws HyracksDataException {
//        checkDimensions(a, b);

        IPointable tempVal1 = new VoidPointable();
        ArrayBackedValueStorage storage1  = new ArrayBackedValueStorage();
        IPointable tempVal2 = new VoidPointable();
        ArrayBackedValueStorage storage2  = new ArrayBackedValueStorage();
        try {
            double sum = 0.0;
            double l1 = 0.0;
            double l2 = 0.0;
            for (int i = 0; i < a.size(); i++) {
                a.getOrWriteItem(i, tempVal1, storage1);
                b.getOrWriteItem(i, tempVal2, storage2);
                l1 = extractNumericVector(tempVal1);
                l2 = extractNumericVector(tempVal2);
                sum += l1 * l2;
            }
            return sum;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

    }




    public static double extractNumericVector(IPointable pointable )
            throws HyracksDataException {
//        IPointable inputVal = new VoidPointable();
        byte[] data = pointable.getByteArray();
        int offset = pointable.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]);
        if (!typeTag.isNumericType()) {
           throw new HyracksDataException("Expected numeric type, but found: " + typeTag);
           // Return an empty array if the item type is not numeric
        }
        switch (typeTag) {
            case TINYINT:
                return AInt8SerializerDeserializer.getByte(data, offset + 1);
            case SMALLINT:
                return AInt16SerializerDeserializer.getShort(data, offset + 1);
            case INTEGER:
                return AInt32SerializerDeserializer.getInt(data, offset + 1);
            case BIGINT:
                return AInt64SerializerDeserializer.getLong(data, offset + 1);
            case FLOAT:
                return AFloatSerializerDeserializer.getFloat(data, offset + 1);
            case DOUBLE:
                return ADoubleSerializerDeserializer.getDouble(data, offset + 1);
            default:
                return 0 ; // Return an empty array if the item type is not numeric
        }
//        return true; // Return an empty array if the item type is not numeric
    }
}


// We get record function
// We use the list accessor to the record
// size check
// what metric
// parses the value [1,2,3] , [9,8,7] ===> 1 and 9
// confirm data type of the array.