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
package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.vector.impls.VectorClusteringTree;
import org.apache.hyracks.storage.am.vector.impls.VCTreeBulkLoder;
import org.apache.hyracks.storage.common.IIndexBulkLoader;

/**
 * Bulk loader operator that processes sorted tuples from ExternalSortOperatorDescriptor.
 * 
 * This operator:
 * 1. Receives sorted tuples with format [centroidId, distance, ...original fields...]
 * 2. Sorts by centroidId first, then distance (handled by ExternalSortOperatorDescriptor)
 * 3. Prints the first 5 values for each centroid ID
 * 4. Passes through all data to the sink operator
 * 
 * The sorting is configured in ExternalSortOperatorDescriptor with sortFields = {0, 1}
 * where field 0 is centroidId and field 1 is distance.
 */
public class VCTreeSortedDataBulkLoaderOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    @SuppressWarnings("unused")
    private final RecordDescriptor inputRecordDescriptor;
    private final IIndexDataflowHelperFactory indexHelperFactory;
    private final float fillFactor;

    public VCTreeSortedDataBulkLoaderOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor inputRecordDescriptor, IIndexDataflowHelperFactory indexHelperFactory, float fillFactor) {
        super(spec, 1, 1); // Input arity 1, Output arity 1
        this.inputRecordDescriptor = inputRecordDescriptor;
        this.indexHelperFactory = indexHelperFactory;
        this.fillFactor = fillFactor;
        this.outRecDescs[0] = inputRecordDescriptor; // Pass through same record descriptor
        System.err.println("VCTreeSortedDataBulkLoaderOperatorDescriptor created with indexHelperFactory and fillFactor: " + fillFactor);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(this.getActivityId(), 0);
        return new VCTreeSortedDataBulkLoaderNodePushable(ctx, partition, nPartitions, inputRecDesc, indexHelperFactory, fillFactor);
    }

    private class VCTreeSortedDataBulkLoaderNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        @SuppressWarnings("unused")
        private final IHyracksTaskContext ctx;
        private final int partition;
        private final int nPartitions;
        @SuppressWarnings("unused")
        private final RecordDescriptor inputRecDesc;
        private final FrameTupleAccessor fta;
        private final FrameTupleReference tuple;
        private IFrameWriter writer;
        @SuppressWarnings("unused")
        private boolean writerOpen = false;

        // VectorClusteringTree bulk loader components
        private final IIndexDataflowHelperFactory indexHelperFactory;
        private final float fillFactor;
        private IIndexDataflowHelper indexHelper;
        private VectorClusteringTree vectorTree;
        private IIndexBulkLoader bulkLoader;

        // Centroid tracking state
        private int currentCentroidId = -1;
        private int tupleCountForCurrentCentroid = 0;
        private final Map<Integer, Integer> centroidTotalCounts = new HashMap<>();
        private int totalTuplesProcessed = 0;

        // Serializers for tuple field extraction
        @SuppressWarnings("rawtypes")
        private final ISerializerDeserializer[] fieldSerdes = {
            IntegerSerializerDeserializer.INSTANCE,  // Field 0: centroidId
            DoubleSerializerDeserializer.INSTANCE    // Field 1: distance
        };

        public VCTreeSortedDataBulkLoaderNodePushable(IHyracksTaskContext ctx, int partition, int nPartitions,
                RecordDescriptor inputRecDesc, IIndexDataflowHelperFactory indexHelperFactory, float fillFactor) throws HyracksDataException {
            this.ctx = ctx;
            this.partition = partition;
            this.nPartitions = nPartitions;
            this.inputRecDesc = inputRecDesc;
            this.indexHelperFactory = indexHelperFactory;
            this.fillFactor = fillFactor;
            this.fta = new FrameTupleAccessor(inputRecDesc);
            this.tuple = new FrameTupleReference();
        }

        @Override
        public void open() throws HyracksDataException {
            System.err.println("=== VCTreeSortedDataBulkLoader OPENING ===");
            System.err.println("Partition: " + partition + "/" + nPartitions);
            
            try {
                // Initialize VectorClusteringTree bulk loader
                initializeVectorClusteringTreeBulkLoader();
                
                System.err.println("VCTreeSortedDataBulkLoader opened successfully with VectorClusteringTree bulk loader");
            } catch (Exception e) {
                System.err.println("ERROR: Failed to open VCTreeSortedDataBulkLoader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Initialize VectorClusteringTree bulk loader.
         * 
         * @throws HyracksDataException if initialization fails
         */
        private void initializeVectorClusteringTreeBulkLoader() throws HyracksDataException {
            try {
                System.err.println("=== INITIALIZING VECTORCLUSTERINGTREE BULK LOADER ===");
                
                // Create index helper
                indexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
                indexHelper.open();
                
                // Get VectorClusteringTree instance
                vectorTree = (VectorClusteringTree) indexHelper.getIndexInstance();
                System.err.println("VectorClusteringTree instance obtained: " + vectorTree);
                
                // Create bulk loader
                bulkLoader = vectorTree.createBulkLoader(fillFactor, false, 0, false, null);
                System.err.println("VCTreeBulkLoder created with fillFactor: " + fillFactor);
                
                System.err.println("✅ VectorClusteringTree bulk loader initialized successfully");
                
            } catch (Exception e) {
                System.err.println("ERROR: Failed to initialize VectorClusteringTree bulk loader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            fta.reset(buffer);

            // Process each tuple in the frame
            for (int i = 0; i < fta.getTupleCount(); i++) {
                tuple.reset(fta, i);
                processSortedTuple(tuple);
            }

            // Pass through the sorted frame to output (sink operator)
            if (writer != null) {
                writer.nextFrame(buffer);
            }
        }

        /**
         * Process sorted tuples from ExternalSortOperatorDescriptor.
         * These tuples are in format: [centroidId, distance, ...original fields...]
         * and are sorted by centroidId first, then distance.
         */
        private void processSortedTuple(ITupleReference tuple) throws HyracksDataException {
            try {
                // Extract centroidId and distance from tuple
                int centroidId = extractCentroidId(tuple);
                double distance = extractDistance(tuple);

                // Check if we've moved to a new centroid
                if (currentCentroidId != centroidId) {
                    // New centroid - log previous centroid summary if any
                    if (currentCentroidId != -1) {
                        logCentroidSummary(currentCentroidId, tupleCountForCurrentCentroid);
                    }
                    // TODO CALVIN DANI: to call bulkload.nextcentroid()
                    // Reset for new centroid
                    currentCentroidId = centroidId;
                    tupleCountForCurrentCentroid = 0;
                    
                    System.err.println("=== Processing Centroid ID: " + centroidId + " ===");
                }

                // Increment counters
                tupleCountForCurrentCentroid++;
                totalTuplesProcessed++;
                centroidTotalCounts.put(centroidId, centroidTotalCounts.getOrDefault(centroidId, 0) + 1);

                // Print first 5 values for this centroid
                if (tupleCountForCurrentCentroid <= 5) {
                    printCentroidData(centroidId, distance, tuple, tupleCountForCurrentCentroid);
                }

                // Add tuple to VectorClusteringTree bulk loader
                if (bulkLoader != null) {
                    try {
                        bulkLoader.add(tuple);
                    } catch (Exception e) {
                        System.err.println("ERROR: Failed to add tuple to VectorClusteringTree bulk loader: " + e.getMessage());
                        e.printStackTrace();
                        // Continue processing other tuples
                    }
                }


            } catch (Exception e) {
                System.err.println("ERROR: Failed to process sorted tuple: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Extract centroidId from field 0 of the tuple.
         */
        private int extractCentroidId(ITupleReference tuple) throws HyracksDataException {
            try {
                // Use TupleUtils to deserialize the integer field
                Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);
                return (Integer) fieldValues[0];
            } catch (Exception e) {
                System.err.println("ERROR: Failed to extract centroidId from tuple: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Extract distance from field 1 of the tuple.
         */
        private double extractDistance(ITupleReference tuple) throws HyracksDataException {
            try {
                // Use TupleUtils to deserialize the double field
                Object[] fieldValues = TupleUtils.deserializeTuple(tuple, fieldSerdes);
                return (Double) fieldValues[1];
            } catch (Exception e) {
                System.err.println("ERROR: Failed to extract distance from tuple: " + e.getMessage());
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Print the first 5 values for each centroid ID.
         */
        private void printCentroidData(int centroidId, double distance, ITupleReference tuple, int tupleIndex) {
            System.err.println("Centroid " + centroidId + ", Tuple " + tupleIndex + 
                             ": distance=" + String.format("%.6f", distance) + 
                             ", totalFields=" + tuple.getFieldCount());
            
            // Print additional tuple information for debugging
            if (tupleIndex <= 3) { // Only for first 3 tuples to avoid spam
                System.err.println("  Tuple details: " + tuple.toString());
            }
        }

        /**
         * Log summary for a completed centroid.
         */
        private void logCentroidSummary(int centroidId, int tupleCount) {
            System.err.println("=== Centroid " + centroidId + " Summary ===");
            System.err.println("Total tuples processed: " + tupleCount);
            System.err.println("First 5 values printed above");
            System.err.println("================================");
        }

        @Override
        public void fail() throws HyracksDataException {
            if (writer != null) {
                writer.fail();
            }
        }

        @Override
        public void flush() throws HyracksDataException {
            if (writer != null) {
                writer.flush();
            }
        }

        @Override
        public void close() throws HyracksDataException {
            try {
                // Finalize VectorClusteringTree bulk loader
                if (bulkLoader != null) {
                    System.err.println("=== FINALIZING VECTORCLUSTERINGTREE BULK LOADER ===");
                    bulkLoader.end();
                    System.err.println("✅ VectorClusteringTree bulk loader finalized successfully");
                }
                
                // Close index helper
                if (indexHelper != null) {
                    indexHelper.close();
                    System.err.println("✅ Index helper closed successfully");
                }
                
                // Log final summary for current centroid if any
                if (currentCentroidId != -1) {
                    logCentroidSummary(currentCentroidId, tupleCountForCurrentCentroid);
                }
                
                // Log overall processing summary
                System.err.println("=== VCTreeSortedDataBulkLoader FINAL SUMMARY ===");
                System.err.println("Total tuples processed: " + totalTuplesProcessed);
                System.err.println("Total centroids processed: " + centroidTotalCounts.size());
                System.err.println("Tuples per centroid:");
                for (Map.Entry<Integer, Integer> entry : centroidTotalCounts.entrySet()) {
                    System.err.println("  Centroid " + entry.getKey() + ": " + entry.getValue() + " tuples");
                }
                System.err.println("===============================================");
                
                if (writer != null) {
                    writer.close();
                }
                
            } catch (Exception e) {
                System.err.println("ERROR: Failed to close VCTreeSortedDataBulkLoader: " + e.getMessage());
                e.printStackTrace();
                throw HyracksDataException.create(e);
            }
        }
    }
}
