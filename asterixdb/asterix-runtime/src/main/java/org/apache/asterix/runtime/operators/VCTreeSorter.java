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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.FixedSizeFrame;

/**
 * VCTreeSorter handles sorting of centroid files by distance.
 * Uses external sorting approach similar to ExternalSortOperatorDescriptor.
 */
public class VCTreeSorter {
    
    private final IHyracksTaskContext ctx;
    private final int frameSize;
    
    // Track sorted files and intermediate runs
    private final Map<Integer, FileReference> centroidSortedFiles = new HashMap<>();
    private final Map<Integer, List<FileReference>> intermediateRunFiles = new HashMap<>();
    
    // Distance comparator for sorting
    private final Comparator<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> distanceComparator = 
        (a, b) -> Double.compare(a.distance, b.distance);
    
    public VCTreeSorter(IHyracksTaskContext ctx) {
        this.ctx = ctx;
        this.frameSize = 32768; // Default frame size
    }
    
    /**
     * Sort a centroid file by distance.
     */
    public FileReference sortCentroidFile(int centroidId, FileReference inputFile) throws HyracksDataException {
        System.err.println("Sorting centroid " + centroidId + " file: " + inputFile);
        
        // Step 1: Read all data from input file
        List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> data = readCentroidData(inputFile);
        
        // Step 2: Create sorted runs
        List<FileReference> sortedRuns = createSortedRuns(centroidId, data);
        
        // Step 3: Merge runs into final sorted file
        FileReference sortedFile = mergeRuns(centroidId, sortedRuns);
        
        // Step 4: Store reference
        centroidSortedFiles.put(centroidId, sortedFile);
        
        System.err.println("Sorted centroid " + centroidId + " to: " + sortedFile);
        return sortedFile;
    }
    
    /**
     * Read all data from centroid file.
     */
    private List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> readCentroidData(FileReference inputFile) throws HyracksDataException {
        List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> data = new ArrayList<>();
        
        RunFileReader reader = new RunFileReader(inputFile, ctx.getIoManager(), inputFile.getFile().length(), false);
        reader.open();
        
        IFrame frame = new FixedSizeFrame(ByteBuffer.allocate(frameSize));
        while (reader.nextFrame(frame)) {
            VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple = parseTupleFromFrame(frame.getBuffer());
            if (tuple != null) {
                data.add(tuple);
            }
        }
        
        reader.close();
        return data;
    }
    
    /**
     * Create sorted runs from data (similar to ExternalSortRunGenerator).
     */
    private List<FileReference> createSortedRuns(int centroidId, List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> data) throws HyracksDataException {
        List<FileReference> runs = new ArrayList<>();
        
        // Sort data by distance
        Collections.sort(data, distanceComparator);
        
        // Create runs (simplified - one run per batch)
        int batchSize = 100; // Process 100 tuples per run
        for (int i = 0; i < data.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, data.size());
            List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> batch = data.subList(i, endIndex);
            
            // Create run file
            FileReference runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                "VCTreeCentroid_" + centroidId + "_run_" + (runs.size()) + "_" + System.currentTimeMillis());
            RunFileWriter writer = new RunFileWriter(runFile, ctx.getIoManager());
            writer.open();
            
            // Write sorted batch to run file
            for (VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple : batch) {
                ByteBuffer frame = createFrameFromTuple(tuple);
                writer.nextFrame(frame);
            }
            
            writer.close();
            runs.add(runFile);
        }
        
        // Store intermediate runs
        intermediateRunFiles.put(centroidId, runs);
        
        System.err.println("Created " + runs.size() + " sorted runs for centroid " + centroidId);
        return runs;
    }
    
    /**
     * Merge sorted runs into final sorted file (similar to ExternalSortRunMerger).
     */
    private FileReference mergeRuns(int centroidId, List<FileReference> runs) throws HyracksDataException {
        if (runs.isEmpty()) {
            throw HyracksDataException.create(new RuntimeException("No runs to merge for centroid " + centroidId));
        }
        
        if (runs.size() == 1) {
            // Only one run, just rename it
            FileReference singleRun = runs.get(0);
            FileReference sortedFile = ctx.getJobletContext().createManagedWorkspaceFile(
                "VCTreeCentroid_" + centroidId + "_sorted_" + System.currentTimeMillis());
            
            // Copy single run to final file
            copyFile(singleRun, sortedFile);
            return sortedFile;
        }
        
        // Multiple runs - merge them
        FileReference sortedFile = ctx.getJobletContext().createManagedWorkspaceFile(
            "VCTreeCentroid_" + centroidId + "_sorted_" + System.currentTimeMillis());
        RunFileWriter writer = new RunFileWriter(sortedFile, ctx.getIoManager());
        writer.open();
        
        // Simple merge: read all runs and merge in memory
        List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> allData = new ArrayList<>();
        
        for (FileReference run : runs) {
            List<VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple> runData = readCentroidData(run);
            allData.addAll(runData);
        }
        
        // Sort all data
        Collections.sort(allData, distanceComparator);
        
        // Write sorted data to final file
        for (VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple : allData) {
            ByteBuffer frame = createFrameFromTuple(tuple);
            writer.nextFrame(frame);
        }
        
        writer.close();
        
        System.err.println("Merged " + runs.size() + " runs into sorted file for centroid " + centroidId);
        return sortedFile;
    }
    
    /**
     * Copy file from source to destination.
     */
    private void copyFile(FileReference source, FileReference destination) throws HyracksDataException {
        RunFileReader reader = new RunFileReader(source, ctx.getIoManager(), source.getFile().length(), false);
        reader.open();
        
        RunFileWriter writer = new RunFileWriter(destination, ctx.getIoManager());
        writer.open();
        
        IFrame frame = new FixedSizeFrame(ByteBuffer.allocate(frameSize));
        while (reader.nextFrame(frame)) {
            writer.nextFrame(frame.getBuffer());
        }
        
        reader.close();
        writer.close();
    }
    
    /**
     * Create frame from tuple.
     */
    private ByteBuffer createFrameFromTuple(VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        
        // Write tuple count (1)
        buffer.putInt(1);
        
        // Write centroid ID
        buffer.putInt(tuple.centroidId);
        
        // Write distance
        buffer.putDouble(tuple.distance);
        
        // Write embedding length and data
        if (tuple.embedding != null) {
            buffer.putInt(tuple.embedding.length);
            for (float f : tuple.embedding) {
                buffer.putDouble((double) f);
            }
        } else {
            buffer.putInt(0);
        }
        
        buffer.flip();
        return buffer;
    }
    
    /**
     * Parse tuple from frame.
     */
    private VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple parseTupleFromFrame(ByteBuffer frame) {
        try {
            VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple tuple = 
                new VCTreeStaticStructureCreatorOperatorDescriptor.DummyTuple();
            
            // Read tuple count
            int tupleCount = frame.getInt();
            if (tupleCount > 0) {
                // Read centroid ID
                tuple.centroidId = frame.getInt();
                
                // Read distance
                tuple.distance = frame.getDouble();
                
                // Read embedding
                int embeddingLength = frame.getInt();
                if (embeddingLength > 0) {
                    tuple.embedding = new float[embeddingLength];
                    for (int i = 0; i < embeddingLength; i++) {
                        tuple.embedding[i] = (float) frame.getDouble();
                    }
                }
                
                tuple.level = 0;
                tuple.clusterId = tuple.centroidId % 3;
                
                return tuple;
            }
        } catch (Exception e) {
            System.err.println("Error parsing tuple from frame: " + e.getMessage());
        }
        
        return null;
    }
    
    /**
     * Get sorted files created during sorting.
     */
    public Map<Integer, FileReference> getSortedFiles() {
        return centroidSortedFiles;
    }
    
    /**
     * Clean up intermediate run files.
     */
    public void cleanupIntermediateFiles() {
        // In a real implementation, you'd delete the intermediate run files
        // For now, just clear the references
        intermediateRunFiles.clear();
    }
}
