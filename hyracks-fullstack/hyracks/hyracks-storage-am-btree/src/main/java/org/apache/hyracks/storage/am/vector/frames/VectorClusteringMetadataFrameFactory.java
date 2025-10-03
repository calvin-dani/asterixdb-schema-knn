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

package org.apache.hyracks.storage.am.vector.frames;

import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.api.IVectorClusteringMetadataFrame;

/**
 * Factory for creating vector clustering metadata frames.
 */
public class VectorClusteringMetadataFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private final ITreeIndexTupleWriter tupleWriter;
    private final int centroidDimensions;

    public VectorClusteringMetadataFrameFactory(ITreeIndexTupleWriter tupleWriter, int centroidDimensions) {
        this.tupleWriter = tupleWriter;
        this.centroidDimensions = centroidDimensions;
    }

    @Override
    public IVectorClusteringMetadataFrame createFrame() {
        return new VectorClusteringMetadataFrame(tupleWriter, centroidDimensions);
    }

    @Override
    public ITreeIndexTupleWriterFactory getTupleWriterFactory() {
        return null;
    }

    public ITreeIndexTupleWriter getTupleWriter() {
        return tupleWriter;
    }

    public int getCentroidDimensions() {
        return centroidDimensions;
    }
}
