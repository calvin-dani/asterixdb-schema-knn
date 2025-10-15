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

package org.apache.hyracks.storage.am.vector;

import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IndexTestContext;
import org.apache.hyracks.storage.common.IIndex;

@SuppressWarnings("rawtypes")
public abstract class AbstractVectorTreeTestContext extends IndexTestContext<CheckTuple> {

    protected final TreeSet<CheckTuple> checkTuples = new TreeSet<CheckTuple>();
    protected final int vectorDimensions;

    public List<Integer> getNumClustersPerLevel() {
        return numClustersPerLevel;
    }

    public void setNumClustersPerLevel(List<Integer> numClustersPerLevel) {
        this.numClustersPerLevel = numClustersPerLevel;
    }
    public List<ITupleReference> getStaticStructureCentroids() {
        return centroids;
    }

    public List<List<Integer>> getNumCentroidsPerLevel() {
        return numCentroidsPerLevel;
    }

    public void setNumCentroidsPerLevel(List<List<Integer>> numCentroidsPerLevel) {
        this.numCentroidsPerLevel = numCentroidsPerLevel;
    }

    public void setStaticStructureCentroids(List<ITupleReference> centroids) {
        this.centroids = centroids;
    }

    protected List<Integer> numClustersPerLevel;
    protected List<List<Integer>> numCentroidsPerLevel;
    protected List<ITupleReference> centroids;

    public AbstractVectorTreeTestContext(ISerializerDeserializer[] fieldSerdes, IIndex index, boolean filtered,
            int vectorDimensions) throws HyracksDataException {
        super(fieldSerdes, index, filtered);
        this.vectorDimensions = vectorDimensions;
    }

    @Override
    public void insertCheckTuple(CheckTuple checkTuple, Collection<CheckTuple> checkTuples) {
        checkTuples.add(checkTuple);
    }

    @Override
    public void deleteCheckTuple(CheckTuple checkTuple, Collection<CheckTuple> checkTuples) {
        checkTuples.remove(checkTuple);
    }

    public void upsertCheckTuple(CheckTuple checkTuple, Collection<CheckTuple> checkTuples) {
        if (checkTuples.contains(checkTuple)) {
            checkTuples.remove(checkTuple);
        }
        checkTuples.add(checkTuple);
    }

    @Override
    public TreeSet<CheckTuple> getCheckTuples() {
        return checkTuples;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }


}
