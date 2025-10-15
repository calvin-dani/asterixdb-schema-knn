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
package org.apache.hyracks.storage.am.vector.impls;

import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

/**
 * Initial state for vector clustering tree cursors.
 * Contains information needed to position the cursor at the appropriate location.
 */
public class VectorCursorInitialState implements ICursorInitialState {

    private long metadataPageId;
    private long targetDataPageId;
    private int rootPageId;
    private double[] queryVector;
    private double[] clusterCentroid;
    private double distanceToCentroid;
    private ICachedPage page;
    private ISearchOperationCallback searchCallback;
    private MultiComparator originalKeyCmp;
    private final IIndexAccessor accessor;

    public VectorCursorInitialState(IIndexAccessor accessor) {
        this.accessor = accessor;
        this.targetDataPageId = -1;
        this.metadataPageId = -1;
    }

    public VectorCursorInitialState(long metadataPageId, double[] queryVector, IIndexAccessor accessor) {
        this.metadataPageId = metadataPageId;
        this.targetDataPageId = -1;
        this.queryVector = queryVector != null ? queryVector.clone() : null;
        this.accessor = accessor;
    }

    public IIndexAccessor getIndexAccessor() {
        return accessor;
    }

    public void setMetadataPageId(long metadataPageId) {
        this.metadataPageId = metadataPageId;
    }

    public long getMetadataPageId() {
        return metadataPageId;
    }

    public void setTargetDataPageId(long targetDataPageId) {
        this.targetDataPageId = targetDataPageId;
    }

    public long getTargetDataPageId() {
        return targetDataPageId;
    }

    public void setQueryVector(double[] queryVector) {
        this.queryVector = queryVector;
    }

    public double[] getQueryVector() {
        return queryVector;
    }

    public void setClusterCentroid(double[] clusterCentroid) {
        this.clusterCentroid = clusterCentroid;
    }

    public double[] getClusterCentroid() {
        return clusterCentroid;
    }

    public double getDistanceToCentroid() {
        return distanceToCentroid;
    }

    public void setDistanceToCentroid(double distanceToCentroid) {
        this.distanceToCentroid = distanceToCentroid;
    }

    public void setRootPageId(int rootPageId) {
        this.rootPageId = rootPageId;
    }

    public int getRootPageId() {
        return rootPageId;
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        this.searchCallback = searchCallback;
    }

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return originalKeyCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.originalKeyCmp = originalCmp;
    }

    @Override
    public String toString() {
        return "VectorCursorInitialState[metadataPageId=" + metadataPageId + ", targetDataPageId=" + targetDataPageId
                + "]";
    }

    public IIndexAccessor getAccessor() {
        return accessor;
    }
}
