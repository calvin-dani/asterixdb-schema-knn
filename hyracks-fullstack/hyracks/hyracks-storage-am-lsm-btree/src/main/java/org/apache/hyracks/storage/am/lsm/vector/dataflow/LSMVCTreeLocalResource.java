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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResource;
import org.apache.hyracks.storage.am.lsm.vector.utils.LSMVCTreeUtils;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IStorageManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LSMVCTreeLocalResource extends LsmResource {

    private static final long serialVersionUID = 2L;
    private static final Logger LOGGER = Logger.getLogger(LSMVCTreeLocalResource.class.getName());

    /** Prefix for quantization sidecar files: .quantization_<indexName> */
    public static final String QUANTIZATION_FILE_PREFIX = ".quantization_";

    protected final int vectorDimensions;
    protected final int[] vectorFields;
    protected final int[] filterFields;
    protected final boolean atomic;

    // Index name for locating sidecar file (optional, only set during index creation)
    protected final String indexName;

    // Quantization parameters (optional, computed during index building)
    // These are non-final so they can be populated from sidecar file in createInstance()
    protected Float confidenceInterval;
    protected Float minQuantile;
    protected Float maxQuantile;
    protected Float alpha;
    protected Integer bits;
    protected Integer sampleCount;

    public LSMVCTreeLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, int vectorDimensions, int[] vectorFields,
            ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector, boolean atomic) {
        this(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable, vectorDimensions,
                vectorFields, nullTypeTraits, nullIntrospector, atomic, null, null, null, null, null, null, null);
    }

    public LSMVCTreeLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, int vectorDimensions, int[] vectorFields,
            ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector, boolean atomic, String indexName,
            Float confidenceInterval, Float minQuantile, Float maxQuantile, Float alpha, Integer bits,
            Integer sampleCount) {
        super(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable, nullTypeTraits,
                nullIntrospector);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.filterFields = filterFields;
        this.atomic = atomic;
        this.indexName = indexName;
        this.confidenceInterval = confidenceInterval;
        this.minQuantile = minQuantile;
        this.maxQuantile = maxQuantile;
        this.alpha = alpha;
        this.bits = bits;
        this.sampleCount = sampleCount;
    }

    protected LSMVCTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int vectorDimensions,
            int[] vectorFields, int[] filterFields, boolean atomic) throws HyracksDataException {
        this(registry, json, vectorDimensions, vectorFields, filterFields, atomic, null, null, null, null, null, null,
                null);
    }

    protected LSMVCTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int vectorDimensions,
            int[] vectorFields, int[] filterFields, boolean atomic, String indexName, Float confidenceInterval,
            Float minQuantile, Float maxQuantile, Float alpha, Integer bits, Integer sampleCount)
            throws HyracksDataException {
        super(registry, json);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.filterFields = filterFields;
        this.atomic = atomic;
        this.indexName = indexName;
        this.confidenceInterval = confidenceInterval;
        this.minQuantile = minQuantile;
        this.maxQuantile = maxQuantile;
        this.alpha = alpha;
        this.bits = bits;
        this.sampleCount = sampleCount;
    }

    @Override
    public IIndex createInstance(INCServiceContext ncServiceCtx) throws HyracksDataException {
        IIOManager ioManager = storageManager.getIoManager(ncServiceCtx);
        NCConfig storageConfig = ((NodeControllerService) ncServiceCtx.getControllerService()).getConfiguration();
        FileReference fileRef = ioManager.resolve(path);

        // Try to read quantization constants from sidecar file if not already set
        if (minQuantile == null && indexName != null) {
            tryReadQuantizationSidecarFile(ioManager, fileRef);
        }

        List<IVirtualBufferCache> virtualBufferCaches = vbcProvider.getVirtualBufferCaches(ncServiceCtx, fileRef);
        ioOpCallbackFactory.initialize(ncServiceCtx, this);
        pageWriteCallbackFactory.initialize(ncServiceCtx, this);
        return LSMVCTreeUtils.createLSMTree(storageConfig, ioManager, virtualBufferCaches, fileRef,
                storageManager.getBufferCache(ncServiceCtx), typeTraits, cmpFactories, 0.01, // bloomFilterFalsePositiveRate
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, ncServiceCtx),
                opTrackerProvider.getOperationTracker(ncServiceCtx, this),
                ioSchedulerProvider.getIoScheduler(ncServiceCtx), ioOpCallbackFactory, pageWriteCallbackFactory, false, // needKeyDupCheck
                vectorDimensions, vectorFields, filterFields, null, // filterFrameFactory
                null, // filterManager
                null, // filterHelper
                durable, metadataPageManagerFactory, atomic, null);
    }

    /**
     * Tries to read quantization constants from a sidecar file.
     * The sidecar file is located in the dataset directory (parent of index directory).
     * File format: .quantization_<indexName> containing 24 bytes of binary data.
     * 
     * @param ioManager The IO manager
     * @param indexFileRef The index file reference (used to derive dataset directory)
     */
    private void tryReadQuantizationSidecarFile(IIOManager ioManager, FileReference indexFileRef) {
        try {
            // Get dataset directory (parent of rebalance directory)
            // Index path: storage/partition_X/<namespace>/<datasetName>/<rebalanceCount>/<indexName>
            // Dataset path: storage/partition_X/<namespace>/<datasetName>
            FileReference rebalanceDir = indexFileRef.getParent(); // .../datasetName/rebalanceCount
            FileReference datasetDir = rebalanceDir.getParent(); // .../datasetName

            // Construct sidecar file path: .quantization_<indexName>
            String sidecarFileName = QUANTIZATION_FILE_PREFIX + indexName;
            FileReference sidecarFile = datasetDir.getChild(sidecarFileName);

            LOGGER.info("[LSMVCTreeLocalResource] Looking for sidecar file: " + sidecarFile.getAbsolutePath());

            if (!ioManager.exists(sidecarFile)) {
                LOGGER.info(
                        "[LSMVCTreeLocalResource] Sidecar file not found, index will be created without quantization");
                return;
            }

            // Read the sidecar file using IIOManager.readAllBytes()
            byte[] data = ioManager.readAllBytes(sidecarFile);
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            this.minQuantile = dis.readFloat();
            this.maxQuantile = dis.readFloat();
            this.alpha = dis.readFloat();
            this.bits = dis.readInt();
            this.confidenceInterval = dis.readFloat();
            this.sampleCount = dis.readInt();

            LOGGER.info("[LSMVCTreeLocalResource] Read quantization constants from sidecar file: " + "minQ="
                    + minQuantile + ", maxQ=" + maxQuantile + ", alpha=" + alpha + ", bits=" + bits + ", sampleCount="
                    + sampleCount);

            // Delete the sidecar file after reading (cleanup)
            try {
                ioManager.delete(sidecarFile);
                LOGGER.info("[LSMVCTreeLocalResource] Deleted sidecar file: " + sidecarFile.getAbsolutePath());
            } catch (Exception e) {
                LOGGER.warning("[LSMVCTreeLocalResource] Failed to delete sidecar file: " + e.getMessage());
            }

        } catch (IOException e) {
            LOGGER.warning("[LSMVCTreeLocalResource] Error reading sidecar file: " + e.getMessage());
            // Continue without quantization - not a fatal error
        }
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode jsonObject = registry.getClassIdentifier(getClass(), serialVersionUID);
        appendToJson(jsonObject, registry); // Call this.appendToJson() to include quantization params
        return jsonObject;
    }

    @Override
    protected void appendToJson(final ObjectNode json, IPersistedResourceRegistry registry)
            throws HyracksDataException {
        super.appendToJson(json, registry);
        json.put("vectorDimensions", vectorDimensions);
        json.putPOJO("vectorFields", vectorFields);
        json.putPOJO("filterFields", filterFields);
        json.put("atomic", atomic);
        // Write quantization parameters only if they are not null
        if (confidenceInterval != null) {
            json.put("confidenceInterval", confidenceInterval);
        }
        if (minQuantile != null) {
            json.put("minQuantile", minQuantile);
        }
        if (maxQuantile != null) {
            json.put("maxQuantile", maxQuantile);
        }
        if (alpha != null) {
            json.put("alpha", alpha);
        }
        if (bits != null) {
            json.put("bits", bits);
        }
        if (sampleCount != null) {
            json.put("sampleCount", sampleCount);
        }
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        //        int vectorDimensions = json.get("vectorDimensions").asInt();
        //        int[] vectorFields = OBJECT_MAPPER.convertValue(json.get("vectorFields"), int[].class);
        //        int[] filterFields = OBJECT_MAPPER.convertValue(json.get("filterFields"), int[].class);
        //        boolean atomic = json.get("atomic").asBoolean();

        //TODO CALVIN DANI : MAKE DYNAMIC
        int vectorDimensions = json.has("vectorDimensions") ? json.get("vectorDimensions").asInt() : 784;
        int[] vectorFields =
                json.has("vectorFields") ? OBJECT_MAPPER.convertValue(json.get("vectorFields"), int[].class) : null;
        int[] filterFields =
                json.has("filterFields") ? OBJECT_MAPPER.convertValue(json.get("filterFields"), int[].class) : null;
        boolean atomic = json.has("atomic") ? json.get("atomic").asBoolean() : false;

        // indexName is only used during index creation, not needed after persistence
        String indexName = json.has("indexName") ? json.get("indexName").asText() : null;

        // Read quantization parameters with backward compatibility (default to null if not present)
        Float confidenceInterval = getOrDefaultFloat(json, "confidenceInterval", null);
        Float minQuantile = getOrDefaultFloat(json, "minQuantile", null);
        Float maxQuantile = getOrDefaultFloat(json, "maxQuantile", null);
        Float alpha = getOrDefaultFloat(json, "alpha", null);
        Integer bits = getOrDefaultInt(json, "bits", null);
        Integer sampleCount = getOrDefaultInt(json, "sampleCount", null);

        return new LSMVCTreeLocalResource(registry, json, vectorDimensions, vectorFields, filterFields, atomic,
                indexName, confidenceInterval, minQuantile, maxQuantile, alpha, bits, sampleCount);
    }

    /**
     * Helper method to read optional float fields from JSON with backward compatibility.
     * Returns null if field is missing (instead of throwing exception).
     */
    protected static Float getOrDefaultFloat(JsonNode jsonNode, String fieldName, Float defaultValue) {
        if (!jsonNode.has(fieldName)) {
            return defaultValue;
        }
        JsonNode node = jsonNode.get(fieldName);
        if (node.isNull()) {
            return defaultValue;
        }
        return (float) node.asDouble();
    }

    /**
     * Helper method to read optional int fields from JSON with backward compatibility.
     * Returns null if field is missing (instead of throwing exception).
     */
    protected static Integer getOrDefaultInt(JsonNode jsonNode, String fieldName, Integer defaultValue) {
        if (!jsonNode.has(fieldName)) {
            return defaultValue;
        }
        JsonNode node = jsonNode.get(fieldName);
        if (node.isNull()) {
            return defaultValue;
        }
        return node.asInt();
    }

    // ==================== Public Getter Methods for Quantization Parameters ====================

    /**
     * Gets the confidence interval used for quantization.
     * @return The confidence interval, or null if not set
     */
    public Float getConfidenceInterval() {
        return confidenceInterval;
    }

    /**
     * Gets the minimum quantile value used for quantization.
     * @return The minimum quantile, or null if not set
     */
    public Float getMinQuantile() {
        return minQuantile;
    }

    /**
     * Gets the maximum quantile value used for quantization.
     * @return The maximum quantile, or null if not set
     */
    public Float getMaxQuantile() {
        return maxQuantile;
    }

    /**
     * Gets the alpha value used for quantization scaling.
     * @return The alpha value, or null if not set
     */
    public Float getAlpha() {
        return alpha;
    }

    /**
     * Gets the number of bits used for quantization.
     * @return The bits value, or null if not set
     */
    public Integer getBits() {
        return bits;
    }

    /**
     * Gets the sample count used to compute quantization parameters.
     * @return The sample count, or null if not set
     */
    public Integer getSampleCount() {
        return sampleCount;
    }

    /**
     * Gets the vector dimensions for this index.
     * @return The number of dimensions in the vectors
     */
    public int getVectorDimensions() {
        return vectorDimensions;
    }

    /**
     * Gets the vector field indices.
     * @return Array of field indices that contain vector data
     */
    public int[] getVectorFields() {
        return vectorFields;
    }

    /**
     * Gets the filter field indices.
     * @return Array of field indices used for filtering
     */
    public int[] getFilterFields() {
        return filterFields;
    }

    /**
     * Checks if the index is atomic.
     * @return true if the index is atomic, false otherwise
     */
    public boolean isAtomic() {
        return atomic;
    }

    /**
     * Gets the index name.
     * @return The index name, or null if not set
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Checks if quantization parameters are available.
     * @return true if all required quantization parameters are set
     */
    public boolean hasQuantizationParams() {
        return bits != null && confidenceInterval != null && minQuantile != null && maxQuantile != null
                && alpha != null;
    }
}
