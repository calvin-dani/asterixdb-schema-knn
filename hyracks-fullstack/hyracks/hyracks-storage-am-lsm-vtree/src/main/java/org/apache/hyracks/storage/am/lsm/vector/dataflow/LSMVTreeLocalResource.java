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

import java.util.List;
import java.util.Map;

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
import org.apache.hyracks.storage.am.common.api.IQuantizedResource;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LsmResource;
import org.apache.hyracks.storage.am.lsm.vector.utils.LSMVTreeUtils;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleCreatorFactory;
import org.apache.hyracks.storage.am.vector.impls.VTreeDataTupleCreatorFactory;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LSMVTreeLocalResource extends LsmResource implements IQuantizedResource {

    private static final long serialVersionUID = 2L;
    private static final Logger LOGGER = LogManager.getLogger();

    protected final int vectorDimensions;
    protected final int[] vectorFields;
    protected final int[] filterFields;
    protected final boolean atomic;
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;
    protected final int numPrimaryKeyFields;
    protected final int numIncludeFields;
    protected final IVTreeDataTupleCreatorFactory dataTupleCreatorFactory;

    // Index name (optional metadata, persisted for resource identification)
    protected final String indexName;

    // Quantization parameters (optional, set by QuantizedIndexCreate during index creation)
    protected Float confidenceInterval;
    protected Float minQuantile;
    protected Float maxQuantile;
    protected Float alpha;
    protected Integer bits;
    protected Integer sampleCount;

    public LSMVTreeLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, int vectorDimensions, int[] vectorFields,
            ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector, boolean atomic,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleCreatorFactory dataTupleCreatorFactory) {
        this(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable, vectorDimensions,
                vectorFields, nullTypeTraits, nullIntrospector, atomic, vectorAccessorFactory, numPrimaryKeyFields,
                numIncludeFields, dataTupleCreatorFactory, null, null, null, null, null, null, null);
    }

    public LSMVTreeLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, int vectorDimensions, int[] vectorFields,
            ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector, boolean atomic,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleCreatorFactory dataTupleCreatorFactory, String indexName, Float confidenceInterval,
            Float minQuantile, Float maxQuantile, Float alpha, Integer bits, Integer sampleCount) {
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
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.numPrimaryKeyFields = numPrimaryKeyFields;
        this.numIncludeFields = numIncludeFields;
        this.dataTupleCreatorFactory = dataTupleCreatorFactory;
    }

    protected LSMVTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int vectorDimensions,
            int[] vectorFields, int[] filterFields, boolean atomic, IVTreeBinaryAccessorFactory vectorAccessorFactory,
            int numPrimaryKeyFields, int numIncludeFields, IVTreeDataTupleCreatorFactory dataTupleCreatorFactory)
            throws HyracksDataException {
        this(registry, json, vectorDimensions, vectorFields, filterFields, atomic, null, null, null, null, null, null,
                null, vectorAccessorFactory, numPrimaryKeyFields, numIncludeFields, dataTupleCreatorFactory);
    }

    protected LSMVTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int vectorDimensions,
            int[] vectorFields, int[] filterFields, boolean atomic, String indexName, Float confidenceInterval,
            Float minQuantile, Float maxQuantile, Float alpha, Integer bits, Integer sampleCount,
            IVTreeBinaryAccessorFactory vectorAccessorFactory, int numPrimaryKeyFields, int numIncludeFields,
            IVTreeDataTupleCreatorFactory dataTupleCreatorFactory) throws HyracksDataException {
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
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.numPrimaryKeyFields = numPrimaryKeyFields;
        this.numIncludeFields = numIncludeFields;
        this.dataTupleCreatorFactory = dataTupleCreatorFactory;
    }

    @Override
    public IIndex createInstance(INCServiceContext ncServiceCtx) throws HyracksDataException {
        IIOManager ioManager = storageManager.getIoManager(ncServiceCtx);
        NCConfig storageConfig = ((NodeControllerService) ncServiceCtx.getControllerService()).getConfiguration();
        FileReference fileRef = ioManager.resolve(path);

        List<IVirtualBufferCache> virtualBufferCaches = vbcProvider.getVirtualBufferCaches(ncServiceCtx, fileRef);
        ioOpCallbackFactory.initialize(ncServiceCtx, this);
        pageWriteCallbackFactory.initialize(ncServiceCtx, this);

        // Create vector accessor factory if not provided (e.g., when loaded from JSON)
        IVTreeBinaryAccessorFactory accessorFactory = vectorAccessorFactory;
        if (accessorFactory == null) {
            // Use reflection to create AOrderedListVectorBinaryAccessorFactory to avoid compile-time dependency
            try {
                Class<?> factoryClass = Class
                        .forName("org.apache.asterix.dataflow.data.common.AOrderedListVectorBinaryAccessorFactory");
                accessorFactory = (IVTreeBinaryAccessorFactory) factoryClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new HyracksDataException("Failed to create vector accessor factory", e);
            }
        }

        // Pack quantization params into float[] for lazy quantizer creation at query time
        float[] quantizationParams = null;
        if (hasQuantizationParams()) {
            quantizationParams = new float[] { minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount };
        }

        return LSMVTreeUtils.createLSMTree(storageConfig, ioManager, virtualBufferCaches, fileRef,
                storageManager.getBufferCache(ncServiceCtx), typeTraits, cmpFactories, 0.01, // bloomFilterFalsePositiveRate
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, ncServiceCtx),
                opTrackerProvider.getOperationTracker(ncServiceCtx, this),
                ioSchedulerProvider.getIoScheduler(ncServiceCtx), ioOpCallbackFactory, pageWriteCallbackFactory, false, // needKeyDupCheck
                vectorDimensions, vectorFields, filterFields, null, // filterFrameFactory
                null, // filterManager
                null, // filterHelper
                durable, metadataPageManagerFactory, atomic, null, accessorFactory, numPrimaryKeyFields,
                numIncludeFields, dataTupleCreatorFactory, quantizationParams, "euclidean"); // TODO: Store distanceMetric in LSMVTreeLocalResource and propagate from Index metadata
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
        json.put("numPrimaryKeyFields", numPrimaryKeyFields);
        json.put("numIncludeFields", numIncludeFields);
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        //        int[] vectorFields = OBJECT_MAPPER.convertValue(json.get("vectorFields"), int[].class);
        //        int[] filterFields = OBJECT_MAPPER.convertValue(json.get("filterFields"), int[].class);
        //        boolean atomic = json.get("atomic").asBoolean();

        //TODO CALVIN DANI : MAKE DYNAMIC
        int vectorDimensions = json.has("vectorDimensions") ? json.get("vectorDimensions").asInt() : -1;
        int numPrimaryKeyFields = json.has("numPrimaryKeyFields") ? json.get("numPrimaryKeyFields").asInt() : 1;
        int numIncludeFields = json.has("numIncludeFields") ? json.get("numIncludeFields").asInt() : 0;
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
        // Determine quantized vs non-quantized based on presence of quantization parameters
        boolean isQuantized = (minQuantile != null);
        IVTreeDataTupleCreatorFactory dataTupleCreatorFactory =
                new VTreeDataTupleCreatorFactory(numIncludeFields, isQuantized);

        return new LSMVTreeLocalResource(registry, json, vectorDimensions, vectorFields, filterFields, atomic,
                indexName, confidenceInterval, minQuantile, maxQuantile, alpha, bits, sampleCount, null,
                numPrimaryKeyFields, numIncludeFields, dataTupleCreatorFactory);
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

    @Override
    public void setQuantizationParameters(Map<String, Object> parameters) {
        if (parameters == null) {
            return;
        }
        if (parameters.containsKey("minQuantile")) {
            this.minQuantile = (Float) parameters.get("minQuantile");
        }
        if (parameters.containsKey("maxQuantile")) {
            this.maxQuantile = (Float) parameters.get("maxQuantile");
        }
        if (parameters.containsKey("alpha")) {
            this.alpha = (Float) parameters.get("alpha");
        }
        if (parameters.containsKey("bits")) {
            this.bits = (Integer) parameters.get("bits");
        }
        if (parameters.containsKey("confidenceInterval")) {
            this.confidenceInterval = (Float) parameters.get("confidenceInterval");
        }
        if (parameters.containsKey("sampleCount")) {
            this.sampleCount = (Integer) parameters.get("sampleCount");
        }
    }
}
