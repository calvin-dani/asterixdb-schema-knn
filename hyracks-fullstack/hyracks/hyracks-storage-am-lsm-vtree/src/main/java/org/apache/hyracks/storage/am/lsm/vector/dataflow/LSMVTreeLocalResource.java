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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LSMVTreeLocalResource extends LsmResource implements IQuantizedResource {

    private static final long serialVersionUID = 2L;

    // Persisted/wire key names for quantization parameters. Shared with QuantizedIndexCreate so
    // the producer and the persisted resource agree on the vocabulary.
    public static final String KEY_MIN_QUANTILE = "minQuantile";
    public static final String KEY_MAX_QUANTILE = "maxQuantile";
    public static final String KEY_ALPHA = "alpha";
    public static final String KEY_BITS = "bits";
    public static final String KEY_CONFIDENCE_INTERVAL = "confidenceInterval";
    public static final String KEY_SAMPLE_COUNT = "sampleCount";

    private static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;
    /** TODO: persist the distance metric on the resource so different indexes can use different metrics. */
    private static final String DEFAULT_DISTANCE_METRIC = "euclidean";
    /**
     * Fully-qualified class name of the AsterixDB binary accessor factory. Loaded reflectively in
     * {@link #createInstance(INCServiceContext)} when the resource is reconstituted from JSON,
     * since at that point the asterixdb-om classpath dependency may not be available at compile time.
     */
    private static final String ACCESSOR_FACTORY_CLASSNAME =
            "org.apache.asterix.dataflow.data.common.AOrderedListVectorBinaryAccessorFactory";

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
            try {
                Class<?> factoryClass = Class.forName(ACCESSOR_FACTORY_CLASSNAME);
                accessorFactory = (IVTreeBinaryAccessorFactory) factoryClass.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new HyracksDataException(
                        "Failed to create vector accessor factory: " + ACCESSOR_FACTORY_CLASSNAME, e);
            }
        }

        // Pack quantization params into float[] for lazy quantizer creation at query time
        float[] quantizationParams = null;
        if (hasQuantizationParams()) {
            quantizationParams = new float[] { minQuantile, maxQuantile, alpha, confidenceInterval, bits, sampleCount };
        }

        return LSMVTreeUtils.createLSMTree(storageConfig, ioManager, virtualBufferCaches, fileRef,
                storageManager.getBufferCache(ncServiceCtx), typeTraits, cmpFactories, BLOOM_FILTER_FALSE_POSITIVE_RATE,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, ncServiceCtx),
                opTrackerProvider.getOperationTracker(ncServiceCtx, this),
                ioSchedulerProvider.getIoScheduler(ncServiceCtx), ioOpCallbackFactory, pageWriteCallbackFactory, false, // needKeyDupCheck
                vectorDimensions, vectorFields, filterFields, null, // filterFrameFactory
                null, // filterManager
                null, // filterHelper
                durable, metadataPageManagerFactory, atomic, null, accessorFactory, numPrimaryKeyFields,
                numIncludeFields, dataTupleCreatorFactory, quantizationParams, DEFAULT_DISTANCE_METRIC);
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
        // Write quantization parameters only when set (a non-quantized index has none).
        putIfNotNull(json, KEY_CONFIDENCE_INTERVAL, confidenceInterval);
        putIfNotNull(json, KEY_MIN_QUANTILE, minQuantile);
        putIfNotNull(json, KEY_MAX_QUANTILE, maxQuantile);
        putIfNotNull(json, KEY_ALPHA, alpha);
        putIfNotNull(json, KEY_BITS, bits);
        putIfNotNull(json, KEY_SAMPLE_COUNT, sampleCount);
        json.put("numPrimaryKeyFields", numPrimaryKeyFields);
        json.put("numIncludeFields", numIncludeFields);
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        int vectorDimensions = json.has("vectorDimensions") ? json.get("vectorDimensions").asInt() : -1;
        int numPrimaryKeyFields = json.has("numPrimaryKeyFields") ? json.get("numPrimaryKeyFields").asInt() : 1;
        int numIncludeFields = json.has("numIncludeFields") ? json.get("numIncludeFields").asInt() : 0;
        int[] vectorFields =
                json.has("vectorFields") ? OBJECT_MAPPER.convertValue(json.get("vectorFields"), int[].class) : null;
        int[] filterFields =
                json.has("filterFields") ? OBJECT_MAPPER.convertValue(json.get("filterFields"), int[].class) : null;
        boolean atomic = json.has("atomic") ? json.get("atomic").asBoolean() : false;
        String indexName = json.has("indexName") ? json.get("indexName").asText() : null;

        // Read quantization parameters with backward compatibility (missing → null).
        Float confidenceInterval = readOptionalFloat(json, KEY_CONFIDENCE_INTERVAL);
        Float minQuantile = readOptionalFloat(json, KEY_MIN_QUANTILE);
        Float maxQuantile = readOptionalFloat(json, KEY_MAX_QUANTILE);
        Float alpha = readOptionalFloat(json, KEY_ALPHA);
        Integer bits = readOptionalInt(json, KEY_BITS);
        Integer sampleCount = readOptionalInt(json, KEY_SAMPLE_COUNT);
        // Determine quantized vs non-quantized based on presence of quantization parameters
        boolean isQuantized = (minQuantile != null);
        IVTreeDataTupleCreatorFactory dataTupleCreatorFactory =
                new VTreeDataTupleCreatorFactory(numIncludeFields, isQuantized);

        return new LSMVTreeLocalResource(registry, json, vectorDimensions, vectorFields, filterFields, atomic,
                indexName, confidenceInterval, minQuantile, maxQuantile, alpha, bits, sampleCount, null,
                numPrimaryKeyFields, numIncludeFields, dataTupleCreatorFactory);
    }

    /** Read an optional float field from JSON; returns {@code null} if the field is missing or null. */
    private static Float readOptionalFloat(JsonNode json, String fieldName) {
        if (!json.has(fieldName)) {
            return null;
        }
        JsonNode node = json.get(fieldName);
        return node.isNull() ? null : (float) node.asDouble();
    }

    /** Read an optional int field from JSON; returns {@code null} if the field is missing or null. */
    private static Integer readOptionalInt(JsonNode json, String fieldName) {
        if (!json.has(fieldName)) {
            return null;
        }
        JsonNode node = json.get(fieldName);
        return node.isNull() ? null : node.asInt();
    }

    private static void putIfNotNull(ObjectNode json, String fieldName, Float value) {
        if (value != null) {
            json.put(fieldName, value);
        }
    }

    private static void putIfNotNull(ObjectNode json, String fieldName, Integer value) {
        if (value != null) {
            json.put(fieldName, value);
        }
    }

    public Float getConfidenceInterval() {
        return confidenceInterval;
    }

    public Float getMinQuantile() {
        return minQuantile;
    }

    public Float getMaxQuantile() {
        return maxQuantile;
    }

    public Float getAlpha() {
        return alpha;
    }

    public Integer getBits() {
        return bits;
    }

    public Integer getSampleCount() {
        return sampleCount;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    public int[] getVectorFields() {
        return vectorFields;
    }

    public int[] getFilterFields() {
        return filterFields;
    }

    public boolean isAtomic() {
        return atomic;
    }

    public String getIndexName() {
        return indexName;
    }

    /** @return true iff all required quantization parameters are present. */
    public boolean hasQuantizationParams() {
        return bits != null && confidenceInterval != null && minQuantile != null && maxQuantile != null
                && alpha != null;
    }

    @Override
    public void setQuantizationParameters(Map<String, Object> parameters) {
        if (parameters == null) {
            return;
        }
        if (parameters.containsKey(KEY_MIN_QUANTILE)) {
            this.minQuantile = (Float) parameters.get(KEY_MIN_QUANTILE);
        }
        if (parameters.containsKey(KEY_MAX_QUANTILE)) {
            this.maxQuantile = (Float) parameters.get(KEY_MAX_QUANTILE);
        }
        if (parameters.containsKey(KEY_ALPHA)) {
            this.alpha = (Float) parameters.get(KEY_ALPHA);
        }
        if (parameters.containsKey(KEY_BITS)) {
            this.bits = (Integer) parameters.get(KEY_BITS);
        }
        if (parameters.containsKey(KEY_CONFIDENCE_INTERVAL)) {
            this.confidenceInterval = (Float) parameters.get(KEY_CONFIDENCE_INTERVAL);
        }
        if (parameters.containsKey(KEY_SAMPLE_COUNT)) {
            this.sampleCount = (Integer) parameters.get(KEY_SAMPLE_COUNT);
        }
    }
}
