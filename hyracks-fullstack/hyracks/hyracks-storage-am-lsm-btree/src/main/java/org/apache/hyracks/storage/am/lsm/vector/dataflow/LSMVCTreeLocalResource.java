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

    private static final long serialVersionUID = 1L;

    protected final int vectorDimensions;
    protected final int[] vectorFields;
    protected final int[] filterFields;
    protected final boolean atomic;

    // Quantization parameters (optional, computed during index building)
    protected final Float confidenceInterval;
    protected final Float minQuantile;
    protected final Float maxQuantile;
    protected final Float alpha;
    protected final Integer bits;
    protected final Integer sampleCount;

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
                vectorFields, nullTypeTraits, nullIntrospector, atomic, null, null, null, null, null, null);
    }

    public LSMVCTreeLocalResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable, int vectorDimensions, int[] vectorFields,
            ITypeTraits nullTypeTraits, INullIntrospector nullIntrospector, boolean atomic, Float confidenceInterval,
            Float minQuantile, Float maxQuantile, Float alpha, Integer bits, Integer sampleCount) {
        super(path, storageManager, typeTraits, cmpFactories, filterTypeTraits, filterCmpFactories, filterFields,
                opTrackerProvider, ioOpCallbackFactory, pageWriteCallbackFactory, metadataPageManagerFactory,
                vbcProvider, ioSchedulerProvider, mergePolicyFactory, mergePolicyProperties, durable, nullTypeTraits,
                nullIntrospector);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.filterFields = filterFields;
        this.atomic = atomic;
        this.confidenceInterval = confidenceInterval;
        this.minQuantile = minQuantile;
        this.maxQuantile = maxQuantile;
        this.alpha = alpha;
        this.bits = bits;
        this.sampleCount = sampleCount;
    }

    protected LSMVCTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int vectorDimensions,
            int[] vectorFields, int[] filterFields, boolean atomic) throws HyracksDataException {
        this(registry, json, vectorDimensions, vectorFields, filterFields, atomic, null, null, null, null, null, null);
    }

    protected LSMVCTreeLocalResource(IPersistedResourceRegistry registry, JsonNode json, int vectorDimensions,
            int[] vectorFields, int[] filterFields, boolean atomic, Float confidenceInterval, Float minQuantile,
            Float maxQuantile, Float alpha, Integer bits, Integer sampleCount) throws HyracksDataException {
        super(registry, json);
        this.vectorDimensions = vectorDimensions;
        this.vectorFields = vectorFields;
        this.filterFields = filterFields;
        this.atomic = atomic;
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

        // Read quantization parameters with backward compatibility (default to null if not present)
        Float confidenceInterval = getOrDefaultFloat(json, "confidenceInterval", null);
        Float minQuantile = getOrDefaultFloat(json, "minQuantile", null);
        Float maxQuantile = getOrDefaultFloat(json, "maxQuantile", null);
        Float alpha = getOrDefaultFloat(json, "alpha", null);
        Integer bits = getOrDefaultInt(json, "bits", null);
        Integer sampleCount = getOrDefaultInt(json, "sampleCount", null);

        return new LSMVCTreeLocalResource(registry, json, vectorDimensions, vectorFields, filterFields, atomic,
                confidenceInterval, minQuantile, maxQuantile, alpha, bits, sampleCount);
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
}
