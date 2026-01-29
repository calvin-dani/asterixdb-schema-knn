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
package org.apache.asterix.transaction.management.resource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.LSMVCTreeLocalResource;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Wrapper class for ILocalResourceRepository that adds an update() method
 * to safely update existing resources without deleting checkpoints.
 */
public class UpdatableLocalResourceRepositoryWrapper implements ILocalResourceRepository {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String METADATA_FILE_MASK_NAME =
            StorageConstants.MASK_FILE_PREFIX + StorageConstants.METADATA_FILE_NAME;

    private final ILocalResourceRepository wrappedRepository;
    private final boolean isPersistentRepository;

    public UpdatableLocalResourceRepositoryWrapper(ILocalResourceRepository repository) {
        this.wrappedRepository = repository;
        this.isPersistentRepository = repository instanceof PersistentLocalResourceRepository;
    }

    /**
     * Updates an existing LocalResource by overwriting its metadata file.
     * This method preserves existing checkpoints and only updates the metadata.
     * 
     * @param resource The updated LocalResource to persist
     * @throws HyracksDataException if the update fails
     */
    public void update(LocalResource resource) throws HyracksDataException {
        LOGGER.info("UPDATE: update() method called for resource path: {}", resource.getPath());
        System.err.println("UPDATE: update() method called for resource path: " + resource.getPath());

        if (!isPersistentRepository) {
            LOGGER.info("UPDATE: Using transient repository, delete+insert pattern");
            // For TransientLocalResourceRepository, use delete+insert pattern
            // This is safe because it's in-memory only
            try {
                wrappedRepository.delete(resource.getPath());
            } catch (HyracksDataException e) {
                // Resource might not exist, which is okay
                LOGGER.debug("Resource {} does not exist for update, will insert", resource.getPath());
            }
            wrappedRepository.insert(resource);
            return;
        }

        // For PersistentLocalResourceRepository, use safe update that preserves checkpoints
        LOGGER.info("UPDATE: Using persistent repository, calling updatePersistentResource");
        System.err.println("UPDATE: Using persistent repository, calling updatePersistentResource");
        PersistentLocalResourceRepository persistentRepo = (PersistentLocalResourceRepository) wrappedRepository;
        updatePersistentResource(persistentRepo, resource);
        LOGGER.info("UPDATE: updatePersistentResource completed");
        System.err.println("UPDATE: updatePersistentResource completed");
    }

    private void updatePersistentResource(PersistentLocalResourceRepository repo, LocalResource resource)
            throws HyracksDataException {
        try {
            // Get required fields via reflection
            IIOManager ioManager = getFieldValue(repo, "ioManager", IIOManager.class);
            IPersistedResourceRegistry persistedResourceRegistry =
                    getFieldValue(repo, "persistedResourceRegistry", IPersistedResourceRegistry.class);

            // Get file reference
            String relativePath = getFileName(resource.getPath());
            FileReference resourceFile = ioManager.resolve(relativePath);

            // Check if resource exists
            if (!resourceFile.getFile().exists()) {
                throw new HyracksDataException(
                        "Cannot update resource: " + resourceFile.getAbsolutePath() + " does not exist");
            }

            // Ensure parent directory exists
            final FileReference parent = resourceFile.getParent();
            if (!ioManager.exists(parent) && !ioManager.makeDirectories(parent)) {
                throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.CANNOT_CREATE_FILE,
                        parent.getAbsolutePath());
            }

            // Use file mask pattern for atomic write (same as insert method)
            try {
                createResourceFileMask(repo, ioManager, resourceFile);

                // DEBUG: Log what we're writing
                LOGGER.info("UPDATE: Writing resource to file: {}", resourceFile.getAbsolutePath());
                System.err.println("UPDATE: Writing resource to file: " + resourceFile.getAbsolutePath());
                LOGGER.info("UPDATE: Resource path: {}", resource.getPath());
                System.err.println("UPDATE: Resource path: " + resource.getPath());
                LOGGER.info("UPDATE: Resource ID: {}, Version: {}", resource.getId(), resource.getVersion());
                System.err.println("UPDATE: Resource ID: " + resource.getId() + ", Version: " + resource.getVersion());

                // DEBUG: Check quantization params in resource object BEFORE serialization
                // Also check if they're actually final fields or if there's a serialization issue
                try {
                    IResource innerResource = resource.getResource();
                    if (innerResource instanceof DatasetLocalResource) {
                        DatasetLocalResource dsWrapper = (DatasetLocalResource) innerResource;
                        IResource wrapped = dsWrapper.getResource();
                        if (wrapped instanceof LSMVCTreeLocalResource) {
                            LSMVCTreeLocalResource vcRes = (LSMVCTreeLocalResource) wrapped;

                            // Use reflection to check field values
                            java.lang.reflect.Field bitsField = vcRes.getClass().getDeclaredField("bits");
                            bitsField.setAccessible(true);
                            Integer bits = (Integer) bitsField.get(vcRes);

                            java.lang.reflect.Field alphaField = vcRes.getClass().getDeclaredField("alpha");
                            alphaField.setAccessible(true);
                            Float alpha = (Float) alphaField.get(vcRes);

                            java.lang.reflect.Field minQField = vcRes.getClass().getDeclaredField("minQuantile");
                            minQField.setAccessible(true);
                            Float minQ = (Float) minQField.get(vcRes);

                            java.lang.reflect.Field maxQField = vcRes.getClass().getDeclaredField("maxQuantile");
                            maxQField.setAccessible(true);
                            Float maxQ = (Float) maxQField.get(vcRes);

                            java.lang.reflect.Field confField = vcRes.getClass().getDeclaredField("confidenceInterval");
                            confField.setAccessible(true);
                            Float conf = (Float) confField.get(vcRes);

                            LOGGER.info(
                                    "UPDATE: BEFORE toJson() - Resource object has bits: {}, alpha: {}, minQ: {}, maxQ: {}, conf: {}",
                                    bits, alpha, minQ, maxQ, conf);
                            System.err.println("UPDATE: BEFORE toJson() - Resource object has bits: " + bits
                                    + ", alpha: " + alpha + ", minQ: " + minQ + ", maxQ: " + maxQ + ", conf: " + conf);

                            // Check if fields are final
                            java.lang.reflect.Field[] fields = vcRes.getClass().getDeclaredFields();
                            for (java.lang.reflect.Field f : fields) {
                                if (f.getName().equals("bits") || f.getName().equals("alpha")
                                        || f.getName().equals("minQuantile") || f.getName().equals("maxQuantile")
                                        || f.getName().equals("confidenceInterval")) {
                                    int modifiers = f.getModifiers();
                                    boolean isFinal = java.lang.reflect.Modifier.isFinal(modifiers);
                                    System.err.println("UPDATE: Field " + f.getName() + " is final: " + isFinal);
                                }
                            }

                            // Try calling appendToJson directly to see what happens
                            com.fasterxml.jackson.databind.node.ObjectNode testJson = OBJECT_MAPPER.createObjectNode();
                            try {
                                // This is protected, so we need reflection
                                java.lang.reflect.Method appendMethod = vcRes.getClass().getDeclaredMethod(
                                        "appendToJson", com.fasterxml.jackson.databind.node.ObjectNode.class,
                                        org.apache.hyracks.api.io.IPersistedResourceRegistry.class);
                                appendMethod.setAccessible(true);
                                appendMethod.invoke(vcRes, testJson, persistedResourceRegistry);
                                System.err.println("UPDATE: Direct appendToJson result - has bits: "
                                        + testJson.has("bits") + ", has alpha: " + testJson.has("alpha"));
                            } catch (Exception e) {
                                System.err.println("UPDATE: Failed to call appendToJson directly: " + e.getMessage());
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("UPDATE: Failed to check resource object before serialization: {}", e.getMessage());
                    System.err.println("UPDATE: Failed to check resource object: " + e.getMessage());
                    e.printStackTrace();
                }

                // Serialize to JSON
                com.fasterxml.jackson.databind.JsonNode jsonNode = resource.toJson(persistedResourceRegistry);

                // DEBUG: Check if quantization params are in JSON - print full JSON structure for debugging
                try {
                    LOGGER.info("UPDATE: Full JSON structure: {}",
                            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));
                    System.err.println("UPDATE: Full JSON structure: "
                            + OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));

                    com.fasterxml.jackson.databind.JsonNode innerResourceNode = jsonNode.get("resource");
                    if (innerResourceNode != null) {
                        // Check if it's DatasetLocalResource
                        if (innerResourceNode.has("datasetId")) {
                            LOGGER.info("UPDATE: Found DatasetLocalResource in JSON");
                            System.err.println("UPDATE: Found DatasetLocalResource in JSON");
                            com.fasterxml.jackson.databind.JsonNode wrappedResourceNode =
                                    innerResourceNode.get("resource");
                            if (wrappedResourceNode != null) {
                                LOGGER.info("UPDATE: Wrapped resource node type: {}, keys: {}",
                                        wrappedResourceNode.getNodeType(),
                                        java.util.Iterator.class.cast(wrappedResourceNode.fieldNames())
                                                .hasNext()
                                                        ? java.util.stream.StreamSupport
                                                                .stream(java.util.Spliterators.spliteratorUnknownSize(
                                                                        (java.util.Iterator<String>) wrappedResourceNode
                                                                                .fieldNames(),
                                                                        java.util.Spliterator.ORDERED), false)
                                                                .collect(java.util.stream.Collectors.toList())
                                                        : "empty");

                                // Print all fields in wrapped resource
                                java.util.Iterator<String> fieldNames = wrappedResourceNode.fieldNames();
                                java.util.List<String> fields = new java.util.ArrayList<>();
                                while (fieldNames.hasNext()) {
                                    fields.add(fieldNames.next());
                                }
                                System.err.println("UPDATE: Wrapped resource fields: " + fields);

                                String bitsStr = wrappedResourceNode.has("bits")
                                        ? String.valueOf(wrappedResourceNode.get("bits")) : "MISSING";
                                String alphaStr = wrappedResourceNode.has("alpha")
                                        ? String.valueOf(wrappedResourceNode.get("alpha")) : "MISSING";
                                String minQStr = wrappedResourceNode.has("minQuantile")
                                        ? String.valueOf(wrappedResourceNode.get("minQuantile")) : "MISSING";
                                String maxQStr = wrappedResourceNode.has("maxQuantile")
                                        ? String.valueOf(wrappedResourceNode.get("maxQuantile")) : "MISSING";
                                String confStr = wrappedResourceNode.has("confidenceInterval")
                                        ? String.valueOf(wrappedResourceNode.get("confidenceInterval")) : "MISSING";
                                LOGGER.info(
                                        "UPDATE: JSON has bits: {}, alpha: {}, minQuantile: {}, maxQuantile: {}, confidenceInterval: {}",
                                        bitsStr, alphaStr, minQStr, maxQStr, confStr);
                                System.err.println("UPDATE: JSON has bits: " + bitsStr + ", alpha: " + alphaStr
                                        + ", minQuantile: " + minQStr + ", maxQuantile: " + maxQStr
                                        + ", confidenceInterval: " + confStr);
                            } else {
                                LOGGER.error("UPDATE: Wrapped resource node is null!");
                                System.err.println("UPDATE: ERROR - Wrapped resource node is null!");
                            }
                        } else {
                            // Direct LSMVCTreeLocalResource
                            LOGGER.info("UPDATE: Direct LSMVCTreeLocalResource (no DatasetLocalResource wrapper)");
                            System.err
                                    .println("UPDATE: Direct LSMVCTreeLocalResource (no DatasetLocalResource wrapper)");
                            LOGGER.info(
                                    "UPDATE: JSON (direct) has bits: {}, alpha: {}, minQuantile: {}, maxQuantile: {}, confidenceInterval: {}",
                                    jsonNode.has("bits") ? jsonNode.get("bits") : "MISSING",
                                    jsonNode.has("alpha") ? jsonNode.get("alpha") : "MISSING",
                                    jsonNode.has("minQuantile") ? jsonNode.get("minQuantile") : "MISSING",
                                    jsonNode.has("maxQuantile") ? jsonNode.get("maxQuantile") : "MISSING",
                                    jsonNode.has("confidenceInterval") ? jsonNode.get("confidenceInterval")
                                            : "MISSING");
                        }
                    } else {
                        LOGGER.error("UPDATE: innerResourceNode is null!");
                        System.err.println("UPDATE: ERROR - innerResourceNode is null!");
                    }
                } catch (Exception e) {
                    LOGGER.warn("UPDATE: Failed to check JSON content: {}", e.getMessage());
                    System.err.println("UPDATE: Failed to check JSON content: " + e.getMessage());
                    e.printStackTrace();
                }

                byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(jsonNode);
                LOGGER.info("UPDATE: Resource path: {}", resource.getPath());
                LOGGER.info("UPDATE: Resource ID: {}, Version: {}", resource.getId(), resource.getVersion());

                // DEBUG: Log quantization params in the resource being written
                try {
                    IResource innerResource = resource.getResource();
                    if (innerResource instanceof DatasetLocalResource) {
                        DatasetLocalResource dsWrapper = (DatasetLocalResource) innerResource;
                        IResource wrapped = dsWrapper.getResource();
                        if (wrapped instanceof LSMVCTreeLocalResource) {
                            LSMVCTreeLocalResource vcRes = (LSMVCTreeLocalResource) wrapped;
                            Integer bits = getFieldValue(vcRes, "bits", Integer.class);
                            Float alpha = getFieldValue(vcRes, "alpha", Float.class);
                            Float minQ = getFieldValue(vcRes, "minQuantile", Float.class);
                            Float maxQ = getFieldValue(vcRes, "maxQuantile", Float.class);
                            Float conf = getFieldValue(vcRes, "confidenceInterval", Float.class);
                            LOGGER.info(
                                    "UPDATE: Quantization params in resource being written - bits: {}, alpha: {}, minQ: {}, maxQ: {}, conf: {}",
                                    bits, alpha, minQ, maxQ, conf);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("UPDATE: Failed to log quantization params: {}", e.getMessage());
                }

                ioManager.overwrite(resourceFile, bytes);
                // NOTE: We do NOT call checkpoint init() here - we preserve existing checkpoints
                deleteResourceFileMask(repo, ioManager, resourceFile);

                // DEBUG: Verify file was written and read it back to check content
                if (resourceFile.getFile().exists()) {
                    LOGGER.info("UPDATE: File successfully written, size: {} bytes", resourceFile.getFile().length());
                    System.err.println(
                            "UPDATE: File successfully written, size: " + resourceFile.getFile().length() + " bytes");

                    // Read the file back and check if quantization params are in it
                    try {
                        byte[] fileBytes = ioManager.readAllBytes(resourceFile);
                        if (fileBytes != null) {
                            com.fasterxml.jackson.databind.JsonNode fileJson = OBJECT_MAPPER.readTree(fileBytes);
                            com.fasterxml.jackson.databind.JsonNode innerResNode = fileJson.get("resource");
                            if (innerResNode != null && innerResNode.has("datasetId")) {
                                com.fasterxml.jackson.databind.JsonNode wrappedResNode = innerResNode.get("resource");
                                if (wrappedResNode != null) {
                                    boolean hasBits = wrappedResNode.has("bits");
                                    boolean hasAlpha = wrappedResNode.has("alpha");
                                    System.err.println("UPDATE: File content check - has bits: " + hasBits + " (value: "
                                            + (hasBits ? wrappedResNode.get("bits") : "null") + "), has alpha: "
                                            + hasAlpha + " (value: " + (hasAlpha ? wrappedResNode.get("alpha") : "null")
                                            + ")");
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("UPDATE: Failed to read back file for verification: " + e.getMessage());
                    }
                } else {
                    LOGGER.error("UPDATE: File does not exist after write!");
                    System.err.println("UPDATE: ERROR - File does not exist after write!");
                }
            } catch (Exception e) {
                cleanupResourceFile(ioManager, resourceFile);
                throw HyracksDataException.create(e);
            }

            // Update cache - use the path without /metadata suffix
            String cacheKey = resource.getPath();
            LOGGER.info("UPDATE: Invalidating cache for path: {}", cacheKey);
            repo.invalidateResource(cacheKey);

            // Force cache refresh by reading the resource
            LocalResource readBack = repo.get(cacheKey);
            if (readBack != null) {
                LOGGER.info("UPDATE: Successfully read back resource after update");
                // DEBUG: Verify quantization params in read-back resource
                try {
                    IResource innerResource = readBack.getResource();
                    if (innerResource instanceof DatasetLocalResource) {
                        DatasetLocalResource dsWrapper = (DatasetLocalResource) innerResource;
                        IResource wrapped = dsWrapper.getResource();
                        if (wrapped instanceof LSMVCTreeLocalResource) {
                            LSMVCTreeLocalResource vcRes = (LSMVCTreeLocalResource) wrapped;
                            Integer bits = getFieldValue(vcRes, "bits", Integer.class);
                            Float alpha = getFieldValue(vcRes, "alpha", Float.class);
                            Float minQ = getFieldValue(vcRes, "minQuantile", Float.class);
                            Float maxQ = getFieldValue(vcRes, "maxQuantile", Float.class);
                            Float conf = getFieldValue(vcRes, "confidenceInterval", Float.class);
                            LOGGER.info(
                                    "UPDATE: Quantization params in read-back resource - bits: {}, alpha: {}, minQ: {}, maxQ: {}, conf: {}",
                                    bits, alpha, minQ, maxQ, conf);
                            System.err.println("UPDATE: Quantization params in read-back resource - bits: " + bits
                                    + ", alpha: " + alpha + ", minQ: " + minQ + ", maxQ: " + maxQ + ", conf: " + conf);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("UPDATE: Failed to log quantization params from read-back: {}", e.getMessage());
                }
            } else {
                LOGGER.error("UPDATE: Failed to read back resource after update!");
            }

            // Handle replication if enabled
            handleReplication(repo, resourceFile);

        } catch (Exception e) {
            if (e instanceof HyracksDataException) {
                throw (HyracksDataException) e;
            }
            throw HyracksDataException.create(e);
        }
    }

    private void createResourceFileMask(PersistentLocalResourceRepository repo, IIOManager ioManager,
            FileReference resourceFile) throws HyracksDataException {
        FileReference maskFile = getResourceMaskFilePath(resourceFile);
        try {
            ioManager.create(maskFile);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void deleteResourceFileMask(PersistentLocalResourceRepository repo, IIOManager ioManager,
            FileReference resourceFile) throws HyracksDataException {
        FileReference maskFile = getResourceMaskFilePath(resourceFile);
        ioManager.delete(maskFile);
    }

    private FileReference getResourceMaskFilePath(FileReference resourceFile) {
        FileReference resourceFileParent = resourceFile.getParent();
        return resourceFileParent.getChild(METADATA_FILE_MASK_NAME);
    }

    private void cleanupResourceFile(IIOManager ioManager, FileReference resourceFile) {
        if (resourceFile.getFile().exists()) {
            try {
                ioManager.delete(resourceFile);
            } catch (Exception e) {
                LOGGER.error("Error cleaning up resource file {}", resourceFile, e);
            }
        }
    }

    private void handleReplication(PersistentLocalResourceRepository repo, FileReference resourceFile) {
        try {
            Boolean isReplicationEnabled = getFieldValue(repo, "isReplicationEnabled", Boolean.class);
            if (Boolean.TRUE.equals(isReplicationEnabled)) {
                Object replicationManager = getFieldValue(repo, "replicationManager", Object.class);
                if (replicationManager != null) {
                    // Use reflection to call createReplicationJob if needed
                    Method createReplicationJobMethod = repo.getClass().getDeclaredMethod("createReplicationJob",
                            ReplicationOperation.class, FileReference.class);
                    createReplicationJobMethod.setAccessible(true);
                    createReplicationJobMethod.invoke(repo, ReplicationOperation.REPLICATE, resourceFile);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to handle replication for resource file {}", resourceFile, e);
            // Don't throw - replication failure shouldn't fail the update
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getFieldValue(Object obj, String fieldName, Class<T> fieldType) throws HyracksDataException {
        try {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(obj);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private static String getFileName(String path) {
        return path.endsWith(File.separator) ? (path + StorageConstants.METADATA_FILE_NAME)
                : (path + File.separator + StorageConstants.METADATA_FILE_NAME);
    }

    // Delegate all standard interface methods to wrapped repository

    @Override
    public LocalResource get(String name) throws HyracksDataException {
        return wrappedRepository.get(name);
    }

    @Override
    public void insert(LocalResource resource) throws HyracksDataException {
        wrappedRepository.insert(resource);
    }

    @Override
    public void delete(String name) throws HyracksDataException {
        wrappedRepository.delete(name);
    }

    @Override
    public Map<Long, LocalResource> getResources(Predicate<LocalResource> filter,
            List<FileReference> resourceFolderRoot) throws HyracksDataException {
        return wrappedRepository.getResources(filter, resourceFolderRoot);
    }

    @Override
    public long maxId() throws HyracksDataException {
        return wrappedRepository.maxId();
    }
}
