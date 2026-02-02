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
package org.apache.asterix.common.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;

/**
 * Manages sidecar files for quantization constants.
 * These files are written by Job 0.5 (quantization computation) and read by Job 1 (index creation).
 * 
 * File format (24 bytes binary):
 * - minQ: 4 bytes (float)
 * - maxQ: 4 bytes (float)
 * - alpha: 4 bytes (float)
 * - bits: 4 bytes (int)
 * - confidenceInterval: 4 bytes (float)
 * - sampleCount: 4 bytes (int)
 */
public class QuantizationConstantsFileManager {
    private static final Logger LOGGER = Logger.getLogger(QuantizationConstantsFileManager.class.getName());

    public static final String FILE_PREFIX = ".quantization_";

    private QuantizationConstantsFileManager() {
        // Utility class
    }

    /**
     * Get the sidecar file name for a given index.
     * 
     * @param indexName The index name
     * @return File name: .quantization_<indexName>
     */
    public static String getFileName(String indexName) {
        return FILE_PREFIX + indexName;
    }

    /**
     * Get the sidecar file reference for a given dataset directory and index name.
     * 
     * @param datasetDir The dataset directory
     * @param indexName The index name
     * @return FileReference to the sidecar file
     */
    public static FileReference getFileReference(FileReference datasetDir, String indexName) {
        return datasetDir.getChild(getFileName(indexName));
    }

    /**
     * Write QuantizationConstants to a sidecar file.
     * 
     * @param ioManager The IO manager
     * @param datasetDir The dataset directory
     * @param indexName The index name
     * @param qc The quantization constants to write
     * @throws HyracksDataException if writing fails
     */
    public static void write(IIOManager ioManager, FileReference datasetDir, String indexName, QuantizationConstants qc)
            throws HyracksDataException {
        FileReference file = getFileReference(datasetDir, indexName);
        LOGGER.info("Writing quantization constants to: " + file.getAbsolutePath());

        try {
            // Serialize to byte array
            ByteArrayOutputStream baos = new ByteArrayOutputStream(24);
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeFloat(qc.getMinQ());
            dos.writeFloat(qc.getMaxQ());
            dos.writeFloat(qc.getAlpha());
            dos.writeInt(qc.getBits());
            dos.writeFloat(qc.getConfidenceInterval());
            dos.writeInt(qc.getSampleCount());
            dos.flush();

            // Write to file using IIOManager.overwrite()
            ioManager.overwrite(file, baos.toByteArray());
            LOGGER.info("Successfully wrote quantization constants: " + qc);
        } catch (IOException e) {
            LOGGER.warning("Failed to write quantization constants: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Read QuantizationConstants from a sidecar file.
     * 
     * @param ioManager The IO manager
     * @param datasetDir The dataset directory
     * @param indexName The index name
     * @return QuantizationConstants or null if file doesn't exist
     * @throws HyracksDataException if reading fails
     */
    public static QuantizationConstants read(IIOManager ioManager, FileReference datasetDir, String indexName)
            throws HyracksDataException {
        FileReference file = getFileReference(datasetDir, indexName);

        if (!ioManager.exists(file)) {
            LOGGER.info("Quantization constants file does not exist: " + file.getAbsolutePath());
            return null;
        }

        LOGGER.info("Reading quantization constants from: " + file.getAbsolutePath());

        try {
            // Read all bytes using IIOManager.readAllBytes()
            byte[] data = ioManager.readAllBytes(file);

            // Deserialize from byte array
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            float minQ = dis.readFloat();
            float maxQ = dis.readFloat();
            float alpha = dis.readFloat();
            int bits = dis.readInt();
            float confidenceInterval = dis.readFloat();
            int sampleCount = dis.readInt();

            QuantizationConstants qc =
                    new QuantizationConstants(minQ, maxQ, alpha, bits, confidenceInterval, sampleCount);
            LOGGER.info("Successfully read quantization constants: " + qc);
            return qc;
        } catch (IOException e) {
            LOGGER.warning("Failed to read quantization constants: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Delete the quantization constants sidecar file.
     * 
     * @param ioManager The IO manager
     * @param datasetDir The dataset directory
     * @param indexName The index name
     * @return true if file was deleted, false if it didn't exist
     * @throws HyracksDataException if deletion fails
     */
    public static boolean delete(IIOManager ioManager, FileReference datasetDir, String indexName)
            throws HyracksDataException {
        FileReference file = getFileReference(datasetDir, indexName);

        if (!ioManager.exists(file)) {
            return false;
        }

        try {
            ioManager.delete(file);
            LOGGER.info("Deleted quantization constants file: " + file.getAbsolutePath());
            return true;
        } catch (Exception e) {
            LOGGER.warning("Failed to delete quantization constants file: " + e.getMessage());
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Check if a quantization constants sidecar file exists.
     * 
     * @param ioManager The IO manager
     * @param datasetDir The dataset directory
     * @param indexName The index name
     * @return true if file exists
     * @throws HyracksDataException if checking fails
     */
    public static boolean exists(IIOManager ioManager, FileReference datasetDir, String indexName)
            throws HyracksDataException {
        FileReference file = getFileReference(datasetDir, indexName);
        return ioManager.exists(file);
    }
}
