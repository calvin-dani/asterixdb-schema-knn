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
package org.apache.hyracks.storage.am.vector.api;

import java.io.Serializable;

/**
 * Factory for creating IVTreeQuantizer instances.
 * Follows the IVTreeBinaryAccessorFactory pattern: Serializable factory
 * that can be passed from the AsterixDB layer to the Hyracks layer.
 *
 * The factory is created once (e.g., from quantization parameters stored on LSMVTreeLocalResource)
 * and stored on LSMVTreeLocalResource. At index activation time,
 * createQuantizer() is called to produce the IVTreeQuantizer instance
 * stored on LSMVTree for reuse by all subsequent queries.
 */
public interface IVTreeQuantizerFactory extends Serializable {

    /**
     * Create a new vector quantizer instance.
     *
     * @return a new IVTreeQuantizer implementation
     */
    IVTreeQuantizer createQuantizer();
}
