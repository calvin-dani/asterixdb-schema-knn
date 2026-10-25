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
package org.apache.asterix.om.utils;

import org.apache.asterix.om.types.ATypeTag;

/**
 * Maps ADM types to JSON Schema (draft 2020-12) keywords. Shared by the row and column schema printers so the
 * inferred schemas they produce stay identical in form.
 */
public final class JsonSchemaUtil {
    public static final String SCHEMA_URI = "https://json-schema.org/draft/2020-12/schema";
    // JSON Schema ignores unknown keywords; this one keeps ADM distinctions JSON has no type for
    public static final String ASTERIX_TYPE_KEYWORD = "x-asterix-type";

    private JsonSchemaUtil() {
    }

    /**
     * @return the JSON Schema {@code type} of values with the given tag
     * @throws IllegalStateException if the tag has no JSON Schema mapping
     */
    public static String getType(ATypeTag tag) {
        switch (tag) {
            case OBJECT:
                return "object";
            case ARRAY:
            case MULTISET:
                return "array";
            case NULL:
            case MISSING:
                return "null";
            case BOOLEAN:
                return "boolean";
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return "integer";
            case FLOAT:
            case DOUBLE:
                return "number";
            case STRING:
            case UUID:
                return "string";
            default:
                throw new IllegalStateException("Unsupported type " + tag);
        }
    }

    /**
     * @return the JSON Schema {@code format} refining the type, or null if there is none
     */
    public static String getFormat(ATypeTag tag) {
        return tag == ATypeTag.UUID ? "uuid" : null;
    }

    /**
     * @return the value of {@link #ASTERIX_TYPE_KEYWORD} for ADM types JSON Schema cannot tell apart, or null
     */
    public static String getAsterixType(ATypeTag tag) {
        return tag == ATypeTag.MULTISET ? "multiset" : null;
    }
}
