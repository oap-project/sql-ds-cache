
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

package org.apache.flink.formats.parquet.utils;

import java.util.List;

import com.google.gson.Gson;

public class RequestedSchemaJsonConvertor {

    private final List<String> fieldType;
    private final List<String> fieldName;

    public RequestedSchemaJsonConvertor(List<String> fieldType, List<String> fieldName) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public String toJson() {
        Gson gson = new Gson();
        RequestedSchema record = new RequestedSchema();
        record.setFields(this.fieldType, this.fieldName);
        record.setType("struct");
        return gson.toJson(record);
    }
}
