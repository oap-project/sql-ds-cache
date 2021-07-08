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

import java.util.ArrayList;
import java.util.List;

public class RequestedSchema {

    private String type;
    private final List<RequestedField> fields = new ArrayList<RequestedField>();

    public void setFields(List<String> fieldType, List<String> fieldName) {
        for (int i = 0; i < fieldName.size(); i++) {
            RequestedField f = new RequestedField();
            f.setName(fieldName.get(i));
            f.setType(fieldType.get(i));
            fields.add(f);
        }
    }

    public List<RequestedField> getFields() {
        return this.fields;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }
}
