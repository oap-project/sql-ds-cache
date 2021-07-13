/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.utils.ape;

import java.util.Arrays;

/**
 * Aggregate expression column.
 */
public class AggregateExprColumn extends AggregateExpr {
    public static final String DEFAULT_NAME = "AttributeReference";

    private String columnName;

    public AggregateExprColumn(String dataType, String columnName) {
        super(DEFAULT_NAME, dataType);
        this.dataType = dataType;
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public boolean checkDataTypes() {
        return !Arrays.asList("FloatType", "DoubleType").contains(dataType);
    }
}
