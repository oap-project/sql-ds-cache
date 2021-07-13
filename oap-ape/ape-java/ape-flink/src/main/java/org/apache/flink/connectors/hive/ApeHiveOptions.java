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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by hive connector.
 */
public class ApeHiveOptions {

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER =
        key("table.exec.hive.parquet-native-reader")
            .booleanType()
            .defaultValue(false)
            .withDescription("Option whether parquet input format uses native file reader. " +
                "Native source reader may provide higher performance and support pushing down");

    public static final ConfigOption<String> TABLE_EXEC_HIVE_PARQUET_NATIVE_READER_MODE =
            key("table.exec.hive.parquet-native-reader.mode")
                    .stringType()
                    .defaultValue("local")
                    .withDescription("Mode of native file reader. Supported modes: local, remote.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_FILTERS =
        key("table.exec.hive.parquet-push-down-filters")
            .booleanType()
            .defaultValue(false)
            .withDescription("If it is true, enabling filters pushing down to source reader." +
                "To enable pushing down, 'table.exec.hive.parquet-native-reader' is also needed " +
                    "to be set as true.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_PARQUET_FILTERS_EVADING_JOIN_REORDER =
            key("table.exec.hive.parquet-filters-evading-join-reorder")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If it is true, Flink will not push down filters when " +
                            "OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED is true." +
                            "Join-reorder may make worse performance when table scan get lower " +
                            "priority than before in joins after filters are pushed down.");

    public static final ConfigOption<Boolean> TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_AGGREGATIONS =
        key("table.exec.hive.parquet-push-down-aggregations")
            .booleanType()
            .defaultValue(false)
            .withDescription("If it is true, enabling aggregations pushing down to source reader." +
                "To enable pushing down, 'table.exec.hive.parquet-native-reader' and " +
                "'table.exec.hive.parquet-push-down-filters' are also needed to be set as true.");

}
