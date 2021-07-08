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

package org.apache.flink.formats.parquet;

/**
 * Config keys used in ape packages.
 */
public class ApeParquetConfKeys {
	// use native parquet reader or not.
	public static final String USE_NATIVE_PARQUET_READER = "table.exec.hive.parquet-native-reader";

	// mode of native parquet reader.
	public static final String NATIVE_PARQUET_READER_MODE = "table.exec.hive.parquet-native-reader.mode";
}
