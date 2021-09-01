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

package org.apache.flink.table.tpch;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connectors.hive.ApeHiveOptions;
import org.apache.flink.connectors.hive.HiveOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.table.api.ApeEnvironmentSettings;
import org.apache.flink.table.api.ApeTableEnvironmentFactory;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.ApeHiveCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.tpch.schema.TpchSchema;
import org.apache.flink.table.tpch.schema.TpchSchemaProvider;
import org.apache.flink.table.types.utils.TypeConversions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * End-to-end test for TPC-H.
 */
public class TpchTestProgram {

	private static final List<String> TPCH_TABLES = Arrays.asList(
			"customer", "lineitem", "nation", "orders",
			"part", "partsupp", "region", "supplier"
	);

	private static final String QUERY_PREFIX = "q";
	private static final String QUERY_SUFFIX = ".sql";
	private static final String DATA_SUFFIX = ".dat";
	private static final String RESULT_SUFFIX = ".ans";
	private static final String COL_DELIMITER = "|";
	private static final String FILE_SEPARATOR = "/";

	private static final String hiveCatalogName = "myHive";
	private static final String hiveDefaultDBName = "default";
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		String sourceTablePath = params.getRequired("sourceTablePath");
		String queryPath = params.getRequired("queryPath");
		String sinkTablePath = params.getRequired("sinkTablePath");
		Boolean useTableStats = params.getBoolean("useTableStats");
		Boolean useHiveMetaStore = params.getBoolean("useHiveMetaStore", false);
		String hiveConfDir = params.get("hiveConfDir", "./conf");
		String hiveDatabaseName = params.get("hiveDatabaseName", hiveDefaultDBName);
		String cmdBeforeQuery = params.get("cmdBeforeQuery", "");
		String querySet = params.get("querySet", "1");
		boolean joinReorder = params.getBoolean("joinReorder", false);
		boolean filtersEvadingReorder = params.getBoolean("filtersEvadingReorder", false);
		int sourceParallelism = params.getInt("sourceParallelism", 60);
		boolean enableApe = params.getBoolean("enableApe", false);
		TableEnvironment tableEnvironment;
		if (useHiveMetaStore) {
			System.out.println("-- using hive catalog");

			if (enableApe) {
				System.out.println("-- using APE's hive table env");
				tableEnvironment = getApeHiveTableEnv(
						hiveConfDir, joinReorder, filtersEvadingReorder, sourceParallelism);

			} else {
				System.out.println("-- using default hive table env");
				tableEnvironment = getDefaultHiveTableEnv(hiveConfDir, joinReorder, sourceParallelism);
			}

			// debug
			System.out.println("-- show databases");
			tableEnvironment.executeSql("show databases").print();
			System.out.println("-- switched to " + hiveDatabaseName);
			tableEnvironment.executeSql("use " + hiveDatabaseName);
			System.out.println("-- show tables");
			tableEnvironment.executeSql("show tables").print();
		} else {
			System.out.println("-- using default catalog");
			tableEnvironment = prepareTableEnv(sourceTablePath, useTableStats);
		}

		//execute queries
		String[] queries = querySet.split(",");
		for (String queryId : queries) {
			System.out.println("[INFO]Run TPC-H query " + queryId + " ...");
			String queryName = QUERY_PREFIX + queryId + QUERY_SUFFIX;
			String queryFilePath = queryPath + FILE_SEPARATOR + queryName;
			String queryString = loadFile2String(queryFilePath);
			Table resultTable = tableEnvironment.sqlQuery(queryString);

			if (cmdBeforeQuery != null && !cmdBeforeQuery.trim().isEmpty()) {
				System.out.println("Run command: " + cmdBeforeQuery);
				try {
					Process proc = Runtime.getRuntime().exec(cmdBeforeQuery);
					int exitCode = proc.waitFor();
					System.out.println("Run command done, exit code: " + exitCode);
				} catch (Exception e) {
					System.out.println("Run command exception");
					e.printStackTrace();
				}
			}

			//register sink table
			String sinkTableName = QUERY_PREFIX + queryId + "_sinkTable";
			((TableEnvironmentInternal) tableEnvironment).registerTableSinkInternal(sinkTableName,
					new CsvTableSink(
						sinkTablePath + FILE_SEPARATOR + queryId + RESULT_SUFFIX,
						COL_DELIMITER,
						1,
						FileSystem.WriteMode.OVERWRITE,
						resultTable.getSchema().getFieldNames(),
						resultTable.getSchema().getFieldDataTypes()
						));
			try {
				TableResult tableResult = resultTable.executeInsert(sinkTableName);
				// wait job finish
				tableResult.getJobClient().get()
					.getJobExecutionResult()
					.get();
				System.out.println("[INFO]Run TPC-H query " + queryId + " success.");
			} catch (Exception e) {
				System.out.println("[INFO]Run TPC-H query " + queryId + " faild.");
				e.printStackTrace();
				continue;
				//TODO: handle exception
			}
		}
	}

	/**
	 * Prepare TableEnvironment for query.
	 *
	 * @param sourceTablePath
	 * @return
	 */
	private static TableEnvironment prepareTableEnv(String sourceTablePath, Boolean useTableStats) {
		//init Table Env
		EnvironmentSettings environmentSettings = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inBatchMode()
				.build();
		TableEnvironment tEnv = TableEnvironment.create(environmentSettings);

		//config Optimizer parameters
		tEnv.getConfig().getConfiguration()
				.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
		tEnv.getConfig().getConfiguration()
				.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, GlobalDataExchangeMode.POINTWISE_EDGES_PIPELINED.toString());
		tEnv.getConfig().getConfiguration()
				.setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10 * 1024 * 1024);
		tEnv.getConfig().getConfiguration()
				.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);

		//register TPC-H tables
		TPCH_TABLES.forEach(table -> {
			TpchSchema schema = TpchSchemaProvider.getTableSchema(table);
			CsvTableSource.Builder builder = CsvTableSource.builder();
			builder.path(sourceTablePath + FILE_SEPARATOR + table + DATA_SUFFIX);
			for (int i = 0; i < schema.getFieldNames().size(); i++) {
				builder.field(
						schema.getFieldNames().get(i),
						TypeConversions.fromDataTypeToLegacyInfo(schema.getFieldTypes().get(i)));
			}
			builder.fieldDelimiter(COL_DELIMITER);
			builder.emptyColumnAsNull();
			builder.lineDelimiter("\n");
			CsvTableSource tableSource = builder.build();
			ConnectorCatalogTable catalogTable = ConnectorCatalogTable.source(tableSource, true);
			tEnv.getCatalog(tEnv.getCurrentCatalog()).ifPresent(catalog -> {
				try {
					catalog.createTable(new ObjectPath(tEnv.getCurrentDatabase(), table), catalogTable, false);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			});
		});

		return tEnv;
	}

	private static String loadFile2String(String filePath) throws Exception {
		StringBuilder stringBuilder = new StringBuilder();
		Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8);
		stream.forEach(s -> stringBuilder.append(s).append('\n'));
		return stringBuilder.toString();
	}

	private static TableEnvironment getDefaultHiveTableEnv(
			String hiveConfDir, boolean joinReorder, int sourceParallelism) {
		// Create APE's hive table env
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);

		// Set APE's HiveCatalog as the catalog of current session
		Catalog catalog = new HiveCatalog(hiveCatalogName, hiveDefaultDBName, hiveConfDir, "2.3.6");
		tableEnv.registerCatalog(hiveCatalogName, catalog);
		tableEnv.useCatalog(hiveCatalogName);

		// Use hive dialect
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

		// Config Optimizer parameters
		tableEnv.getConfig().getConfiguration()
				.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
						GlobalDataExchangeMode.POINTWISE_EDGES_PIPELINED.toString());
		tableEnv.getConfig().getConfiguration()
				.setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10 * 1024 * 1024);
//		tableEnv.getConfig().getConfiguration()
//				.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, false);

		if (joinReorder) {
			tableEnv.getConfig().getConfiguration()
					.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
		}

		tableEnv.getConfig().getConfiguration()
				.setInteger(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX, sourceParallelism);

		return tableEnv;
	}

	private static TableEnvironment getApeHiveTableEnv(
			String hiveConfDir, boolean joinReorder, boolean filtersEvadingReorder, int sourceParallelism) {
		// Create APE's hive table env
		ApeEnvironmentSettings settings = ApeEnvironmentSettings.newInstance().inBatchMode().build();
		TableEnvironment tableEnv = ApeTableEnvironmentFactory.create(settings);

		// Set APE's HiveCatalog as the catalog of current session
		Catalog catalog = new ApeHiveCatalog(hiveCatalogName, hiveDefaultDBName, hiveConfDir, "2.3.6");
		tableEnv.registerCatalog(hiveCatalogName, catalog);
		tableEnv.useCatalog(hiveCatalogName);

		// Use hive dialect
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

		// Config Optimizer parameters
		tableEnv.getConfig().getConfiguration()
				.setString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE,
						GlobalDataExchangeMode.POINTWISE_EDGES_PIPELINED.toString());
		tableEnv.getConfig().getConfiguration()
				.setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10 * 1024 * 1024);
//		tableEnv.getConfig().getConfiguration()
//				.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, false);

		if (joinReorder) {
			tableEnv.getConfig().getConfiguration()
					.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
		}
		if (filtersEvadingReorder) {
			tableEnv.getConfig().getConfiguration()
					.setBoolean(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_FILTERS_EVADING_JOIN_REORDER, true);
		}

		tableEnv.getConfig().getConfiguration()
				.setInteger(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX, sourceParallelism);

		// Config APE's native parquet reader
		tableEnv.getConfig().getConfiguration().setBoolean(
				ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(), true);

		tableEnv.getConfig().getConfiguration().setString(
				ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_NATIVE_READER_MODE.key(), "local");

		tableEnv.getConfig().getConfiguration().setBoolean(
				ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_FILTERS.key(), true);
		tableEnv.getConfig().getConfiguration().setBoolean(
				ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_AGGREGATIONS.key(), false);

		return tableEnv;
	}
}
