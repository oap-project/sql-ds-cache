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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.read.HiveContinuousPartitionFetcher;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsAggregationPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.ape.AggregateExprs;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.hive.util.HivePartitionUtils.getAllPartitions;
import static org.apache.flink.table.catalog.hive.util.HiveTableUtil.checkAcidTable;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_INCLUDE;
import static org.apache.flink.table.filesystem.FileSystemOptions.STREAMING_SOURCE_PARTITION_ORDER;

/**
 * A TableSource implementation to read data from Hive tables.
 */
public class ApeHiveTableSource extends HiveTableSource implements
		SupportsFilterPushDown,
		SupportsAggregationPushDown {

	private static final Logger LOG = LoggerFactory.getLogger(ApeHiveTableSource.class);
	private static final Duration DEFAULT_SCAN_MONITOR_INTERVAL = Duration.ofMinutes(1L);

	// Remaining partition specs after partition pruning is performed. Null if pruning is not pushed down.
	@Nullable
	private List<Map<String, String>> remainingPartitions = null;
	protected int[] projectedFields;
	private Long limit = null;

	public static final String JOB_CONF_KEY_PREDICATES =
		"parquet.private.native.reader.filter.predicate";
	public static final String JOB_CONF_KEY_PREDICATES_HUMAN =
		"parquet.private.native.reader.filter.predicate.human.readable";

	public static final String JOB_CONF_KEY_AGGREGATES =
		"parquet.private.native.reader.agg.expressions";

	protected FilterPredicate predicates = null;

	protected List<String> aggOutputNames;
	protected List<DataType> aggOutputTypes;
	protected AggregateExprs aggregateExprs;

	public ApeHiveTableSource(
			JobConf jobConf, ReadableConfig flinkConf, ObjectPath tablePath, CatalogTable catalogTable) {
		super(jobConf, flinkConf, tablePath, catalogTable);

		jobConf.setBoolean(
			ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER.key(),
			flinkConf.get(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER)
		);

		jobConf.set(
				ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_NATIVE_READER_MODE.key(),
				flinkConf.get(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_NATIVE_READER_MODE)
		);
	}

	@VisibleForTesting
	protected DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
		validateScanConfigurations();
		checkAcidTable(catalogTable, tablePath);

		// reset filters in job conf
		setFilterPredicate(predicates);
		if (predicates != null) {
			LOG.info("Filters are pushed down to parquet reader.");
		}

		// reset aggregates in job conf
		setAggregateExpr(aggregateExprs);
		if (aggregateExprs != null) {
			LOG.info("Aggregates are pushed down to parquet reader.");
		}

		List<HiveTablePartition> allHivePartitions = getAllPartitions(
				jobConf,
				hiveVersion,
				tablePath,
				catalogTable,
				hiveShim,
				remainingPartitions);
		Configuration configuration = Configuration.fromMap(catalogTable.getOptions());

		ApeHiveSource.HiveSourceBuilder sourceBuilder = new ApeHiveSource.HiveSourceBuilder(
				jobConf,
				tablePath,
				catalogTable,
				allHivePartitions,
				limit,
				hiveVersion,
				flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER),
				(RowType) getProducedDataType().getLogicalType());
		if (isStreamingSource()) {
			if (catalogTable.getPartitionKeys().isEmpty()) {
				String consumeOrderStr = configuration.get(STREAMING_SOURCE_PARTITION_ORDER);
				ConsumeOrder consumeOrder = ConsumeOrder.getConsumeOrder(consumeOrderStr);
				if (consumeOrder != ConsumeOrder.CREATE_TIME_ORDER) {
					throw new UnsupportedOperationException(
							"Only " + ConsumeOrder.CREATE_TIME_ORDER + " is supported for non partition table.");
				}
			}

			Duration monitorInterval = configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL) == null
					? DEFAULT_SCAN_MONITOR_INTERVAL
					: configuration.get(STREAMING_SOURCE_MONITOR_INTERVAL);
			sourceBuilder.monitorContinuously(monitorInterval);

			if (!catalogTable.getPartitionKeys().isEmpty()) {
				sourceBuilder.setFetcher(new HiveContinuousPartitionFetcher());
				final String defaultPartitionName = jobConf.get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
						HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);
				HiveContinuousPartitionFetcherContext<?> fetcherContext = new HiveContinuousPartitionFetcherContext(
						tablePath,
						hiveShim,
						new JobConfWrapper(jobConf),
						catalogTable.getPartitionKeys(),
						getProducedTableSchema().getFieldDataTypes(),
						getProducedTableSchema().getFieldNames(),
						configuration,
						defaultPartitionName);
				sourceBuilder.setFetcherContext(fetcherContext);
			}
		}

		ApeHiveSource hiveSource = sourceBuilder.build();
		DataStreamSource<RowData> source = execEnv.fromSource(
				hiveSource, WatermarkStrategy.noWatermarks(), "HiveSource-" + tablePath.getFullName());

		if (isStreamingSource()) {
			return source;
		} else {
			int parallelism = new HiveParallelismInference(tablePath, flinkConf)
					.infer(
							() -> HiveSourceFileEnumerator.getNumFiles(allHivePartitions, jobConf),
							() -> HiveSourceFileEnumerator.createInputSplits(0, allHivePartitions, jobConf).size())
					.limit(limit);
			return source.setParallelism(parallelism);
		}
	}

	private void validateScanConfigurations() {
		String partitionInclude = catalogTable.getOptions().getOrDefault(
				STREAMING_SOURCE_PARTITION_INCLUDE.key(),
				STREAMING_SOURCE_PARTITION_INCLUDE.defaultValue());
		Preconditions.checkArgument(
				"all".equals(partitionInclude),
				String.format(
						"The only supported '%s' is 'all' in hive table scan, but is '%s'",
						STREAMING_SOURCE_PARTITION_INCLUDE.key(),
						partitionInclude));
	}

	private DataType getProducedDataType() {
		return getProducedTableSchema().toRowDataType().bridgedTo(RowData.class);
	}

	protected TableSchema getProducedTableSchema() {
		TableSchema fullSchema = getTableSchema();
		if (aggOutputNames != null) {
			return TableSchema.builder().fields(
				aggOutputNames.toArray(new String[0]),
				aggOutputTypes.toArray(new DataType[0])
			).build();
		} else if (projectedFields != null) {
			String[] fullNames = fullSchema.getFieldNames();
			DataType[] fullTypes = fullSchema.getFieldDataTypes();
			return TableSchema.builder().fields(
				Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
				Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new)).build();
		} else {
			return fullSchema;
		}
	}

	@Override
	public DynamicTableSource copy() {
		ApeHiveTableSource source = new ApeHiveTableSource(jobConf, flinkConf, tablePath, catalogTable);
		source.remainingPartitions = remainingPartitions;
		source.projectedFields = projectedFields;
		source.limit = limit;

		source.predicates = predicates;
		source.aggOutputNames = aggOutputNames;
		source.aggOutputTypes = aggOutputTypes;
		source.aggregateExprs = aggregateExprs;
		return source;
	}

	@Override
	public Result applyFilters(List<ResolvedExpression> filters) {
		// check if APE is applicable
		if (!isAPENativeFilterApplicable()) {
			return Result.of(Collections.emptyList(), filters);
		}

		// convert filters from Expressions to FilterPredicate
		List<FilterPredicate> convertedPredicates = new ArrayList<>(filters.size());
		List<ResolvedExpression> acceptedFilters = new ArrayList<>(filters.size());
		List<ResolvedExpression> remainingFilters = new ArrayList<>(filters.size());

		for (ResolvedExpression filter : filters) {
			FilterPredicate convertedPredicate = filter.accept(new ExpressionToPredicateConverter());
			if (convertedPredicate != null) {
				convertedPredicates.add(convertedPredicate);
				acceptedFilters.add(filter);
			} else {
				remainingFilters.add(filter);
			}
		}

		// construct single Parquet FilterPredicate
		FilterPredicate parquetPredicate = null;
		if (!convertedPredicates.isEmpty()) {
			// concat converted predicates with AND
			parquetPredicate = convertedPredicates.get(0);

			for (FilterPredicate converted : convertedPredicates.subList(1, convertedPredicates.size())) {
				parquetPredicate = FilterApi.and(parquetPredicate, converted);
			}

			// optimize the filter tree
			parquetPredicate = parquetPredicate.accept(new PredicateOptimizers.AddNotNull());
		}

		// Here, just set predicate to table source.
		// We don't set predicate to jobConf now to avoid overriding filters.
		predicates = parquetPredicate;

		// remove pushed filters from remainingFilters
		return Result.of(acceptedFilters, remainingFilters);
	}

	private void setFilterPredicate(FilterPredicate parquetPredicate) {
		if (parquetPredicate == null) {
			jobConf.unset(JOB_CONF_KEY_PREDICATES_HUMAN);
			jobConf.unset(JOB_CONF_KEY_PREDICATES);
			return;
		}

		jobConf.set(JOB_CONF_KEY_PREDICATES_HUMAN, parquetPredicate.toString());

		try {
			SerializationUtil.writeObjectToConfAsBase64(JOB_CONF_KEY_PREDICATES,
				parquetPredicate, jobConf);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	private void setAggregateExpr(AggregateExprs aggregateExprs) {
		if (aggregateExprs == null) {
			jobConf.unset(JOB_CONF_KEY_AGGREGATES);
			return;
		}

		try {
			SerializationUtil.writeObjectToConfAsBase64(
					JOB_CONF_KEY_AGGREGATES,
					aggregateExprs,
					jobConf);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	private boolean allPartitionsAreParquet() {
		List<HiveTablePartition> allHivePartitions = getAllPartitions(
			jobConf,
			hiveVersion,
			tablePath,
			catalogTable,
			hiveShim,
			remainingPartitions);

		for (HiveTablePartition partition : allHivePartitions) {
			boolean isParquet = partition.getStorageDescriptor().getSerdeInfo().getSerializationLib()
				.toLowerCase().contains("parquet");

			if (!isParquet) {
				return false;
			}
		}

		return true;
	}

	private boolean allColumnsSupportVectorization() {
		RowType producedRowType = (RowType) getProducedTableSchema().toRowDataType().bridgedTo(RowData.class).getLogicalType();
		for (RowType.RowField field : producedRowType.getFields()) {
			if (isVectorizationUnsupported(field.getType())) {
				LOG.info("Fallback to hadoop mapred reader, unsupported field type: " + field.getType());
				return false;
			}
		}

		return true;
	}

	private static boolean isVectorizationUnsupported(LogicalType t) {
		switch (t.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case BOOLEAN:
			case BINARY:
			case VARBINARY:
			case DECIMAL:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return false;
			case TIMESTAMP_WITH_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
			case ARRAY:
			case MULTISET:
			case MAP:
			case ROW:
			case DISTINCT_TYPE:
			case STRUCTURED_TYPE:
			case NULL:
			case RAW:
			case SYMBOL:
			default:
				return true;
		}
	}

	@Override
	public boolean applyAggregations(
		List<String> outputNames,
		List<DataType> outputTypes,
		AggregateExprs aggregateExprs) {

		if (!isAPENativeAggApplicable()) {
			return false;
		}

		// transform data type names in aggregate expressions
		try {
			aggregateExprs.translateDataTypeName();
		} catch (RuntimeException ex) {
			LOG.warn(ex.toString());
			return false;
		}

		// check data types
		if (!aggregateExprs.checkDataTypes()) {
			LOG.info("Will not push down aggregates since data type checking not passed");
			return false;
		}

		aggOutputNames = outputNames;
		aggOutputTypes = outputTypes;
		this.aggregateExprs = aggregateExprs;

		return true;
	}

	private boolean isAPENativeFilterApplicable() {

		boolean useMapRedReader = flinkConf.get(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER);
		boolean useNativeParquetReader = flinkConf.get(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_USE_NATIVE_READER);
		boolean filterPushingDownEnabled = flinkConf.get(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_FILTERS);
		// join re-order does not know filters are pushed down.
		// This may lead to worse efficiency when it moves backward tables with filters pushed down in joins.
		boolean joinReorderEnabled = flinkConf.get(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED);
		// configuration that tells Flink whether push down filters or not when join re-order is enabled.
		boolean evadingJoinReorder = flinkConf.get(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_FILTERS_EVADING_JOIN_REORDER);

		// check configurations
		if (useMapRedReader
				|| !useNativeParquetReader
				|| !filterPushingDownEnabled
				|| (joinReorderEnabled && evadingJoinReorder)) {
			return false;
		}

		// check parquet format and column types
		return allColumnsSupportVectorization() && allPartitionsAreParquet();
	}

	private boolean isAPENativeAggApplicable() {
		boolean aggPushingDownEnabled = flinkConf.get(ApeHiveOptions.TABLE_EXEC_HIVE_PARQUET_PUSH_DOWN_AGGREGATIONS);
		return isAPENativeFilterApplicable() && aggPushingDownEnabled;
	}
}
