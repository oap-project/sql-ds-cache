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

package org.apache.flink.table.tpch.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to provide all TPC-H tables' schema information.
 * The data type of column use {@link DataType}
 */
public class TpchSchemaProvider {

	private static final int tpcdsTableNums = 8;
	private static final Map<String, TpchSchema> schemaMap = createTableSchemas();

	private static Map<String, TpchSchema> createTableSchemas() {
		final Map<String, TpchSchema> schemaMap = new HashMap<>(tpcdsTableNums);
		schemaMap.put("customer", new TpchSchema(
				Arrays.asList(
					new Column("c_custkey", DataTypes.BIGINT()),
					new Column("c_name", DataTypes.STRING()),
					new Column("c_address", DataTypes.STRING()),
					new Column("c_nationkey", DataTypes.BIGINT()),
					new Column("c_phone", DataTypes.STRING()),
					new Column("c_acctbal", DataTypes.DOUBLE()),
					new Column("c_mktsegment", DataTypes.STRING()),
					new Column("c_comment", DataTypes.STRING())
				)));
		schemaMap.put("lineitem", new TpchSchema(
				Arrays.asList(
					new Column("l_orderkey", DataTypes.BIGINT()),
					new Column("l_partkey", DataTypes.BIGINT()),
					new Column("l_suppkey", DataTypes.BIGINT()),
					new Column("l_linenumber", DataTypes.INT()),
					new Column("l_quantity", DataTypes.DOUBLE()),
					new Column("l_extendedprice", DataTypes.DOUBLE()),
					new Column("l_discount", DataTypes.DOUBLE()),
					new Column("l_tax", DataTypes.DOUBLE()),
					new Column("l_returnflag", DataTypes.STRING()),
					new Column("l_linestatus", DataTypes.STRING()),
					new Column("l_shipdate", DataTypes.DATE()),
					new Column("l_commitdate", DataTypes.DATE()),
					new Column("l_receiptdate", DataTypes.DATE()),
					new Column("l_shipinstruct", DataTypes.STRING()),
					new Column("l_shipmode", DataTypes.STRING()),
					new Column("l_comment", DataTypes.STRING())
				)));
		schemaMap.put("nation", new TpchSchema(
				Arrays.asList(
					new Column("n_nationkey", DataTypes.BIGINT()),
					new Column("n_name", DataTypes.STRING()),
					new Column("n_regionkey", DataTypes.BIGINT()),
					new Column("n_comment", DataTypes.STRING())
				)));
		schemaMap.put("orders", new TpchSchema(Arrays.asList(
			new Column("o_orderkey", DataTypes.BIGINT()),
			new Column("o_custkey", DataTypes.BIGINT()),
			new Column("o_orderstatus", DataTypes.STRING()),
			new Column("o_totalprice", DataTypes.DOUBLE()),
			new Column("o_orderdate", DataTypes.DATE()),
			new Column("o_orderpriority", DataTypes.STRING()),
			new Column("o_clerk", DataTypes.STRING()),
			new Column("o_shippriority", DataTypes.INT()),
			new Column("o_comment", DataTypes.STRING())
		)));
		schemaMap.put("part", new TpchSchema(Arrays.asList(
			new Column("p_partkey", DataTypes.BIGINT()),
			new Column("p_name", DataTypes.STRING()),
			new Column("p_mfgr", DataTypes.STRING()),
			new Column("p_brand", DataTypes.STRING()),
			new Column("p_type", DataTypes.STRING()),
			new Column("p_size", DataTypes.INT()),
			new Column("p_container", DataTypes.STRING()),
			new Column("p_retailprice", DataTypes.DOUBLE()),
			new Column("p_comment", DataTypes.STRING())
		)));
		schemaMap.put("partsupp", new TpchSchema(Arrays.asList(
			new Column("ps_partkey", DataTypes.BIGINT()),
			new Column("ps_suppkey", DataTypes.BIGINT()),
			new Column("ps_availqty", DataTypes.INT()),
			new Column("ps_supplycost", DataTypes.DOUBLE()),
			new Column("ps_comment", DataTypes.STRING())
		)));
		schemaMap.put("region", new TpchSchema(Arrays.asList(
			new Column("r_regionkey", DataTypes.BIGINT()),
			new Column("r_name", DataTypes.STRING()),
			new Column("r_comment", DataTypes.STRING())
		)));
		schemaMap.put("supplier", new TpchSchema(Arrays.asList(
			new Column("s_suppkey", DataTypes.BIGINT()),
			new Column("s_name", DataTypes.STRING()),
			new Column("s_address", DataTypes.STRING()),
			new Column("s_nationkey", DataTypes.BIGINT()),
			new Column("s_phone", DataTypes.STRING()),
			new Column("s_acctbal", DataTypes.DOUBLE()),
			new Column("s_comment", DataTypes.STRING())
		)));
		return schemaMap;
	}

	public static TpchSchema getTableSchema(String tableName) {
		TpchSchema result = schemaMap.get(tableName);
		if (result != null) {
			return result;
		}
		else {
			throw new IllegalArgumentException("Table schema of table " + tableName + " does not exist.");
		}
	}
}

