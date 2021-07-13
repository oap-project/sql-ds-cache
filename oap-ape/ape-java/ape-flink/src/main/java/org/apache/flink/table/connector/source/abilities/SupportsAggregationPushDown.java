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

package org.apache.flink.table.connector.source.abilities;

import java.util.List;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.ape.AggregateExprs;

/**
 * For particular agg functions (eg. SUM, MAX, MIN, AVG, COUNT), the workload can be pushed down
 * to data source.
 * Commonly, filters should be pushed down too so that the data source can get the partial
 * aggregation result.
 *
 * <p>
 * For example,
 *     select sum(d_date_sk) from date_dim where d_year = 2000;
 * The original plan of above sql in Flink may be:
 * == Optimized Logical Plan ==
 * HashAggregate(isMerge=[true], select=[Final_SUM(sum$0) AS EXPR$0])
 * +- Exchange(distribution=[single])
 *    +- LocalHashAggregate(select=[Partial_SUM(d_date_sk) AS sum$0])
 *       +- Calc(select=[d_date_sk], where=[=(d_year, 2000)])
 *          +- TableSourceScan(table=[[myhive, tpcds_1g_snappy, date_dim, filter=[],
 *              project=[d_date_sk, d_year]]], fields=[d_date_sk, d_year])
 *
 * The plan after filters are pushed down:
 *== Optimized Logical Plan ==
 * HashAggregate(isMerge=[true], select=[Final_SUM(sum$0) AS EXPR$0])
 * +- Exchange(distribution=[single])
 *    +- LocalHashAggregate(select=[Partial_SUM(d_date_sk) AS sum$0])
 *       +- TableSourceScan(table=[[myhive, tpcds_1g_snappy, date_dim,
 *          filter=[equals(d_year, 2000)], project=[d_date_sk]]], fields=[d_date_sk])
 *</p>
 *
 * <p>
 * Moreover, aggregations are pushed down, the optimized plan becomes:
 * == Optimized Logical Plan ==
 * HashAggregate(isMerge=[true], select=[Final_SUM(sum$0) AS EXPR$0])
 * +- Exchange(distribution=[single])
 *    +- TableSourceScan(table=[[myhive, tpcds_1g_snappy, date_dim,
 *          filter=[equals(d_year, 2000)], project=[d_date_sk], aggregation=[sum$0]]],
 *          fields=[sum$0])
 *</p>
 */
public interface SupportsAggregationPushDown {

    boolean applyAggregations(
        List<String> outputNames,
        List<DataType> outputTypes,
        AggregateExprs aggregateExprs);

}
