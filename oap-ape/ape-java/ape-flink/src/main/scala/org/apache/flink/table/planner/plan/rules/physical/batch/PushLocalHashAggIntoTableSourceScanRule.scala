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
package org.apache.flink.table.planner.plan.rules.physical.batch

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.flink.table.connector.source.abilities.SupportsAggregationPushDown
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecExchange, BatchExecLocalHashAggregate, BatchExecTableSourceScan}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.AggregateUtil
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.utils.ape.{AggregateExpr, AggregateExprColumn, AggregateExprs}


/**
 * Some aggregation functions are carried out tow-phase aggregate, that is:
 * BatchExecTableSourceScan
 *   -> BatchExecLocalHashAggregate
 *     -> BatchExecExchange
 *       -> BatchExecHashAggregate
 *
 * If the `input` mentioned above can processing agg functions partially, then we can push down
 * aggregations to the input. And the `LocalHashAggregate` should be removed from the plan.
 */
class PushLocalHashAggIntoTableSourceScanRule extends RelOptRule(
  operand(classOf[BatchExecExchange],
    operand(classOf[BatchExecLocalHashAggregate],
      operand(classOf[BatchExecTableSourceScan], FlinkConventions.BATCH_PHYSICAL, any))),
  "PushLocalHashAggIntoTableSourceScanRule")
  with ApeBatchExecAggRuleBase {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val exchange = call.rels(0).asInstanceOf[BatchExecExchange]
    val localAgg = call.rels(1).asInstanceOf[BatchExecLocalHashAggregate]
    val input = call.rels(2).asInstanceOf[BatchExecTableSourceScan]

    val tableSourceTable = input.getTable.unwrap(classOf[TableSourceTable])

    // check if source table applicable
    if (tableSourceTable != null
        && tableSourceTable.tableSource.isInstanceOf[SupportsAggregationPushDown]
        && !tableSourceTable.extraDigests.exists(str => str.startsWith("aggregation=["))) {

      // apply aggregations to table source
      val newTableSource = tableSourceTable.tableSource.copy

      // make agg exprs
      val aggregateExprs = makeAggregateExprs(localAgg, input)

      // apply agg to table source
      var applied = false
      if (aggregateExprs.isDefined) {
        applied =
          newTableSource.asInstanceOf[SupportsAggregationPushDown].applyAggregations(
            localAgg.getRowType.getFieldList.asScala.map(_.getName).asJava,
            localAgg.getRowType.getFieldList.asScala.map(
              f => TypeConversions.fromLogicalToDataType(FlinkTypeFactory.toLogicalType(f.getType))
            ).asJava,
            aggregateExprs.get
          )
      }

      // replace local agg with new scan
      if (applied) {
        val newTableSourceTable = tableSourceTable.copy(
          newTableSource,
          localAgg.getRowType,
          Array[String]("aggregation=[" + localAgg.getRowType.getFieldNames.asScala.mkString(",") + "]"))

        val newScan = new BatchExecTableSourceScan(
          input.getCluster, input.getTraitSet, newTableSourceTable)

        // replace input of exchange
        val newExchange = exchange.copy(exchange.getTraitSet, newScan, exchange.distribution)
        call.transformTo(newExchange)
      }
    }
  }

  def makeAggregateExprs(localAgg: BatchExecLocalHashAggregate,
                         input: BatchExecTableSourceScan
                        ): Option[AggregateExprs] = {

    // 1. wrap input columns of table with expression class
    val inputColumns = new ArrayBuffer[AggregateExpr]
    input.getRowType.getFieldList.asScala.foreach(field => {
      inputColumns.append(new AggregateExprColumn(field.getType.toString, field.getName))
    })

    // 2. collect grouped columns from input columns
    val groups = new ArrayBuffer[AggregateExpr]
    localAgg.getGrouping.foreach(index => {
      inputColumns(index) match {
        case c: AggregateExprColumn => groups.append(c)
        case _ => return None
      }
    })

    // 3. collect output columns of agg functions from input columns
    // some functions may produce more than 1 type. eg. sum

    // parse output types of agg functions
    val (_, aggOutputTypes, _) = AggregateUtil.transformToBatchAggregateFunctions(
      localAgg.getAggCallList, localAgg.getInput(0).getRowType)

    val aggregateColumns = flattenAggFunctions(localAgg, aggOutputTypes, inputColumns)
    if (aggregateColumns.isEmpty) {
      return None
    }

    // 4. wrap all output columns into 1 expression object
    val aggregates = new ArrayBuffer[AggregateExpr]
    val inputColumnNames = input.getRowType.getFieldNames
    var aggColumnIndex = 0

    localAgg.getRowType.getFieldList.asScala.foreach(field => {
      val inputColumnIndex = inputColumnNames.indexOf(field.getName)
      if (inputColumnIndex >= 0) {
        // this output column is from input
        aggregates.append(inputColumns(inputColumnIndex))
      } else {
        // this output column is from agg functions
        val aggRoot = aggregateColumns.get(aggColumnIndex)
        aggRoot.setDataType(field.getType.toString)
        aggRoot.getChild.setDataType(field.getType.toString)
        aggRoot.setAliasName(field.getName)
        aggregates.append(aggRoot)

        aggColumnIndex += 1
      }
    })

    val aggregateExprs = new AggregateExprs(groups.asJava, aggregates.asJava)
    Some(aggregateExprs)
  }

}

object PushLocalHashAggIntoTableSourceScanRule {
  val INSTANCE = new PushLocalHashAggIntoTableSourceScanRule
}
