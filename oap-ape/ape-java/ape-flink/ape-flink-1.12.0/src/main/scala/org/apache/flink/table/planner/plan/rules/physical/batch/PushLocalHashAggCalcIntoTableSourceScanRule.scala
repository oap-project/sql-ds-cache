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
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexLocalRef}
import org.apache.calcite.sql.SqlBinaryOperator
import org.apache.calcite.sql.`type`.{SqlTypeFamily, SqlTypeName}
import org.apache.flink.table.connector.source.abilities.SupportsAggregationPushDown
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecCalc, BatchExecExchange, BatchExecLocalHashAggregate, BatchExecTableSourceScan}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.AggregateUtil
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.utils.ape._


/**
 * Some aggregation functions are carried out tow-phase aggregate with calc, that is:
 * BatchExecTableSourceScan
 *   -> BatchExecCalc
 *     -> BatchExecLocalHashAggregate
 *       -> BatchExecExchange
 *         -> BatchExecHashAggregate
 *
 * If the `input` mentioned above can processing agg functions partially, then we can push down
 * aggregations and calculation to the input.
 * After that `LocalHashAggregate` and `Calc` should be removed from the plan.
 */
class PushLocalHashAggCalcIntoTableSourceScanRule extends RelOptRule(
  operand(classOf[BatchExecExchange],
    operand(classOf[BatchExecLocalHashAggregate],
      operand(classOf[BatchExecCalc],
        operand(classOf[BatchExecTableSourceScan], FlinkConventions.BATCH_PHYSICAL, any)))),
  "PushLocalHashAggIntoTableSourceScanRule")
  with ApeBatchExecAggRuleBase {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val exchange = call.rels(0).asInstanceOf[BatchExecExchange]
    val localAgg = call.rels(1).asInstanceOf[BatchExecLocalHashAggregate]
    val calc = call.rels(2).asInstanceOf[BatchExecCalc]
    val input = call.rels(3).asInstanceOf[BatchExecTableSourceScan]

    val tableSourceTable = input.getTable.unwrap(classOf[TableSourceTable])

    // check if source table applicable
    if (tableSourceTable != null
        && tableSourceTable.tableSource.isInstanceOf[SupportsAggregationPushDown]
        && !tableSourceTable.extraDigests.exists(str => str.startsWith("aggregation=["))) {

      // apply aggregations to table source
      val newTableSource = tableSourceTable.tableSource.copy

      // make agg exprs
      val aggregateExprs = makeAggregateExprs(localAgg, calc, input)

      // apply agg to table source
      var applied = false
      if (aggregateExprs.isDefined) {
        applied =
            newTableSource.asInstanceOf[SupportsAggregationPushDown].applyAggregations(
              localAgg.getRowType.getFieldList.asScala.map(_.getName).asJava,
              localAgg.getRowType.getFieldList.asScala.map(
                f =>
                  TypeConversions.fromLogicalToDataType(FlinkTypeFactory.toLogicalType(f.getType))
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
                         calc: BatchExecCalc,
                         input: BatchExecTableSourceScan
                        ): Option[AggregateExprs] = {

    // 1. wrap input columns of table with expression class
    val inputColumns = new ArrayBuffer[AggregateExprColumn]
    input.getRowType.getFieldList.asScala.foreach(field => {
      inputColumns.append(new AggregateExprColumn(field.getType.toString, field.getName))
    })

    // 2. collect calc columns from input columns
    val calcOutputColumns = getAggregateExprsFromCalc(calc, inputColumns)
    if (calcOutputColumns.isEmpty) {
      return None
    }

    // 3. collect grouped columns from calc columns
    val groups = new ArrayBuffer[AggregateExpr]
    localAgg.getGrouping.foreach(index => {
      calcOutputColumns.get(index) match {
        case c: AggregateExprColumn => groups.append(c)
        case _ => return None
      }
    })

    // 4. collect output columns of agg functions from input columns
    // some functions may produce more than 1 type. eg. sum

    // parse output types of agg functions
    val (_, aggOutputTypes, _) = AggregateUtil.transformToBatchAggregateFunctions(
      localAgg.getAggCallList, localAgg.getInput(0).getRowType)

    val aggregateColumns = flattenAggFunctions(localAgg, aggOutputTypes, calcOutputColumns.get)
    if (aggregateColumns.isEmpty) {
      return None
    }

    // 5. wrap all output columns into 1 expression object
    val aggregates = new ArrayBuffer[AggregateExpr]
    val calcColumnNames = calc.getRowType.getFieldNames
    var aggColumnIndex = 0

    localAgg.getRowType.getFieldList.asScala.foreach(field => {
      val calcColumnIndex = calcColumnNames.indexOf(field.getName)
      if (calcColumnIndex >= 0) {
        // this output column is from input
        aggregates.append(calcOutputColumns.get(calcColumnIndex))
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

  def getAggregateExprsFromCalc(calc: BatchExecCalc, inputColumns: ArrayBuffer[AggregateExprColumn])
  : Option[ArrayBuffer[AggregateExpr]] = {

    val program = calc.getProgram

    // check if condition exists. all filters should have been pushed down
    if (program.getCondition != null) {
      return None
    }

    // make temp expr columns
    val calcExprColumns = new ArrayBuffer[AggregateExpr]
    program.getExprList.asScala.foreach {
      case input: RexInputRef =>
        calcExprColumns.append(inputColumns(input.getIndex))

      case literal: RexLiteral =>
        if (literal.getTypeName.getFamily == SqlTypeFamily.NUMERIC) {
          // convert all numeric types to decimal, making it easier to compute in c++ layer
          val decimalValue = new java.math.BigDecimal(literal.getValue.toString)
          calcExprColumns.append(
            new AggregateExprLiteral(
              "%s(%d,%d)".format(
                SqlTypeName.DECIMAL.toString, decimalValue.precision(), decimalValue.scale()),
              literal.getValue.toString)
          )
        } else {
          calcExprColumns.append(
            new AggregateExprLiteral(literal.getType.toString, literal.getValue.toString)
          )
        }

      case call: RexCall =>
        call.getOperator match {
          case binaryOp: SqlBinaryOperator =>
            val opType = AggregateExprBinaryOper.Type.fromSqlKind(binaryOp.kind.sql)
            if (opType != null
                && call.getOperands.get(0).isInstanceOf[RexLocalRef]
                && call.getOperands.get(1).isInstanceOf[RexLocalRef]) {
              val index0 = call.getOperands.get(0).asInstanceOf[RexLocalRef].getIndex
              val index1 = call.getOperands.get(1).asInstanceOf[RexLocalRef].getIndex
              val binaryExpr = new AggregateExprBinaryOper(
                opType,
                call.getType.toString,
                calcExprColumns(index0),
                calcExprColumns(index1)
              )
              if (call.getType != null) {
                binaryExpr.setCheckOverflow(true)
                binaryExpr.setCheckOverflowType(call.getType.toString)
              }
              calcExprColumns.append(binaryExpr)
            } else {
              return None
            }
          case _ =>
            return None
        }

      case _ =>
        return None
    }

    // project columns
    val calcOutputColumns = new ArrayBuffer[AggregateExpr]
    program.getProjectList.asScala.foreach(r => calcOutputColumns.append(calcExprColumns(r.getIndex)))

    Some(calcOutputColumns)
  }

}

object PushLocalHashAggCalcIntoTableSourceScanRule {
  val INSTANCE = new PushLocalHashAggCalcIntoTableSourceScanRule()
}
