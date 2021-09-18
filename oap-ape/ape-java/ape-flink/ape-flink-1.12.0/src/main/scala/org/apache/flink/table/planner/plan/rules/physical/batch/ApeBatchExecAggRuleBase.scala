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

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.aggfunctions.DeclarativeAggregateFunction
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecLocalHashAggregate
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.ape.{AggregateExpr, AggregateExprLiteral, AggregateExprRoot, AggregateExprRootChild}

trait ApeBatchExecAggRuleBase extends BatchExecAggRuleBase{

  def flattenAggFunctions(localAgg: BatchExecLocalHashAggregate,
                          aggOutputTypes: Array[Array[DataType]],
                          inputColumns: ArrayBuffer[AggregateExpr])
  : Option[ArrayBuffer[AggregateExprRoot]] = {

    val aggregateColumns = new ArrayBuffer[AggregateExprRoot]
    var aggIndex = 0
    val typeFactory = localAgg.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    aggOutputTypes.foreach(types => {
      val aggCall = localAgg.getAggCallList(aggIndex)
      val aggCallFunction = localAgg.getAggCallToAggFunction.map(_._2).asJava.get(aggIndex)
      val isDistinct = aggCall.isDistinct

      // get agg function names
      val aggFuncNames = aggCallFunction match {
        case aggFunc: DeclarativeAggregateFunction =>
          aggFunc.aggBufferAttributes().map(_.getName)
        case _ => new Array[String](0)
      }
      if (aggFuncNames.length == 0 || aggFuncNames.length != types.length) {
        return None
      }

      // in this rule, arg list should mostly contains 1 argument. count1 have 0 argument
      if (aggCall.getArgList.asScala.length > 1
          || (aggCall.getArgList.isEmpty && !aggFuncNames(0).equals("count1"))) {
        return None
      }

      // flatten output types of each agg call
      val column = if (aggCall.getArgList.size() > 0) {
        inputColumns(aggCall.getArgList.get(0))
      } else {
        val dataType = typeFactory.createSqlType(SqlTypeName.INTEGER)
        new AggregateExprLiteral(dataType.toString, "1")
      }
      var typeIndex = 0
      types.foreach(_ => {
        val aggType = AggregateExprRootChild.Type.fromSqlKind(aggFuncNames(typeIndex))

        // check if not supported
        if (aggType == null) {
          return None
        }

        val aggChild = new AggregateExprRootChild(aggType, column)
        val aggRoot = new AggregateExprRoot(isDistinct, aggChild)
        aggregateColumns.append(aggRoot)

        typeIndex += 1
      })

      aggIndex += 1
    })

    Some(aggregateColumns)
  }


}
