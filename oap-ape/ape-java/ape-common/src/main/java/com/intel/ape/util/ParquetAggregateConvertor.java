/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.ape.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.*;

import java.util.ArrayList;
import java.util.List;

public class ParquetAggregateConvertor {
  public static String toJsonString(List<Expression> groupByExprs, List<Expression> aggExprs) {
    return toJson(groupByExprs, aggExprs).toString();
  }

  public static JsonNode toJson(List<Expression> groupByExprs, List<Expression> aggExprs) {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.createObjectNode();

    ArrayNode groupByArrayNode = objectMapper.createArrayNode();
    ArrayList<JsonNode> groupByList = new ArrayList<>();
    for (Expression expr : groupByExprs) {
      groupByList.add(constructTree(expr, null));
    }
    groupByArrayNode.addAll(groupByList);
    ((ObjectNode) rootNode).put("GroupByExprs", groupByArrayNode);

    ArrayNode AggArrayNode = objectMapper.createArrayNode();
    ArrayList<JsonNode> aggList = new ArrayList<>();
    for (Expression expr : aggExprs) {
      aggList.add(constructTree(expr, null));
    }
    AggArrayNode.addAll(aggList);
    ((ObjectNode) rootNode).put("AggregateExprs", AggArrayNode);

    return rootNode;
  }

  public static String toJsonString(Expression expr) {
    return constructTree(expr, null).toString();
  }

  private static JsonNode constructTree(Expression expr, JsonNode rootNode) {
    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode tmpNode = rootNode == null ? objectMapper.createObjectNode() : rootNode;
    List<Expression> exprs = scala.collection.JavaConverters.seqAsJavaList(expr.children());

    if (expr instanceof Alias) {
      Alias tmpExpr = (Alias) expr;
      ((ObjectNode) tmpNode).put("AliasName", tmpExpr.name());
      return constructTree(exprs.get(0), tmpNode);
    } else if (expr instanceof AggregateExpression) {  // this will be root node for a agg expr.
      AggregateExpression tmpExpr = (AggregateExpression) expr;
      ((ObjectNode) tmpNode).put("ExprName", "rootAgg");
      ((ObjectNode) tmpNode).put("isDistinct", tmpExpr.isDistinct());
      ((ObjectNode) tmpNode).put("child", constructTree(exprs.get(0), null));
      return tmpNode;
    } else if (expr instanceof Cast) {
      assert (exprs.size() == 1); // should only have one node
      Cast tmpExpr = (Cast) expr;
      ((ObjectNode) tmpNode).put("CastType", tmpExpr.dataType().toString());
      return constructTree(exprs.get(0), tmpNode);

    } else if (expr instanceof PromotePrecision) {
      assert (exprs.size() == 1); // should only have one node
      PromotePrecision tmpExpr = (PromotePrecision) expr;
      ((ObjectNode) tmpNode).put("PromotePrecision", true);
      return constructTree(exprs.get(0), tmpNode);

    } else if (expr instanceof CheckOverflow) {
      assert (exprs.size() == 1); // should only have one node
      CheckOverflow tmpExpr = (CheckOverflow) expr;
      ((ObjectNode) tmpNode).put("CheckOverflow", true);
      ((ObjectNode) tmpNode).put("CheckOverflowType", tmpExpr.dataType().toString());
      ((ObjectNode) tmpNode).put("nullOnOverFlow", tmpExpr.nullOnOverflow());
      return constructTree(exprs.get(0), tmpNode);

    } else if (expr instanceof Sum || expr instanceof Min || expr instanceof Max ||
            expr instanceof Average || expr instanceof Count) {  // use DeclarativeAggregate ?
      assert (exprs.size() == 1);
      ((ObjectNode) tmpNode).put("ExprName", expr.nodeName());
      ((ObjectNode) tmpNode).put("child", constructTree(exprs.get(0), null));
      return tmpNode;
    } else if (expr instanceof BinaryArithmetic) { // Add sub Multiply ...
      assert (exprs.size() == 2);
      ((ObjectNode) tmpNode).put("ExprName", expr.nodeName());
      ((ObjectNode) tmpNode).put("LeftNode", constructTree(exprs.get(0), null));
      ((ObjectNode) tmpNode).put("RightNode", constructTree(exprs.get(1), null));
      return tmpNode;

    } else if (expr instanceof AttributeReference) {  // leaf node AttributeReference
      assert (exprs.size() == 0);
      AttributeReference tmpExpr = (AttributeReference) expr;
      ((ObjectNode) tmpNode).put("ExprName", "AttributeReference");
      ((ObjectNode) tmpNode).put("DataType", tmpExpr.dataType().toString());
      ((ObjectNode) tmpNode).put("ColumnName", tmpExpr.name());
      return tmpNode;

    } else if (expr instanceof Literal) {   // leaf node AttributeReference
      assert (exprs.size() == 0);
      Literal tmpExpr = (Literal) expr;
      ((ObjectNode) tmpNode).put("ExprName", "Literal");
      ((ObjectNode) tmpNode).put("DataType", tmpExpr.dataType().toString());
      ((ObjectNode) tmpNode).put("Value", tmpExpr.value().toString());
      return tmpNode;

    } else {
      //TODO: will include other type?
      throw new UnsupportedOperationException("should not reach here.");

    }

  }

}
