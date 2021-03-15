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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.intel.ape.parquet.ApeLikeFilter;
import org.apache.parquet.filter2.predicate.*;


public class ParquetFilterPredicateConvertor {
  public static String toJsonString(FilterPredicate predicate) {
    return constructTree(predicate).toString();
  }

  public static JsonNode toJson(FilterPredicate predicate) {
    return constructTree(predicate);
  }

  private static JsonNode constructTree(FilterPredicate predicate) {
    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode rootNode = objectMapper.createObjectNode();

    if (predicate instanceof Operators.Not) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("child",
              constructTree(((Operators.Not) predicate).getPredicate()));
    } else if (predicate instanceof Operators.And) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("LeftNode",
              constructTree(((Operators.And) predicate).getLeft()));
      ((ObjectNode) rootNode).put("RightNode",
              constructTree(((Operators.And) predicate).getRight()));
    } else if (predicate instanceof Operators.Or) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("LeftNode",
              constructTree(((Operators.Or) predicate).getLeft()));
      ((ObjectNode) rootNode).put("RightNode",
              constructTree(((Operators.Or) predicate).getRight()));
    } else if (predicate instanceof Operators.Lt) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("ColumnName",
              ((Operators.Lt) predicate).getColumn().getColumnPath().toDotString());
      ((ObjectNode) rootNode).put("ColumnType",
              ((Operators.Lt) predicate).getValue().getClass().getSimpleName());
      ((ObjectNode) rootNode).put("Value",
              ((Operators.Lt) predicate).getValue().toString());
    } else if (predicate instanceof Operators.LtEq) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("ColumnName",
              ((Operators.LtEq) predicate).getColumn().getColumnPath().toDotString());
      ((ObjectNode) rootNode).put("ColumnType",
              ((Operators.LtEq) predicate).getValue().getClass().getSimpleName());
      ((ObjectNode) rootNode).put("Value",
              ((Operators.LtEq) predicate).getValue().toString());
    } else if (predicate instanceof Operators.Eq) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("ColumnName",
              ((Operators.Eq) predicate).getColumn().getColumnPath().toDotString());
      Object v = ((Operators.Eq) predicate).getValue();
      if (v == null) {
        ((ObjectNode) rootNode).put("ColumnType", "Null");
        ((ObjectNode) rootNode).put("Value", "null");
      } else {
        ((ObjectNode) rootNode).put("ColumnType",
                ((Operators.Eq) predicate).getValue().getClass().getSimpleName());
        ((ObjectNode) rootNode).put("Value",
                ((Operators.Eq) predicate).getValue().toString());
      }
    } else if (predicate instanceof Operators.NotEq) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("ColumnName",
              ((Operators.NotEq) predicate).getColumn().getColumnPath().toDotString());
      Object v = ((Operators.NotEq) predicate).getValue();
      if (v == null) {
        ((ObjectNode) rootNode).put("ColumnType", "Null");
        ((ObjectNode) rootNode).put("Value", "null");
      } else {
        ((ObjectNode) rootNode).put("ColumnType",
                ((Operators.NotEq) predicate).getValue().getClass().getSimpleName());
        ((ObjectNode) rootNode).put("Value",
                ((Operators.NotEq) predicate).getValue().toString());
      }
    } else if (predicate instanceof Operators.Gt) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("ColumnName",
              ((Operators.Gt) predicate).getColumn().getColumnPath().toDotString());
      ((ObjectNode) rootNode).put("ColumnType",
              ((Operators.Gt) predicate).getValue().getClass().getSimpleName());
      ((ObjectNode) rootNode).put("Value",
              ((Operators.Gt) predicate).getValue().toString());
    } else if (predicate instanceof Operators.GtEq) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              predicate.getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("ColumnName",
              ((Operators.GtEq) predicate).getColumn().getColumnPath().toDotString());
      ((ObjectNode) rootNode).put("ColumnType",
              ((Operators.GtEq) predicate).getValue().getClass().getSimpleName());
      ((ObjectNode) rootNode).put("Value",
              ((Operators.GtEq) predicate).getValue().toString());
    } else if (predicate instanceof Operators.UserDefined) {
      ((ObjectNode) rootNode).put("FilterTypeName",
              ((Operators.UserDefined) predicate).getUserDefinedPredicate()
                      .getClass().getSimpleName().toLowerCase());
      ((ObjectNode) rootNode).put("ColumnName",
              ((Operators.UserDefined) predicate).getColumn().getColumnPath().toDotString());
      ((ObjectNode) rootNode).put("ColumnType",
              ((Operators.UserDefined) predicate).getColumn().getClass().getSimpleName());
      if (((Operators.UserDefined) predicate).getUserDefinedPredicate() instanceof ApeLikeFilter) {
        ((ObjectNode) rootNode).put("Value",
              ((ApeLikeFilter)((Operators.UserDefined)predicate)
                      .getUserDefinedPredicate()).getValue());
      }
    }

    return rootNode;
  }


}
