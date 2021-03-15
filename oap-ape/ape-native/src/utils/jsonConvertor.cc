// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/util/logging.h>

#include "src/utils/jsonConvertor.h"

namespace ape {

std::shared_ptr<Expression> JsonConvertor::parseToFilterExpression(
    std::string jsonString) {
  ARROW_LOG(INFO) << "json string " << jsonString;
  auto json = nlohmann::json::parse(jsonString);
  return parseToFilterExpression(json);
}

std::shared_ptr<Expression> JsonConvertor::parseToFilterExpression(nlohmann::json root) {
  std::shared_ptr<Expression> ex;
  std::string type = root["FilterTypeName"];
  if (type.compare("not") == 0) {
    ex = std::make_shared<NotFilterExpression>(type,
                                               parseToFilterExpression(root["Child"]));
  } else if (type.compare("and") == 0 || type.compare("or") == 0) {
    nlohmann::json leftNode = root["LeftNode"];
    nlohmann::json RightNode = root["RightNode"];
    ex = std::make_shared<BinaryFilterExpression>(type, parseToFilterExpression(leftNode),
                                                  parseToFilterExpression(RightNode));
  } else if (type.compare("lt") == 0 || type.compare("lteq") == 0 ||
             type.compare("gt") == 0 || type.compare("gteq") == 0 ||
             type.compare("eq") == 0 || type.compare("noteq") == 0) {
    std::string colType = root["ColumnType"];
    std::string columnName = root["ColumnName"];
    std::string valueString = root["Value"];
    if (valueString.compare("null") == 0) {  // this will only match 'eq', 'noteq' type.
      NullStruct nullStruct;
      ex = std::make_shared<NullUnaryFilterExpression>(type, columnName, nullStruct);
    } else if (colType.compare("Boolean") == 0) {
      bool value = valueString == "false" ? false : true;  // FIXME
      ex = std::make_shared<BoolUnaryFilterExpression>(type, columnName, value);
    } else if (colType.compare("Integer") == 0) {
      int32_t value = std::stoi(valueString);
      ex = std::make_shared<Int32UnaryFilterExpression>(type, columnName, value);
    } else if (colType.compare("Long") == 0) {
      int64_t value = std::stol(valueString);
      ex = std::make_shared<Int64UnaryFilterExpression>(type, columnName, value);
    } else if (colType.compare("Float") == 0) {
      float value = std::stof(valueString);
      ex = std::make_shared<FloatUnaryFilterExpression>(type, columnName, value);
    } else if (colType.compare("Double") == 0) {
      double value = std::stod(valueString);
      ex = std::make_shared<DoubleUnaryFilterExpression>(type, columnName, value);
    } else if (colType.compare("FromStringBinary") == 0) {
      // the binary is like Binary{\"xxxxx\"}
      int binLen = valueString.length() - 10;
      std::string value = valueString.substr(8, binLen);
      uint8_t* buf = new uint8_t[binLen];
      // FIXME: memory leak!
      std::memcpy(buf, value.data(), binLen);
      parquet::ByteArray byteArray(binLen, buf);
      ex = std::make_shared<ByteArrayUnaryFilterExpression>(type, columnName, byteArray);
    } else {
      // WARNING: NOT support yet;
      ARROW_LOG(WARNING) << "unsupported data type";
    }
  } else if (type.compare("apestartwithfilter") == 0) {
    std::string colType = root["ColumnType"];  // should be BinaryColumn
    std::string columnName = root["ColumnName"];
    std::string valueString = root["Value"];  // start with string
    ex = std::make_shared<StartWithFilterExpression>(type, columnName, valueString);
  } else if (type.compare("apeendwithfilter") == 0) {
    std::string colType = root["ColumnType"];  // should be BinaryColumn
    std::string columnName = root["ColumnName"];
    std::string valueString = root["Value"];  // end with string
    ex = std::make_shared<EndWithFilterExpression>(type, columnName, valueString);
  } else if (type.compare("apecontainsfilter") == 0) {
    std::string colType = root["ColumnType"];  // should be BinaryColumn
    std::string columnName = root["ColumnName"];
    std::string valueString = root["Value"];  // end with string
    ex = std::make_shared<ContainsFilterExpression>(type, columnName, valueString);
  } else {
    ARROW_LOG(WARNING) << "unsupported Expression type" << type;
  }

  return ex;
}

std::vector<std::shared_ptr<Expression>> JsonConvertor::parseToGroupByExpressions(
    std::string jsonString) {
  ARROW_LOG(INFO) << "json string " << jsonString;
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(jsonString);
  } catch (const nlohmann::json::parse_error e) {
    std::cerr << e.what() << '\n';
    return std::vector<std::shared_ptr<Expression>>();
  }
  return parseToGroupByExpressions(json);
}

std::vector<std::shared_ptr<Expression>> JsonConvertor::parseToGroupByExpressions(
    nlohmann::json root) {
  auto start = std::chrono::steady_clock::now();
  std::vector<std::shared_ptr<Expression>> v;
  auto exprs = root["groupByExprs"];
  for (int i = 0; i < exprs.size(); i++) {
    auto expr = exprs[i];
    v.push_back(parseToAggExpressionsHelper(expr));
  }
  std::chrono::duration<double> duration = std::chrono::steady_clock::now() - start;
  ARROW_LOG(INFO) << "Parsing json takes " << duration.count() * 1000 << " ms.";
  return v;
}

std::vector<std::shared_ptr<Expression>> JsonConvertor::parseToAggExpressions(
    std::string jsonString) {
  ARROW_LOG(INFO) << "json string " << jsonString;
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(jsonString);
  } catch (const nlohmann::json::parse_error e) {
    std::cerr << e.what() << '\n';
    return std::vector<std::shared_ptr<Expression>>();
  }

  return parseToAggExpressions(json);
}

std::vector<std::shared_ptr<Expression>> JsonConvertor::parseToAggExpressions(
    nlohmann::json root) {
  auto start = std::chrono::steady_clock::now();
  std::vector<std::shared_ptr<Expression>> v;
  auto exprs = root["aggregateExprs"];
  for (int i = 0; i < exprs.size(); i++) {
    auto expr = exprs[i];
    v.push_back(parseToAggExpressionsHelper(expr));
  }
  std::chrono::duration<double> duration = std::chrono::steady_clock::now() - start;
  ARROW_LOG(INFO) << "Parsing json takes " << duration.count() * 1000 << " ms.";
  return v;
}

std::shared_ptr<WithResultExpression> JsonConvertor::parseToAggExpressionsHelper(
    nlohmann::json root) {
  std::string name = root["exprName"];
  if (name.compare("AttributeReference") == 0) {
    auto res = std::make_shared<AttributeReferenceExpression>();
    res->setAttribute(
        root["columnName"], root["dataType"],
        root.contains("castType") ? root["castType"] : "",
        root.contains("promotePrecision") ? (bool)root["promotePrecision"] : false);
    res->setType(name);
    return res;
  } else if (name.compare("Literal") == 0) {
    auto res = std::make_shared<LiteralExpression>();
    res->setAttribute(root["dataType"], root["value"]);
    res->setType(name);
    return res;
  } else if (name.compare("Sum") == 0 || name.compare("Average") == 0 ||
             name.compare("Count") == 0 || name.compare("Max") == 0 ||
             name.compare("Min") == 0) {
    std::shared_ptr<AggExpression> res = Gen::genAggExpression(name);
    res->setDataType(root.contains("dataType") ? root["dataType"] : "");
    res->setChild(parseToAggExpressionsHelper(root["child"]));
    res->setType(name);
    return res;
  } else if (name.compare("Add") == 0 || name.compare("Subtract") == 0 ||
             name.compare("Multiply") == 0 || name.compare("Divide") == 0 ||
             name.compare("Mod") == 0) {
    std::shared_ptr<ArithmeticExpression> res = Gen::genArithmeticExpression(name);
    res->setDataType(root.contains("dataType") ? root["dataType"] : "");
    res->setLeft(parseToAggExpressionsHelper(root["leftNode"]));
    res->setRight(parseToAggExpressionsHelper(root["rightNode"]));
    res->setAttribute(
        root.contains("checkOverflowType") ? root["checkOverflowType"] : "",
        root.contains("castType") ? root["castType"] : "",
        root.contains("promotePrecision") ? (bool)root["promotePrecision"] : false,
        root.contains("checkOverflow") ? (bool)root["checkOverflow"] : false,
        root.contains("nullOnOverFlow") ? (bool)root["nullOnOverFlow"] : false);
    res->setType(name);
    return res;
  } else if (name.compare("RootAgg") == 0) {
    auto res = std::make_shared<RootAggExpression>();
    res->setDataType(root.contains("dataType") ? root["dataType"] : "");
    res->setChild(parseToAggExpressionsHelper(root["child"]));
    res->setAttribute(root.contains("isDistinct") ? (bool)root["isDistinct"] : false,
                      root.contains("aliasName") ? root["aliasName"] : "");
    res->setType(name);
    return res;
  } else {
    ARROW_LOG(WARNING) << "Unsupported Expression " << name;
    return nullptr;
  }
}

}  // namespace ape
