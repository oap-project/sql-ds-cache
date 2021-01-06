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

#include "jsonConvertor.h"

namespace ape {

std::shared_ptr<Expression> JsonConvertor::parseToFilterExpression(
    std::string jsonString) {
  auto json = nlohmann::json::parse(jsonString);
  return parseToFilterExpression(json);
};

std::shared_ptr<Expression> JsonConvertor::parseToFilterExpression(nlohmann::json root) {
  std::shared_ptr<Expression> ex;
  std::string type = root["FilterTypeName"];
  if (type.compare("not") == 0) {
    ex = std::make_shared<NotFilterExpression>(type,
                                               parseToFilterExpression(root["child"]));
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
    } else {
      // WARNING: NOT support yet;
      ARROW_LOG(WARNING) << "unsupported data type";
    }
  } else {
    ARROW_LOG(WARNING) << "unsupported Expression type";
  }

  return ex;
};

}  // namespace ape
