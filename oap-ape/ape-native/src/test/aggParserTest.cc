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

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "src/utils/AggExpression.h"
#include "src/utils/JsonConvertor.h"

TEST(AggParserTest, IllegalInput) {
  std::string json = "";
  auto exprs = ape::JsonConvertor::parseToAggExpressions(json);
  EXPECT_EQ(exprs.size(), 0);
}

TEST(AggParserTest, SimpleCase) {
  std::string json =
      " { \"aggregateExprs\": [{\"exprName\": \"AttributeReference\",\"dataType\": "
      "\"StringType\",\"columnName\": \"l_returnflag\"},{\"exprName\": "
      "\"AttributeReference\",\"dataType\": \"StringType\",\"columnName\": "
      "\"l_linestatus\"},{\"aliasName\": \"sum_qty\",\"exprName\": "
      "\"RootAgg\",\"isDistinct\": false,\"child\": {\"exprName\": \"Sum\",\"child\": "
      "{\"exprName\": \"AttributeReference\",\"dataType\": "
      "\"DecimalType(12,2)\",\"columnName\": \"l_quantity\"}}}]}";
  auto exprs = ape::JsonConvertor::parseToAggExpressions(json);
  EXPECT_EQ(exprs.size(), 3);
}
