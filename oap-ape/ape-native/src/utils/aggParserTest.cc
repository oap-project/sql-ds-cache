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

#include "AggExpression.h"
#include "jsonConvertor.h"

void case1() {
  std::cout << "test illegal input string case " << std::endl;
  std::string json = "";
  auto exprs = ape::JsonConvertor::parseToAggExpressions(json);
  assert(exprs.size() == 0);
}

void case2() {
  std::cout << "test simple agg case " << std::endl;
  std::string json =
      " { \"AggregateExprs\": [{\"ExprName\": \"AttributeReference\",\"DataType\": "
      "\"StringType\",\"ColumnName\": \"l_returnflag\"},{\"ExprName\": "
      "\"AttributeReference\",\"DataType\": \"StringType\",\"ColumnName\": "
      "\"l_linestatus\"},{\"AliasName\": \"sum_qty\",\"ExprName\": "
      "\"RootAgg\",\"IsDistinct\": false,\"child\": {\"ExprName\": \"Sum\",\"Child\": "
      "{\"ExprName\": \"AttributeReference\",\"DataType\": "
      "\"DecimalType(12,2)\",\"ColumnName\": \"l_quantity\"}}}]}";
  auto exprs = ape::JsonConvertor::parseToAggExpressions(json);
  assert(exprs.size() == 3);
  for (auto expr : exprs) {
    std::static_pointer_cast<ape::WithResultExpression>(expr)->getResult();
  }
}

int main() {
  case1();
  case2();
  return 0;
}
