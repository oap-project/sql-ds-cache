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

#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include <nlohmann/json.hpp>

#include "src/utils/AggExpression.h"
#include "src/utils/FilterExpression.h"
#include "src/utils/PredicateExpression.h"
#include "src/utils/Expression.h"

namespace ape {

class JsonConvertor {
 public:
  static std::shared_ptr<Expression> parseToFilterExpression(std::string jsonString);
  static std::shared_ptr<Expression> parseToFilterExpression(nlohmann::json root);

  static std::shared_ptr<PredicateExpression> parseToPredicateExpression(std::string jsonString);
  static std::shared_ptr<PredicateExpression> parseToPredicateExpression(nlohmann::json root);

  static std::vector<std::shared_ptr<Expression>> parseToGroupByExpressions(
      std::string jsonString);
  static std::vector<std::shared_ptr<Expression>> parseToGroupByExpressions(
      nlohmann::json root);

  static std::vector<std::shared_ptr<Expression>> parseToAggExpressions(
      std::string jsonString,
      std::unordered_map<std::string, std::shared_ptr<WithResultExpression>>& cache);
  static std::vector<std::shared_ptr<Expression>> parseToAggExpressions(
      nlohmann::json root,
      std::unordered_map<std::string, std::shared_ptr<WithResultExpression>>& cache);

 private:
  static std::shared_ptr<WithResultExpression> parseToAggExpressionsHelper(
      nlohmann::json root,
      std::unordered_map<std::string, std::shared_ptr<WithResultExpression>>& cache);
};

}  // namespace ape
