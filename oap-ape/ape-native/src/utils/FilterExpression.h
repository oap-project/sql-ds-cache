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

#include "expression.h"
#include "type.h"

namespace ape {

class FilterExpression : public Expression {
 public:
  FilterExpression(std::string type_);
  virtual void Execute() {};
  ~FilterExpression();
};

class NotFilterExpression : public FilterExpression {
 public:
  NotFilterExpression(std::string type_, std::shared_ptr<Expression> child_);
  void Execute(){};
  ~NotFilterExpression();

 private:
  std::shared_ptr<Expression> child;
};

class BinaryFilterExpression : public FilterExpression {
 public:
  BinaryFilterExpression(std::string type_, std::shared_ptr<Expression> left_,
                         std::shared_ptr<Expression> right_);
  void Execute(){};
  ~BinaryFilterExpression();

 private:
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;
};

template <typename T>
class TypedUnaryFilterExpression : public FilterExpression {
 public:
  TypedUnaryFilterExpression(std::string type_, std::string columnName_, T value_);
  void Execute(){};
  ~TypedUnaryFilterExpression();

 private:
  std::string columnName;
  T value;
};

using BoolUnaryFilterExpression = TypedUnaryFilterExpression<bool>;
using Int32UnaryFilterExpression = TypedUnaryFilterExpression<int32_t>;
using Int64UnaryFilterExpression = TypedUnaryFilterExpression<int64_t>;
// using Int96UnaryFilterExpression = TypedUnaryExpression<int96_t>;
using FloatUnaryFilterExpression = TypedUnaryFilterExpression<float>;
using DoubleUnaryFilterExpression = TypedUnaryFilterExpression<double>;
using NullUnaryFilterExpression = TypedUnaryFilterExpression<NullStruct>;

}  // namespace ape
