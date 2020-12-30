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

#include "expression.h"
#include "FilterExpression.h"
#include "type.h"

namespace ape {

FilterExpression::FilterExpression(std::string type_) : Expression() { type = type_; }

FilterExpression::~FilterExpression() {};

NotFilterExpression::NotFilterExpression(std::string type_,
                                         std::shared_ptr<Expression> child_)
    : FilterExpression(type_) {
  child = child_;
}
NotFilterExpression::~NotFilterExpression() {};

BinaryFilterExpression::BinaryFilterExpression(std::string type_,
                                               std::shared_ptr<Expression> left_,
                                               std::shared_ptr<Expression> right_)
    : FilterExpression(type_) {
  left = left_;
  right = right_;
}

BinaryFilterExpression::~BinaryFilterExpression() {};

template <typename T>
TypedUnaryFilterExpression<T>::TypedUnaryFilterExpression(std::string type_,
                                                          std::string columnName_,
                                                          T value_)
    : FilterExpression(type_) {
  columnName = columnName_;
  value = value_;
}

template<typename T>
TypedUnaryFilterExpression<T>::~TypedUnaryFilterExpression() {};

// Force compile these classes.
template class TypedUnaryFilterExpression<bool>;
template class TypedUnaryFilterExpression<int>;
template class TypedUnaryFilterExpression<long>;
template class TypedUnaryFilterExpression<float>;
template class TypedUnaryFilterExpression<double>;
template class TypedUnaryFilterExpression<NullStruct>;

}  // namespace ape
