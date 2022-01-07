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

#include <parquet/types.h>
#include <parquet/api/reader.h>

#include "src/utils/Expression.h"
#include "src/utils/Type.h"

namespace ape {

class PredicateExpression : public Expression {
 public:
  explicit PredicateExpression(std::string type_);
  virtual void Execute() {}
  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                               const std::vector<int64_t>& nullBuffers,
                               std::vector<int8_t>& outBuffers) {
    return 0;
  }
  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {}
  virtual int PredicateWithParam(int8_t& out) = 0;
  virtual void setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_) = 0;
  ~PredicateExpression();
};

class RootPredicateExpression : public PredicateExpression {
 public:
  RootPredicateExpression(std::string type_, std::shared_ptr<PredicateExpression> child_);
  void Execute() {}
  int PredicateWithParam(int8_t& out);
  ~RootPredicateExpression();
  void setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_){
    child->setStatistic(rgMataData_);
  }
  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
    schema = schema_;
    child->setSchema(schema);
  }

 private:
  std::shared_ptr<PredicateExpression> child;
};

class NotPredicateExpression : public PredicateExpression {
 public:
  NotPredicateExpression(std::string type_, std::shared_ptr<PredicateExpression> child_);
  void Execute() {}
  int PredicateWithParam(int8_t& out);
  void setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_){
    child->setStatistic(rgMataData_);
  }
  ~NotPredicateExpression();
  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
    schema = schema_;
    child->setSchema(schema);
  }
  std::shared_ptr<PredicateExpression> getChild() { return child; }

 private:
  std::shared_ptr<PredicateExpression> child;
};

class BinaryPredicateExpression : public PredicateExpression {
 public:
  BinaryPredicateExpression(std::string type_, std::shared_ptr<PredicateExpression> left_,
                         std::shared_ptr<PredicateExpression> right_);
  void Execute() {}
  int PredicateWithParam(int8_t& out);
  ~BinaryPredicateExpression();
  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
    schema = schema_;
    left->setSchema(schema);
    right->setSchema(schema);
  }
  void setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_){
    left->setStatistic(rgMataData_);
    right->setStatistic(rgMataData_);
  }
  std::shared_ptr<PredicateExpression> getLeftChild() { return left; }
  std::shared_ptr<PredicateExpression> getRightChild() { return right; }

 private:
  std::shared_ptr<PredicateExpression> left;
  std::shared_ptr<PredicateExpression> right;
  std::string opType;
};

class UnaryPredicateExpression : public PredicateExpression {
 public:
  UnaryPredicateExpression(std::string type_, std::string columnName_)
      : PredicateExpression(type_) {
    columnName = columnName_;
  }
  ~UnaryPredicateExpression() {}
  std::string getColumnName();

 protected:
  std::string columnName;
};

template <typename T>
class TypedUnaryPredicateExpression : public UnaryPredicateExpression {
 public:
  TypedUnaryPredicateExpression(std::string type_, std::string columnName_, T value_);
  void Execute() {}
  int PredicateWithParam(int8_t& out);
  ~TypedUnaryPredicateExpression();
  void setSchema(std::shared_ptr<std::vector<Schema>> schema_);
  void setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_);

 private:
  std::string compareType;
  T value;
  T minVal;
  T maxVal;
  bool hasMinMax;
  int columnIndex;
};

class StringPredicateExpression : public UnaryPredicateExpression {
 public:
  StringPredicateExpression(std::string type_, std::string columnName_, std::string value_);
  ~StringPredicateExpression() {}
  void setSchema(std::shared_ptr<std::vector<Schema>> schema_);
  void setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_) {}
  int PredicateWithParam(int8_t& out) = 0;
  std::string getColumnName();

 protected:
  std::string type;
  std::string value;
  int columnIndex;
};

class StartWithPredicateExpression : public StringPredicateExpression {
 public:
  StartWithPredicateExpression(std::string type_, std::string columnName_,
                            std::string value_)
      : StringPredicateExpression(type_, columnName_, value_) {}
  int PredicateWithParam(int8_t& out);
  ~StartWithPredicateExpression() {}
};

class EndWithPredicateExpression : public StringPredicateExpression {
 public:
  EndWithPredicateExpression(std::string type_, std::string columnName_, std::string value_)
      : StringPredicateExpression(type_, columnName_, value_) {}
  int PredicateWithParam(int8_t& out);
  ~EndWithPredicateExpression() {}
};

class ContainsPredicateExpression : public StringPredicateExpression {
 public:
  ContainsPredicateExpression(std::string type_, std::string columnName_, std::string value_)
      : StringPredicateExpression(type_, columnName_, value_) {}
  int PredicateWithParam(int8_t& out);
  ~ContainsPredicateExpression() {}
};

using BoolUnaryPredicateExpression = TypedUnaryPredicateExpression<bool>;
using Int32UnaryPredicateExpression = TypedUnaryPredicateExpression<int32_t>;
using Int64UnaryPredicateExpression = TypedUnaryPredicateExpression<int64_t>;
// using Int96UnaryPredicateExpression = TypedUnaryExpression<int96_t>;
using FloatUnaryPredicateExpression = TypedUnaryPredicateExpression<float>;
using DoubleUnaryPredicateExpression = TypedUnaryPredicateExpression<double>;
using NullUnaryPredicateExpression = TypedUnaryPredicateExpression<NullStruct>;
using ByteArrayUnaryPredicateExpression = TypedUnaryPredicateExpression<parquet::ByteArray>;

}  // namespace ape
