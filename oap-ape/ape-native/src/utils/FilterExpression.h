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

#include "src/utils/BinaryOp.h"
#include "src/utils/UnaryFilter.h"
#include "src/utils/expression.h"
#include "src/utils/type.h"

namespace ape {

class FilterExpression : public Expression {
 public:
  explicit FilterExpression(std::string type_);
  virtual void Execute() {}
  virtual int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                               char* outBuffers) {
    return 0;
  }
  void setSchema(std::vector<Schema> schema_) {}
  ~FilterExpression();
};

class RootFilterExpression : public FilterExpression {
 public:
  RootFilterExpression(std::string type_, std::shared_ptr<FilterExpression> child_);
  void Execute() {}
  int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                       char* outBuffers);
  ~RootFilterExpression();
  void setSchema(std::vector<Schema> schema_) {
    schema = schema_;
    child->setSchema(schema);
  }

 private:
  std::shared_ptr<Expression> child;
};

class NotFilterExpression : public FilterExpression {
 public:
  NotFilterExpression(std::string type_, std::shared_ptr<Expression> child_);
  void Execute() {}
  int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                       char* outBuffers);
  ~NotFilterExpression();
  void setSchema(std::vector<Schema> schema_) {
    schema = schema_;
    child->setSchema(schema);
  }
  std::shared_ptr<Expression> getChild() { return child; }

 private:
  std::shared_ptr<Expression> child;
};

class BinaryFilterExpression : public FilterExpression {
 public:
  BinaryFilterExpression(std::string type_, std::shared_ptr<Expression> left_,
                         std::shared_ptr<Expression> right_);
  void Execute() {}
  int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                       char* outBuffers);
  ~BinaryFilterExpression();
  void setSchema(std::vector<Schema> schema_) {
    schema = schema_;
    left->setSchema(schema);
    right->setSchema(schema);
  }
  std::shared_ptr<Expression> getLeftChild() { return left; }
  std::shared_ptr<Expression> getRightChild() { return right; }

 private:
  std::shared_ptr<Expression> left;
  std::shared_ptr<Expression> right;
  std::shared_ptr<BinaryOp> op;
};

class UnaryFilterExpression : public FilterExpression {
 public:
  UnaryFilterExpression(std::string type_, std::string columnName_)
      : FilterExpression(type_) {
    columnName = columnName_;
  }
  ~UnaryFilterExpression() {}
  std::string getColumnName();

 protected:
  std::string columnName;
};

template <typename T>
class TypedUnaryFilterExpression : public UnaryFilterExpression {
 public:
  TypedUnaryFilterExpression(std::string type_, std::string columnName_, T value_);
  void Execute() {}
  int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                       char* outBuffers);
  ~TypedUnaryFilterExpression();
  void setSchema(std::vector<Schema> schema_);

 private:
  std::shared_ptr<UnaryFilter<T>> filter;
  T value;
  int columnIndex;
};

class StringFilterExpression : public UnaryFilterExpression {
 public:
  StringFilterExpression(std::string type_, std::string columnName_, std::string value_);
  ~StringFilterExpression() {}
  void setSchema(std::vector<Schema> schema_);
  int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                       char* outBuffers) = 0;
  std::string getColumnName();

 protected:
  std::string type;
  std::string columnName;
  std::string value;
  int columnIndex;
};

class StartWithFilterExpression : public StringFilterExpression {
 public:
  StartWithFilterExpression(std::string type_, std::string columnName_,
                            std::string value_)
      : StringFilterExpression(type_, columnName_, value_) {}
  int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                       char* outBuffers);
  ~StartWithFilterExpression() {}
};

class EndWithFilterExpression : public StringFilterExpression {
 public:
  EndWithFilterExpression(std::string type_, std::string columnName_, std::string value_)
      : StringFilterExpression(type_, columnName_, value_) {}
  int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                       char* outBuffers);
  ~EndWithFilterExpression() {}
};

class ContainsFilterExpression : public StringFilterExpression {
 public:
  ContainsFilterExpression(std::string type_, std::string columnName_, std::string value_)
      : StringFilterExpression(type_, columnName_, value_) {}
  int ExecuteWithParam(int batchSize, int64_t* dataBuffers, int64_t* nullBuffers,
                       char* outBuffers);
  ~ContainsFilterExpression() {}
};

using BoolUnaryFilterExpression = TypedUnaryFilterExpression<bool>;
using Int32UnaryFilterExpression = TypedUnaryFilterExpression<int32_t>;
using Int64UnaryFilterExpression = TypedUnaryFilterExpression<int64_t>;
// using Int96UnaryFilterExpression = TypedUnaryExpression<int96_t>;
using FloatUnaryFilterExpression = TypedUnaryFilterExpression<float>;
using DoubleUnaryFilterExpression = TypedUnaryFilterExpression<double>;
using NullUnaryFilterExpression = TypedUnaryFilterExpression<NullStruct>;
using ByteArrayUnaryFilterExpression = TypedUnaryFilterExpression<parquet::ByteArray>;

}  // namespace ape
