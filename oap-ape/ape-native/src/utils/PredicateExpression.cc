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

#include <algorithm>
#include <chrono>
#include <cstring>
#include <vector>

#include <arrow/util/logging.h>

#include "src/utils/PredicateExpression.h"
#include "src/utils/Expression.h"
#include "src/utils/Type.h"

namespace ape {

class prFinder {
 public:
  explicit prFinder(const std::string& cmp_str) : str(cmp_str) {}

  bool operator()(Schema& v) { return v.getColName().compare(str) == 0; }

 private:
  const std::string str;
};

// Base class
PredicateExpression::PredicateExpression(std::string type_) : Expression() { type = type_; }

PredicateExpression::~PredicateExpression() {}

// RootPredicateExpression
RootPredicateExpression::RootPredicateExpression(std::string type_,
                                           std::shared_ptr<PredicateExpression> child_)
    : PredicateExpression(type_) {
  child = child_;
}

int RootPredicateExpression::PredicateWithParam(int8_t& out) {
  // root node doesn't need outbuffer
  int8_t childOut;
  auto start1 = std::chrono::steady_clock::now();
  child->PredicateWithParam(childOut);
  auto end1 = std::chrono::steady_clock::now();
  ARROW_LOG(DEBUG) << "exec takes "
                   << static_cast<std::chrono::duration<double>>(end1 - start1).count() *
                          1000
                   << " ms";

  auto end2 = std::chrono::steady_clock::now();
  ARROW_LOG(DEBUG) << "copy takes "
                   << static_cast<std::chrono::duration<double>>(end2 - end1).count() *
                          1000
                   << " ms";
  out = childOut;
  return 0;
}

RootPredicateExpression::~RootPredicateExpression() {}

// NotPredicateExpression
NotPredicateExpression::NotPredicateExpression(std::string type_,
                                         std::shared_ptr<PredicateExpression> child_)
    : PredicateExpression(type_) {
  child = child_;
}

NotPredicateExpression::~NotPredicateExpression() {}

int NotPredicateExpression::PredicateWithParam(int8_t& out) {
  int8_t childOut;
  child->PredicateWithParam(childOut);

  out = 1 - childOut;

  return 0;
}

// BinaryPredicateExpression
BinaryPredicateExpression::BinaryPredicateExpression(std::string type_,
                                               std::shared_ptr<PredicateExpression> left_,
                                               std::shared_ptr<PredicateExpression> right_)
    : PredicateExpression(type_) {
  left = left_;
  right = right_;
  opType = type_;
}

BinaryPredicateExpression::~BinaryPredicateExpression() {}

int BinaryPredicateExpression::PredicateWithParam(int8_t& out) {
  int8_t leftChild;
  int8_t rightChild;
  
  left->PredicateWithParam(leftChild);

  if (opType.compare("and") == 0 && leftChild < 1) {
    out = 0;
    return 0;
  } else if (opType.compare("or") == 0 && leftChild > 0) {
    out = 1;
    return 0;
  }

  right->PredicateWithParam(rightChild);

  if (opType.compare("and") == 0) {
    out = leftChild & rightChild;
  } else if (opType.compare("or") == 0) {
    out = leftChild | rightChild;
  }

  return 0;
}

// StringPredicateExpression
StringPredicateExpression::StringPredicateExpression(std::string type_, std::string columnName_,
                                               std::string value_)
    : UnaryPredicateExpression(type_, columnName_) {
  value = value_;
}

void StringPredicateExpression::setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
  schema = schema_;
  ptrdiff_t pos = std::distance(
      schema->begin(), std::find_if(schema->begin(), schema->end(), prFinder(columnName)));
  columnIndex = pos;
}

std::string StringPredicateExpression::getColumnName() { return columnName; }

// StartWithPredicateExpression
int StartWithPredicateExpression::PredicateWithParam(int8_t& out) {
  //currently string type doesn't support predicate operation
  out = 1;
  return 0;
}

// EndWithPredicateExpression
int EndWithPredicateExpression::PredicateWithParam(int8_t& out) {
  //currently string type doesn't support predicate operation
  out = 1;
  return 0;
}

// ContainsPredicateExpression
int ContainsPredicateExpression::PredicateWithParam(int8_t& out) {
  //currently string type doesn't support predicate operation
  out = 1;
  return 0;
}

// UnaryPredicateExpression
template <typename T>
TypedUnaryPredicateExpression<T>::TypedUnaryPredicateExpression(std::string type_,
                                                          std::string columnName_,
                                                          T value_)
    : UnaryPredicateExpression(type_, columnName_) {
  value = value_;
  compareType = type_;
}

template <>
int TypedUnaryPredicateExpression<NullStruct>::PredicateWithParam(int8_t& out) {
  out = 1;

  return 0;
}

template <>
int TypedUnaryPredicateExpression<parquet::ByteArray>::PredicateWithParam(int8_t& out) {
  out = 1;

  return 0;
}

template <typename T>
int TypedUnaryPredicateExpression<T>::PredicateWithParam(int8_t& out) {

  if(!hasMinMax)
  {
    out = 1;
    return 0;
  }
  ARROW_LOG(DEBUG)<<"columnName: "<<columnName<<"  minVal: "<<minVal<<"  maxVal: "<<maxVal<<"  ,Destvalue: ";
  if (compareType.compare("gt") == 0) {
    out = maxVal > value ? 1 : 0;
  } else if (compareType.compare("gteq") == 0) {
    out = maxVal >= value ? 1 : 0;
  } else if (compareType.compare("eq") == 0) {
    out = (maxVal >= value && minVal <= value) ? 1 : 0;
  } else if (compareType.compare("noteq") == 0) {
    out = 1;
  } else if (compareType.compare("lt") == 0) {
    out = minVal < value ? 1 : 0;
  } else if (compareType.compare("lteq") == 0) {
    out = minVal <= value ? 1 : 0;
  } else {
    ARROW_LOG(WARNING) << "NOT support Predicate type!";
  }

  return 0;
}

template <>
void TypedUnaryPredicateExpression<int>::setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_)
{
  std::unique_ptr<parquet::ColumnChunkMetaData> columnChunkMeta = rgMataData_->ColumnChunk(columnIndex);
  std::shared_ptr<parquet::Statistics> statistic = columnChunkMeta->statistics();
  hasMinMax = statistic->HasMinMax();
  auto int32Statistic = std::static_pointer_cast<parquet::Int32Statistics>(statistic);
  maxVal = int32Statistic->max();
  minVal = int32Statistic->min();
}

template <>
void TypedUnaryPredicateExpression<int64_t>::setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_)
{
  std::unique_ptr<parquet::ColumnChunkMetaData> columnChunkMeta = rgMataData_->ColumnChunk(columnIndex);
  std::shared_ptr<parquet::Statistics> statistic = columnChunkMeta->statistics();
  hasMinMax = statistic->HasMinMax();
  auto int64Statistic = std::static_pointer_cast<parquet::Int64Statistics>(statistic);
  maxVal = int64Statistic->max();
  minVal = int64Statistic->min();
}

template <>
void TypedUnaryPredicateExpression<float>::setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_)
{
  std::unique_ptr<parquet::ColumnChunkMetaData> columnChunkMeta = rgMataData_->ColumnChunk(columnIndex);
  std::shared_ptr<parquet::Statistics> statistic = columnChunkMeta->statistics();
  hasMinMax = statistic->HasMinMax();
  auto floatStatistic = std::static_pointer_cast<parquet::FloatStatistics>(statistic);
  maxVal = floatStatistic->max();
  minVal = floatStatistic->min();
}

template <>
void TypedUnaryPredicateExpression<double>::setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_)
{
  std::unique_ptr<parquet::ColumnChunkMetaData> columnChunkMeta = rgMataData_->ColumnChunk(columnIndex);
  std::shared_ptr<parquet::Statistics> statistic = columnChunkMeta->statistics();
  hasMinMax = statistic->HasMinMax();
  auto doubleStatistic = std::static_pointer_cast<parquet::DoubleStatistics>(statistic);
  maxVal = doubleStatistic->max();
  minVal = doubleStatistic->min();
}

template <typename T>
void TypedUnaryPredicateExpression<T>::setStatistic(std::shared_ptr<parquet::RowGroupMetaData> rgMataData_)
{
  hasMinMax = false;
}

template <typename T>
void TypedUnaryPredicateExpression<T>::setSchema(
    std::shared_ptr<std::vector<Schema>> schema_) {
  schema = schema_;
  ptrdiff_t pos = std::distance(
      schema->begin(), std::find_if(schema->begin(), schema->end(), prFinder(columnName)));
  columnIndex = pos;
}

std::string UnaryPredicateExpression::getColumnName() { return columnName; }

template <typename T>
TypedUnaryPredicateExpression<T>::~TypedUnaryPredicateExpression() {}

// Force compile these classes.
template class TypedUnaryPredicateExpression<bool>;
template class TypedUnaryPredicateExpression<int>;
template class TypedUnaryPredicateExpression<int64_t>;
template class TypedUnaryPredicateExpression<float>;
template class TypedUnaryPredicateExpression<double>;
template class TypedUnaryPredicateExpression<NullStruct>;
template class TypedUnaryPredicateExpression<parquet::ByteArray>;

}  // namespace ape
