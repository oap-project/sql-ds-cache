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

#include <arrow/util/decimal.h>
#include <arrow/util/logging.h>

#include "src/utils/ApeDecimal.h"
#include "src/utils/DecimalConvertor.h"
#include "src/utils/DecimalUtil.h"
#include "src/utils/Expression.h"

namespace ape {

class WithResultExpression : public Expression {
 public:
  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                       const std::vector<int64_t>& nullBuffers,
                       std::vector<int8_t>& outBuffers) {
    return 0;
  }
  virtual void reset() { done = false; }
  void Execute() {}
  ~WithResultExpression() {}

  virtual void getResult(DecimalVector& result, const int& groupNum = 1,
                         const std::vector<int>& index = std::vector<int>()) {
    // should never be called.
  }

  void setDataType(std::string dataType_) { dataType = dataType_; }
  void setType(std::string type_) { type = type_; }

  std::string getDataType() { return dataType; }
  std::string getColumnName() { return columnName; }

 protected:
  std::string dataType;
  std::string columnName;
  std::string castType;
  bool isDistinct = false;
  bool done = false;
};

class RootAggExpression : public WithResultExpression {
 public:
  ~RootAggExpression() {}
  void setChild(std::shared_ptr<WithResultExpression> child_) { child = child_; }
  void setAttribute(bool isDistinct_, std::string aliasName_) {
    isDistinct = isDistinct_;
    aliasName = aliasName_;
  }
  std::shared_ptr<Expression> getChild() { return child; }

  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                       const std::vector<int64_t>& nullBuffers,
                       std::vector<int8_t>& outBuffers);
  void reset() override {
    done = false;
    child->reset();
  }

  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
    schema = schema_;
    child->setSchema(schema);
  }

  void getResult(DecimalVector& result, const int& groupNum = 1,
                 const std::vector<int>& index = std::vector<int>()) override {
    child->getResult(result, groupNum, index);
  }

 private:
  bool isDistinct;
  std::string aliasName;
  std::shared_ptr<WithResultExpression> child;
};

class AggExpression : public WithResultExpression {
 public:
  // Avg will have two elements for Sum and Count
  ~AggExpression() {}
  void setChild(std::shared_ptr<WithResultExpression> child_) { child = child_; }
  std::shared_ptr<WithResultExpression> getChild() { return child; }

  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                       const std::vector<int64_t>& nullBuffers,
                       std::vector<int8_t>& outBuffers);
  void reset() override {
    done = false;
    child->reset();
  }

  void getResult(DecimalVector& result, const int& groupNum = 1,
                 const std::vector<int>& index = std::vector<int>()) override {
    if (!done) {
      if (groupNum == 1) {
        getResultInternal(result);
      } else {
        getResultInternalWithGroup(result, groupNum, index);
      }
      resultCache = result;
      done = true;
    } else {
      result = resultCache;
    }
  }

  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
    schema = schema_;
    child->setSchema(schema);
  }

 protected:
  std::shared_ptr<WithResultExpression> child;
  DecimalVector resultCache;

  // build cached DecimalVector resultCache
  virtual void getResultInternal(DecimalVector& result) {
    ARROW_LOG(INFO) << "should never be called";
  };

  virtual void getResultInternalWithGroup(DecimalVector& result, const int& groupNum,
                                          const std::vector<int>& index) {
    ARROW_LOG(INFO) << "should never be called";
  }

  // return -1 if all null.
  int findFisrtNonNull(const std::shared_ptr<std::vector<uint8_t>>& nullVector) {
    for (int i = 0; i < nullVector->size(); i++) {
      if (nullVector->at(i) == 1) return i;
    }
    return -1;
  }
};

class Sum : public AggExpression {
 public:
  ~Sum() {}

 private:
  void getResultInternal(DecimalVector& result) override {
    if (result.nullVector->size() == 0) {  // result should have only one row
      result.nullVector->resize(1);
    }
    auto tmp = DecimalVector();
    child->getResult(tmp);

    int first = findFisrtNonNull(tmp.nullVector);
    if (first != -1) {                      // if we found valid value in this batch
      if (result.nullVector->at(0) == 0) {  // if result never been initilized.
        assert(result.data.size() == 0);
        result.data.push_back(arrow::BasicDecimal128(0));
        result.nullVector->at(0) = 1;
        result.precision = tmp.precision;
        result.scale = tmp.scale;
        result.type = GetResultType(dataType);
      }
    } else {  // this batch is all null so we can return directly.
      return;
    }

    for (int i = 0; i < tmp.data.size(); i++) {
      if (tmp.nullVector->at(i)) {
        result.data[0] += tmp.data[i];
      }
    }
  }

  void getResultInternalWithGroup(DecimalVector& result, const int& groupNum,
                                  const std::vector<int>& index) override {
    if (result.data.size() != groupNum || result.nullVector->size() != groupNum) {
      result.data.resize(groupNum);
      result.nullVector->resize(groupNum);
    }
    auto tmp = DecimalVector();
    child->getResult(tmp);

    for (int i = 0; i < tmp.data.size(); i++) {
      if (tmp.nullVector->at(i)) {
        if (!result.nullVector->at(index[i])) {  // first time, do init
          result.nullVector->at(index[i]) = 1;
          result.data[index[i]] = arrow::BasicDecimal128(0);
        }
        result.data[index[i]] += tmp.data[i];
      }
    }
    result.precision = 38;  // tmp.precision;
    result.scale = tmp.scale;
    result.type = GetResultType(dataType);
  }
};

class Min : public AggExpression {
 public:
  ~Min() {}

 private:
  void getResultInternal(DecimalVector& result) override {
    if (result.nullVector->size() == 0) {  // result should have only one row
      result.nullVector->resize(1);
    }
    auto tmp = DecimalVector();
    child->getResult(tmp);

    int first = findFisrtNonNull(tmp.nullVector);
    if (first != -1) {                      // if we found valid value in this batch
      if (result.nullVector->at(0) == 0) {  // if result never been initilized.
        assert(result.data.size() == 0);
        result.data.push_back(arrow::BasicDecimal128(tmp.data[first]));
        result.nullVector->at(0) = 1;
        result.precision = tmp.precision;
        result.scale = tmp.scale;
        result.type = GetResultType(dataType);
      }
    } else {  // this batch is all null so we can return directly.
      return;
    }

    for (int i = 0; i < tmp.data.size(); i++) {
      if (tmp.nullVector->at(i)) {
        result.data[0] = result.data[0] < tmp.data[i] ? result.data[0] : tmp.data[i];
      }
    }
  }

  void getResultInternalWithGroup(DecimalVector& result, const int& groupNum,
                                  const std::vector<int>& index) override {
    auto tmp = DecimalVector();
    child->getResult(tmp);
    for (int i = 0; i < tmp.data.size(); i++) {
      if (tmp.nullVector->at(i)) {
        if (!result.nullVector->at(index[i])) {
          result.nullVector->at(index[i]) = 1;
          result.data[index[i]] = tmp.data[i];
        } else
          result.data[index[i]] =
              result.data[index[i]] < tmp.data[i] ? result.data[index[i]] : tmp.data[i];
      }
    }

    result.precision = 38;  // tmp.precision;
    result.scale = tmp.scale;
    result.type = GetResultType(dataType);
  }
};

class Max : public AggExpression {
 public:
  ~Max() {}

 private:
  void getResultInternal(DecimalVector& result) override {
    if (result.nullVector->size() == 0) {  // result should have only one row
      result.nullVector->resize(1);
    }
    auto tmp = DecimalVector();
    child->getResult(tmp);

    int first = findFisrtNonNull(tmp.nullVector);
    if (first != -1) {                      // if we found valid value in this batch
      if (result.nullVector->at(0) == 0) {  // if result never been initilized.
        assert(result.data.size() == 0);
        result.data.push_back(arrow::BasicDecimal128(tmp.data[first]));
        result.nullVector->at(0) = 1;
        result.precision = tmp.precision;
        result.scale = tmp.scale;
        result.type = GetResultType(dataType);
      }
    } else {  // this batch is all null so we can return directly.
      return;
    }

    for (int i = 0; i < tmp.data.size(); i++) {
      if (tmp.nullVector->at(i)) {
        result.data[0] = result.data[0] > tmp.data[i] ? result.data[0] : tmp.data[i];
      }
    }
  }

  void getResultInternalWithGroup(DecimalVector& result, const int& groupNum,
                                  const std::vector<int>& index) override {
    auto tmp = DecimalVector();
    child->getResult(tmp);
    for (int i = 0; i < tmp.data.size(); i++) {
      if (tmp.nullVector->at(i)) {
        if (!result.nullVector->at(index[i])) {
          result.nullVector->at(index[i]) = 1;
          result.data[index[i]] = tmp.data[i];
        } else
          result.data[index[i]] =
              result.data[index[i]] > tmp.data[i] ? result.data[index[i]] : tmp.data[i];
      }
    }
    result.precision = 38;  // tmp.precision;
    result.scale = tmp.scale;
    result.type = GetResultType(dataType);
  }
};

class Count : public AggExpression {
 public:
  ~Count() {}
  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                       const std::vector<int64_t>& nullBuffers,
                       std::vector<int8_t>& outBuffers) override {
    if (!done) {
      count = 0;
      batchSize_ = batchSize;  // for count(*)
      child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
    }
    return 0;
  }

 private:
  int count = 0;
  std::vector<int> group;
  int batchSize_ = 0;
  void getResultInternal(DecimalVector& result) override;
  void getResultInternalWithGroup(DecimalVector& result, const int& groupNum,
                                  const std::vector<int>& index) override;
};

class ArithmeticExpression : public WithResultExpression {
 public:
  ~ArithmeticExpression() {}
  void setLeft(std::shared_ptr<WithResultExpression> left) { leftChild = left; }
  void setRight(std::shared_ptr<WithResultExpression> right) { rightChild = right; }
  void setAttribute(std::string checkOverFlowType_, std::string castType_,
                    bool promotePrecision_, bool checkOverflow_, bool nullOnOverFlow_) {
    checkOverFlowType = checkOverFlowType_;
    castType = castType_;
    promotePrecision = promotePrecision_;
    checkOverflow = checkOverflow_;
    nullOnOverFlow = nullOnOverFlow_;
  }
  std::shared_ptr<WithResultExpression> getLeftChild() { return leftChild; }
  std::shared_ptr<WithResultExpression> getRightChild() { return rightChild; }

  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
    schema = schema_;
    leftChild->setSchema(schema);
    rightChild->setSchema(schema);
  }

  void getResult(DecimalVector& result, const int& groupNum = 1,
                 const std::vector<int>& index = std::vector<int>()) override {
    if (!done) {
      getResultInternal(resultCache);
      done = true;
    }
    result = resultCache;
  }

  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                       const std::vector<int64_t>& nullBuffers,
                       std::vector<int8_t>& outBuffers);
  void reset() override {
    done = false;
    resultCache.data.clear();
    leftChild->reset();
    rightChild->reset();
  }
  virtual void getResultInternal(DecimalVector& result) {
    ARROW_LOG(INFO) << "should never be called";
  };

 protected:
  std::shared_ptr<WithResultExpression> leftChild;
  std::shared_ptr<WithResultExpression> rightChild;
  std::string checkOverFlowType;
  std::string castType;
  bool promotePrecision;
  bool checkOverflow;
  bool nullOnOverFlow;
  DecimalVector resultCache;
};

class Add : public ArithmeticExpression {
 public:
  ~Add() {}
  void getResultInternal(DecimalVector& result) override {
    auto left = DecimalVector();
    auto right = DecimalVector();
    leftChild->getResult(left);
    rightChild->getResult(right);
    int32_t scale;
    int32_t precision;
    if (!checkOverFlowType.empty()) {
      getPrecisionAndScaleFromDecimalType(checkOverFlowType, precision, scale);
    } else {
      std::shared_ptr<arrow::Decimal128Type> t1 =
          std::dynamic_pointer_cast<arrow::Decimal128Type>(
              arrow::decimal(left.precision, left.scale));
      std::shared_ptr<arrow::Decimal128Type> t2 =
          std::dynamic_pointer_cast<arrow::Decimal128Type>(
              arrow::decimal(right.precision, right.scale));
      std::shared_ptr<arrow::Decimal128Type> type;
      DecimalUtil::GetResultType(DecimalUtil::kOpAdd, {t1, t2}, &type);
      precision = type->precision();
      scale = type->scale();
    }
    result.precision = precision;
    result.scale = scale;
    if (left.data.size() == 1) {
      arrow::Decimal128 leftValue = left.data[0];

      // rescale may be needed. e.g. literal value in SQL
      if (left.scale != result.scale) {
        leftValue = leftValue.Rescale(left.scale, result.scale).ValueOrDie();
      }
      for (int i = 0; i < right.data.size(); i++) {
        arrow::BasicDecimal128 out = leftValue + right.data[i];
        result.data.push_back(out);
      }
      result.nullVector = right.nullVector;
    } else if (right.data.size() == 1) {
      arrow::Decimal128 rightValue = right.data[0];

      // rescale may be needed. e.g. literal value in SQL
      if (right.scale != result.scale) {
        rightValue = rightValue.Rescale(right.scale, result.scale).ValueOrDie();
      }
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] + rightValue;
        result.data.push_back(out);
      }
      result.nullVector = left.nullVector;
    } else if (left.data.size() == right.data.size()) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] + right.data[i];
        result.data.push_back(out);
      }
      std::vector<uint8_t> nullVec(left.data.size());
      for (int i = 0; i < left.data.size(); i++) {
        nullVec[i] = left.nullVector->at(i) & right.nullVector->at(i);
      }
      result.nullVector = std::make_shared<std::vector<uint8_t>>(nullVec);
    } else {
      ARROW_LOG(ERROR) << "Oops...why left and right has different size?";
    }
  }
};

class Sub : public ArithmeticExpression {
 public:
  ~Sub() {}
  void getResultInternal(DecimalVector& result) override {
    auto left = DecimalVector();
    auto right = DecimalVector();
    leftChild->getResult(left);
    rightChild->getResult(right);
    int32_t scale;
    int32_t precision;
    if (!checkOverFlowType.empty()) {
      getPrecisionAndScaleFromDecimalType(checkOverFlowType, precision, scale);
    } else {
      std::shared_ptr<arrow::Decimal128Type> t1 =
          std::dynamic_pointer_cast<arrow::Decimal128Type>(
              arrow::decimal(left.precision, left.scale));
      std::shared_ptr<arrow::Decimal128Type> t2 =
          std::dynamic_pointer_cast<arrow::Decimal128Type>(
              arrow::decimal(right.precision, right.scale));
      std::shared_ptr<arrow::Decimal128Type> type;
      DecimalUtil::GetResultType(DecimalUtil::kOpSubtract, {t1, t2}, &type);
      precision = type->precision();
      scale = type->scale();
    }
    result.precision = precision;
    result.scale = scale;
    if (left.data.size() == 1) {
      arrow::Decimal128 leftValue = left.data[0];

      // rescale may be needed. e.g. literal value in SQL
      if (left.scale != result.scale) {
        leftValue = leftValue.Rescale(left.scale, result.scale).ValueOrDie();
      }
      for (int i = 0; i < right.data.size(); i++) {
        arrow::BasicDecimal128 out = leftValue - right.data[i];
        result.data.push_back(out);
      }
      result.nullVector = right.nullVector;
    } else if (right.data.size() == 1) {
      arrow::Decimal128 rightValue = right.data[0];

      // rescale may be needed. e.g. literal value in SQL
      if (right.scale != result.scale) {
        rightValue = rightValue.Rescale(right.scale, result.scale).ValueOrDie();
      }
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] - rightValue;
        result.data.push_back(out);
      }
      result.nullVector = left.nullVector;
    } else if (left.data.size() == right.data.size()) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] - right.data[i];
        result.data.push_back(out);
      }
      std::vector<uint8_t> nullVec(left.data.size());
      for (int i = 0; i < left.data.size(); i++) {
        nullVec[i] = left.nullVector->at(i) & right.nullVector->at(i);
      }
      result.nullVector = std::make_shared<std::vector<uint8_t>>(nullVec);
    } else {
      ARROW_LOG(ERROR) << "Oops...why left and right has different size?";
    }
  }
};

class Multiply : public ArithmeticExpression {
 public:
  ~Multiply() {}
  void getResultInternal(DecimalVector& result) override {
    auto left = DecimalVector();
    auto right = DecimalVector();
    leftChild->getResult(left);
    rightChild->getResult(right);
    int32_t scale;
    int32_t precision;
    if (!checkOverFlowType.empty()) {
      getPrecisionAndScaleFromDecimalType(checkOverFlowType, precision, scale);
    } else {
      std::shared_ptr<arrow::Decimal128Type> t1 =
          std::dynamic_pointer_cast<arrow::Decimal128Type>(
              arrow::decimal(left.precision, left.scale));
      std::shared_ptr<arrow::Decimal128Type> t2 =
          std::dynamic_pointer_cast<arrow::Decimal128Type>(
              arrow::decimal(right.precision, right.scale));
      std::shared_ptr<arrow::Decimal128Type> type;
      DecimalUtil::GetResultType(DecimalUtil::kOpMultiply, {t1, t2}, &type);
      precision = type->precision();
      scale = type->scale();
    }
    result.precision = precision;
    result.scale = scale;
    if (left.data.size() == 1) {
      for (int i = 0; i < right.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[0] * right.data[i];
        result.data.push_back(out);
      }
      result.nullVector = right.nullVector;
    } else if (right.data.size() == 1) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] * right.data[0];
        result.data.push_back(out);
      }
      result.nullVector = left.nullVector;
    } else if (left.data.size() == right.data.size()) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] * right.data[i];
        result.data.push_back(out);
      }
      std::vector<uint8_t> nullVec(left.data.size());
      for (int i = 0; i < left.data.size(); i++) {
        nullVec[i] = left.nullVector->at(i) & right.nullVector->at(i);
      }
      result.nullVector = std::make_shared<std::vector<uint8_t>>(nullVec);
    } else {
      ARROW_LOG(ERROR) << "Oops...why left and right has different size?";
    }
  }
};

// TODO: Impl Divide and Mod.
class Divide : public ArithmeticExpression {
 public:
  ~Divide() {}
  void getResultInternal(DecimalVector& result) override {
    auto left = DecimalVector();
    auto right = DecimalVector();
    leftChild->getResult(left);
    rightChild->getResult(right);
  }
};

class Mod : public ArithmeticExpression {
 public:
  ~Mod() {}
  void getResultInternal(DecimalVector& result) override {
    auto left = DecimalVector();
    auto right = DecimalVector();
    leftChild->getResult(left);
    rightChild->getResult(right);
  }
};  // ...

class AttributeReferenceExpression : public WithResultExpression {
 public:
  int columnIndex = 0;
  ~AttributeReferenceExpression() {}
  // TODO: get value buffer and trans to Decimal()
  void getResult(DecimalVector& res, const int& groupNum = 1,
                 const std::vector<int>& index = std::vector<int>()) override {
    res.data.clear();
    for (auto e : result.data) {
      res.data.push_back(e);
    }
    res.precision = result.precision;
    res.scale = result.scale;
    res.nullVector = result.nullVector;
  }

  void setAttribute(std::string columnName_, std::string dataType_, std::string castType_,
                    bool PromotePrecision_) {
    columnName = columnName_;
    dataType = dataType_;
    castType = castType_;
    PromotePrecision = PromotePrecision_;
  }

  void setSchema(std::shared_ptr<std::vector<Schema>> schema_);

  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                       const std::vector<int64_t>& nullBuffers,
                       std::vector<int8_t>& outBuffers);
  void reset() override { done = false; }

 private:
  DecimalVector result;
  bool PromotePrecision;
};

class LiteralExpression : public WithResultExpression {
 public:
  ~LiteralExpression() {}
  void getResult(DecimalVector& res, const int& groupNum = 1,
                 const std::vector<int>& index = std::vector<int>()) override {
    res.data.clear();
    res.data.push_back(value);
    res.precision = precision_;
    res.scale = scale_;
    std::vector<uint8_t> nullVec{1};
    res.nullVector = std::make_shared<std::vector<uint8_t>>(nullVec);
  }
  void setAttribute(std::string dataType_, std::string valueString_) {
    dataType = dataType_;
    valueString = valueString_;
    if (isDecimalType(dataType)) {
      arrow::Decimal128 decimal;
      int32_t scaleFromValue;
      int32_t scaleFromType;
      int32_t precision;
      arrow::Decimal128::FromString(valueString, &decimal, &precision, &scaleFromValue);
      if (!dataType.empty()) {
        getPrecisionAndScaleFromDecimalType(dataType, precision, scaleFromType);
        if (scaleFromType != scaleFromValue) {
          decimal = decimal.Rescale(scaleFromValue, scaleFromType).ValueOrDie();
        }
      }
      value = std::move(decimal);
      precision_ = precision;
      scale_ = scaleFromType;
    }
  }

 private:
  std::string valueString;
  arrow::BasicDecimal128 value;  // build this in consturctor
  int32_t precision_;
  int32_t scale_;
};

class Gen {
 public:
  static std::shared_ptr<ArithmeticExpression> genArithmeticExpression(std::string name) {
    if (name.compare("Add") == 0)
      return std::make_shared<Add>();
    else if (name.compare("Subtract") == 0)
      return std::make_shared<Sub>();
    else if (name.compare("Multiply") == 0)
      return std::make_shared<Multiply>();
    else if (name.compare("Divide") == 0)
      return std::make_shared<Divide>();
    else if (name.compare("Mod") == 0)
      return std::make_shared<Mod>();
    ARROW_LOG(ERROR) << "not support arithmetic expression:" << name;
    return nullptr;
  }

  static std::shared_ptr<AggExpression> genAggExpression(std::string name) {
    if (name.compare("Sum") == 0)
      return std::make_shared<Sum>();
    else if (name.compare("Max") == 0)
      return std::make_shared<Max>();
    else if (name.compare("Min") == 0)
      return std::make_shared<Min>();
    else if (name.compare("Count") == 0)
      return std::make_shared<Count>();

    ARROW_LOG(ERROR) << "not support agg expression:" << name;
    return nullptr;
  }
};

}  // namespace ape
