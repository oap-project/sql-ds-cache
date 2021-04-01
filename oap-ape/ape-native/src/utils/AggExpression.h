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

static inline bool isDecimalType(std::string& dataType) {
  bool isDecimal = false;
  std::string decimalType("DecimalType");
  if (dataType.compare(0, decimalType.length(), decimalType) == 0) {
    isDecimal = true;
  }
  return isDecimal;
}

static int getPrecisionAndScaleFromDecimalType(std::string& decimalType, int& precision,
                                               int& scale) {
  if (isDecimalType(decimalType)) {
    char str[64];
    sscanf(decimalType.c_str(), "%11s(%d,%d)", str, &precision, &scale);
    return 0;
  }
  return -1;
}

class WithResultExpression : public Expression {
 public:
  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                       const std::vector<int64_t>& nullBuffers,
                       std::vector<int8_t>& outBuffers) {
    return 0;
  }
  void Execute() {}
  ~WithResultExpression() {}

  virtual void getResult(DecimalVector& result) {
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

  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
    schema = schema_;
    child->setSchema(schema);
  }

  void getResult(DecimalVector& result) { child->getResult(result); }

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

  void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {
    schema = schema_;
    child->setSchema(schema);
  }

 protected:
  std::shared_ptr<WithResultExpression> child;
};

class Sum : public AggExpression {
 public:
  ~Sum() {}
  void getResult(DecimalVector& result) override {
    auto tmp = DecimalVector();
    child->getResult(tmp);
    arrow::BasicDecimal128 out;
    for (auto e : tmp.data) {
      out += e;
    }
    result.data.push_back(out);
    result.precision = 38;  // tmp.precision;
    result.scale = tmp.scale;
  }
};

class Min : public AggExpression {
 public:
  ~Min() {}
  void getResult(DecimalVector& result) override {
    auto tmp = DecimalVector();
    child->getResult(tmp);
    arrow::BasicDecimal128 out(tmp.data[0]);
    for (auto e : tmp.data) out = out < e ? out : e;
    result.data.push_back(out);
    result.precision = tmp.precision;
    result.scale = tmp.scale;
  }
};

class Max : public AggExpression {
 public:
  ~Max() {}
  void getResult(DecimalVector& result) override {
    auto tmp = DecimalVector();
    child->getResult(tmp);
    arrow::BasicDecimal128 out(tmp.data[0]);
    for (auto e : tmp.data) out = out > e ? out : e;
    result.data.push_back(out);
    result.precision = tmp.precision;
    result.scale = tmp.scale;
  }
};

class Count : public AggExpression {
 public:
  ~Count() {}
  void getResult(DecimalVector& result) override {
    auto tmp = DecimalVector();
    child->getResult(tmp);
    result.data.push_back(arrow::BasicDecimal128(tmp.data.size()));
    result.precision = tmp.precision;
    result.scale = tmp.scale;
  }
};

class Avg : public AggExpression {
 public:
  ~Avg() {}
  void getResult(DecimalVector& result) override {
    auto tmp = DecimalVector();
    child->getResult(tmp);
    arrow::BasicDecimal128 sum;
    for (auto e : tmp.data) {
      sum += e;
    }
    result.data.push_back(sum);
    result.data.push_back(arrow::BasicDecimal128(tmp.data.size()));
    result.precision = 38;  // tmp.precision;
    result.scale = tmp.scale;
  }
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

  int ExecuteWithParam(int batchSize, const std::vector<int64_t>& dataBuffers,
                       const std::vector<int64_t>& nullBuffers,
                       std::vector<int8_t>& outBuffers);

 protected:
  std::shared_ptr<WithResultExpression> leftChild;
  std::shared_ptr<WithResultExpression> rightChild;
  std::string checkOverFlowType;
  std::string castType;
  bool promotePrecision;
  bool checkOverflow;
  bool nullOnOverFlow;
};

class Add : public ArithmeticExpression {
 public:
  ~Add() {}
  void getResult(DecimalVector& result) override {
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
      for (int i = 0; i < right.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[0] + right.data[i];
        result.data.push_back(out);
      }
    } else if (right.data.size() == 1) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] + right.data[0];
        result.data.push_back(out);
      }
    } else if (left.data.size() == right.data.size()) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] + right.data[i];
        result.data.push_back(out);
      }
    } else {
      ARROW_LOG(ERROR) << "Oops...why left and right has different size?";
    }
  }
};

class Sub : public ArithmeticExpression {
 public:
  ~Sub() {}
  void getResult(DecimalVector& result) override {
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
      for (int i = 0; i < right.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[0] - right.data[i];
        result.data.push_back(out);
      }
    } else if (right.data.size() == 1) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] - right.data[0];
        result.data.push_back(out);
      }
    } else if (left.data.size() == right.data.size()) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] - right.data[i];
        result.data.push_back(out);
      }
    } else {
      ARROW_LOG(ERROR) << "Oops...why left and right has different size?";
    }
  }
};

class Multiply : public ArithmeticExpression {
 public:
  ~Multiply() {}
  void getResult(DecimalVector& result) override {
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
    } else if (right.data.size() == 1) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] * right.data[0];
        result.data.push_back(out);
      }
    } else if (left.data.size() == right.data.size()) {
      for (int i = 0; i < left.data.size(); i++) {
        arrow::BasicDecimal128 out = left.data[i] * right.data[i];
        result.data.push_back(out);
      }
    } else {
      ARROW_LOG(ERROR) << "Oops...why left and right has different size?";
    }
  }
};

class Divide : public ArithmeticExpression {
 public:
  ~Divide() {}
  void getResult(DecimalVector& result) override {
    auto left = DecimalVector();
    auto right = DecimalVector();
    leftChild->getResult(left);
    rightChild->getResult(right);
  }
};

class Mod : public ArithmeticExpression {
 public:
  ~Mod() {}
  void getResult(DecimalVector& result) override {
    auto left = DecimalVector();
    auto right = DecimalVector();
    leftChild->getResult(left);
    rightChild->getResult(right);
  }
};  // ...

class AttributeReferenceExpression : public WithResultExpression {
 public:
  ~AttributeReferenceExpression() {}
  // TODO: get value buffer and trans to Decimal()
  void getResult(DecimalVector& res) override {
    res.data.clear();
    for (auto e : result.data) {
      res.data.push_back(e);
    }
    res.precision = result.precision;
    res.scale = result.scale;
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

 private:
  DecimalVector result;
  bool PromotePrecision;
  int columnIndex;
};

class LiteralExpression : public WithResultExpression {
 public:
  ~LiteralExpression() {}
  void getResult(DecimalVector& res) override {
    res.data.clear();
    res.data.push_back(value);
    res.precision = precision_;
    res.scale = scale_;
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
    else if (name.compare("Average") == 0)
      return std::make_shared<Avg>();
    else if (name.compare("Count") == 0)
      return std::make_shared<Count>();

    ARROW_LOG(ERROR) << "not support agg expression:" << name;
    return nullptr;
  }
};

}  // namespace ape
