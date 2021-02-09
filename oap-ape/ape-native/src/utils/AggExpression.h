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

#include "expression.h"

namespace ape {

class WithResultExpression : public Expression {
 public:
  int ExecuteWithParam(int batchSize, long* dataBuffers, long* nullBuffers,
                       char* outBuffers) {
    return 0;
  }
  void Execute(){};
  ~WithResultExpression(){};

  virtual std::vector<arrow::Decimal128> getResult() {
    // should never be called.
    return {arrow::Decimal128(0, 0)};
  };

  void setDataType(std::string dataType_) { dataType = dataType_; }

 protected:
  std::string dataType;
  std::string castType;
  bool isDistinct = false;
};

class RootAggExpression : public WithResultExpression {
 public:
  ~RootAggExpression(){};
  void setChild(std::shared_ptr<WithResultExpression> child_) { child = child_; }
  void setAttribute(bool isDistinct_, std::string aliasName_) {
    isDistinct = isDistinct_;
    aliasName = aliasName_;
  }

 private:
  bool isDistinct;
  std::string aliasName;
  std::shared_ptr<WithResultExpression> child;
};

class AggExpression : public WithResultExpression {
 public:
  // Avg will have two elements for Sum and Count
  ~AggExpression(){};
  void setChild(std::shared_ptr<WithResultExpression> child_) { child = child_; }

 protected:
  std::shared_ptr<WithResultExpression> child;
};

class Sum : public AggExpression {
 public:
  ~Sum(){};
  std::vector<arrow::Decimal128> getResult() override {
    auto tmp = child->getResult();
    arrow::Decimal128 sum(0, 0);
    for (auto e : tmp) sum += e;
    return {sum};
  };
};

class Min : public AggExpression {
 public:
  ~Min(){};
  std::vector<arrow::Decimal128> getResult() override {
    auto tmp = child->getResult();
    arrow::Decimal128 min = tmp[0];
    for (auto e : tmp) min = min < e ? min : e;
    return {min};
  };
};

class Max : public AggExpression {
 public:
  ~Max(){};
  std::vector<arrow::Decimal128> getResult() override {
    auto tmp = child->getResult();
    arrow::Decimal128 max = tmp[0];
    for (auto e : tmp) max = max > e ? max : e;
    return {max};
  };
};

class Count : public AggExpression {
 public:
  ~Count(){};
  std::vector<arrow::Decimal128> getResult() override {
    auto tmp = child->getResult();
    arrow::Decimal128 res(0, tmp.size());
    return {res};
  };
};

class Avg : public AggExpression {
 public:
  ~Avg(){};
  std::vector<arrow::Decimal128> getResult() override {
    auto tmp = child->getResult();
    arrow::Decimal128 count(0, tmp.size());
    arrow::Decimal128 sum(0, 0);
    for (auto e : tmp) sum += e;

    return {sum, count};
  };
};

class ArithmeticExpression : public WithResultExpression {
 public:
  ~ArithmeticExpression(){};
  void setLeft(std::shared_ptr<WithResultExpression> left) { leftChild = left; };
  void setRight(std::shared_ptr<WithResultExpression> right) { rightChild = right; };
  void setAttribute(std::string checkOverFlowType_, std::string castType_,
                    bool promotePrecision_, bool checkOverflow_, bool nullOnOverFlow_){};

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
  ~Add(){};

  // Add result will not change row number
  std::vector<arrow::Decimal128> getResult() override {
    auto left = leftChild->getResult();
    auto right = rightChild->getResult();
    std::vector<arrow::Decimal128> res;
    if (left.size() == 1) {
      for (int i = 0; i < right.size(); i++) {
        res.push_back(left[0] + right[i]);
      }
    } else if (right.size() == 1) {
      for (int i = 0; i < left.size(); i++) {
        res.push_back(left[i] + right[0]);
      }
    } else if (left.size() == right.size()) {
      for (int i = 0; i < left.size(); i++) {
        res.push_back(left[i] + right[i]);
      }
    } else {
      // should not reach here.
    }

    return res;
  }
};

class Sub : public ArithmeticExpression {
 public:
  ~Sub(){};
};
class Multiply : public ArithmeticExpression {
 public:
  ~Multiply(){};
};
class Divide : public ArithmeticExpression {
 public:
  ~Divide(){};
};
class Mod : public ArithmeticExpression {
 public:
  ~Mod(){};
};  // ...

class AttributeReferenceExpression : public WithResultExpression {
 public:
  ~AttributeReferenceExpression(){};
  // TODO: get value buffer and trans to Decimal()
  std::vector<arrow::Decimal128> getResult() override { return result; }
  void setAttribute(std::string columnName_, std::string dataType_, std::string castType_,
                    bool PromotePrecision_) {
    columnName = columnName_;
    dataType = dataType_;
    castType = castType_;
    PromotePrecision = PromotePrecision_;
  };

 private:
  std::vector<arrow::Decimal128> result;
  std::string columnName;
  bool PromotePrecision;
};

class LiteralExpression : public WithResultExpression {
 public:
  ~LiteralExpression(){};
  std::vector<arrow::Decimal128> getResult() override { return {value}; }
  void setAttribute(std::string dataType_, std::string valueString_) {
    dataType = dataType_;
    valueString = valueString_;
    // value = valueString;
  };

 private:
  std::string valueString;
  arrow::Decimal128 value;  // build this in consturctor
};

class Gen {
 public:
  static std::shared_ptr<ArithmeticExpression> genArithmeticExpression(std::string name) {
    if (name.compare("Add"))
      return std::make_shared<Add>();
    else if (name.compare("Sub"))
      return std::make_shared<Sub>();
    else if (name.compare("Multiply"))
      return std::make_shared<Multiply>();
    else if (name.compare("Divide"))
      return std::make_shared<Divide>();
    else if (name.compare("Mod"))
      return std::make_shared<Mod>();
    std::cerr << "not support!" << std::endl;
    return nullptr;
  };

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

    std::cerr << "not supported!" << std::endl;
    return nullptr;
  }
};

}  // namespace ape
