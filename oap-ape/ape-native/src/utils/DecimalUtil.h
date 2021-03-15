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

#include <cstdint>
#include <vector>

#include <arrow/status.h>
#include <arrow/type.h>

#include "ApeDecimal.h"

namespace ape {

class DecimalUtil {
 public:
  enum Op {
    kOpAdd,
    kOpSubtract,
    kOpMultiply,
    kOpDivide,
    kOpMod,
  };

  using Decimal128TypePtr = std::shared_ptr<arrow::Decimal128Type>;
  using Decimal128TypeVector = std::vector<Decimal128TypePtr>;

  static void GetResultType(Op op, const Decimal128TypeVector& in_types,
                            Decimal128TypePtr* out_type);

  /// Sum of 'x' and 'y'.
  static arrow::BasicDecimal128 Add(const ApeDecimal128& x, const ApeDecimal128& y,
                                    int32_t out_precision, int32_t out_scale);
  /// Subtract 'y' from 'x'.
  arrow::BasicDecimal128 Subtract(const ApeDecimal128& x, const ApeDecimal128& y,
                                  int32_t out_precision, int32_t out_scale);

  /// Multiply 'x' from 'y'.
  arrow::BasicDecimal128 Multiply(const ApeDecimal128& x, const ApeDecimal128& y,
                                  int32_t out_precision, int32_t out_scale);

  /// Divide 'x' by 'y'.
  arrow::BasicDecimal128 Divide(const ApeDecimal128& x, const ApeDecimal128& y,
                                int32_t out_precision, int32_t out_scale);

  /// Divide 'x' by 'y'.
  arrow::BasicDecimal128 Mod(const ApeDecimal128& x, const ApeDecimal128& y,
                             int32_t out_precision, int32_t out_scale);

  /// Compare two decimals. Returns :
  ///  0 if x == y
  ///  1 if x > y
  /// -1 if x < y
  int32_t Compare(const ApeDecimal128& x, const ApeDecimal128& y);
};

}  // namespace ape
