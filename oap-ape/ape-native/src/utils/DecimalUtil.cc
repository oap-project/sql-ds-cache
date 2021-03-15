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

// Adapted from gandiva decimal_ops.cc

#include "src/utils/DecimalUtil.h"

#include <algorithm>
#include <cmath>
#include <limits>

#include <arrow/type.h>
#include <arrow/util/logging.h>

namespace ape {

using arrow::BasicDecimal128;

#define DCHECK_TYPE(type)                        \
  {                                              \
    DCHECK_GE(type->scale(), 0);                 \
    DCHECK_LE(type->precision(), kMaxPrecision); \
  }

/// The maximum precision representable by a 4-byte decimal
static constexpr int32_t kMaxDecimal32Precision = 9;

/// The maximum precision representable by a 8-byte decimal
static constexpr int32_t kMaxDecimal64Precision = 18;

/// The maximum precision representable by a 16-byte decimal
static constexpr int32_t kMaxPrecision = 38;

// When the output exceed the max precision, the scale can be reduced
static constexpr int32_t kMinAdjustedScale = 6;

// The maximum scale representable.
static constexpr int32_t kMaxScale = kMaxPrecision;

static std::shared_ptr<arrow::Decimal128Type> MakeAdjustedType(int32_t precision,
                                                               int32_t scale) {
  if (precision > kMaxPrecision) {
    int32_t min_scale = std::min(scale, kMinAdjustedScale);
    int32_t delta = precision - kMaxPrecision;
    precision = kMaxPrecision;
    scale = std::max(scale - delta, min_scale);
  }
  return std::dynamic_pointer_cast<arrow::Decimal128Type>(
      arrow::decimal(precision, scale));
}

void DecimalUtil::GetResultType(Op op, const Decimal128TypeVector& in_types,
                                Decimal128TypePtr* out_type) {
  DCHECK_EQ(in_types.size(), 2);

  *out_type = nullptr;
  auto t1 = in_types[0];
  auto t2 = in_types[1];
  DCHECK_TYPE(t1);
  DCHECK_TYPE(t2);

  int32_t s1 = t1->scale();
  int32_t s2 = t2->scale();
  int32_t p1 = t1->precision();
  int32_t p2 = t2->precision();
  int32_t result_scale = 0;
  int32_t result_precision = 0;

  switch (op) {
    case kOpAdd:
    case kOpSubtract:
      result_scale = std::max(s1, s2);
      result_precision = std::max(p1 - s1, p2 - s2) + result_scale + 1;
      break;

    case kOpMultiply:
      result_scale = s1 + s2;
      result_precision = p1 + p2 + 1;
      break;

    case kOpDivide:
      result_scale = std::max(kMinAdjustedScale, s1 + p2 + 1);
      result_precision = p1 - s1 + s2 + result_scale;
      break;

    case kOpMod:
      result_scale = std::max(s1, s2);
      result_precision = std::min(p1 - s1, p2 - s2) + result_scale;
      break;
  }
  *out_type = MakeAdjustedType(result_precision, result_scale);
  return;
}

static BasicDecimal128 CheckAndIncreaseScale(const BasicDecimal128& in, int32_t delta) {
  return (delta <= 0) ? in : in.IncreaseScaleBy(delta);
}

static BasicDecimal128 CheckAndReduceScale(const BasicDecimal128& in, int32_t delta) {
  return (delta <= 0) ? in : in.ReduceScaleBy(delta);
}

// Suppose we have a number that requires x bits to be represented and we scale it up by
// 10^scale_by. Let's say now y bits are required to represent it. This function returns
// the maximum possible y - x for a given 'scale_by'.
inline int32_t MaxBitsRequiredIncreaseAfterScaling(int32_t scale_by) {
  // We rely on the following formula:
  // bits_required(x * 10^y) <= bits_required(x) + floor(log2(10^y)) + 1
  // We precompute floor(log2(10^x)) + 1 for x = 0, 1, 2...75, 76
  DCHECK_GE(scale_by, 0);
  DCHECK_LE(scale_by, 76);
  static const int32_t floor_log2_plus_one[] = {
      0,   4,   7,   10,  14,  17,  20,  24,  27,  30,  34,  37,  40,  44,  47,  50,
      54,  57,  60,  64,  67,  70,  74,  77,  80,  84,  87,  90,  94,  97,  100, 103,
      107, 110, 113, 117, 120, 123, 127, 130, 133, 137, 140, 143, 147, 150, 153, 157,
      160, 163, 167, 170, 173, 177, 180, 183, 187, 190, 193, 196, 200, 203, 206, 210,
      213, 216, 220, 223, 226, 230, 233, 236, 240, 243, 246, 250, 253};
  return floor_log2_plus_one[scale_by];
}

// If we have a number with 'num_lz' leading zeros, and we scale it up by 10^scale_by,
// this function returns the minimum number of leading zeros the result can have.
inline int32_t MinLeadingZerosAfterScaling(int32_t num_lz, int32_t scale_by) {
  DCHECK_GE(scale_by, 0);
  DCHECK_LE(scale_by, 76);
  int32_t result = num_lz - MaxBitsRequiredIncreaseAfterScaling(scale_by);
  return result;
}

// Returns the maximum possible number of bits required to represent num * 10^scale_by.
inline int32_t MaxBitsRequiredAfterScaling(const ApeDecimal128& num, int32_t scale_by) {
  auto value = num.value();
  auto value_abs = value.Abs();

  int32_t num_occupied = 128 - value_abs.CountLeadingBinaryZeros();
  DCHECK_GE(scale_by, 0);
  DCHECK_LE(scale_by, 76);
  return num_occupied + MaxBitsRequiredIncreaseAfterScaling(scale_by);
}

// Returns the minimum number of leading zero x or y would have after one of them gets
// scaled up to match the scale of the other one.
inline int32_t MinLeadingZeros(const ApeDecimal128& x, const ApeDecimal128& y) {
  auto x_value = x.value();
  auto x_value_abs = x_value.Abs();

  auto y_value = y.value();
  auto y_value_abs = y_value.Abs();

  int32_t x_lz = x_value_abs.CountLeadingBinaryZeros();
  int32_t y_lz = y_value_abs.CountLeadingBinaryZeros();
  if (x.scale() < y.scale()) {
    x_lz = MinLeadingZerosAfterScaling(x_lz, y.scale() - x.scale());
  } else if (x.scale() > y.scale()) {
    y_lz = MinLeadingZerosAfterScaling(y_lz, x.scale() - y.scale());
  }
  return std::min(x_lz, y_lz);
}

BasicDecimal128 DecimalUtil::Add(const ApeDecimal128& x, const ApeDecimal128& y,
                                 int32_t out_precision, int32_t out_scale) {
  DCHECK_LT(out_precision, kMaxPrecision);
  auto higher_scale = std::max(x.scale(), y.scale());

  auto x_scaled = CheckAndIncreaseScale(x.value(), higher_scale - x.scale());
  auto y_scaled = CheckAndIncreaseScale(y.value(), higher_scale - y.scale());
  return x_scaled + y_scaled;
}

BasicDecimal128 DecimalUtil::Subtract(const ApeDecimal128& x, const ApeDecimal128& y,
                                      int32_t out_precision, int32_t out_scale) {
  return Add(x, {-y.value(), y.precision(), y.scale()}, out_precision, out_scale);
}

// Multiply when the out_precision is 38, and there is no trimming of the scale i.e
// the intermediate value is the same as the final value.
static BasicDecimal128 MultiplyMaxPrecisionNoScaleDown(const ApeDecimal128& x,
                                                       const ApeDecimal128& y,
                                                       int32_t out_scale,
                                                       bool* overflow) {
  DCHECK_EQ(x.scale() + y.scale(), out_scale);

  BasicDecimal128 result;
  auto x_abs = BasicDecimal128::Abs(x.value());
  auto y_abs = BasicDecimal128::Abs(y.value());

  if (x_abs > BasicDecimal128::GetMaxValue() / y_abs) {
    *overflow = true;
  } else {
    // We've verified that the result will fit into 128 bits.
    *overflow = false;
    result = x.value() * y.value();
  }
  return result;
}

BasicDecimal128 DecimalUtil::Multiply(const ApeDecimal128& x, const ApeDecimal128& y,
                                      int32_t out_precision, int32_t out_scale) {
  BasicDecimal128 result;
  if (ARROW_PREDICT_TRUE(out_precision < kMaxPrecision)) {
    // fast-path multiply
    result = x.value() * y.value();
    DCHECK_EQ(x.scale() + y.scale(), out_scale);
    DCHECK_LE(BasicDecimal128::Abs(result), BasicDecimal128::GetMaxValue());
  } else if (x.value() == 0 || y.value() == 0) {
    // Handle this separately to avoid divide-by-zero errors.
    result = BasicDecimal128(0, 0);
  } else {
    ARROW_LOG(FATAL) << "Need 256-bit for multiply";
    return 0;
  }
  DCHECK(BasicDecimal128::Abs(result) <= BasicDecimal128::GetMaxValue());
  return result;
}

BasicDecimal128 DecimalUtil::Divide(const ApeDecimal128& x, const ApeDecimal128& y,
                                    int32_t out_precision, int32_t out_scale) {
  if (y.value() == 0) {
    ARROW_LOG(FATAL) << "Divide by zero error";
    return 0;
  }

  // scale up to the output scale, and do an integer division.
  int32_t delta_scale = out_scale + y.scale() - x.scale();
  DCHECK_GE(delta_scale, 0);

  BasicDecimal128 result;
  auto num_bits_required_after_scaling = MaxBitsRequiredAfterScaling(x, delta_scale);
  if (ARROW_PREDICT_TRUE(num_bits_required_after_scaling <= 127)) {
    auto x_scaled = CheckAndIncreaseScale(x.value(), delta_scale);
    BasicDecimal128 remainder;
    auto status = x_scaled.Divide(y.value(), &result, &remainder);
    DCHECK_EQ(status, arrow::DecimalStatus::kSuccess);

    // round-up
    if (BasicDecimal128::Abs(2 * remainder) >= BasicDecimal128::Abs(y.value())) {
      result += (x.value().Sign() ^ y.value().Sign()) + 1;
    }
    return result;
  } else {
    // convert to 256-bit and do the divide.
    ARROW_LOG(FATAL) << "Need 256-bit for divide";
    return 0;
  }
}

BasicDecimal128 DecimalUtil::Mod(const ApeDecimal128& x, const ApeDecimal128& y,
                                 int32_t out_precision, int32_t out_scale) {
  if (y.value() == 0) {
    ARROW_LOG(FATAL) << "Divide by zero error";
    return 0;
  }

  BasicDecimal128 result;
  int32_t min_lz = MinLeadingZeros(x, y);
  if (ARROW_PREDICT_TRUE(min_lz >= 2)) {
    auto higher_scale = std::max(x.scale(), y.scale());
    auto x_scaled = CheckAndIncreaseScale(x.value(), higher_scale - x.scale());
    auto y_scaled = CheckAndIncreaseScale(y.value(), higher_scale - y.scale());
    result = x_scaled % y_scaled;
    DCHECK_LE(BasicDecimal128::Abs(result), BasicDecimal128::GetMaxValue());
  } else {
    ARROW_LOG(FATAL) << "Need 256-bits for Mod operator";
  }
  DCHECK(BasicDecimal128::Abs(result) <= BasicDecimal128::Abs(x.value()) ||
         BasicDecimal128::Abs(result) <= BasicDecimal128::Abs(y.value()));
  return result;
}

int32_t CompareSameScale(const BasicDecimal128& x, const BasicDecimal128& y) {
  if (x == y) {
    return 0;
  } else if (x < y) {
    return -1;
  } else {
    return 1;
  }
}

int32_t DecimalUtil::Compare(const ApeDecimal128& x, const ApeDecimal128& y) {
  int32_t delta_scale = x.scale() - y.scale();

  // fast-path : both are of the same scale.
  if (delta_scale == 0) {
    return CompareSameScale(x.value(), y.value());
  }

  // Check if we'll need more than 256-bits after adjusting the scale.
  bool need256 = (delta_scale < 0 && x.precision() - delta_scale > kMaxPrecision) ||
                 (y.precision() + delta_scale > kMaxPrecision);
  if (ARROW_PREDICT_FALSE(need256)) {
    ARROW_LOG(FATAL) << "Need 256-bits for Compare";
    return 0;
  } else {
    BasicDecimal128 x_scaled;
    BasicDecimal128 y_scaled;

    if (delta_scale < 0) {
      x_scaled = x.value().IncreaseScaleBy(-delta_scale);
      y_scaled = y.value();
    } else {
      x_scaled = x.value();
      y_scaled = y.value().IncreaseScaleBy(delta_scale);
    }
    return CompareSameScale(x_scaled, y_scaled);
  }
}

}  // namespace ape
