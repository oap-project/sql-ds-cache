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

#include <arrow/util/basic_decimal.h>
#include <cstdint>

namespace ape {

/// Represents a 128-bit decimal value along with its precision and scale.
class ApeDecimal128 {
 public:
  constexpr ApeDecimal128(int64_t high_bits, uint64_t low_bits, int32_t precision,
                          int32_t scale)
      : value_(high_bits, low_bits), precision_(precision), scale_(scale) {}

  constexpr ApeDecimal128(const arrow::BasicDecimal128& value, int32_t precision,
                          int32_t scale)
      : value_(value), precision_(precision), scale_(scale) {}

  constexpr ApeDecimal128(int32_t precision, int32_t scale)
      : precision_(precision), scale_(scale) {}

  template <typename T,
            typename = typename std::enable_if<
                std::is_integral<T>::value && (sizeof(T) <= sizeof(uint64_t)), T>::type>
  constexpr ApeDecimal128(T value) noexcept
      : value_(
            arrow::BasicDecimal128(value >= T{0} ? 0 : -1, static_cast<uint64_t>(value))),
        precision_(18),
        scale_(0) {}

  constexpr ApeDecimal128() : precision_(18), scale_(0) {}

  int32_t scale() const { return scale_; }

  int32_t precision() const { return precision_; }

  const arrow::BasicDecimal128& value() const { return value_; }

 private:
  arrow::BasicDecimal128 value_;
  int32_t precision_;
  int32_t scale_;
};

inline bool operator==(const ApeDecimal128& left, const ApeDecimal128& right) {
  return left.value() == right.value() && left.precision() == right.precision() &&
         left.scale() == right.scale();
}

inline ApeDecimal128 operator-(const ApeDecimal128& operand) {
  return ApeDecimal128{-operand.value(), operand.precision(), operand.scale()};
}

}  // namespace ape
