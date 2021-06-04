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

#include <arrow/type.h>
#include <arrow/util/basic_decimal.h>
#include <arrow/util/decimal.h>
#include <cstdint>

namespace ape {
// TODO: should use inline here?
static inline void decimalToBytes(arrow::BasicDecimal128 input, int32_t precision,
                                  uint8_t* out) {
  int h = 0;
  int l = 0;
  int highShift = 0;
  int lowShift = 0;
  int8_t decimalBuffer[16];

  int numBytes = arrow::DecimalType::DecimalSize(precision);

  if (numBytes > 8) {
    highShift = 8 * (numBytes - 8 - 1);
    lowShift = 56;
  } else {
    lowShift = 8 * (numBytes - 1);
  }

  int leftBytes = numBytes;
  if (numBytes > 8) {
    while (h < numBytes - 8) {
      decimalBuffer[h] = (input.high_bits() >> highShift) & 0xFF;
      h++;
      highShift -= 8;
    }
    leftBytes = 8;
  }
  while (l < leftBytes) {
    decimalBuffer[h + l] = (input.low_bits() >> lowShift) & 0xFF;
    l++;
    lowShift -= 8;
  }

  int index = 0;
  int8_t signByte;
  if (decimalBuffer[0] < 0)
    signByte = -1;
  else
    signByte = 0;
  for (int i = 0; i < 16 - numBytes; i++) {
    out[index++] = signByte;
  }
  for (int i = 0; i < numBytes; i++) {
    out[index++] = decimalBuffer[i];
  }
}

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

  int32_t toInt32() { return static_cast<int32_t>(value_.low_bits()); }

  int64_t toInt64() { return static_cast<int64_t>(value_.low_bits()); }

  void toBytes(uint8_t* out) {
    int h = 0;
    int l = 0;
    int highShift = 0;
    int lowShift = 0;
    int8_t decimalBuffer[16];

    int numBytes = arrow::DecimalType::DecimalSize(precision_);

    if (numBytes > 8) {
      highShift = 8 * (numBytes - 8 - 1);
      lowShift = 56;
    } else {
      lowShift = 8 * (numBytes - 1);
    }

    int leftBytes = numBytes;
    if (numBytes > 8) {
      while (h < numBytes - 8) {
        decimalBuffer[h] = (value_.high_bits() >> highShift) & 0xFF;
        h++;
        highShift -= 8;
      }
      leftBytes = 8;
    }
    while (l < leftBytes) {
      decimalBuffer[h + l] = (value_.low_bits() >> lowShift) & 0xFF;
      l++;
      lowShift -= 8;
    }

    int index = 0;
    int8_t signByte;
    if (decimalBuffer[0] < 0)
      signByte = -1;
    else
      signByte = 0;
    for (int i = 0; i < 16 - numBytes; i++) {
      out[index++] = signByte;
    }
    for (int i = 0; i < numBytes; i++) {
      out[index++] = decimalBuffer[i];
    }
  }

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
