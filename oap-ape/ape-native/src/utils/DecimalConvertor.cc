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

#include <climits>

#include <arrow/util/bit_util.h>
#include <arrow/util/int_util.h>
#include <arrow/util/logging.h>
#include <parquet/types.h>

#include "src/utils/DecimalConvertor.h"

namespace ape {

/// Signed left shift with well-defined behaviour on negative numbers or overflow
template <typename SignedInt, typename Shift>
SignedInt SafeLeftShift(SignedInt u, Shift shift) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) << shift);
}

// TODO: Code copy from arrow::Decimal128::FromBigEndian, should think how to use this
// function directly.
static inline uint64_t UInt64FromBigEndian(const uint8_t* bytes, int32_t length) {
  // We don't bounds check the length here because this is called by
  // FromBigEndian that has a Decimal128 as its out parameters and
  // that function is already checking the length of the bytes and only
  // passes lengths between zero and eight.
  uint64_t result = 0;
  // Using memcpy instead of special casing for length
  // and doing the conversion in 16, 32 parts, which could
  // possibly create unaligned memory access on certain platforms
  memcpy(reinterpret_cast<uint8_t*>(&result) + 8 - length, bytes, length);
  return ::arrow::BitUtil::FromBigEndian(result);
}

static void FromBigEndian(const uint8_t* bytes, int32_t length, int64_t* out_high,
                          uint64_t* out_low) {
  static constexpr int32_t kMinDecimalBytes = 1;
  static constexpr int32_t kMaxDecimalBytes = 16;

  DCHECK_GE(length, kMinDecimalBytes);
  DCHECK_LE(length, kMaxDecimalBytes);

  int64_t high, low;

  // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  // sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  // 1. Extract the high bytes
  // Stop byte of the high bytes
  const int32_t high_bits_offset = std::max(0, length - 8);
  const auto high_bits = UInt64FromBigEndian(bytes, high_bits_offset);

  if (high_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    high = high_bits;
  } else {
    high = -1 * (is_negative && length < kMaxDecimalBytes);
    // Shift left enough bits to make room for the incoming int64_t
    high = SafeLeftShift(high, high_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    high |= high_bits;
  }

  // 2. Extract the low bytes
  // Stop byte of the low bytes
  const int32_t low_bits_offset = std::min(length, 8);
  const auto low_bits =
      UInt64FromBigEndian(bytes + high_bits_offset, length - high_bits_offset);

  if (low_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    low = low_bits;
  } else {
    // Sign extend the low bits if necessary
    low = -1 * (is_negative && length < 8);
    // Shift left enough bits to make room for the incoming int64_t
    low = SafeLeftShift(low, low_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    low |= low_bits;
  }

  *out_high = high;
  *out_low = static_cast<uint64_t>(low);

  return;
}

void DecimalConvertor::ConvertFixLengthByteArrayToDecimal128(
    const uint8_t* values, int32_t num_values, int32_t type_length, int32_t precision,
    int32_t scale, ApeDecimal128Vector& out) {
  parquet::FixedLenByteArray* fixed_length_byte_array =
      (parquet::FixedLenByteArray*)(values);
  int64_t high;
  uint64_t low;
  for (int32_t i = 0; i < num_values; ++i) {
    parquet::FixedLenByteArray value = fixed_length_byte_array[i];
    FromBigEndian(value.ptr, type_length, &high, &low);
    out.push_back(std::make_shared<ApeDecimal128>(high, low, precision, scale));
  }

  return;
}

void DecimalConvertor::ConvertByteArrayToDecimal128(const uint8_t* values,
                                                    int32_t num_values, int32_t precision,
                                                    int32_t scale,
                                                    ApeDecimal128Vector& out) {
  parquet::ByteArray* byte_array = (parquet::ByteArray*)(values);
  int64_t high;
  uint64_t low;
  for (int32_t i = 0; i < num_values; ++i) {
    parquet::ByteArray value = byte_array[i];
    FromBigEndian(value.ptr, value.len, &high, &low);
    out.push_back(std::make_shared<ApeDecimal128>(high, low, precision, scale));
  }

  return;
}

}  // namespace ape
