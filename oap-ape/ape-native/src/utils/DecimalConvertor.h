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

#include <parquet/column_reader.h>

#include <arrow/result.h>
#include <arrow/util/decimal.h>

#include "src/utils/ApeDecimal.h"

namespace ape {

using Decimal128Vector = std::vector<arrow::BasicDecimal128>;

enum ResultType {
  IntType,
  LongType,
  FloatType,
  DoubleType,
  Decimal64Type,
  Decimal128Type,
  ErrorType
};

struct DecimalVector {
  Decimal128Vector data;
  int32_t precision;
  int32_t scale;
  ResultType type;
};

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

static ResultType GetResultType(std::string s) {
  if (s.compare("IntType") == 0) return IntType;
  if (s.compare("LongType") == 0) return LongType;
  if (s.compare("FloatType") == 0) return FloatType;
  if (s.compare("DoubleType") == 0) return DoubleType;
  int a, b;
  if (getPrecisionAndScaleFromDecimalType(s, a, b) == 0) {
    if (a <= 18)
      return Decimal64Type;
    else if (a <= 38)
      return Decimal128Type;
  }
  return ErrorType;
}

static void dumpToJavaBuffer(uint8_t* bufferAddr, DecimalVector& result) {
  switch (result.type) {
    case ResultType::IntType: {
      *((int32_t*)bufferAddr) = static_cast<int32_t>(result.data[0].low_bits());
      break;
    }
    case ResultType::LongType: {
      *((int64_t*)bufferAddr) = static_cast<int64_t>(result.data[0].low_bits());
      break;
    }
    case ResultType::FloatType: {
      // TODO: convert
      break;
    }
    case ResultType::DoubleType: {
      // TODO: convert
      break;
    }
    case ResultType::Decimal64Type: {
      *((int64_t*)bufferAddr) = static_cast<int64_t>(result.data[0].low_bits());
      break;
    }
    case ResultType::Decimal128Type: {
      decimalToBytes(result.data[0], result.precision, (uint8_t*)(bufferAddr));
      break;
    }
    default: {
      ARROW_LOG(WARNING) << "Type not support!";
      break;
    }
  }
}

class DecimalConvertor {
 public:
  template <typename ParquetIntegerType>
  static void ConvertIntegerToDecimal128(const uint8_t* values, int32_t num_values,
                                         int32_t precision, int32_t scale,
                                         DecimalVector& out) {
    using ElementType = typename ParquetIntegerType::c_type;
    static_assert(std::is_same<ElementType, int32_t>::value ||
                      std::is_same<ElementType, int64_t>::value,
                  "ElementType must be int32_t or int64_t");

    const auto elements = reinterpret_cast<const ElementType*>(values);

    out.data.clear();
    if (out.data.capacity() < num_values) out.data.reserve(num_values);

    uint64_t high;
    uint64_t low;
    for (int32_t i = 0; i < num_values; ++i) {
      const auto value = static_cast<int64_t>(elements[i]);
      low = arrow::BitUtil::FromLittleEndian(static_cast<uint64_t>(value));
      high = static_cast<uint64_t>(value < 0 ? -1 : 0);
      out.data.push_back(arrow::BasicDecimal128(high, low));
    }
    out.precision = precision;
    out.scale = scale;
    return;
  }

  static void ConvertFixLengthByteArrayToDecimal128(const uint8_t* values,
                                                    int32_t num_values,
                                                    int32_t type_length,
                                                    int32_t precision, int32_t scale,
                                                    DecimalVector& out);

  static void ConvertByteArrayToDecimal128(const uint8_t* values, int32_t num_values,
                                           int32_t precision, int32_t scale,
                                           DecimalVector& out);

  template <typename ParquetRealType>
  static arrow::Status ConvertRealToDecimal128(const uint8_t* values, int32_t num_values,
                                               int32_t precision, int32_t scale,
                                               DecimalVector& out) {
    using ElementType = typename ParquetRealType::c_type;
    static_assert(std::is_same<ElementType, float>::value ||
                      std::is_same<ElementType, double>::value,
                  "ElementType must be float or double");
    const auto elements = reinterpret_cast<const ElementType*>(values);
    out.data.clear();
    if (out.data.capacity() < num_values) out.data.reserve(num_values);
    for (int32_t i = 0; i < num_values; ++i) {
      ARROW_ASSIGN_OR_RAISE(auto decimal,
                            arrow::Decimal128::FromReal(elements[i], precision, scale));
      out.data.push_back(arrow::BasicDecimal128(decimal.high_bits(), decimal.low_bits()));
    }
    out.precision = precision;
    out.scale = scale;
    return arrow::Status::OK();
  }
};

}  // namespace ape
