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

#include "ApeDecimal.h"

namespace ape {

using ApeDecimal128Ptr = std::shared_ptr<ApeDecimal128>;
using ApeDecimal128Vector = std::vector<ApeDecimal128Ptr>;

class DecimalConvertor {
 public:
  template <typename ParquetIntegerType>
  static void ConvertIntegerToDecimal128(const uint8_t* values, int32_t num_values,
                                         int32_t precision, int32_t scale,
                                         ApeDecimal128Vector& out) {
    using ElementType = typename ParquetIntegerType::c_type;
    static_assert(std::is_same<ElementType, int32_t>::value ||
                      std::is_same<ElementType, int64_t>::value,
                  "ElementType must be int32_t or int64_t");

    const auto elements = reinterpret_cast<const ElementType*>(values);

    uint64_t high;
    uint64_t low;
    for (int32_t i = 0; i < num_values; ++i) {
      const auto value = static_cast<int64_t>(elements[i]);
      low = arrow::BitUtil::FromLittleEndian(static_cast<uint64_t>(value));
      high = static_cast<uint64_t>(value < 0 ? -1 : 0);
      out.push_back(std::make_shared<ApeDecimal128>(high, low, precision, scale));
    }

    return;
  }

  static void ConvertFixLengthByteArrayToDecimal128(const uint8_t* values,
                                                    int32_t num_values,
                                                    int32_t type_length,
                                                    int32_t precision, int32_t scale,
                                                    ApeDecimal128Vector& out);

  static void ConvertByteArrayToDecimal128(const uint8_t* values, int32_t num_values,
                                           int32_t precision, int32_t scale,
                                           ApeDecimal128Vector& out);
};

}  // namespace ape
