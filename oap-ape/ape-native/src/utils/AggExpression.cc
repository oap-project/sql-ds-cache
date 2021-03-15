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

#include <chrono>

#include <arrow/util/decimal.h>
#include <arrow/util/logging.h>

#include "src/utils/AggExpression.h"

namespace ape {

class finder {
 public:
  explicit finder(const std::string& cmp_str) : str(cmp_str) {}

  bool operator()(Schema& v) { return v.getColName().compare(str) == 0; }

 private:
  const std::string str;
};

void AttributeReferenceExpression::setSchema(std::vector<Schema> schema_) {
  schema = schema_;
  ptrdiff_t pos = std::distance(
      schema.begin(), std::find_if(schema.begin(), schema.end(), finder(columnName)));
  columnIndex = pos;
}

int RootAggExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                        int64_t* nullBuffers, char* outBuffers) {
  auto start1 = std::chrono::steady_clock::now();
  child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
  auto end1 = std::chrono::steady_clock::now();
  ARROW_LOG(DEBUG) << "exec takes "
                   << static_cast<std::chrono::duration<double>>(end1 - start1).count() *
                          1000
                   << " ms";

  return 0;
}

int AggExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                    int64_t* nullBuffers, char* outBuffers) {
  child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
  return 0;
}

int ArithmeticExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                           int64_t* nullBuffers, char* outBuffers) {
  leftChild->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
  rightChild->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
  return 0;
}

int AttributeReferenceExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                                   int64_t* nullBuffers,
                                                   char* outBuffers) {
  int64_t dataPtr = *(dataBuffers + columnIndex);
  int64_t nullPtr = *(nullBuffers + columnIndex);
  std::string decimalType("DecimalType");
  if (dataType.compare(0, decimalType.length(), decimalType) == 0) {
    int precision, scale;
    getPrecisionAndScaleFromDecimalType(dataType, precision, scale);
    parquet::Type::type columnType = schema[columnIndex].getColType();
    result.clear();
    if (result.capacity() < batchSize) {
      result.reserve(batchSize);
    }
    if (columnType == parquet::Type::INT64) {
      DecimalConvertor::ConvertIntegerToDecimal128<parquet::Int64Type>(
          (const uint8_t*)(dataPtr), batchSize, precision, scale, result);
    } else if (columnType == parquet::Type::INT32) {
      DecimalConvertor::ConvertIntegerToDecimal128<parquet::Int32Type>(
          (const uint8_t*)(dataPtr), batchSize, precision, scale, result);
    } else if (columnType == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      // TODO: get flba length from column desc
      // DecimalConvertor::ConvertFixLengthByteArrayToDecimal128(
      //    (const uint8_t *)(dataPtr),
      //    batchSize, type_length, precision, scale, result);
    } else if (columnType == parquet::Type::BYTE_ARRAY) {
      DecimalConvertor::ConvertByteArrayToDecimal128((const uint8_t*)(dataPtr), batchSize,
                                                     precision, scale, result);
    }

  } else {
    // TODO: Add other type support
  }
  return 0;
}

}  // namespace ape
