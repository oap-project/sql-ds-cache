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

void AttributeReferenceExpression::setSchema(
    std::shared_ptr<std::vector<Schema>> schema_) {
  schema = schema_;
  ptrdiff_t pos = std::distance(
      schema->begin(), std::find_if(schema->begin(), schema->end(), finder(columnName)));
  columnIndex = pos;
}

int RootAggExpression::ExecuteWithParam(int batchSize,
                                        const std::vector<int64_t>& dataBuffers,
                                        const std::vector<int64_t>& nullBuffers,
                                        std::vector<int8_t>& outBuffers) {
  if (!done) {
    auto start1 = std::chrono::steady_clock::now();
    child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
    auto end1 = std::chrono::steady_clock::now();
    ARROW_LOG(DEBUG)
        << "exec takes "
        << static_cast<std::chrono::duration<double>>(end1 - start1).count() * 1000
        << " ms";
    done = true;
  }
  return 0;
}

int AggExpression::ExecuteWithParam(int batchSize,
                                    const std::vector<int64_t>& dataBuffers,
                                    const std::vector<int64_t>& nullBuffers,
                                    std::vector<int8_t>& outBuffers) {
  if (!done) {
    child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
  }
  return 0;
}

void Count::getResult(DecimalVector& result) {
  if (typeid(*child) == typeid(LiteralExpression)) {  // for count(*) or count(1)
    result.data.push_back(arrow::BasicDecimal128(batchSize_));
    return;
  }
  if (!done) {
    done = true;
    auto tmp = DecimalVector();
    child->getResult(tmp);
    ARROW_LOG(INFO) << "count node child size: " << tmp.data.size();
    for (int i = 0; i < tmp.data.size(); i++) {
      if (tmp.nullVector->at(i)) count++;
    }
  }
  result.data.push_back(arrow::BasicDecimal128(count));
  result.type = ResultType::LongType;
}

int ArithmeticExpression::ExecuteWithParam(int batchSize,
                                           const std::vector<int64_t>& dataBuffers,
                                           const std::vector<int64_t>& nullBuffers,
                                           std::vector<int8_t>& outBuffers) {
  if (!done) {
    leftChild->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
    rightChild->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, outBuffers);
    // done = true;
  }
  return 0;
}

int AttributeReferenceExpression::ExecuteWithParam(
    int batchSize, const std::vector<int64_t>& dataBuffers,
    const std::vector<int64_t>& nullBuffers, std::vector<int8_t>& outBuffers) {
  if (!done) {
    done = true;
    int64_t dataPtr = dataBuffers[columnIndex];
    int64_t nullPtr = nullBuffers[columnIndex];
    std::vector<uint8_t> nullVec(batchSize);
    std::memcpy(nullVec.data(), (uint8_t*)nullPtr, batchSize);
    result.nullVector = std::make_shared<std::vector<uint8_t>>(nullVec);
    parquet::Type::type columnType = (*schema)[columnIndex].getColType();
    if (isDecimalType(dataType)) {
      int precision, scale;
      getPrecisionAndScaleFromDecimalType(dataType, precision, scale);
      result.data.clear();
      if (result.data.capacity() < batchSize) {
        result.data.reserve(batchSize);
      }
      if (columnType == parquet::Type::INT64) {
        DecimalConvertor::ConvertIntegerToDecimal128<parquet::Int64Type>(
            (const uint8_t*)(dataPtr), batchSize, precision, scale, result);
      } else if (columnType == parquet::Type::INT32) {
        DecimalConvertor::ConvertIntegerToDecimal128<parquet::Int32Type>(
            (const uint8_t*)(dataPtr), batchSize, precision, scale, result);
      } else if (columnType == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        int typeLength = (*schema)[columnIndex].getTypeLength();
        DecimalConvertor::ConvertFixLengthByteArrayToDecimal128(
            (const uint8_t*)(dataPtr), batchSize, typeLength, precision, scale, result);
      } else if (columnType == parquet::Type::BYTE_ARRAY) {
        DecimalConvertor::ConvertByteArrayToDecimal128(
            (const uint8_t*)(dataPtr), batchSize, precision, scale, result);
      }
    } else {
      if (columnType == parquet::Type::INT64) {
        DecimalConvertor::ConvertIntegerToDecimal128<parquet::Int64Type>(
            (const uint8_t*)(dataPtr), batchSize, 18, 0, result);
      } else if (columnType == parquet::Type::INT32) {
        DecimalConvertor::ConvertIntegerToDecimal128<parquet::Int32Type>(
            (const uint8_t*)(dataPtr), batchSize, 9, 0, result);
      } else if (columnType == parquet::Type::DOUBLE) {
        // TODO: get precision,scale
        // getPrecisionAndScaleFromDecimalType(dataType, precision, scale);
        int precision = 38, scale = 2;
        DecimalConvertor::ConvertRealToDecimal128<parquet::DoubleType>(
            (const uint8_t*)(dataPtr), batchSize, precision, scale, result);
      } else if (columnType == parquet::Type::FLOAT) {
        // TODO: get precision,scale
        // getPrecisionAndScaleFromDecimalType(dataType, precision, scale);
        int precision = 38, scale = 2;
        DecimalConvertor::ConvertRealToDecimal128<parquet::FloatType>(
            (const uint8_t*)(dataPtr), batchSize, precision, scale, result);
      } else {
        ARROW_LOG(ERROR) << "Unsupport dataType: " << columnType;
      }
    }
  }
  return 0;
}

}  // namespace ape
