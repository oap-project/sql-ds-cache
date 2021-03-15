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

#include <algorithm>
#include <chrono>
#include <cstring>
#include <vector>

#include <arrow/util/logging.h>

#include "src/utils/FilterExpression.h"
#include "src/utils/expression.h"
#include "src/utils/type.h"

namespace ape {

class finder {
 public:
  explicit finder(const std::string& cmp_str) : str(cmp_str) {}

  bool operator()(Schema& v) { return v.getColName().compare(str) == 0; }

 private:
  const std::string str;
};

// Base class
FilterExpression::FilterExpression(std::string type_) : Expression() { type = type_; }

FilterExpression::~FilterExpression() {}

// RootFilterExpression
RootFilterExpression::RootFilterExpression(std::string type_,
                                           std::shared_ptr<FilterExpression> child_)
    : FilterExpression(type_) {
  child = child_;
}

int RootFilterExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                           int64_t* nullBuffers, char* outBuffers) {
  // root node doesn't need outbuffer
  char* childBuffer = new char[batchSize];
  auto start1 = std::chrono::steady_clock::now();
  child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, childBuffer);
  auto end1 = std::chrono::steady_clock::now();
  ARROW_LOG(DEBUG) << "exec takes "
                   << static_cast<std::chrono::duration<double>>(end1 - start1).count() *
                          1000
                   << " ms";

  // TODO: NOT support String type well now.
  int hitIndex = 0;
  for (int i = 0; i < batchSize; i++) {
    if (childBuffer[i] == 1) {
      for (int j = 0; j < schema.size(); j++) {
        void* dataPtr = (void*)(dataBuffers[j]);
        char* nullPtr = (char*)(nullBuffers[j]);
        std::memcpy(dataPtr + hitIndex * schema[j].getDefaultSize(),
                    dataPtr + i * schema[j].getDefaultSize(), schema[j].getDefaultSize());
        nullPtr[hitIndex] = nullPtr[i];
      }
      hitIndex++;
    }
  }
  // we should set left to zero
  for (int i = 0; i < schema.size(); i++) {
    void* dataPtr = (void*)(dataBuffers[i]);
    char* nullPtr = (char*)(nullBuffers[i]);
    // std::memset(dataPtr + hitIndex * defaultSize, 0, (batchSize - hitIndex) *
    // defaultSize)
  }

  auto end2 = std::chrono::steady_clock::now();
  ARROW_LOG(DEBUG) << "copy takes "
                   << static_cast<std::chrono::duration<double>>(end2 - end1).count() *
                          1000
                   << " ms";

  delete[] childBuffer;
  return hitIndex;
}

RootFilterExpression::~RootFilterExpression() {}

// NotFilterExpression
NotFilterExpression::NotFilterExpression(std::string type_,
                                         std::shared_ptr<Expression> child_)
    : FilterExpression(type_) {
  child = child_;
}

NotFilterExpression::~NotFilterExpression() {}

int NotFilterExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                          int64_t* nullBuffers, char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  char* childBuffer = new char[batchSize];
  child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, childBuffer);

  for (int i = 0; i < batchSize; i++) {
    if (childBuffer[i] == 1) {
      outBuffers[i] = 0;
    } else if (childBuffer[i] == 0) {
      outBuffers[i] = 1;
    } else {
      ARROW_LOG(WARNING) << "Impossible case!";
    }
  }
  delete[] childBuffer;

  return 0;
}

// BinaryFilterExpression
BinaryFilterExpression::BinaryFilterExpression(std::string type_,
                                               std::shared_ptr<Expression> left_,
                                               std::shared_ptr<Expression> right_)
    : FilterExpression(type_) {
  left = left_;
  right = right_;

  if (type_.compare("or") == 0) {
    op = std::make_shared<Or>();
  } else if (type_.compare("and") == 0) {
    op = std::make_shared<And>();
  }
}

BinaryFilterExpression::~BinaryFilterExpression() {}

int BinaryFilterExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                             int64_t* nullBuffers, char* outBuffers) {
  // assert(outBuffers != nullptr);
  std::memset(outBuffers, 0, batchSize);
  char* leftBuffer = new char[batchSize];
  char* rightBuffer = new char[batchSize];
  left->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, leftBuffer);
  right->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, rightBuffer);

  op->execute(batchSize, leftBuffer, rightBuffer, outBuffers);

  delete[] leftBuffer;
  delete[] rightBuffer;

  return 0;
}

// StringFilterExpression
StringFilterExpression::StringFilterExpression(std::string type_, std::string columnName_,
                                               std::string value_)
    : UnaryFilterExpression(type_, columnName_) {
  value = value_;
}

void StringFilterExpression::setSchema(std::vector<Schema> schema_) {
  schema = schema_;
  ptrdiff_t pos = std::distance(
      schema.begin(), std::find_if(schema.begin(), schema.end(), finder(columnName)));
  columnIndex = pos;
}

std::string StringFilterExpression::getColumnName() { return columnName; }

// StartWithFilterExpression
int StartWithFilterExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                                int64_t* nullBuffers, char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  int64_t dataPtr = *(dataBuffers + columnIndex);
  parquet::ByteArray* data = (parquet::ByteArray*)dataPtr;
  int len = value.length();
  for (int i = 0; i < batchSize; i++) {
    if (data[i].len >= value.length() &&
        std::memcmp(data[i].ptr, value.data(), len) == 0) {
      outBuffers[i] = 1;
    }
  }
  return 0;
}

// EndWithFilterExpression
int EndWithFilterExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                              int64_t* nullBuffers, char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  int64_t dataPtr = *(dataBuffers + columnIndex);
  parquet::ByteArray* data = (parquet::ByteArray*)dataPtr;
  int len = value.length();
  for (int i = 0; i < batchSize; i++) {
    if (data[i].len >= value.length() &&
        std::memcmp(data[i].ptr + (data[i].len - len), value.data(), len) == 0) {
      outBuffers[i] = 1;
    }
  }
  return 0;
}

// ContainsFilterExpression
int ContainsFilterExpression::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                               int64_t* nullBuffers, char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  int64_t dataPtr = *(dataBuffers + columnIndex);
  parquet::ByteArray* data = (parquet::ByteArray*)dataPtr;
  int len = value.length();
  for (int i = 0; i < batchSize; i++) {
    std::string s = std::string((char*)data[i].ptr, data[i].len);
    if (data[i].len >= value.length() && s.find(value) != std::string::npos) {
      outBuffers[i] = 1;
    }
  }
  return 0;
}

// UnaryFilterExpression
template <typename T>
TypedUnaryFilterExpression<T>::TypedUnaryFilterExpression(std::string type_,
                                                          std::string columnName_,
                                                          T value_)
    : UnaryFilterExpression(type_, columnName_) {
  value = value_;
  if (type.compare("gt") == 0) {
    filter = std::make_shared<Gt<T>>();
  } else if (type.compare("gteq") == 0) {
    filter = std::make_shared<GtEq<T>>();
  } else if (type.compare("eq") == 0) {
    filter = std::make_shared<Eq<T>>();
  } else if (type.compare("noteq") == 0) {
    filter = std::make_shared<NotEq<T>>();
  } else if (type.compare("lt") == 0) {
    filter = std::make_shared<Lt<T>>();
  } else if (type.compare("lteq") == 0) {
    filter = std::make_shared<LtEq<T>>();
  } else {
    ARROW_LOG(WARNING) << "NOT support Filter type!";
  }
}

template <>
int TypedUnaryFilterExpression<NullStruct>::ExecuteWithParam(int batchSize,
                                                             int64_t* dataBuffers,
                                                             int64_t* nullBuffers,
                                                             char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  int64_t nullPtr = *(nullBuffers + columnIndex);
  char* ptr = (char*)nullPtr;
  NullStruct nullSturct;
  filter->execute((NullStruct*)ptr, nullSturct, batchSize, outBuffers);

  return 0;
}

template <typename T>
int TypedUnaryFilterExpression<T>::ExecuteWithParam(int batchSize, int64_t* dataBuffers,
                                                    int64_t* nullBuffers,
                                                    char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  int64_t dataPtr = *(dataBuffers + columnIndex);
  int64_t nullPtr = *(nullBuffers + columnIndex);
  T* ptr = (T*)dataPtr;

  filter->execute(ptr, value, batchSize, outBuffers);

  return 0;
}

template <typename T>
void TypedUnaryFilterExpression<T>::setSchema(std::vector<Schema> schema_) {
  schema = schema_;
  ptrdiff_t pos = std::distance(
      schema.begin(), std::find_if(schema.begin(), schema.end(), finder(columnName)));
  columnIndex = pos;
}

std::string UnaryFilterExpression::getColumnName() { return columnName; }

template <typename T>
TypedUnaryFilterExpression<T>::~TypedUnaryFilterExpression() {}

// Force compile these classes.
template class TypedUnaryFilterExpression<bool>;
template class TypedUnaryFilterExpression<int>;
template class TypedUnaryFilterExpression<int64_t>;
template class TypedUnaryFilterExpression<float>;
template class TypedUnaryFilterExpression<double>;
template class TypedUnaryFilterExpression<NullStruct>;
template class TypedUnaryFilterExpression<parquet::ByteArray>;

}  // namespace ape
