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
#include <cstring>
#include <vector>

#include <arrow/util/logging.h>

#include "FilterExpression.h"
#include "expression.h"
#include "type.h"

namespace ape {

class finder {
 public:
  finder(const std::string& cmp_str) : str(cmp_str) {}

  bool operator()(Schema& v) { return v.getColName().compare(str) == 0; }

 private:
  const std::string str;
};

// Base class
FilterExpression::FilterExpression(std::string type_) : Expression() { type = type_; }

FilterExpression::~FilterExpression(){};

// RootFilterExpression
RootFilterExpression::RootFilterExpression(std::string type_,
                                           std::shared_ptr<FilterExpression> child_)
    : FilterExpression(type_) {
  child = child_;
};

int RootFilterExpression::ExecuteWithParam(int batchSize, long* dataBuffers,
                                           long* nullBuffers, std::vector<Schema>& schema,
                                           char* outBuffers) {
  // root node doesn't need outbuffer
  char* childBuffer = new char[batchSize];
  child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, schema, childBuffer);

  // TODO: NOT support String type well now.
  int hitIndex = 0;
  for (int i = 0; i < batchSize; i++) {
    if (childBuffer[i] == 1) {
      for (int j = 0; j < schema.size(); j++) {
        void* dataPtr = (void*)(dataBuffers[j]);
        char* nullPtr = (char*)(nullBuffers[j]);
        std::memcpy(dataPtr + hitIndex * schema[j].getDefaultSize(),
                    dataPtr + i * schema[j].getDefaultSize(), schema[j].getDefaultSize());
        nullPtr[j] = nullPtr[i];
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

  delete[] childBuffer;
  return hitIndex;
}

RootFilterExpression::~RootFilterExpression(){};

// NotFilterExpression
NotFilterExpression::NotFilterExpression(std::string type_,
                                         std::shared_ptr<Expression> child_)
    : FilterExpression(type_) {
  child = child_;
}

NotFilterExpression::~NotFilterExpression(){};

int NotFilterExpression::ExecuteWithParam(int batchSize, long* dataBuffers,
                                          long* nullBuffers, std::vector<Schema>& schema,
                                          char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  char* childBuffer = new char[batchSize];
  child->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, schema, childBuffer);

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
}

BinaryFilterExpression::~BinaryFilterExpression(){};

int BinaryFilterExpression::ExecuteWithParam(int batchSize, long* dataBuffers,
                                             long* nullBuffers,
                                             std::vector<Schema>& schema,
                                             char* outBuffers) {
  // assert(outBuffers != nullptr);
  std::memset(outBuffers, 0, batchSize);
  char* leftBuffer = new char[batchSize];
  char* rightBuffer = new char[batchSize];
  left->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, schema, leftBuffer);
  right->ExecuteWithParam(batchSize, dataBuffers, nullBuffers, schema, rightBuffer);

  if (type.compare("or") == 0) {
    for (int i = 0; i < batchSize; i++) {
      if (leftBuffer[i] == 1 || rightBuffer[i] == 1) {
        outBuffers[i] = 1;
      }
    }
  } else if (type.compare("and") == 0) {
    for (int i = 0; i < batchSize; i++) {
      if (leftBuffer[i] == 1 && rightBuffer[i] == 1) {
        outBuffers[i] = 1;
      }
    }
  } else {
    ARROW_LOG(WARNING) << "Impossible case!";
  }

  delete[] leftBuffer;
  delete[] rightBuffer;

  return 0;
}

// UnaryFilterExpression
template <typename T>
TypedUnaryFilterExpression<T>::TypedUnaryFilterExpression(std::string type_,
                                                          std::string columnName_,
                                                          T value_)
    : FilterExpression(type_) {
  columnName = columnName_;
  value = value_;
}

template <>
int TypedUnaryFilterExpression<NullStruct>::ExecuteWithParam(int batchSize,
                                                             long* dataBuffers,
                                                             long* nullBuffers,
                                                             std::vector<Schema>& schema,
                                                             char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  ptrdiff_t pos = std::distance(
      schema.begin(), std::find_if(schema.begin(), schema.end(), finder(columnName)));
  long nullPtr = *(nullBuffers + pos);
  char* ptr = (char*)nullPtr;
  // todo: eq / noteq???
  if (type.compare("noteq") == 0) {  // not equal to null, we can return buffer directly.
    std::memcpy(outBuffers, ptr, batchSize);
    for (int i = 0; i < batchSize; i++) {
      if (ptr[i] == 1) {
        outBuffers[i] = 1;
      } else if (ptr[i] == 0) {
        outBuffers[i] = 0;
      } else {
        ARROW_LOG(WARNING) << "Impossible case!";
      }
    }
  } else if (type.compare("eq") == 0) {
    for (int i = 0; i < batchSize; i++) {
      if (ptr[i] == 0) {
        outBuffers[i] = 1;
      } else if (ptr[i] == 1) {
        outBuffers[i] = 0;
      } else {
        ARROW_LOG(WARNING) << "Impossible case!";
      }
    }
  }
  return 0;
}

template <typename T>
int TypedUnaryFilterExpression<T>::ExecuteWithParam(int batchSize, long* dataBuffers,
                                                    long* nullBuffers,
                                                    std::vector<Schema>& schema,
                                                    char* outBuffers) {
  std::memset(outBuffers, 0, batchSize);
  ptrdiff_t pos = std::distance(
      schema.begin(), std::find_if(schema.begin(), schema.end(), finder(columnName)));
  long dataPtr = *(dataBuffers + pos);
  long nullPtr = *(nullBuffers + pos);
  T* ptr = (T*)dataPtr;

  if (type.compare("lt") == 0) {
    // todo: make it vectorized
    for (int i = 0; i < batchSize; i++) {
      if (ptr[i] < value) {
        outBuffers[i] = 1;
      }
    }
  } else if (type.compare("lteq") == 0) {
    for (int i = 0; i < batchSize; i++) {
      if (ptr[i] <= value) {
        outBuffers[i] = 1;
      }
    }
  } else if (type.compare("gt") == 0) {
    for (int i = 0; i < batchSize; i++) {
      if (ptr[i] > value) {
        outBuffers[i] = 1;
      }
    }
  } else if (type.compare("gteq") == 0) {
    for (int i = 0; i < batchSize; i++) {
      if (ptr[i] >= value) {
        outBuffers[i] = 1;
      }
    }
  } else if (type.compare("eq") == 0) {
    for (int i = 0; i < batchSize; i++) {
      if (ptr[i] == value) {
        outBuffers[i] = 1;
      }
    }
  } else if (type.compare("noteq") == 0) {
    for (int i = 0; i < batchSize; i++) {
      if (ptr[i] != value) {
        outBuffers[i] = 1;
      }
    }
  } else {
    ARROW_LOG(WARNING) << "Impossible case!";
  }
  return 0;
}

template <typename T>
TypedUnaryFilterExpression<T>::~TypedUnaryFilterExpression(){};

// Force compile these classes.
template class TypedUnaryFilterExpression<bool>;
template class TypedUnaryFilterExpression<int>;
template class TypedUnaryFilterExpression<long>;
template class TypedUnaryFilterExpression<float>;
template class TypedUnaryFilterExpression<double>;
template class TypedUnaryFilterExpression<NullStruct>;

}  // namespace ape
