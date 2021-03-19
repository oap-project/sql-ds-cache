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

#include <arrow/util/logging.h>
#include <parquet/types.h>

#include "src/utils/type.h"

namespace ape {

template <typename T>
class UnaryFilter {
 public:
  virtual void execute(T* dataBuffer, char* nullBuffer, T value, int batchSize, char* out) {
    ARROW_LOG(WARNING) << "should never be called!";
  }
};

template <typename T>
class Gt : public UnaryFilter<T> {
 public:
  void execute(T* dataBuffer, char* nullBuffer, T value, int batchSize, char* out);
};

template <typename T>
class GtEq : public UnaryFilter<T> {
 public:
  void execute(T* dataBuffer, char* nullBuffer, T value, int batchSize, char* out);
};

template <typename T>
class Eq : public UnaryFilter<T> {
 public:
  void execute(T* dataBuffer, char* nullBuffer, T value, int batchSize, char* out);
};

template <typename T>
class NotEq : public UnaryFilter<T> {
 public:
  void execute(T* dataBuffer, char* nullBuffer, T value, int batchSize, char* out);
};

template <typename T>
class Lt : public UnaryFilter<T> {
 public:
  void execute(T* dataBuffer, char* nullBuffer, T value, int batchSize, char* out);
};

template <typename T>
class LtEq : public UnaryFilter<T> {
 public:
  void execute(T* dataBuffer, char* nullBuffer, T value, int batchSize, char* out);
};

template class Gt<int>;
template class Gt<int64_t>;
template class Gt<float>;
template class Gt<double>;
template class Gt<bool>;
// template class Gt<NullStuct>;

template class GtEq<int>;
template class GtEq<int64_t>;
template class GtEq<float>;
template class GtEq<double>;
template class GtEq<bool>;
// template class GtEq<NullStuct>;

template class Eq<int>;
template class Eq<int64_t>;
template class Eq<float>;
template class Eq<double>;
template class Eq<bool>;
// template class Eq<NullStuct>;

template class NotEq<int>;
template class NotEq<int64_t>;
template class NotEq<float>;
template class NotEq<double>;
template class NotEq<bool>;
// template class NotEq<NullStuct>;

template class Lt<int>;
template class Lt<int64_t>;
template class Lt<float>;
template class Lt<double>;
template class Lt<bool>;
// template class Lt<NullStuct>;

template class LtEq<int>;
template class LtEq<int64_t>;
template class LtEq<float>;
template class LtEq<double>;
template class LtEq<bool>;
// template class LtEq<NullStuct>;

}  // namespace ape
