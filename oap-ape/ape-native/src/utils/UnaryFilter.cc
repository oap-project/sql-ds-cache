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

#include "UnaryFilter.h"

#ifdef USE_LIB_QPL
#undef USE_LIB_QPL
#endif

#ifdef USE_AVX
#undef USE_AVX
#endif

#define NOT_SUPPORT(dataType, filterType)                                             \
  template <>                                                                         \
  void filterType<dataType>::execute(dataType* buffer, dataType value, int batchSize, \
                                     char* out) {                                     \
    ARROW_LOG(WARNING) << "Not support!";                                             \
  }

namespace ape {

// For NullStruct and ByteArray, we will not use these methods.
NOT_SUPPORT(NullStruct, Gt);
NOT_SUPPORT(NullStruct, GtEq);
NOT_SUPPORT(NullStruct, Lt);
NOT_SUPPORT(NullStruct, LtEq);

NOT_SUPPORT(parquet::ByteArray, Gt);
NOT_SUPPORT(parquet::ByteArray, GtEq);
NOT_SUPPORT(parquet::ByteArray, Lt);
NOT_SUPPORT(parquet::ByteArray, LtEq);

template <>
void Eq<parquet::ByteArray>::execute(parquet::ByteArray* buffer, parquet::ByteArray value,
                                     int batchSize, char* out) {
  int len = value.len;
  for (int i = 0; i < batchSize; i++) {
    if (buffer[i].len == len && !std::memcmp(buffer[i].ptr, value.ptr, len)) {
      out[i] = 1;
    }
  }
}

template <>
void NotEq<parquet::ByteArray>::execute(parquet::ByteArray* buffer,
                                        parquet::ByteArray value, int batchSize,
                                        char* out) {
  int len = value.len;
  for (int i = 0; i < batchSize; i++) {
    if (buffer[i].len != len || std::memcmp(buffer[i].ptr, value.ptr, len)) {
      out[i] = 1;
    }
  }
}

template <>
void Eq<NullStruct>::execute(NullStruct* buffer, NullStruct value, int batchSize,
                             char* out) {
  // it's trick that we did such cast.
  char* buf = (char*)buffer;
  for (int i = 0; i < batchSize; i++) {
    out[i] = !buf[i];
  }
}

template <>
void NotEq<NullStruct>::execute(NullStruct* buffer, NullStruct value, int batchSize,
                                char* out) {
  char* buf = (char*)buffer;
  for (int i = 0; i < batchSize; i++) {
    out[i] = buf[i];
  }
}

// impl execute.
// TODO: add AVX/LIBQPL integration.
template <typename T>
void Gt<T>::execute(T* buffer, T value, int batchSize, char* out) {
  ARROW_LOG(DEBUG) << "gt";
#ifdef USE_LIB_QPL
  // use QPL to evalute
#elif USE_AVX
  // use AVX to evalute
#else
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] > value) ? 1 : 0;
  }
#endif
};

template <typename T>
void GtEq<T>::execute(T* buffer, T value, int batchSize, char* out) {
  ARROW_LOG(DEBUG) << "gteq";
#ifdef USE_LIB_QPL
  // use QPL to evalute
#elif USE_AVX
  // use AVX to evalute
#else
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] >= value) ? 1 : 0;
  }
#endif
}

template <typename T>
void Eq<T>::execute(T* buffer, T value, int batchSize, char* out) {
#ifdef USE_LIB_QPL
  // use QPL to evalute
#elif USE_AVX
  // use AVX to evalute
#else
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] == value) ? 1 : 0;
  }
#endif
}

template <typename T>
void NotEq<T>::execute(T* buffer, T value, int batchSize, char* out) {
#ifdef USE_LIB_QPL
  // use QPL to evalute
#elif USE_AVX
  // use AVX to evalute
#else
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] != value) ? 1 : 0;
  }
#endif
}

template <typename T>
void Lt<T>::execute(T* buffer, T value, int batchSize, char* out) {
#ifdef USE_LIB_QPL
  // use QPL to evalute
#elif USE_AVX
  // use AVX to evalute
#else
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] < value) ? 1 : 0;
  }
#endif
}

template <typename T>
void LtEq<T>::execute(T* buffer, T value, int batchSize, char* out) {
#ifdef USE_LIB_QPL
  // use QPL to evalute
#elif USE_AVX
  // use AVX to evalute
#else
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] <= value) ? 1 : 0;
  }
#endif
}

}  // namespace ape
