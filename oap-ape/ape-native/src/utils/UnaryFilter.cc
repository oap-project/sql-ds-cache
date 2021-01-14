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

#ifdef APE_WITH_QPL
#include <type_traits>
#include "QplFilter.h"
#endif

#ifdef USE_AVX
#undef USE_AVX
#endif

namespace ape {

// For NullStruct, we will not use this method now.
template <>
void Gt<NullStruct>::execute(NullStruct* buffer, NullStruct value, int batchSize,
                             char* out) {
  ARROW_LOG(WARNING) << "Not support!";
}

template <>
void GtEq<NullStruct>::execute(NullStruct* buffer, NullStruct value, int batchSize,
                               char* out) {
  ARROW_LOG(WARNING) << "Not support!";
}

template <>
void Lt<NullStruct>::execute(NullStruct* buffer, NullStruct value, int batchSize,
                             char* out) {
  ARROW_LOG(WARNING) << "Not support!";
}

template <>
void LtEq<NullStruct>::execute(NullStruct* buffer, NullStruct value, int batchSize,
                               char* out) {
  ARROW_LOG(WARNING) << "Not support!";
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
#ifdef APE_WITH_QPL
  if (std::is_same<T, int>::value) {
    const auto *in = reinterpret_cast<const uint8_t *>(buffer);
    QplFilter(qpl::greater, false, value, in, out, batchSize);
  } else {
#elif USE_AVX
  // use AVX to evalute
#endif
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] > value) ? 1 : 0;
  }
#ifdef APE_WITH_QPL
  }
#endif
};

template <typename T>
void GtEq<T>::execute(T* buffer, T value, int batchSize, char* out) {
  ARROW_LOG(DEBUG) << "gteq";
#ifdef APE_WITH_QPL
  if (std::is_same<T, int>::value) {
    const auto *in = reinterpret_cast<const uint8_t *>(buffer);
    QplFilter(qpl::greater, true, value, in, out, batchSize);
  } else {
#elif USE_AVX
  // use AVX to evalute
#endif
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] >= value) ? 1 : 0;
  }
#ifdef APE_WITH_QPL
  }
#endif
}

template <typename T>
void Eq<T>::execute(T* buffer, T value, int batchSize, char* out) {
#ifdef APE_WITH_QPL
  if (std::is_same<T, int>::value) {
    const auto *in = reinterpret_cast<const uint8_t *>(buffer);
    QplFilter(qpl::equals, true, value, in, out, batchSize);
  } else {
#elif USE_AVX
  // use AVX to evalute
#endif
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] == value) ? 1 : 0;
  }
#ifdef APE_WITH_QPL
  }
#endif
}

template <typename T>
void NotEq<T>::execute(T* buffer, T value, int batchSize, char* out) {
#ifdef APE_WITH_QPL
  if (std::is_same<T, int>::value) {
    const auto *in = reinterpret_cast<const uint8_t *>(buffer);
    QplFilter(qpl::not_equals, true, value, in, out, batchSize);
  } else {
#elif USE_AVX
  // use AVX to evalute
#endif
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] != value) ? 1 : 0;
  }
#ifdef APE_WITH_QPL
  }
#endif
}

template <typename T>
void Lt<T>::execute(T* buffer, T value, int batchSize, char* out) {
#ifdef APE_WITH_QPL
  if (std::is_same<T, int>::value) {
    const auto *in = reinterpret_cast<const uint8_t *>(buffer);
    QplFilter(qpl::less, false, value, in, out, batchSize);
  } else {
#elif USE_AVX
  // use AVX to evalute
#endif
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] < value) ? 1 : 0;
  }
#ifdef APE_WITH_QPL
  }
#endif
}

template <typename T>
void LtEq<T>::execute(T* buffer, T value, int batchSize, char* out) {
#ifdef APE_WITH_QPL
  if (std::is_same<T, int>::value) {
    const auto *in = reinterpret_cast<const uint8_t *>(buffer);
    QplFilter(qpl::less, true, value, in, out, batchSize);
  } else {
#elif USE_AVX
  // use AVX to evalute
#endif
  for (int i = 0; i < batchSize; i++) {
    out[i] = (buffer[i] <= value) ? 1 : 0;
  }
#ifdef APE_WITH_QPL
  }
#endif
}

}  // namespace ape
