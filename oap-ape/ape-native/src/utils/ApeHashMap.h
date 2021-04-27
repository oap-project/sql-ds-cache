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

#include <variant>
#include <vector>
#include <unordered_map>
#include <functional>

#include <arrow/util/logging.h>
#include <parquet/api/reader.h>

namespace std {

template <>
struct hash<parquet::ByteArray> {
  std::size_t operator()(parquet::ByteArray const& s) const noexcept {
    std::string str((const char*)(s.ptr), s.len);
    std::hash<string> hasher;
    return hasher(str);
  }
};

}  // namespace std

namespace ape {

// TODO: add FixedLenByteArray
using PartialKey = std::variant<int32_t, int64_t, float, double, parquet::ByteArray>;
using Key = std::vector<PartialKey>;

struct container_hash {
  std::size_t operator()(Key const& key) const {
    size_t ret = 0;
    for (int i = 0; i < key.size(); i++) {
      ret = (ret << 1) + std::hash<PartialKey>{}(key[i]);
    }
    return ret;
  }
};

struct key_eq {
  bool operator()(const Key& lhs, const Key& rhs) const {
    if (lhs.size() == rhs.size()) {
      for (int i = 0; i < lhs.size(); i++) {
        if (lhs[i].index() != rhs[i].index()) return false;
        if (lhs[i] != rhs[i]) return false;
      }
      return true;
    }
    return false;
  }
};

// a hash map for group by which could hold multiple keys(type and number)
using ApeHashMap = std::unordered_map<Key, int, container_hash, key_eq>;

}  // namespace ape
