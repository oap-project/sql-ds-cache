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

#include <iostream>
#include <string>

#include <parquet/types.h>

namespace ape {

struct NullStruct {
  friend std::ostream& operator<<(std::ostream& os, const NullStruct& nullStruct);
  NullStruct() {}
  NullStruct(NullStruct&& nullStruct) {}
  NullStruct(NullStruct& nullStruct) {}
  NullStruct& operator=(NullStruct nullStruct) { return *this; }
  NullStruct& operator=(NullStruct&& nullStruct) { return *this; }
  char valid;  // sizeof(NullStruct) = 1 byte
};

struct Int96Struct {};

class Schema {
 public:
  Schema(std::string colName_, parquet::Type::type colType_)
      : colName(colName_), colType(colType_) {
    switch (colType) {
      case parquet::Type::BOOLEAN: {
        defaultSize = 1;
        break;
      }
      case parquet::Type::INT32: {
        defaultSize = 4;
        break;
      }
      case parquet::Type::INT64: {
        defaultSize = 8;
        break;
      }
      case parquet::Type::INT96: {
        defaultSize = 12;
        break;
      }
      case parquet::Type::FLOAT: {
        defaultSize = 4;
        break;
      }
      case parquet::Type::DOUBLE: {
        defaultSize = 8;
        break;
      }
      case parquet::Type::BYTE_ARRAY: {
        defaultSize = 16;
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
        defaultSize = 8;
        break;
      }
      default:
        defaultSize = -1;
        break;
    }
  }

  std::string getColName() { return colName; }
  parquet::Type::type getColType() { return colType; }
  int getDefaultSize() { return defaultSize; }

 private:
  std::string colName;
  parquet::Type::type colType;
  int defaultSize;
};

}  // namespace ape
