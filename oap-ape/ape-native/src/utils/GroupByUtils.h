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

#include <parquet/types.h>

#include "src/utils/ApeHashMap.h"
#include "src/utils/AggExpression.h"
#include "src/utils/Expression.h"

namespace ape {
class GroupByUtils {
 public:
  static void groupBy(ApeHashMap& map, std::vector<int>& index, const int& batchSize,
                      std::vector<std::shared_ptr<Expression>>& groupByExprs,
                      const std::vector<int64_t>& buffersPtr,
                      const std::vector<int64_t>& nullPtr, std::vector<Key>& keys,
                      const std::vector<parquet::Type::type> typeVector) {
    int groupBySize = groupByExprs.size();
    int totalGroup = 0;
    std::vector<int> columnIndexes(groupBySize);
    for (int i = 0; i < groupBySize; i++) {
      std::shared_ptr<AttributeReferenceExpression> groupByExpr =
          std::static_pointer_cast<AttributeReferenceExpression>(groupByExprs[i]);

      columnIndexes[i] = groupByExpr->columnIndex;
      ARROW_LOG(DEBUG) << " columnIndex is " << columnIndexes[i];
    }
    for (int i = 0; i < batchSize; i++) {
      Key key;
      for (int j = 0; j < groupBySize; j++) {
        PartialKey pKey;
        switch (typeVector[columnIndexes[j]]) {
          case parquet::Type::INT32: {
            pKey = *((int32_t*)(buffersPtr[columnIndexes[j]]) + i);
            break;
          }

          case parquet::Type::INT64: {
            pKey = *((int64_t*)(buffersPtr[columnIndexes[j]]) + i);
            break;
          }

          case parquet::Type::FLOAT: {
            pKey = *((float*)(buffersPtr[columnIndexes[j]]) + i);
            break;
          }
          case parquet::Type::DOUBLE: {
            pKey = *((double*)(buffersPtr[columnIndexes[j]]) + i);
            break;
          }

          case parquet::Type::BYTE_ARRAY: {
            pKey = *((parquet::ByteArray*)(buffersPtr[columnIndexes[j]]) + i);
            break;
          }

          default: {
            ARROW_LOG(WARNING) << "Do not support yet";
            break;
          }
        }
        key.push_back(pKey);
      }
      if (map.find(key) == map.end()) {
        map.insert({key, totalGroup});
        index[i] = totalGroup;
        totalGroup++;
        keys.push_back(key);
      } else {
        index[i] = map[key];
      }
    }
    ARROW_LOG(INFO) << "Total group num: " << totalGroup;
  }
};

}  // namespace ape
