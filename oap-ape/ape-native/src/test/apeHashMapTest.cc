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

#include <gtest/gtest.h>

#include "src/utils/ApeHashMap.h"

TEST(ApeHashMapTest, KeyTest) {
  ape::Key key;
  key.push_back(1);      // int
  key.push_back(100L);   // long
  key.push_back(1.0f);   // float
  key.push_back(0.1);    // double
  key.push_back("aaa");  // std::string

  EXPECT_EQ(key.size(), 5);
  EXPECT_EQ(key[0].index(), 0);
  EXPECT_EQ(key[1].index(), 1);
  EXPECT_EQ(key[2].index(), 2);
  EXPECT_EQ(key[3].index(), 3);
  EXPECT_EQ(key[4].index(), 4);
}

TEST(ApeHashMapTest, HashMapTest) {
  ape::Key key;
  key.push_back(1);      // int
  key.push_back(100L);   // long
  key.push_back(1.0f);   // float
  key.push_back(0.1);    // double
  key.push_back("aaa");  // std::string

  ape::ApeHashMap map;
  map.insert({key, 1});

  ape::Key key2 = key;
  EXPECT_EQ(map[key2], 1);

  ape::Key key3;
  key3.push_back(2);
  EXPECT_TRUE(map.find(key3) == map.end());
}

TEST(ApeHashMapTest, GropuByTest) {
  int row_num = 10000;
  // sum(column3) group by column1, column2
  std::vector<int32_t> column1(row_num);
  std::vector<std::string> column2(row_num);
  std::vector<int64_t> column3(row_num);
  std::vector<int32_t> index(row_num);

  for (int i = 0; i < row_num; i++) {
    if (i % 2 == 0)
      column1[i] = 10;
    else
      column1[i] = 20;

    if (i % 3 == 0)
      column2[i] = "aaaa";
    else if (i % 3 == 1)
      column2[i] = "bbbb";
    else
      column2[i] = "cccc";

    column3[i] = i;
  }

  std::vector<int64_t> result;

  ape::ApeHashMap map;

  int total = 0;

  for (int i = 0; i < row_num; i++) {
    ape::Key tmp_key = ape::Key{column1[i], column2[i]};
    if (map.find(tmp_key) == map.end()) {  // a new key
      map.insert({tmp_key, total});
      index[i] = total;
      total++;
    } else {
      index[i] = map[tmp_key];
    }
  }
  EXPECT_EQ(map.size(), 6);

  std::cout << "total groups: " << total << std::endl
            << "map size " << map.size() << std::endl;

  result.resize(total);

  // do sum
  for (int i = 0; i < row_num; i++) {
    result[index[i]] += column3[i];
  }

  for (auto iter = map.begin(); iter != map.end(); iter++) {
    std::cout << "key ";
    ape::Key tmp_key = iter->first;
    for (int j = 0; j < tmp_key.size(); j++) {
      ape::PartialKey partial_key = tmp_key[j];
      if (j == 0)
        std::cout << std::get<0>(partial_key)
                  << " ";  // consider how to convert type back.
      else
        std::cout << std::get<4>(partial_key) << " ";
    }
    std::cout << " result " << result[iter->second] << std::endl;
  }
}