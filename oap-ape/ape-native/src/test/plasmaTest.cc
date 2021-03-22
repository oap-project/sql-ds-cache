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

#include <iostream>
#include <gtest/gtest.h>

#include "src/utils/PlasmaCacheManager.h"

// in this case, we didn't write cache(call create), so it will fail. And you
// need to start a Plasma server before run test.
TEST(PlasmaCacheTest, cacheTest) {
  auto file =
      "hdfs://sr490:9000/tpch_1t_snappy/lineitem/"
      "part-00036-01f81a98-e2b0-4bd4-9134-62fd522bc891-c000.snappy.parquet";

  ape::PlasmaCacheManager cacheManager(file);
  auto range = arrow::io::ReadRange();
  range.offset = 587558587;
  range.length = 19723749;

  auto c = cacheManager.containsFileRange(range);

  EXPECT_EQ(c, 0);

  auto g = cacheManager.getFileRange(range);

  EXPECT_EQ(g, nullptr);
}

// TODO: add a successful case.
