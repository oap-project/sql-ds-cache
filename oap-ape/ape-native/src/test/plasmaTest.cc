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
#include <chrono>

#include "src/utils/PlasmaCacheManager.h"

// need to start a Plasma server before run test.
TEST(PlasmaCacheTest, cacheTest) {
  auto file = "scheme://host:port/file_path";

  auto range = arrow::io::ReadRange();
  range.offset = 587558533;
  range.length = 19723749;

  std::shared_ptr<plasma::PlasmaClient> client = std::make_shared<plasma::PlasmaClient>();
  auto status = client->Connect("/tmp/plasmaStore", "", 0);
  if (status.ok()) {
    ape::PlasmaCacheManager cacheManager(file);
    bool exists = cacheManager.containsFileRange(range);
    EXPECT_EQ(exists, 0);

    // create new object
    std::shared_ptr<Buffer> cache_data;
    plasma::ObjectID oid = ape::CacheKeyGenerator::objectIdOfFileRange(file, range);
    client->Create(oid, range.length, nullptr, 0, &cache_data);

    // print buffer addresses
    uint8_t* buffer = new uint8_t[range.length];
    std::cout << "buffer addr: " << static_cast<void*>(buffer) << std::endl;
    std::cout << "cache addr: " << static_cast<const void*>(cache_data->data())
              << std::endl;

    // record data reading time
    auto start = std::chrono::steady_clock::now();
    memcpy(cache_data->mutable_data(), buffer, range.length);
    // memcpy(buffer, cache_data->data(), range.length);
    // cache_data->CopySlice(0, range.length);

    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - start);
    std::cout << "test reading cache of " << range.length << " bytes. takes "
              << duration.count() << " ns." << std::endl;

    // abort creating object
    client->Abort(oid);

    exists = cacheManager.containsFileRange(range);
    EXPECT_EQ(exists, 0);

    delete buffer;
  }
}

// TODO: add a successful case.
