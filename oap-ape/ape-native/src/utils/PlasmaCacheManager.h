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

#include <parquet/api/reader.h>
#include <plasma/client.h>
#include <sw/redis++/redis++.h>

namespace ape {

class PlasmaCacheManager : public parquet::CacheManager {
 public:
  explicit PlasmaCacheManager(std::string file_path);
  ~PlasmaCacheManager();
  bool connected();
  void close();
  void release();
  plasma::ObjectID objectIdOfColumnChunk(::arrow::io::ReadRange range);

  void setCacheRedis(std::shared_ptr<sw::redis::ConnectionOptions> options);

  // override methods
  bool containsColumnChunk(::arrow::io::ReadRange range) override;
  std::shared_ptr<Buffer> getColumnChunk(::arrow::io::ReadRange range) override;
  bool cacheColumnChunk(::arrow::io::ReadRange range,
                        std::shared_ptr<Buffer> data) override;
  bool deleteColumnChunk(::arrow::io::ReadRange range) override;

 protected:
  std::string cacheKeyofColumnChunk(::arrow::io::ReadRange range);
  void setCacheInfoToRedis();

 private:
  std::shared_ptr<plasma::PlasmaClient> client_ = nullptr;
  std::string file_path_;
  std::vector<plasma::ObjectID> object_ids;
  std::shared_ptr<sw::redis::Redis> redis_;

  // data which will be saved to redis
  std::string hostname;
  int cache_hit_count_ = 0;
  int cache_miss_count_ = 0;
  std::vector<::arrow::io::ReadRange> cached_ranges_;
};

}  // namespace ape
