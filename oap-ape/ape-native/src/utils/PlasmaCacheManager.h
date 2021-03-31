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

#include <queue>
#include <mutex>

#include <parquet/api/reader.h>
#include <plasma/client.h>
#include <sw/redis++/redis++.h>

namespace ape {

struct CacheObject {
  CacheObject(::arrow::io::ReadRange r, std::shared_ptr<Buffer> d) : range(r), data(d) {}

  ::arrow::io::ReadRange range;
  std::shared_ptr<Buffer> data;
};

enum class CacheWriterState { INIT, STARTED, STOPING, STOPED };

class AsyncCacheWriter {
 public:
  static constexpr int default_loop_interval_micro_seconds = 500 * 1000;

  AsyncCacheWriter()
      : loop_interval_micro_seconds_(default_loop_interval_micro_seconds){};
  virtual ~AsyncCacheWriter() = default;
  // start a new thread to write cache objects
  void startCacheWriting();
  // wait until all objects are written, then stop writing thread
  void stopCacheWriting();
  // set the loop interval to check the object queue and the `stop` state
  void setLoopIntervalMicroSeconds(int interval);
  // insert a new object which need to be written
  void insertCacheObject(::arrow::io::ReadRange range, std::shared_ptr<Buffer> data);
  // a function to write cache.
  // it will be called in the cache writing thread.
  // this should be implemented by derived classes.
  virtual bool writeCacheObject(::arrow::io::ReadRange range,
                                std::shared_ptr<Buffer> data) = 0;

 protected:
  // get an object that need to be written
  std::shared_ptr<CacheObject> popCacheObject();
  // a while loop watching the object queue and the `stop` state
  void loopOnCacheWriting();

 private:
  // a mutex to protect the obejct
  std::mutex cache_mutex_;
  // a queue holds all the objects which need to be witten
  std::queue<std::shared_ptr<CacheObject>> cache_objects_;
  // current state of this writer
  CacheWriterState state_;
  //
  int loop_interval_micro_seconds_;
};

class PlasmaCacheManager : public parquet::CacheManager, public AsyncCacheWriter {
 public:
  explicit PlasmaCacheManager(std::string file_path);
  ~PlasmaCacheManager();
  bool connected();
  void close();
  void release();
  plasma::ObjectID objectIdOfFileRange(::arrow::io::ReadRange range);

  void setCacheRedis(std::shared_ptr<sw::redis::ConnectionOptions> options);
  void setCacheWriter(std::shared_ptr<PlasmaCacheManager> cache_writer);

  // override methods
  bool containsFileRange(::arrow::io::ReadRange range) override;
  std::shared_ptr<Buffer> getFileRange(::arrow::io::ReadRange range) override;
  bool cacheFileRange(::arrow::io::ReadRange range,
                      std::shared_ptr<Buffer> data) override;
  bool deleteFileRange(::arrow::io::ReadRange range) override;

  bool writeCacheObject(::arrow::io::ReadRange range,
                        std::shared_ptr<Buffer> data) override;

 protected:
  bool cacheFileRangeInternal(::arrow::io::ReadRange range, std::shared_ptr<Buffer> data);
  std::string cacheKeyofFileRange(::arrow::io::ReadRange range);
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

  // a child cache manger to write cache asynchronously
  std::shared_ptr<PlasmaCacheManager> cache_writer_;
};

class PlasmaCacheManagerProvider : public parquet::CacheManagerProvider {
 public:
  explicit PlasmaCacheManagerProvider(std::string file_path);
  ~PlasmaCacheManagerProvider();
  void close();
  bool connected();
  void setCacheRedis(std::shared_ptr<sw::redis::ConnectionOptions> options);

  // override methods
  std::shared_ptr<parquet::CacheManager> defaultCacheManager() override;
  std::shared_ptr<parquet::CacheManager> newCacheManager() override;

 private:
  std::string file_path_;
  std::vector<std::shared_ptr<PlasmaCacheManager>> managers_;
  std::shared_ptr<sw::redis::ConnectionOptions> redis_options_;
};

}  // namespace ape
