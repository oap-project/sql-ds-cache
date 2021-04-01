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

#include <unistd.h>
#include <chrono>

#include <openssl/sha.h>

#include <arrow/util/logging.h>

#include "src/utils/PlasmaCacheManager.h"

namespace ape {

void AsyncCacheWriter::startCacheWriting() {
  // start a new thread
  ARROW_LOG(INFO) << "cache writer, starting loop thread";
  some_threads_.push_back(std::thread(&AsyncCacheWriter::loopOnCacheWriting, this));
  ARROW_LOG(INFO) << "cache writer, started loop thread";

  state_ = CacheWriterState::STARTED;
}

void AsyncCacheWriter::stopCacheWriting() {
  if (state_ == CacheWriterState::INIT || state_ == CacheWriterState::STOPPED) {
    return;
  }

  ARROW_LOG(DEBUG) << "cache writer, stopping, current state: "
                   << static_cast<int>(state_);

  std::unique_lock<std::mutex> lck(cache_mutex_);
  state_ = CacheWriterState::STOPPING;
  lck.unlock();
  event_cv_.notify_one();

  // wait until writing finish
  auto start = std::chrono::steady_clock::now();
  for (auto& t : some_threads_) {
    t.join();
  }
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);
  ARROW_LOG(INFO) << "cache writer, stopping takes " << duration.count() << " ms.";
}

void AsyncCacheWriter::insertCacheObject(::arrow::io::ReadRange range,
                                         std::shared_ptr<Buffer> data) {
  std::unique_lock<std::mutex> lck(cache_mutex_);

  std::shared_ptr<CacheObject> obj = std::make_shared<CacheObject>(range, data);
  cache_objects_.push(obj);

  lck.unlock();
  event_cv_.notify_one();
}

std::shared_ptr<CacheObject> AsyncCacheWriter::popCacheObject() {
  if (cache_objects_.empty()) {
    return nullptr;
  }

  auto ret = cache_objects_.front();
  cache_objects_.pop();

  return ret;
}

void AsyncCacheWriter::loopOnCacheWriting() {
  ARROW_LOG(INFO) << "cache writer, loop started";
  while (true) {
    std::unique_lock<std::mutex> lck(cache_mutex_);
    auto obj = popCacheObject();

    // no cache objects that need to be written
    if (obj == nullptr) {
      // check if this loop should stop
      if (state_ != CacheWriterState::STOPPING) {
        ARROW_LOG(DEBUG) << "cache writer, loop wait...";
        event_cv_.wait(lck);
        continue;
      } else {
        ARROW_LOG(DEBUG) << "cache writer, loop stopping";
        lck.unlock();
        break;
      }
    }
    lck.unlock();

    // write cache
    this->writeCacheObject(obj->range, obj->data);
  }

  // change writer state
  state_ = CacheWriterState::STOPPED;
  ARROW_LOG(INFO) << "cache writer, loop stopped";
}

PlasmaCacheManager::PlasmaCacheManager(std::string file_path) : file_path_(file_path) {
  ARROW_LOG(INFO) << "plasma, init cache manager with path: " << file_path;

  std::shared_ptr<plasma::PlasmaClient> client = std::make_shared<plasma::PlasmaClient>();
  arrow::Status status = client->Connect("/tmp/plasmaStore", "", 0);

  if (status.ok()) {
    client_ = client;
    ARROW_LOG(INFO) << "plasma, cache manager initialized";

    char buff[1024];
    gethostname(buff, sizeof(buff));
    hostname = buff;
  } else {
    ARROW_LOG(WARNING) << "plasma, Connect failed: " << status.message();
  }
}

PlasmaCacheManager::~PlasmaCacheManager() {}

bool PlasmaCacheManager::connected() { return client_ != nullptr; }

void PlasmaCacheManager::release() {
  ARROW_LOG(INFO) << "plasma, release objects";

  if (!client_) {
    return;
  }

  for (auto oid : object_ids) {
    client_->Release(oid);
  }

  object_ids.clear();
}

void PlasmaCacheManager::close() {
  ARROW_LOG(INFO) << "plasma, close cache manager";

  // stop the cache writer
  if (cache_writer_) {
    cache_writer_->stopCacheWriting();
    cache_writer_->close();
  }

  if (!client_) {
    return;
  }

  auto start = std::chrono::steady_clock::now();

  // release objects
  release();

  // disconnct
  arrow::Status status = client_->Disconnect();
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Disconnect failed: " << status.message();
  }

  client_ = nullptr;

  // save cache info to redis
  setCacheInfoToRedis();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);
  ARROW_LOG(INFO) << "cache manager, closing takes " << duration.count() << " ms.";
}

void PlasmaCacheManager::setCacheInfoToRedis() {
  if (redis_) {
    try {
      redis_->incrby("pmem_cache_global_cache_hit", cache_hit_count_);
      redis_->incrby("pmem_cache_global_cache_missed", cache_miss_count_);

      // save locations of cached data
      std::unordered_map<std::string, double> scores;
      char buff[1024];
      for (auto range : cached_ranges_) {
        snprintf(buff, sizeof(buff), "%d_%d_%s", range.offset, range.length,
                 hostname.c_str());
        std::string member = buff;
        scores.insert({member, range.offset});
      }

      if (scores.size() > 0) {
        redis_->zadd(file_path_, scores.begin(), scores.end());
      }

      cache_hit_count_ = 0;
      cache_miss_count_ = 0;
      cached_ranges_.clear();

      ARROW_LOG(INFO) << "plasma, saved cache info to redis";
    } catch (const sw::redis::Error& e) {
      ARROW_LOG(WARNING) << "plasma, save cache info to redis failed: " << e.what();
    }
  }
}

std::string PlasmaCacheManager::cacheKeyofFileRange(::arrow::io::ReadRange range) {
  char buff[1024];
  snprintf(buff, sizeof(buff), "plasma_cache:parquet_chunk:%s:%d_%d", file_path_.c_str(),
           range.offset, range.length);
  std::string ret = buff;
  return ret;
}

plasma::ObjectID PlasmaCacheManager::objectIdOfFileRange(::arrow::io::ReadRange range) {
  std::string cache_key = cacheKeyofFileRange(range);

  unsigned char hash[SHA_DIGEST_LENGTH];
  SHA1((const unsigned char*)cache_key.c_str(), cache_key.length(), hash);

  return plasma::ObjectID::from_binary(std::string(hash, hash + sizeof(hash)));
}

void PlasmaCacheManager::setCacheRedis(
    std::shared_ptr<sw::redis::ConnectionOptions> options) {
  if (cache_writer_) {
    cache_writer_->setCacheRedis(options);
  }

  try {
    sw::redis::ConnectionOptions connection_options;
    connection_options.host = options->host;
    connection_options.port = options->port;
    connection_options.password = options->password;

    auto redis = std::make_shared<sw::redis::Redis>(connection_options);
    redis_ = redis;
    ARROW_LOG(INFO) << "plasma, set cache redis: " << options->host;
  } catch (const sw::redis::Error& e) {
    ARROW_LOG(WARNING) << "plasma, set redis failed: " << e.what();
  }
}

void PlasmaCacheManager::setCacheWriter(
    std::shared_ptr<PlasmaCacheManager> cache_writer) {
  if (cache_writer_ != nullptr) {
    return;
  }

  cache_writer_ = cache_writer;
  cache_writer_->startCacheWriting();
}

bool PlasmaCacheManager::containsFileRange(::arrow::io::ReadRange range) {
  bool has_object;

  arrow::Status status = client_->Contains(objectIdOfFileRange(range), &has_object);
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Contains failed: " << status.message();
    return false;
  }

  // we don't increase cache_hit_count_ here when has_object == true.
  // cache_hit_count_ will be updated in `get()`.
  if (!has_object) {
    cache_miss_count_ += 1;
  }

  return has_object;
}

std::shared_ptr<Buffer> PlasmaCacheManager::getFileRange(::arrow::io::ReadRange range) {
  std::vector<plasma::ObjectID> oids;
  plasma::ObjectID oid = objectIdOfFileRange(range);
  oids.push_back(oid);

  std::vector<plasma::ObjectBuffer> obufs(1);

  arrow::Status status = client_->Get(oids.data(), 1, 1000, obufs.data());
  if (!status.ok() || obufs[0].data == nullptr) {
    ARROW_LOG(WARNING) << "plasma, Get failed: " << status.message();
    cache_miss_count_ += 1;
    return nullptr;
  }

  // save object id for future release()
  object_ids.push_back(oid);

  cache_hit_count_ += 1;
  cached_ranges_.push_back(range);

  ARROW_LOG(DEBUG) << "plasma, get object from cache: " << file_path_ << ", "
                   << range.offset << ", " << range.length;

  return obufs[0].data;
}

bool PlasmaCacheManager::cacheFileRange(::arrow::io::ReadRange range,
                                        std::shared_ptr<Buffer> data) {
  if (cache_writer_ != nullptr) {
    cache_writer_->insertCacheObject(range, data);
    return true;
  } else {
    return cacheFileRangeInternal(range, data);
  }
}

bool PlasmaCacheManager::cacheFileRangeInternal(::arrow::io::ReadRange range,
                                                std::shared_ptr<Buffer> data) {
  std::vector<plasma::ObjectID> oids;
  plasma::ObjectID oid = objectIdOfFileRange(range);

  // create new object
  std::shared_ptr<Buffer> saved_data;
  Status status = client_->Create(oid, data->size(), nullptr, 0, &saved_data);
  if (plasma::IsPlasmaObjectExists(status)) {
    ARROW_LOG(WARNING) << "plasma, Create failed, PlasmaObjectExists: "
                       << status.message();
    return false;
  }
  if (plasma::IsPlasmaStoreFull(status)) {
    ARROW_LOG(WARNING) << "plasma, Create failed, PlasmaStoreFull: " << status.message();
    return false;
  }
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Create failed: " << status.message();
    return false;
  }

  // copy data
  memcpy(saved_data->mutable_data(), data->data(), data->size());

  // seal object
  status = client_->Seal(oid);
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Seal failed: " << status.message();

    // abort object
    status = client_->Abort(oid);
    if (!status.ok()) {
      ARROW_LOG(WARNING) << "plasma, Abort failed: " << status.message();
    }

    // release object
    status = client_->Release(oid);
    if (!status.ok()) {
      ARROW_LOG(WARNING) << "plasma, Release failed: " << status.message();
      return false;
    }

    return false;
  }

  // release object
  status = client_->Release(oid);
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Release failed: " << status.message();
    return false;
  }

  cached_ranges_.push_back(range);

  ARROW_LOG(DEBUG) << "plasma, object cached: " << file_path_ << ", " << range.offset
                   << ", " << range.length;

  return true;
}

bool PlasmaCacheManager::deleteFileRange(::arrow::io::ReadRange range) {
  arrow::Status status = client_->Delete(objectIdOfFileRange(range));
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Delete failed: " << status.message();
    return false;
  }

  ARROW_LOG(INFO) << "plasma, delete object from cache: " << file_path_ << ", "
                  << range.offset << ", " << range.length;
  return true;
}

bool PlasmaCacheManager::writeCacheObject(::arrow::io::ReadRange range,
                                          std::shared_ptr<Buffer> data) {
  return cacheFileRangeInternal(range, data);
}

PlasmaCacheManagerProvider::PlasmaCacheManagerProvider(std::string file_path)
    : file_path_(file_path) {
  auto default_manager = std::make_shared<PlasmaCacheManager>(file_path);
  auto cache_writer = std::make_shared<PlasmaCacheManager>(file_path);
  default_manager->setCacheWriter(cache_writer);
  managers_.push_back(default_manager);
}

PlasmaCacheManagerProvider::~PlasmaCacheManagerProvider() {}

void PlasmaCacheManagerProvider::close() {
  for (auto manager : managers_) {
    manager->close();
  }
}

bool PlasmaCacheManagerProvider::connected() { return managers_[0]->connected(); }

void PlasmaCacheManagerProvider::setCacheRedis(
    std::shared_ptr<sw::redis::ConnectionOptions> options) {
  redis_options_ = options;

  for (auto manager : managers_) {
    manager->setCacheRedis(options);
  }
}

std::shared_ptr<parquet::CacheManager> PlasmaCacheManagerProvider::defaultCacheManager() {
  return managers_[0];
}

std::shared_ptr<parquet::CacheManager> PlasmaCacheManagerProvider::newCacheManager() {
  auto new_manager = std::make_shared<PlasmaCacheManager>(file_path_);
  auto cache_writer = std::make_shared<PlasmaCacheManager>(file_path_);
  new_manager->setCacheWriter(cache_writer);
  managers_.push_back(new_manager);

  if (redis_options_) {
    new_manager->setCacheRedis(redis_options_);
  }

  return new_manager;
}

}  // namespace ape
