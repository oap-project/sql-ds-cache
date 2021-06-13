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

#include <arrow/util/logging.h>

#include "src/utils/PlasmaCacheManager.h"

namespace ape {

std::string CacheKeyGenerator::cacheKeyofFileRange(std::string file_path,
                                                   ::arrow::io::ReadRange range) {
  char buff[1024];
  snprintf(buff, sizeof(buff), "plasma_cache:parquet_chunk:%s:%ld_%ld", file_path.c_str(),
           range.offset, range.length);
  std::string ret = buff;
  return ret;
}

plasma::ObjectID CacheKeyGenerator::objectIdOfFileRange(std::string file_path,
                                                        ::arrow::io::ReadRange range) {
  std::string cache_key = cacheKeyofFileRange(file_path, range);

  unsigned char hash[SHA_DIGEST_LENGTH];
  SHA1((const unsigned char*)cache_key.c_str(), cache_key.length(), hash);

  return plasma::ObjectID::from_binary(std::string(hash, hash + sizeof(hash)));
}

void AsyncCacheWriter::startCacheWriting() {
  // start a new thread
  ARROW_LOG(DEBUG) << "cache writer, starting loop thread";
  some_threads_.push_back(std::thread(&AsyncCacheWriter::loopOnCacheWriting, this));
  ARROW_LOG(DEBUG) << "cache writer, started loop thread";

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
  ARROW_LOG(DEBUG) << "cache writer, loop started";
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
  ARROW_LOG(DEBUG) << "cache writer, loop stopped";
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
        snprintf(buff, sizeof(buff), "%ld_%ld_%s", range.offset, range.length,
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
    ARROW_LOG(DEBUG) << "plasma, set cache redis: " << options->host;
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

  arrow::Status status = client_->Contains(
      CacheKeyGenerator::objectIdOfFileRange(file_path_, range), &has_object);
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
  plasma::ObjectID oid = CacheKeyGenerator::objectIdOfFileRange(file_path_, range);
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
  plasma::ObjectID oid = CacheKeyGenerator::objectIdOfFileRange(file_path_, range);

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
  arrow::Status status =
      client_->Delete(CacheKeyGenerator::objectIdOfFileRange(file_path_, range));
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

void RedisBackedCacheManagerProvider::setCacheRedis(
    std::shared_ptr<sw::redis::ConnectionOptions> options) {
  redis_options_ = options;
}

PlasmaCacheManagerProvider::PlasmaCacheManagerProvider(std::string file_path,
                                                       bool enable_cache_writer)
    : file_path_(file_path), enable_cache_writer_(enable_cache_writer) {
  auto default_manager = std::make_shared<PlasmaCacheManager>(file_path);

  if (enable_cache_writer_) {
    auto cache_writer = std::make_shared<PlasmaCacheManager>(file_path);
    default_manager->setCacheWriter(cache_writer);
  }

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

  if (enable_cache_writer_) {
    auto cache_writer = std::make_shared<PlasmaCacheManager>(file_path_);
    new_manager->setCacheWriter(cache_writer);
  }

  managers_.push_back(new_manager);

  if (redis_options_) {
    new_manager->setCacheRedis(redis_options_);
  }

  return new_manager;
}

PlasmaClientPool::PlasmaClientPool(int capacity) : capacity_(capacity) {
  ARROW_LOG(INFO) << "PlasmaClientPool, initializing plasma client pool, capacity: "
                  << capacity;

  // initialize plasma clients
  for (int i = 0; i < capacity; i++) {
    std::shared_ptr<plasma::PlasmaClient> client =
        std::make_shared<plasma::PlasmaClient>();
    arrow::Status status = client->Connect("/tmp/plasmaStore", "", 0);

    if (status.ok()) {
      allocated_clients_.push_back(client);
    } else {
      ARROW_LOG(WARNING) << "plasma, Connect failed: " << status.message();
    }
  }

  ARROW_LOG(INFO) << "PlasmaClientPool, initialized plasma client pool, size: "
                  << allocated_clients_.size();
}

PlasmaClientPool::~PlasmaClientPool() { close(); }

void PlasmaClientPool::close() {
  if (allocated_clients_.empty()) {
    return;
  }

  for (int i = 0; i < allocated_clients_.size(); i++) {
    // disconnct
    arrow::Status status = allocated_clients_[i]->Disconnect();
    if (!status.ok()) {
      ARROW_LOG(WARNING) << "plasma, Disconnect failed: " << status.message();
    }
  }

  allocated_clients_.clear();

  ARROW_LOG(INFO) << "PlasmaClientPool, closed";
}

int PlasmaClientPool::capacity() { return capacity_; }

std::shared_ptr<plasma::PlasmaClient> PlasmaClientPool::take() {
  int i = current_;
  current_ = (current_ + 1) % allocated_clients_.size();
  return allocated_clients_[i];
}

void PlasmaClientPool::put(std::shared_ptr<plasma::PlasmaClient> client) {
  // do nothing
  return;
}

ShareClientPlasmaCacheManager::ShareClientPlasmaCacheManager(
    std::string file_path, PlasmaClientPool* client_pool)
    : file_path_(file_path), client_pool_(client_pool) {
  ARROW_LOG(INFO) << "plasma, init cache manager (sharing client) with path: "
                  << file_path;

  char buff[1024];
  gethostname(buff, sizeof(buff));
  hostname = buff;
}

ShareClientPlasmaCacheManager::~ShareClientPlasmaCacheManager() {}

bool ShareClientPlasmaCacheManager::connected() {
  if (client_pool_ != NULL) {
    auto client = client_pool_->take();
    client_pool_->put(client);
    return true;
  }

  return false;
}

void ShareClientPlasmaCacheManager::release() {
  ARROW_LOG(INFO) << "plasma, release objects";

  for (auto oid : object_ids) {
    preferred_client_->Release(oid);
  }

  object_ids.clear();
}

void ShareClientPlasmaCacheManager::close() {
  ARROW_LOG(INFO) << "plasma, close cache manager";

  auto start = std::chrono::steady_clock::now();

  // release objects
  release();

  // save cache info to redis
  setCacheInfoToRedis();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);
  ARROW_LOG(INFO) << "cache manager, closing takes " << duration.count() << " ms.";
}

void ShareClientPlasmaCacheManager::setCacheInfoToRedis() {
  if (redis_) {
    try {
      redis_->incrby("pmem_cache_global_cache_hit", cache_hit_count_);
      redis_->incrby("pmem_cache_global_cache_missed", cache_miss_count_);

      // save locations of cached data
      std::unordered_map<std::string, double> scores;
      char buff[1024];
      for (auto range : cached_ranges_) {
        snprintf(buff, sizeof(buff), "%ld_%ld_%s", range.offset, range.length,
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

void ShareClientPlasmaCacheManager::setCacheRedis(
    std::shared_ptr<sw::redis::ConnectionOptions> options) {
  try {
    sw::redis::ConnectionOptions connection_options;
    connection_options.host = options->host;
    connection_options.port = options->port;
    connection_options.password = options->password;

    auto redis = std::make_shared<sw::redis::Redis>(connection_options);
    redis_ = redis;
    ARROW_LOG(DEBUG) << "plasma, set cache redis: " << options->host;
  } catch (const sw::redis::Error& e) {
    ARROW_LOG(WARNING) << "plasma, set redis failed: " << e.what();
  }
}

bool ShareClientPlasmaCacheManager::containsFileRange(::arrow::io::ReadRange range) {
  bool has_object;

  auto client = client_pool_->take();
  arrow::Status status = client->Contains(
      CacheKeyGenerator::objectIdOfFileRange(file_path_, range), &has_object);
  client_pool_->put(client);
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

std::shared_ptr<Buffer> ShareClientPlasmaCacheManager::getFileRange(
    ::arrow::io::ReadRange range) {
  std::vector<plasma::ObjectID> oids;
  plasma::ObjectID oid = CacheKeyGenerator::objectIdOfFileRange(file_path_, range);
  oids.push_back(oid);

  std::vector<plasma::ObjectBuffer> obufs(1);

  if (preferred_client_ == nullptr) {
    auto client = client_pool_->take();
    client_pool_->put(client);
    preferred_client_ = client;
  }

  arrow::Status status = preferred_client_->Get(oids.data(), 1, 1000, obufs.data());
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

  // allocate new buffer in main memory
  auto buffer_alloc_result = arrow::AllocateResizableBuffer(obufs[0].data->size());
  if (!buffer_alloc_result.ok()) {
    ARROW_LOG(WARNING) << "plasma, failed to allocate new buffer";
    return obufs[0].data;
  }

  // copy data from cache media to main memory
  auto new_buffer = std::move(buffer_alloc_result).ValueUnsafe();
  const uint8_t* src = obufs[0].data->data();
  uint8_t* dest = new_buffer->mutable_data();
  int length = obufs[0].data->size();

  // memcpy(dest, src, length);

  const int BLOCK_SIZE = 128 * 1024;  // 128 KB
  int src_offset = 0;
  int dest_offset = 0;
  while (length > 0L) {
    int size = std::min(length, BLOCK_SIZE);
    memcpy(dest + dest_offset, src + src_offset, size);
    length -= size;
    src_offset += size;
    dest_offset += size;
  }

  return std::move(new_buffer);
}

bool ShareClientPlasmaCacheManager::cacheFileRange(::arrow::io::ReadRange range,
                                                   std::shared_ptr<Buffer> data) {
  auto client = client_pool_->take();
  bool ret = cacheFileRangeInternal(range, data, client);
  client_pool_->put(client);

  return ret;
}

bool ShareClientPlasmaCacheManager::cacheFileRangeInternal(
    ::arrow::io::ReadRange range, std::shared_ptr<Buffer> data,
    std::shared_ptr<plasma::PlasmaClient> client) {
  std::vector<plasma::ObjectID> oids;
  plasma::ObjectID oid = CacheKeyGenerator::objectIdOfFileRange(file_path_, range);

  // create new object
  std::shared_ptr<Buffer> saved_data;
  Status status = client->Create(oid, data->size(), nullptr, 0, &saved_data);
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
  status = client->Seal(oid);
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Seal failed: " << status.message();

    // abort object
    status = client->Abort(oid);
    if (!status.ok()) {
      ARROW_LOG(WARNING) << "plasma, Abort failed: " << status.message();
    }

    // release object
    status = client->Release(oid);
    if (!status.ok()) {
      ARROW_LOG(WARNING) << "plasma, Release failed: " << status.message();
      return false;
    }

    return false;
  }

  // release object
  status = client->Release(oid);
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Release failed: " << status.message();
    return false;
  }

  cached_ranges_.push_back(range);

  ARROW_LOG(DEBUG) << "plasma, object cached: " << file_path_ << ", " << range.offset
                   << ", " << range.length;

  return true;
}

bool ShareClientPlasmaCacheManager::deleteFileRange(::arrow::io::ReadRange range) {
  auto client = client_pool_->take();
  arrow::Status status =
      client->Delete(CacheKeyGenerator::objectIdOfFileRange(file_path_, range));
  client_pool_->put(client);
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Delete failed: " << status.message();
    return false;
  }

  ARROW_LOG(INFO) << "plasma, delete object from cache: " << file_path_ << ", "
                  << range.offset << ", " << range.length;
  return true;
}

ShareClientPlasmaCacheManagerProvider::ShareClientPlasmaCacheManagerProvider(
    std::string file_path, PlasmaClientPool* client_pool)
    : file_path_(file_path), client_pool_(client_pool) {
  auto default_manager =
      std::make_shared<ShareClientPlasmaCacheManager>(file_path, client_pool);

  managers_.push_back(default_manager);
}

ShareClientPlasmaCacheManagerProvider::~ShareClientPlasmaCacheManagerProvider() {}

void ShareClientPlasmaCacheManagerProvider::close() {
  for (auto manager : managers_) {
    manager->close();
  }
}

bool ShareClientPlasmaCacheManagerProvider::connected() {
  return managers_[0]->connected();
}

void ShareClientPlasmaCacheManagerProvider::setCacheRedis(
    std::shared_ptr<sw::redis::ConnectionOptions> options) {
  redis_options_ = options;

  for (auto manager : managers_) {
    manager->setCacheRedis(options);
  }
}

std::shared_ptr<parquet::CacheManager>
ShareClientPlasmaCacheManagerProvider::defaultCacheManager() {
  return managers_[0];
}

std::shared_ptr<parquet::CacheManager>
ShareClientPlasmaCacheManagerProvider::newCacheManager() {
  auto new_manager =
      std::make_shared<ShareClientPlasmaCacheManager>(file_path_, client_pool_);

  managers_.push_back(new_manager);

  if (redis_options_) {
    new_manager->setCacheRedis(redis_options_);
  }

  return new_manager;
}

}  // namespace ape
