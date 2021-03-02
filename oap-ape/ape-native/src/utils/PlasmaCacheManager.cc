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

#include <arrow/util/logging.h>

#include <openssl/sha.h>

#include "PlasmaCacheManager.h"

namespace ape {

PlasmaCacheManager::PlasmaCacheManager(std::string file_path) : file_path_(file_path) {
  ARROW_LOG(INFO) << "plasma, init cache manager with path: " << file_path;

  std::shared_ptr<plasma::PlasmaClient> client = std::make_shared<plasma::PlasmaClient>();
  arrow::Status status = client->Connect("/tmp/plasmaStore", "", 0);

  if (status.ok()) {
    client_ = client;
    ARROW_LOG(INFO) << "plasma, cache manager initialized";
  } else {
    ARROW_LOG(WARNING) << "plasma, Connect failed: " << status.message();
  }
}

PlasmaCacheManager::~PlasmaCacheManager() {
  
}

bool PlasmaCacheManager::connected() {
  return client_ != nullptr;
}

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

  if (!client_) {
    return;
  }

  // release objects
  release();

  arrow:Status status = client_->Disconnect();
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Disconnect failed: " << status.message();
  }
  
  client_ = nullptr;
}

std::string PlasmaCacheManager::cacheKeyofColumnChunk(::arrow::io::ReadRange range) {
  char buff[1024];
  snprintf(buff, sizeof(buff), "plasma_cache:parquet_chunk:%s:%d_%d", file_path_.c_str(), range.offset, range.length);
  std::string ret = buff;
  return ret;
}

plasma::ObjectID PlasmaCacheManager::objectIdOfColumnChunk(::arrow::io::ReadRange range) {
  std::string cache_key = cacheKeyofColumnChunk(range);

  unsigned char hash[SHA_DIGEST_LENGTH];
  SHA1((const unsigned char*)cache_key.c_str(), cache_key.length(), hash);

  return plasma::ObjectID::from_binary(std::string(hash, hash + sizeof(hash)));
}

bool PlasmaCacheManager::containsColumnChunk(::arrow::io::ReadRange range) {
  bool has_object;

  arrow::Status status = client_->Contains(objectIdOfColumnChunk(range), &has_object);
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Contains failed: " << status.message();
    return false;
  }

  return has_object;
}

std::shared_ptr<Buffer> PlasmaCacheManager::getColumnChunk(::arrow::io::ReadRange range) {
  std::vector<plasma::ObjectID> oids;
  plasma::ObjectID oid = objectIdOfColumnChunk(range);
  oids.push_back(oid);

  std::vector<plasma::ObjectBuffer> obufs(1);

  arrow:Status status = client_->Get(oids.data(), 1, -1, obufs.data());
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Get failed: " << status.message();
    return nullptr;
  }

  // save object id for future release()
  object_ids.push_back(oid);

  ARROW_LOG(DEBUG) << "plasma, get object from cache: " << file_path_ << ", " << range.offset << ", " << range.length;

  return obufs[0].data;
}

bool PlasmaCacheManager::cacheColumnChunk(::arrow::io::ReadRange range, std::shared_ptr<Buffer> data) {
  std::vector<plasma::ObjectID> oids;
  plasma::ObjectID oid = objectIdOfColumnChunk(range);

  // create new object
  std::shared_ptr<Buffer> saved_data;
  Status status = client_->Create(oid, data->size(), nullptr, 0, &saved_data);
  if (plasma::IsPlasmaObjectExists(status)) {
    ARROW_LOG(WARNING) << "plasma, Create failed, PlasmaObjectExists: " << status.message();
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

  // save object id for future release()
  object_ids.push_back(oid);

  // copy data
  memcpy(saved_data->mutable_data(), data->data(), data->size());

  // seal object
  status = client_->Seal(oid);

  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Seal failed: " << status.message();
    return false;
  }
  
  ARROW_LOG(DEBUG) << "plasma, object cached: " << file_path_ << ", " << range.offset << ", " << range.length;

  return true;
}

bool PlasmaCacheManager::deleteColumnChunk(::arrow::io::ReadRange range) {
  arrow::Status status = client_->Delete(objectIdOfColumnChunk(range));
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "plasma, Delete failed: " << status.message();
    return false;
  }

  ARROW_LOG(INFO) << "plasma, delete object from cache: " << file_path_ << ", " << range.offset << ", " << range.length;
}

}
