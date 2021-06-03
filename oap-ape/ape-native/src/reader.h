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

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/result.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>

#include "utils/AggExpression.h"
#include "utils/FilterExpression.h"
#include "utils/PlasmaCacheManager.h"
#include "utils/JsonConvertor.h"
#include "utils/Type.h"
#include "src/utils/ApeHashMap.h"
#include "src/utils/GroupByUtils.h"
#include "src/utils/DumpUtils.h"

namespace ape {
class Reader {
 public:
  Reader();

  void init(std::string fileName, std::string hdfsHost, int hdfsPort,
            std::string requiredSchema, int firstRowGroup, int rowGroupToRead);

  void initCacheManager(std::string fileName, std::string hdfsHost, int hdfsPort);

  int readBatch(int32_t batchSize, int64_t* buffersPtr, int64_t* nullsPtr);

  bool hasNext();

  bool skipNextRowGroup();

  void close();

  void setFilter(std::string filterJsonStr);

  void setAgg(std::string aggStr);

  void setPlasmaCacheEnabled(bool isEnabled, bool asyncCaching,
                             PlasmaClientPool* clientPool);

  void setPlasmaCacheRedis(std::string host, int port, std::string password);

  void setPreBufferEnabled(bool isEnabled);

  static bool isNativeEnabled();

 private:
  void convertSchema(std::string requiredColumnName);

  void preBufferRowGroups();

  void initRowGroupReaders();

  bool checkEndOfRowGroup();

  void setFilterColumnNames(std::shared_ptr<Expression> filter);
  int allocateFilterBuffers(int batchSize);
  void freeFilterBuffers();

  void setAggColumnNames(std::shared_ptr<Expression> agg);
  int allocateAggBuffers(int batchSize);
  void freeAggBuffers();

  int doReadBatch(int batchSize, std::vector<int64_t>& buffersPtr,
                  std::vector<int64_t>& nullsPtr);
  int doFilter(int batchSize, std::vector<int64_t>& buffersPtr,
               std::vector<int64_t>& nullsPtr);
  int doAggregation(int batchSize, ApeHashMap& map, std::vector<Key>& keys,
                    std::vector<DecimalVector>& results, std::vector<int64_t>& buffersPtr,
                    std::vector<int64_t>& nullsPtr);

  int allocateExtraBuffers(int batchSize, std::vector<int64_t>& buffersPtr,
                           std::vector<int64_t>& nullsPtr);

  int dumpBufferAfterAgg(int groupBySize, int aggExprsSize, const std::vector<Key>& keys,
                         const std::vector<DecimalVector>& results, int64_t* oriBufferPtr,
                         int64_t* oriNullsPtr);

  arrow::Result<std::shared_ptr<arrow::fs::HadoopFileSystem>> fsResult;
  arrow::fs::HdfsOptions* options;
  std::shared_ptr<arrow::fs::FileSystem> fs;
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::unique_ptr<parquet::ParquetFileReader> parquetReader;
  std::shared_ptr<parquet::FileMetaData> fileMetaData;

  std::vector<std::shared_ptr<parquet::RowGroupReader>> rowGroupReaders;
  std::shared_ptr<parquet::RowGroupReader> rowGroupReader;

  std::vector<int> requiredColumnIndex;
  std::vector<std::string> requiredColumnNames;
  std::shared_ptr<std::vector<Schema>> schema = std::make_shared<std::vector<Schema>>();
  std::vector<std::shared_ptr<parquet::ColumnReader>> columnReaders;
  std::vector<int> requiredRowGroupId;

  int totalRowGroups = 0;
  int totalRowGroupsRead = 0;
  int totalColumns = 0;
  int64_t totalRows = 0;
  int firstRowGroupIndex = 0;

  int currentRowGroup = 0;
  int64_t totalRowsRead = 0;
  int64_t totalRowsLoadedSoFar = 0;

  std::shared_ptr<RootFilterExpression> filterExpression;
  std::chrono::duration<double> filterTime = std::chrono::nanoseconds::zero();
  std::chrono::duration<double> aggTime = std::chrono::nanoseconds::zero();

  std::vector<char*> extraByteArrayBuffers;

  bool filterReset = false;
  int currentBatchSize = 0;
  int initRequiredColumnCount = 0;
  std::vector<std::string> filterColumnNames;
  std::vector<char*> filterDataBuffers;
  std::vector<char*> filterNullBuffers;

  int initPlusFilterRequiredColumnCount = 0;
  bool aggReset = false;
  std::vector<std::string> aggColumnNames;
  std::vector<char*> aggDataBuffers;
  std::vector<char*> aggNullBuffers;
  std::vector<std::shared_ptr<Expression>> aggExprs;
  std::vector<std::shared_ptr<Expression>> groupByExprs;

  bool plasmaCacheEnabled = false;
  bool plasmaCacheAsync = false;
  PlasmaClientPool* plasmaClientPool = NULL;
  std::shared_ptr<RedisBackedCacheManagerProvider> cacheManagerProvider;
  std::shared_ptr<sw::redis::ConnectionOptions> redisConnectionOptions;

  bool preBufferEnabled = false;
  int currentBufferedRowGroup = -1;

  std::vector<int> usedInitBufferIndex;
  std::vector<parquet::Type::type> typeVector = std::vector<parquet::Type::type>();

  std::vector<DecimalVector> results = std::vector<DecimalVector>();
  std::vector<Key> keys = std::vector<Key>();
  ApeHashMap map;
};
}  // namespace ape
