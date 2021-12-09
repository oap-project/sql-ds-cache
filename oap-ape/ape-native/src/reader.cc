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

#include <algorithm>
#include <nlohmann/json.hpp>
#include <iostream>
#include "arrow/util/cpu_info.h"

#undef NDEBUG
#include <assert.h>

#include "src/reader.h"

namespace ape {

Reader::Reader() {}

void Reader::init(std::string fileName, std::string hdfsHost, int hdfsPort,
                  std::string requiredSchema, int firstRowGroup, int rowGroupToRead) {
  lastRead = (struct readReady *)malloc(sizeof(struct readReady));
  lastRead->totalRowGroups = 0;
  lastRead->totalRowGroupsRead = 0;
  lastRead->totalColumns = 0;
  lastRead->totalRows = 0;
  lastRead->firstRowGroupIndex = 0;
  lastRead->currentRowGroup = 0;
  lastRead->totalRowsRead = 0;
  lastRead->totalRowsLoadedSoFar = 0;
  lastRead->currentBufferedRowGroup = -1;
  std::cout<<"Reader::init new"<<"\n";

  options = new arrow::fs::HdfsOptions();
  ARROW_LOG(DEBUG) << "hdfsHost " << hdfsHost << " port " << hdfsPort;

  options->ConfigureEndPoint(hdfsHost, hdfsPort);
  // todo: if we delete `options`, it will core dump, seems like free twice.
  auto result = arrow::fs::HadoopFileSystem::Make(*options);
  if (!result.ok()) {
    ARROW_LOG(WARNING) << "HadoopFileSystem Make failed! err msg:"
                       << result.status().ToString();
    exit(-1);
  }
  ARROW_LOG(DEBUG) << "HadoopFileSystem Make succeed. ";

  // move and keep the result to prevent the FileSystem from destruction
  fsResult = result;
  std::shared_ptr<arrow::fs::FileSystem> fs =
      std::make_shared<arrow::fs::SubTreeFileSystem>("", fsResult.ValueOrDie());
  auto fileResult = fs->OpenInputFile(fileName);
  if (!fileResult.ok()) {
    ARROW_LOG(WARNING) << "Open hdfs file failed! err msg: "
                       << fileResult.status().ToString();
    exit(-1);
  }
  ARROW_LOG(DEBUG) << "Open hdfs file succeed. ";

  file = fileResult.ValueOrDie();

  parquet::ReaderProperties properties;
  parquetReader = parquet::ParquetFileReader::Open(file, properties, NULLPTR);

  // init and set cache manager to parquet reader.
  if (plasmaCacheEnabled) {
    initCacheManager(fileName, hdfsHost, hdfsPort);
  }

  fileMetaData = parquetReader->metadata();

  this->lastRead->firstRowGroupIndex = firstRowGroup;
  this->lastRead->totalRowGroups = rowGroupToRead;

  lastRead->totalColumns = fileMetaData->num_columns();

  ARROW_LOG(DEBUG) << "schema is " << fileMetaData->schema()->ToString();
  ARROW_LOG(DEBUG) << "required schema is " << requiredSchema;
  convertSchema(requiredSchema);

  lastRead->currentRowGroup = lastRead->firstRowGroupIndex;

  columnReaders.resize(requiredColumnIndex.size());
  initRequiredColumnCount = requiredColumnIndex.size();
  initPlusFilterRequiredColumnCount = initRequiredColumnCount;
  std::cout<<"Reader::init 97 initRequiredColumnCount"<<initRequiredColumnCount<<" initPlusFilterRequiredColumnCount"<<initPlusFilterRequiredColumnCount<<"\n";
  ARROW_LOG(DEBUG) << "initRequiredColumnCount is " << initRequiredColumnCount;

  ARROW_LOG(INFO) << "init done, totalRowGroups " << lastRead->totalRowGroups;
}

void Reader::initCacheManager(std::string fileName, std::string hdfsHost, int hdfsPort) {
  char buff[1024];
  snprintf(buff, sizeof(buff), "hdfs://%s:%d%s", hdfsHost.c_str(), hdfsPort,
           fileName.c_str());
  std::string path = buff;

  std::shared_ptr<RedisBackedCacheManagerProvider> provider = nullptr;
  if (plasmaClientPool == NULL) {
    provider = std::make_shared<PlasmaCacheManagerProvider>(path, plasmaCacheAsync);
  } else {
    provider =
        std::make_shared<ShareClientPlasmaCacheManagerProvider>(path, plasmaClientPool);
  }

  if (provider->connected()) {
    cacheManagerProvider = provider;

    if (redisConnectionOptions != nullptr) {
      cacheManagerProvider->setCacheRedis(redisConnectionOptions);
    }

    parquetReader->setCacheManagerProvider(cacheManagerProvider);
    ARROW_LOG(INFO) << "set cache manager in parquet reader";
  }
}

// TODO: need consider column sequence?
void Reader::convertSchema(std::string requiredColumnName) {
  auto j = nlohmann::json::parse(requiredColumnName);
  int filedsNum = j["fields"].size();
  for (int i = 0; i < filedsNum; i++) {
    std::string columnName = j["fields"][i]["name"];
    int columnIndex = fileMetaData->schema()->ColumnIndex(columnName);
    if (columnIndex >= 0) {
      usedInitBufferIndex.push_back(i);
      requiredColumnIndex.push_back(columnIndex);
      schema->push_back(
          Schema(columnName, fileMetaData->schema()->Column(columnIndex)->physical_type(),
                 fileMetaData->schema()->Column(columnIndex)->type_length()));
      requiredColumnNames.push_back(columnName);
    }
  }
}

void convertBitMap(uint8_t* srcBitMap, uint8_t* dstByteMap, int len) {
  for (int i = 0; i < len / 8; i++) {
    dstByteMap[i * 8 + 0] = (srcBitMap[i] & (0b00000001)) != 0;
    dstByteMap[i * 8 + 1] = (srcBitMap[i] & (0b00000010)) != 0;
    dstByteMap[i * 8 + 2] = (srcBitMap[i] & (0b00000100)) != 0;
    dstByteMap[i * 8 + 3] = (srcBitMap[i] & (0b00001000)) != 0;
    dstByteMap[i * 8 + 4] = (srcBitMap[i] & (0b00010000)) != 0;
    dstByteMap[i * 8 + 5] = (srcBitMap[i] & (0b00100000)) != 0;
    dstByteMap[i * 8 + 6] = (srcBitMap[i] & (0b01000000)) != 0;
    dstByteMap[i * 8 + 7] = (srcBitMap[i] & (0b10000000)) != 0;
  }
  for (int i = 0; i < len % 8; i++) {
    dstByteMap[len / 8 * 8 + i] = (srcBitMap[len / 8] & (1 << i)) != 0;
  }
}

int Reader::readBatch(int32_t batchSize, int64_t* buffersPtr_, int64_t* nullsPtr_) {
  std::cout<<"Reader::readBatch 162"<<"\n";
  // Pre buffer row groups.
  // This is not called in `init` because `requiredColumnIndex`
  // may be changed by `setFilter` after `init`.

  if (lastRead->totalRowsRead==0){
    preBufferRowGroups();
    // Init grow group readers.
    // This should be called after preBufferRowGroups
    initRowGroupReaders();

    // this reader have read all rows
    if (lastRead->totalRowsRead >= lastRead->totalRows && dumpAggCursor == 0) {
      return -1;
    }
    checkEndOfRowGroup();
    std::cout<<"Reader::readBatch 177"<<"\n";
    std::vector<int64_t> fbuffersPtr(initRequiredColumnCount);
    std::vector<int64_t> fnullsPtr(initRequiredColumnCount);
    for (int i = 0; i < usedInitBufferIndex.size(); i++) {
    fbuffersPtr[i] = buffersPtr_[usedInitBufferIndex[i]];
    fnullsPtr[i] = nullsPtr_[usedInitBufferIndex[i]];
  }
    lastRead->buffersPtr= &fbuffersPtr;
    lastRead->nullsPtr= &fnullsPtr;
    //to do_________________________________________________________________________________________________
    if (aggExprs.size() == 0) {  // will not do agg
    std::cout<<"Reader::readBatch 180"<<"\n";
    int rowsToRead = doReadBatch(batchSize, *(lastRead->buffersPtr), *(lastRead->nullsPtr));
    lastRead->totalRowsRead += rowsToRead;
    lastRead->rowsToRead = rowsToRead;
    ARROW_LOG(DEBUG) << "total rows read yet: " << lastRead->totalRowsRead;
    } else {
      std::cout<<"Reader::readBatch 186"<<"\n";
      if (dumpAggCursor == 0) {
        results.resize(aggExprs.size());
        for (int i = 0; i < aggExprs.size(); i++) {
          std::vector<uint8_t> nullVector(1);
          results[i].nullVector = std::make_shared<std::vector<uint8_t>>(nullVector);
        }
        while (lastRead->totalRowsRead < lastRead->totalRows && !checkEndOfRowGroup()) {
          std::cout<<"Reader::readBatch 194"<<"\n";
          int rowsToRead = doReadBatch(batchSize, *(lastRead->buffersPtr), *(lastRead->nullsPtr));
          lastRead->totalRowsRead += rowsToRead;
          lastRead->rowsToRead = rowsToRead;
          ARROW_LOG(DEBUG) << "total rows read yet: " << lastRead->totalRowsRead;
        }
      } 
    }
    std::cout<<"Reader::readBatch 202"<<"\n";
  }


  preBufferRowGroups();
  // Init grow group readers.
  // This should be called after preBufferRowGroups
  initRowGroupReaders();

  // this reader have read all rows
  if (lastRead->totalRowsRead >= lastRead->totalRows && dumpAggCursor == 0) {
    return -1;
  }
  checkEndOfRowGroup();
  struct readReady *ReadReady = (struct readReady *)malloc(sizeof(struct readReady));
  ReadReady-> totalRowGroups = lastRead->totalRowGroups;
  ReadReady-> totalRowGroupsRead = lastRead->totalRowGroupsRead;
  ReadReady-> totalColumns = lastRead->totalColumns;
  ReadReady-> totalRows = lastRead->totalRows;
  ReadReady-> firstRowGroupIndex = lastRead->firstRowGroupIndex;
  ReadReady-> currentRowGroup = lastRead->currentRowGroup;
  ReadReady-> totalRowsRead = lastRead->totalRowsRead;
  ReadReady-> totalRowsLoadedSoFar = lastRead->totalRowsLoadedSoFar;
  ReadReady-> rowsToRead=lastRead->rowsToRead;
  ReadReady-> currentBufferedRowGroup=lastRead->currentBufferedRowGroup;

  std::vector<int64_t> buffersPtr(initRequiredColumnCount);
  std::vector<int64_t> nullsPtr(initRequiredColumnCount);
  std::cout<<"Reader::readBatch 219"<<"\n";

  // Not all input buffers can be used for column data loading.
  // espeically when agg pushing down is enabled.
  // E.g. input buffers could be in types of "tbl_col_a, sum(tbl_col_b)",
  // in which only the first buffer can be used for column data loading.
  for (int i = 0; i < usedInitBufferIndex.size(); i++) {
    buffersPtr[i] = buffersPtr_[usedInitBufferIndex[i]];
    nullsPtr[i] = nullsPtr_[usedInitBufferIndex[i]];
  }
  ReadReady->buffersPtr= &buffersPtr;
  ReadReady->nullsPtr= &nullsPtr;
  std::cout<<"Reader::readBatch 231"<<"\n";


  allocateExtraBuffers(batchSize, *(ReadReady->buffersPtr), *(ReadReady->nullsPtr));

  currentBatchSize = batchSize;
  int rowsRet = 0;
  if (aggExprs.size() == 0) {  // will not do agg
  std::cout<<"Reader::readBatch 239"<<"\n";
    int rowsToRead = doReadBatch(batchSize, *(lastRead->buffersPtr), *(lastRead->nullsPtr));
    lastRead->totalRowsRead += rowsToRead;
    lastRead->rowsToRead = rowsToRead;
    ARROW_LOG(DEBUG) << "total rows read yet: " << lastRead->totalRowsRead;
    rowsRet = doFilter(ReadReady->rowsToRead, *(ReadReady->buffersPtr), *(ReadReady->nullsPtr));
  } else {
    std::cout<<"Reader::readBatch 246"<<"\n";
    if (dumpAggCursor == 0) {  // will read a whole RowGroup and do agg
      results.resize(aggExprs.size());
      for (int i = 0; i < aggExprs.size(); i++) {
        std::cout<<"Reader::readBatch 250"<<"\n";
        std::vector<uint8_t> nullVector(1);
        results[i].nullVector = std::make_shared<std::vector<uint8_t>>(nullVector);
      }
      while (lastRead->totalRowsRead < lastRead->totalRows && !checkEndOfRowGroup()) {
        std::cout<<"Reader::readBatch 255"<<"\n";
        int rowsToRead = doReadBatch(batchSize, *(lastRead->buffersPtr), *(lastRead->nullsPtr));
        lastRead->totalRowsRead += rowsToRead;
        lastRead->rowsToRead = rowsToRead;
        ARROW_LOG(DEBUG) << "total rows read yet: " << lastRead->totalRowsRead;

        int rowsAfterFilter = doFilter(ReadReady->rowsToRead, *(ReadReady->buffersPtr), *(ReadReady->nullsPtr));
        ARROW_LOG(DEBUG) << "after filter " << rowsAfterFilter;

        int tmp =
            doAggregation(rowsAfterFilter, map, keys, results, *(ReadReady->buffersPtr), *(ReadReady->nullsPtr));
        // if the last batch are empty after filter, it will return 0 regard less of the
        // group num
        if (tmp != 0) rowsRet = tmp;
      }
      int rowsDump = rowsRet;
      if (rowsRet > batchSize) {
        std::cout<<"Reader::readBatch 272"<<"\n";
        rowsDump = batchSize;
        dumpAggCursor = batchSize;
      }

      if (aggExprs.size()) {
        std::cout<<"Reader::readBatch 278"<<"\n";
        dumpBufferAfterAgg(groupByExprs.size(), aggExprs.size(), keys, results,
                           buffersPtr_, nullsPtr_, 0, rowsDump);
      }
      if (rowsRet <=
          batchSize) {  // return all result in one call, so clear buffers here.
          std::cout<<"Reader::readBatch 284"<<"\n";
        map.clear();
        keys.clear();
        results.clear();
      }
      rowsRet = rowsDump;
    } else {  // this row group aggregation result is more than default batch size, we
      // will return them via mutilple call
      std::cout<<"Reader::readBatch 292"<<"\n";
      rowsRet = ((keys.size() - dumpAggCursor) > batchSize)
                    ? batchSize
                    : ((keys.size() - dumpAggCursor));
      if (aggExprs.size()) {
        std::cout<<"Reader::readBatch 297"<<"\n";
        dumpBufferAfterAgg(groupByExprs.size(), aggExprs.size(), keys, results,
                           buffersPtr_, nullsPtr_, dumpAggCursor, rowsRet);
      }
      if ((keys.size() - dumpAggCursor) <=
          batchSize) {  // the last batch, let's clear buffers
          std::cout<<"Reader::readBatch 303"<<"\n";
        map.clear();
        keys.clear();
        results.clear();
        dumpAggCursor = 0;
      } else {
        std::cout<<"Reader::readBatch 309"<<"\n";
        dumpAggCursor += batchSize;
      }
    }
  }
  std::cout<<"Reader::readBatch 314"<<"\n";

  free(ReadReady);
  ReadReady=NULL;
  ARROW_LOG(DEBUG) << "ret rows " << rowsRet;
  return rowsRet;
}

int Reader::doReadBatch(int batchSize, std::vector<int64_t>& buffersPtr,
                        std::vector<int64_t>& nullsPtr) {
  std::cout<<"Reader::doReadBatch 324"<<"\n";
  int rowsToRead = std::min((int64_t)batchSize, lastRead->totalRowsLoadedSoFar - lastRead->totalRowsRead);
  std::vector<int16_t> defLevel(rowsToRead);
  std::vector<int16_t> repLevel(rowsToRead);
  std::vector<uint8_t> nullBitMap(rowsToRead);
  ARROW_LOG(DEBUG) << "will read " << rowsToRead << " rows";
  for (int i = 0; i < columnReaders.size(); i++) {
    std::cout<<"Reader::doReadBatch 331"<<"\n";
    int64_t levelsRead = 0, valuesRead = 0, nullCount = 0;
    int rows = 0;
    int tmpRows = 0;
    // ReadBatchSpaced API will return rows left in a data page
    while (rows < rowsToRead) {
      std::cout<<"Reader::doReadBatch 337"<<"\n";
      // TODO: refactor. it's ugly, but didn't find some better way.
      switch (typeVector[i]) {
        std::cout<<"Reader::doReadBatch 340"<<"\n";
        case parquet::Type::BOOLEAN: {
          std::cout<<"Reader::doReadBatch 342"<<"\n";
          parquet::BoolReader* boolReader =
              static_cast<parquet::BoolReader*>(columnReaders[i].get());
          tmpRows = boolReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel.data(), repLevel.data(),
              (bool*)buffersPtr[i] + rows, nullBitMap.data(), 0, &levelsRead, &valuesRead,
              &nullCount);
          break;
        }

        case parquet::Type::INT32: {
          std::cout<<"Reader::doReadBatch 353"<<"\n";
          parquet::Int32Reader* int32Reader =
              static_cast<parquet::Int32Reader*>(columnReaders[i].get());
          tmpRows = int32Reader->ReadBatchSpaced(
              rowsToRead - rows, defLevel.data(), repLevel.data(),
              (int32_t*)buffersPtr[i] + rows, nullBitMap.data(), 0, &levelsRead,
              &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::INT64: {
          std::cout<<"Reader::doReadBatch 363"<<"\n";
          parquet::Int64Reader* int64Reader =
              static_cast<parquet::Int64Reader*>(columnReaders[i].get());
          tmpRows = int64Reader->ReadBatchSpaced(
              rowsToRead - rows, defLevel.data(), repLevel.data(),
              (int64_t*)buffersPtr[i] + rows, nullBitMap.data(), 0, &levelsRead,
              &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::INT96: {
          std::cout<<"Reader::doReadBatch 373"<<"\n";
          parquet::Int96Reader* int96Reader =
              static_cast<parquet::Int96Reader*>(columnReaders[i].get());
          tmpRows = int96Reader->ReadBatchSpaced(
              rowsToRead - rows, defLevel.data(), repLevel.data(),
              (parquet::Int96*)buffersPtr[i] + rows, nullBitMap.data(), 0, &levelsRead,
              &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::FLOAT: {
          std::cout<<"Reader::doReadBatch 383"<<"\n";
          parquet::FloatReader* floatReader =
              static_cast<parquet::FloatReader*>(columnReaders[i].get());
          tmpRows = floatReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel.data(), repLevel.data(),
              (float*)buffersPtr[i] + rows, nullBitMap.data(), 0, &levelsRead,
              &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::DOUBLE: {
          std::cout<<"Reader::doReadBatch 393"<<"\n";
          parquet::DoubleReader* doubleReader =
              static_cast<parquet::DoubleReader*>(columnReaders[i].get());
          tmpRows = doubleReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel.data(), repLevel.data(),
              (double*)buffersPtr[i] + rows, nullBitMap.data(), 0, &levelsRead,
              &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::BYTE_ARRAY: {
          std::cout<<"Reader::doReadBatch 403"<<"\n";
          parquet::ByteArrayReader* byteArrayReader =
              static_cast<parquet::ByteArrayReader*>(columnReaders[i].get());
          tmpRows = byteArrayReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel.data(), repLevel.data(),
              (parquet::ByteArray*)buffersPtr[i] + rows, nullBitMap.data(), 0,
              &levelsRead, &valuesRead, &nullCount);
          parquet::ByteArray* p = (parquet::ByteArray*)buffersPtr[i] + rows;
          // we do need to read twice, buffer may be overwrite, so let's do an extra
          // memory copy.
          if (tmpRows + rows < rowsToRead) {
            ARROW_LOG(DEBUG) << "read rows: " << tmpRows << " need to do memory copy!";
            // calculate total size
            uint32_t totalLen = 0;
            for (int k = 0; k < tmpRows; k++) {
              totalLen += p[k].len;
            }
            char* buffer = new char[totalLen];
            extraByteArrayBuffers.push_back(buffer);
            uint32_t write = 0;
            for (int k = 0; k < tmpRows; k++) {
              std::memcpy(buffer + write, p[k].ptr, p[k].len);
              p[k].ptr = (uint8_t*)(buffer + write);
              write += p[k].len;
            }
          }

          break;
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
          std::cout<<"Reader::doReadBatch 433"<<"\n";
          parquet::FixedLenByteArrayReader* fixedLenByteArrayReader =
              static_cast<parquet::FixedLenByteArrayReader*>(columnReaders[i].get());
          tmpRows = fixedLenByteArrayReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel.data(), repLevel.data(),
              (parquet::FixedLenByteArray*)buffersPtr[i] + rows, nullBitMap.data(), 0,
              &levelsRead, &valuesRead, &nullCount);
          break;
        }
        default:
          ARROW_LOG(WARNING) << "Unsupported Type!";
          break;
      }
      convertBitMap(nullBitMap.data(), (uint8_t*)nullsPtr[i] + rows, tmpRows);
      rows += tmpRows;
      std::cout<<"Reader::doReadBatch 449"<<"\n";
    }
    assert(rowsToRead == rows);
    ARROW_LOG(DEBUG) << "columnReader read rows: " << rows;
    std::cout<<"Reader::doReadBatch 453"<<"\n";
  }
  std::cout<<"Reader::doReadBatch 393"<<"\n";
  return rowsToRead;
}

int Reader::doFilter(int batchSize, std::vector<int64_t>& buffersPtr,
                     std::vector<int64_t>& nullsPtr) {
  std::cout<<"Reader::doFilter 457"<<"\n";
  if (filterExpression) {
    std::cout<<"Reader::doFilter 459"<<"\n";
    auto start = std::chrono::steady_clock::now();
    std::vector<int8_t> tmp(0);
    int rowsRet =
        filterExpression->ExecuteWithParam(batchSize, buffersPtr, nullsPtr, tmp);
    filterTime += std::chrono::steady_clock::now() - start;
    return rowsRet;
  }
  return batchSize;
}

int Reader::doAggregation(int batchSize, ApeHashMap& map, std::vector<Key>& keys,
                          std::vector<DecimalVector>& results,
                          std::vector<int64_t>& buffersPtr,
                          std::vector<int64_t>& nullsPtr) {
  std::cout<<"Reader::doAggregation 457"<<"\n";
  int rowsRet = batchSize;
  if (batchSize > 0 &&
      aggExprs.size()) {  // if rows after filter is 0, no need to do agg.
    std::cout<<"Reader::doAggregation 478"<<"\n";
    auto start = std::chrono::steady_clock::now();
    int groupBySize = groupByExprs.size();
    ARROW_LOG(DEBUG) << "group by size " << groupBySize;
    std::vector<int> indexes(batchSize);

    // build hash map and index
    if (groupBySize > 0) {
      std::cout<<"Reader::doAggregation 486"<<"\n";
      GroupByUtils::groupBy(map, indexes, batchSize, groupByExprs, buffersPtr, nullsPtr,
                            keys, typeVector);
    }

    for (int i = 0; i < aggExprs.size(); i++) {
      std::cout<<"Reader::doAggregation 492"<<"\n";
      auto agg = aggExprs[i];
      if (typeid(*agg) == typeid(RootAggExpression)) {
        std::cout<<"Reader::doAggregation 495"<<"\n";
        std::vector<int8_t> tmp(0);
        agg->ExecuteWithParam(batchSize, buffersPtr, nullsPtr, tmp);
        if (groupBySize) {  // do agg based on indexes
          std::dynamic_pointer_cast<RootAggExpression>(agg)->getResult(
              results[i], keys.size(), indexes);
        } else {
          std::dynamic_pointer_cast<RootAggExpression>(agg)->getResult(results[i]);
        }
      } else {
        std::cout<<"Reader::doAggregation 505"<<"\n";
        ARROW_LOG(DEBUG) << "skipping groupBy column when doing aggregation";
      }
    }

    for (int i = 0; i < aggExprs.size(); i++) {
      std::cout<<"Reader::doAggregation 511"<<"\n";
      auto agg = aggExprs[i];
      if (typeid(*agg) == typeid(RootAggExpression)) {
        std::cout<<"Reader::doAggregation 514"<<"\n";
        std::dynamic_pointer_cast<RootAggExpression>(agg)->reset();
      }
    }
    rowsRet = groupBySize > 0 ? keys.size() : 1;

    aggTime += std::chrono::steady_clock::now() - start;
  }
  return rowsRet;
}

// TODO : will some case only do group by but no agg?
int Reader::dumpBufferAfterAgg(int groupBySize, int aggExprsSize,
                               const std::vector<Key>& keys,
                               const std::vector<DecimalVector>& results,
                               int64_t* oriBufferPtr, int64_t* oriNullsPtr,
                               int32_t offset, int32_t length) {
  // dump buffers
  for (int i = 0; i < groupBySize; i++) {
    std::shared_ptr<AttributeReferenceExpression> groupByExpr =
        std::static_pointer_cast<AttributeReferenceExpression>(groupByExprs[i]);
    int typeIndex = groupByExpr->columnIndex;
    DumpUtils::dumpGroupByKeyToJavaBuffer(keys, (uint8_t*)(oriBufferPtr[i]),
                                          (uint8_t*)(oriNullsPtr[i]), i,
                                          typeVector[typeIndex], offset, length);
  }

  for (int i = groupBySize; i < aggExprs.size(); i++) {
    DumpUtils::dumpToJavaBuffer((uint8_t*)(oriBufferPtr[i]), (uint8_t*)(oriNullsPtr[i]),
                                results[i], offset, length);
  }

  return 0;
}

int Reader::allocateExtraBuffers(int batchSize, std::vector<int64_t>& buffersPtr,
                                 std::vector<int64_t>& nullsPtr) {
  std::cout<<"Reader::allocateExtraBuffers 575"<<"\n";
  if (filterExpression) {
    std::cout<<"Reader::allocateExtraBuffers 577"<<"\n";
    allocateFilterBuffers(batchSize);
  }

  if (aggExprs.size()) {  // todo: group by agg size
  std::cout<<"Reader::allocateExtraBuffers 582"<<"\n";
    allocateAggBuffers(batchSize);
  }
  std::cout<<"Reader::allocateExtraBuffers 585"<<"\n";
  int filterBufferCount = filterDataBuffers.size();
  int aggBufferCount = aggDataBuffers.size();

  if (filterBufferCount > 0 || aggBufferCount > 0) {
    ARROW_LOG(DEBUG) << "use extra filter buffers count: " << filterBufferCount
                     << "use extra agg buffers count: " << aggBufferCount;
    std::cout<<"Reader::allocateExtraBuffers 592"<<"\n";

    buffersPtr.resize(initRequiredColumnCount + filterBufferCount + aggBufferCount);
    nullsPtr.resize(initRequiredColumnCount + filterBufferCount + aggBufferCount);

    for (int i = 0; i < filterBufferCount; i++) {
      std::cout<<"Reader::allocateExtraBuffers 598"<<"\n";
      buffersPtr[initRequiredColumnCount + i] = (int64_t)filterDataBuffers[i];
      nullsPtr[initRequiredColumnCount + i] = (int64_t)filterNullBuffers[i];
    }

    for (int i = 0; i < aggBufferCount; i++) {
      std::cout<<"Reader::allocateExtraBuffers 604"<<"\n";
      buffersPtr[initRequiredColumnCount + filterBufferCount + i] =
          (int64_t)aggDataBuffers[i];
      nullsPtr[initRequiredColumnCount + filterBufferCount + i] =
          (int64_t)aggNullBuffers[i];
    }
  }
  std::cout<<"Reader::allocateExtraBuffers 611"<<"\n";
  return initRequiredColumnCount + filterBufferCount + aggBufferCount;
}

bool Reader::hasNext() { return dumpAggCursor > 0 || columnReaders[0]->HasNext(); }

bool Reader::skipNextRowGroup() {
  if (lastRead->totalRowGroupsRead == lastRead->totalRowGroups) {
    return false;
  }
  lastRead->currentRowGroup++;
  lastRead->totalRowGroupsRead++;
  return true;
}

void Reader::close() {
  ARROW_LOG(INFO) << "Filter takes " << filterTime.count() * 1000 << " ms. "
                  << "Agg takes " << aggTime.count() * 1000 << " ms";
  filterTime = std::chrono::nanoseconds::zero();
  aggTime = std::chrono::nanoseconds::zero();

  // No need to call parquetReader->Close(). It will be done in destructor.

  file->Close();
  for (auto ptr : extraByteArrayBuffers) {
    delete[] ptr;
  }
  freeFilterBuffers();
  freeAggBuffers();

  if (cacheManagerProvider) {
    cacheManagerProvider->close();
    cacheManagerProvider = nullptr;
  }

  // delete options;
}

void Reader::preBufferRowGroups() {
  if (!preBufferEnabled || lastRead->currentBufferedRowGroup >= lastRead->currentRowGroup) {
    return;
  }

  int maxBufferCount = 100;  // TODO
  std::vector<int> rowGroups;
  std::vector<int> columns = requiredColumnIndex;
  int maxRowGroupIndex = lastRead->firstRowGroupIndex + lastRead->totalRowGroups - 1;
  for (int i = 0; i < maxBufferCount && lastRead->currentBufferedRowGroup < maxRowGroupIndex; i++) {
    lastRead->currentBufferedRowGroup = lastRead->currentRowGroup + i;
    rowGroups.push_back(lastRead->currentBufferedRowGroup);
  }

  ::arrow::io::AsyncContext ctx;
  ::arrow::io::CacheOptions options = ::arrow::io::CacheOptions::Defaults();

  if (rowGroups.size() > 0) {
    ARROW_LOG(INFO) << "PreBuffer, " << rowGroups.size() << " row group(s), "
                    << columns.size() << " columns in each group";

    parquetReader->PreBuffer(rowGroups, columns, ctx, options);
  }
}

void Reader::initRowGroupReaders() {
  if (rowGroupReaders.size() > 0) {
    return;
  }

  rowGroupReaders.resize(lastRead->totalRowGroups);
  for (int i = 0; i < lastRead->totalRowGroups; i++) {
    rowGroupReaders[i] = parquetReader->RowGroup(lastRead->firstRowGroupIndex + i);
    lastRead->totalRows += rowGroupReaders[i]->metadata()->num_rows();
    ARROW_LOG(DEBUG) << "this rg have rows: "
                     << rowGroupReaders[i]->metadata()->num_rows();
  }
}

bool Reader::checkEndOfRowGroup() {
  if (lastRead->totalRowsRead != lastRead->totalRowsLoadedSoFar || dumpAggCursor != 0) return false;
  // if a splitFile contains rowGroup [2,5], currentRowGroup is 2
  // rowGroupReaders index starts from 0
  ARROW_LOG(DEBUG) << "totalRowsLoadedSoFar: " << lastRead->totalRowsLoadedSoFar;
  rowGroupReader = rowGroupReaders[lastRead->currentRowGroup - lastRead->firstRowGroupIndex];
  lastRead->currentRowGroup++;
  lastRead->totalRowGroupsRead++;

  // Do not release CacheManager's objects when going to next row group.
  // release() may free all useful buffers loaded by preBufferRowGroups.

  for (int i = 0; i < requiredColumnIndex.size(); i++) {
    columnReaders[i] = rowGroupReader->Column(requiredColumnIndex[i]);
  }

  if (typeVector.size() == 0) {
    for (int i = 0; i < requiredColumnIndex.size(); i++) {
      typeVector.push_back(
          fileMetaData->schema()->Column(requiredColumnIndex[i])->physical_type());
    }
  }

  lastRead->totalRowsLoadedSoFar += rowGroupReader->metadata()->num_rows();
  return true;
}

void Reader::setFilter(std::string filterJsonStr) {
  std::shared_ptr<Expression> tmpExpression =
      JsonConvertor::parseToFilterExpression(filterJsonStr);

  filterExpression = std::make_shared<RootFilterExpression>(
      "root", std::dynamic_pointer_cast<FilterExpression>(tmpExpression));

  // get column names from expression
  filterColumnNames.clear();
  setFilterColumnNames(tmpExpression);

  // reset required columns to initial size
  requiredColumnIndex.resize(initRequiredColumnCount);
  requiredColumnNames.resize(initRequiredColumnCount);
  schema->erase(schema->begin() + initRequiredColumnCount, schema->end());
  columnReaders.resize(initRequiredColumnCount);

  // Check with filtered column names. Append column if not present in the initial
  // required columns.
  for (int i = 0; i < filterColumnNames.size(); i++) {
    std::string columnName = filterColumnNames[i];
    if (std::find(requiredColumnNames.begin(), requiredColumnNames.end(), columnName) ==
        requiredColumnNames.end()) {
      int columnIndex = fileMetaData->schema()->ColumnIndex(columnName);

      // append column
      requiredColumnIndex.push_back(columnIndex);
      requiredColumnNames.push_back(columnName);
      schema->push_back(
          Schema(columnName, fileMetaData->schema()->Column(columnIndex)->physical_type(),
                 fileMetaData->schema()->Column(columnIndex)->type_length()));
      columnReaders.resize(requiredColumnIndex.size());
    }
  }

  filterExpression->setSchema(schema);
  filterReset = true;
  initPlusFilterRequiredColumnCount = requiredColumnIndex.size();
  std::cout<<"Reader::setFilter 646 initRequiredColumnCount"<<initRequiredColumnCount<<" initPlusFilterRequiredColumnCount"<<initPlusFilterRequiredColumnCount<<"\n";
}

int Reader::allocateFilterBuffers(int batchSize) {
  std::cout<<"Reader::allocateFilterBuffers 757"<<"\n";
  if (!filterReset && batchSize <= currentBatchSize) {
    std::cout<<"Reader::allocateFilterBuffers 758"<<"\n";
    return 0;
  }
  filterReset = false;
  std::cout<<"Reader::allocateFilterBuffers 762"<<"\n";

  // free current filter buffers
  freeFilterBuffers();
  std::cout<<"Reader::allocateFilterBuffers 766"<<"\n";

  // allocate new filter buffers
  int extraBufferNum = 0;
  std::cout<<"Reader::allocateFilterBuffers 772 initRequiredColumnCount"<<initRequiredColumnCount<<" initPlusFilterRequiredColumnCount"<<initPlusFilterRequiredColumnCount<<"\n";
  for (int i = initRequiredColumnCount; i < initPlusFilterRequiredColumnCount; i++) {
    std::cout<<"Reader::allocateFilterBuffers 771"<<"\n";
    int columnIndex = requiredColumnIndex[i];
    // allocate memory buffer
    char* dataBuffer;
    switch (fileMetaData->schema()->Column(columnIndex)->physical_type()) {
      case parquet::Type::BOOLEAN:
        dataBuffer = (char*)new bool[batchSize];
        break;
      case parquet::Type::INT32:
        dataBuffer = (char*)new int32_t[batchSize];
        break;
      case parquet::Type::INT64:
        dataBuffer = (char*)new int64_t[batchSize];
        break;
      case parquet::Type::INT96:
        dataBuffer = (char*)new parquet::Int96[batchSize];
        break;
      case parquet::Type::FLOAT:
        dataBuffer = (char*)new float[batchSize];
        break;
      case parquet::Type::DOUBLE:
        dataBuffer = (char*)new double[batchSize];
        break;
      case parquet::Type::BYTE_ARRAY:
        dataBuffer = (char*)new parquet::ByteArray[batchSize];
        break;
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        dataBuffer = (char*)new parquet::FixedLenByteArray[batchSize];
        break;
      default:
        ARROW_LOG(WARNING) << "Unsupported Type!";
        continue;
    }

    char* nullBuffer = new char[batchSize];
    filterDataBuffers.push_back(dataBuffer);
    filterNullBuffers.push_back(nullBuffer);
    extraBufferNum++;
  }
  std::cout<<"Reader::allocateFilterBuffers 810"<<"\n";

  ARROW_LOG(INFO) << "create extra filter buffers count: " << extraBufferNum;
  std::cout<<"Reader::allocateFilterBuffers 813"<<"\n";
  return extraBufferNum;
}

void Reader::freeFilterBuffers() {
  for (auto ptr : filterDataBuffers) {
    delete[] ptr;
  }
  filterDataBuffers.clear();
  for (auto ptr : filterNullBuffers) {
    delete[] ptr;
  }
  filterNullBuffers.clear();
}

void Reader::setFilterColumnNames(std::shared_ptr<Expression> filter) {
  std::string type = filter->getType();

  if (type.compare("not") == 0) {
    setFilterColumnNames(
        std::dynamic_pointer_cast<NotFilterExpression>(filter)->getChild());
  } else if (type.compare("and") == 0 || type.compare("or") == 0) {
    setFilterColumnNames(
        std::dynamic_pointer_cast<BinaryFilterExpression>(filter)->getLeftChild());
    setFilterColumnNames(
        std::dynamic_pointer_cast<BinaryFilterExpression>(filter)->getRightChild());
  } else if (type.compare("lt") == 0 || type.compare("lteq") == 0 ||
             type.compare("gt") == 0 || type.compare("gteq") == 0 ||
             type.compare("eq") == 0 || type.compare("noteq") == 0 ||
             type.compare("apestartwithfilter") == 0 ||
             type.compare("apeendwithfilter") == 0 ||
             type.compare("apecontainsfilter") == 0) {
    std::string columnName =
        std::dynamic_pointer_cast<UnaryFilterExpression>(filter)->getColumnName();
    if (std::find(filterColumnNames.begin(), filterColumnNames.end(), columnName) ==
        filterColumnNames.end()) {
      filterColumnNames.push_back(columnName);
    }
  } else {
    ARROW_LOG(WARNING) << "unsupported Expression type" << type;
    return;
  }
}

int Reader::allocateAggBuffers(int batchSize) {
  std::cout<<"Reader::allocateAggBuffers 857"<<"\n";
  if (!aggReset && batchSize <= currentBatchSize) {
    std::cout<<"Reader::allocateAggBuffers 859"<<"\n";
    return 0;
  }
  aggReset = false;

  // free current agg buffers
  freeAggBuffers();
  std::cout<<"Reader::allocateAggBuffers 866"<<"\n";

  // allocate new agg buffers
  int extraBufferNum = 0;
  for (int i = initPlusFilterRequiredColumnCount; i < requiredColumnIndex.size(); i++) {
    std::cout<<"Reader::allocateAggBuffers 871"<<"\n";
    int columnIndex = requiredColumnIndex[i];
    // allocate memory buffer
    char* dataBuffer;
    switch (fileMetaData->schema()->Column(columnIndex)->physical_type()) {
      case parquet::Type::BOOLEAN:
        dataBuffer = (char*)new bool[batchSize];
        break;
      case parquet::Type::INT32:
        dataBuffer = (char*)new int32_t[batchSize];
        break;
      case parquet::Type::INT64:
        dataBuffer = (char*)new int64_t[batchSize];
        break;
      case parquet::Type::INT96:
        dataBuffer = (char*)new parquet::Int96[batchSize];
        break;
      case parquet::Type::FLOAT:
        dataBuffer = (char*)new float[batchSize];
        break;
      case parquet::Type::DOUBLE:
        dataBuffer = (char*)new double[batchSize];
        break;
      case parquet::Type::BYTE_ARRAY:
        dataBuffer = (char*)new parquet::ByteArray[batchSize];
        break;
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        dataBuffer = (char*)new parquet::FixedLenByteArray[batchSize];
        break;
      default:
        ARROW_LOG(WARNING) << "Unsupported Type!";
        continue;
    }

    char* nullBuffer = new char[batchSize];
    aggDataBuffers.push_back(dataBuffer);
    aggNullBuffers.push_back(nullBuffer);
    extraBufferNum++;
  }
  std::cout<<"Reader::allocateAggBuffers 910"<<"\n";

  ARROW_LOG(INFO) << "create extra agg buffers count: " << extraBufferNum;
  return extraBufferNum;
}

void Reader::freeAggBuffers() {
  for (auto ptr : aggDataBuffers) {
    delete[] ptr;
  }
  aggDataBuffers.clear();
  for (auto ptr : aggNullBuffers) {
    delete[] ptr;
  }
  aggNullBuffers.clear();
}

void Reader::setAggColumnNames(std::shared_ptr<Expression> agg) {
  auto expr = std::dynamic_pointer_cast<WithResultExpression>(agg);
  if (typeid(*expr) == typeid(AttributeReferenceExpression)) {
    std::string columnName = expr->getColumnName();
    if (std::find(aggColumnNames.begin(), aggColumnNames.end(), columnName) ==
        aggColumnNames.end()) {
      aggColumnNames.push_back(columnName);
    }
  } else if (typeid(*expr) == typeid(RootAggExpression)) {
    setAggColumnNames(std::dynamic_pointer_cast<RootAggExpression>(expr)->getChild());
  } else if (std::dynamic_pointer_cast<AggExpression>(expr)) {
    setAggColumnNames(std::dynamic_pointer_cast<AggExpression>(expr)->getChild());
  } else if (std::dynamic_pointer_cast<ArithmeticExpression>(expr)) {
    setAggColumnNames(
        std::dynamic_pointer_cast<ArithmeticExpression>(expr)->getLeftChild());
    setAggColumnNames(
        std::dynamic_pointer_cast<ArithmeticExpression>(expr)->getRightChild());
  }
}

void Reader::setAgg(std::string aggStr) {
  groupByExprs = JsonConvertor::parseToGroupByExpressions(aggStr);
  std::unordered_map<std::string, std::shared_ptr<WithResultExpression>> cache;
  aggExprs = JsonConvertor::parseToAggExpressions(aggStr, cache);

  // get column names from expression
  aggColumnNames.clear();
  for (auto agg : aggExprs) {
    setAggColumnNames(agg);
  }

  // reset required columns to initial size
  requiredColumnIndex.resize(initPlusFilterRequiredColumnCount);
  requiredColumnNames.resize(initPlusFilterRequiredColumnCount);
  schema->erase(schema->begin() + initPlusFilterRequiredColumnCount, schema->end());
  columnReaders.resize(initPlusFilterRequiredColumnCount);

  // Check with agg column names. Append column if not present in the initial required
  // columns.
  for (int i = 0; i < aggColumnNames.size(); i++) {
    std::string columnName = aggColumnNames[i];
    if (std::find(requiredColumnNames.begin(), requiredColumnNames.end(), columnName) ==
        requiredColumnNames.end()) {
      int columnIndex = fileMetaData->schema()->ColumnIndex(columnName);

      // append column
      requiredColumnIndex.push_back(columnIndex);
      requiredColumnNames.push_back(columnName);
      schema->push_back(
          Schema(columnName, fileMetaData->schema()->Column(columnIndex)->physical_type(),
                 fileMetaData->schema()->Column(columnIndex)->type_length()));
      columnReaders.resize(requiredColumnIndex.size());
    }
  }

  for (auto agg : aggExprs) {
    agg->setSchema(schema);
  }
  for (auto agg : groupByExprs) {
    agg->setSchema(schema);
  }
  aggReset = true;
}

void Reader::setPlasmaCacheEnabled(bool isEnabled, bool asyncCaching,
                                   PlasmaClientPool* clientPool) {
  plasmaCacheEnabled = isEnabled;
  plasmaCacheAsync = asyncCaching;
  plasmaClientPool = clientPool;
}

void Reader::setPlasmaCacheRedis(std::string host, int port, std::string password) {
  auto options = std::make_shared<sw::redis::ConnectionOptions>();
  options->host = host;
  options->port = port;
  if (password.size() > 0) {
    options->password = password;
  }
  redisConnectionOptions = options;

  if (cacheManagerProvider != nullptr) {
    cacheManagerProvider->setCacheRedis(options);
  }
}

void Reader::setPreBufferEnabled(bool isEnabled) { preBufferEnabled = isEnabled; }

bool Reader::isNativeEnabled() {
  return arrow::internal::CpuInfo::GetInstance()->vendor() ==
         arrow::internal::CpuInfo::Vendor::Intel;
}

}  // namespace ape
