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

#undef NDEBUG
#include <assert.h>

#include "src/reader.h"

namespace ape {

Reader::Reader() {}

void Reader::init(std::string fileName, std::string hdfsHost, int hdfsPort,
                  std::string requiredSchema, int firstRowGroup, int rowGroupToRead) {
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

  this->firstRowGroupIndex = firstRowGroup;
  this->totalRowGroups = rowGroupToRead;

  totalColumns = fileMetaData->num_columns();

  ARROW_LOG(DEBUG) << "schema is " << fileMetaData->schema()->ToString();
  ARROW_LOG(DEBUG) << "required schema is " << requiredSchema;
  convertSchema(requiredSchema);

  currentRowGroup = firstRowGroupIndex;

  rowGroupReaders.resize(totalRowGroups);
  for (int i = 0; i < totalRowGroups; i++) {
    rowGroupReaders[i] = parquetReader->RowGroup(firstRowGroupIndex + i);
    totalRows += rowGroupReaders[i]->metadata()->num_rows();
    ARROW_LOG(DEBUG) << "this rg have rows: "
                     << rowGroupReaders[i]->metadata()->num_rows();
  }
  columnReaders.resize(requiredColumnIndex.size());
  initRequiredColumnCount = requiredColumnIndex.size();
  initPlusFilterRequiredColumnCount = initRequiredColumnCount;
  ARROW_LOG(DEBUG) << "initRequiredColumnCount is " << initRequiredColumnCount;

  ARROW_LOG(INFO) << "init done, totalRows " << totalRows;
}

void Reader::initCacheManager(std::string fileName, std::string hdfsHost, int hdfsPort) {
  char buff[1024];
  snprintf(buff, sizeof(buff), "hdfs://%s:%d%s", hdfsHost.c_str(), hdfsPort,
           fileName.c_str());
  std::string path = buff;

  std::shared_ptr<PlasmaCacheManager> cacheManager =
      std::make_shared<PlasmaCacheManager>(path);
  if (cacheManager->connected()) {
    plasmaCacheManager = cacheManager;

    if (redisConnectionOptions != nullptr) {
      plasmaCacheManager->setCacheRedis(redisConnectionOptions);
    }

    parquetReader->setCacheManager(cacheManager);
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
    requiredColumnIndex.push_back(columnIndex);
    schema.push_back(
        Schema(columnName, fileMetaData->schema()->Column(columnIndex)->physical_type()));
    requiredColumnNames.push_back(columnName);
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
  // this reader have read all rows
  if (totalRowsRead >= totalRows) {
    return -1;
  }
  checkEndOfRowGroup();

  int64_t* buffersPtr = buffersPtr_;
  int64_t* nullsPtr = nullsPtr_;

  // allocate extra memory for filtered columns if needed
  if (filterExpression) {
    allocateFilterBuffers(batchSize);
  }

  if (aggExprs.size()) {
    allocateAggBuffers(batchSize);
  }

  int filterBufferCount = filterDataBuffers.size();
  int aggBufferCount = aggDataBuffers.size();
  if (filterBufferCount > 0 || aggBufferCount > 0) {
    ARROW_LOG(DEBUG) << "use extra filter buffers count: " << filterBufferCount;
    ARROW_LOG(DEBUG) << "use extra agg buffers count: " << aggBufferCount;

    buffersPtr =
        new int64_t[initRequiredColumnCount + filterBufferCount + aggBufferCount];
    nullsPtr = new int64_t[initRequiredColumnCount + filterBufferCount + aggBufferCount];

    for (int i = 0; i < initRequiredColumnCount; i++) {
      buffersPtr[i] = buffersPtr_[i];
      nullsPtr[i] = nullsPtr_[i];
    }

    for (int i = 0; i < filterBufferCount; i++) {
      buffersPtr[initRequiredColumnCount + i] = (int64_t)filterDataBuffers[i];
      nullsPtr[initRequiredColumnCount + i] = (int64_t)filterNullBuffers[i];
    }

    for (int i = 0; i < aggBufferCount; i++) {
      buffersPtr[initRequiredColumnCount + filterBufferCount + i] =
          (int64_t)aggDataBuffers[i];
      nullsPtr[initRequiredColumnCount + filterBufferCount + i] =
          (int64_t)aggNullBuffers[i];
    }
  }

  currentBatchSize = batchSize;
  int rowsToRead = std::min((int64_t)batchSize, totalRowsLoadedSoFar - totalRowsRead);
  int16_t* defLevel = new int16_t[rowsToRead];
  int16_t* repLevel = new int16_t[rowsToRead];
  uint8_t* nullBitMap = new uint8_t[rowsToRead];
  ARROW_LOG(DEBUG) << "will read " << rowsToRead << " rows";
  for (int i = 0; i < columnReaders.size(); i++) {
    int64_t levelsRead = 0, valuesRead = 0, nullCount = 0;
    int rows = 0;
    int tmpRows = 0;
    // ReadBatchSpaced API will return rows left in a data page
    while (rows < rowsToRead) {
      // TODO: refactor. it's ugly, but didn't find some better way.
      switch (fileMetaData->schema()->Column(requiredColumnIndex[i])->physical_type()) {
        case parquet::Type::BOOLEAN: {
          parquet::BoolReader* boolReader =
              static_cast<parquet::BoolReader*>(columnReaders[i].get());
          tmpRows = boolReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel, repLevel, (bool*)buffersPtr[i] + rows,
              (uint8_t*)nullBitMap, 0, &levelsRead, &valuesRead, &nullCount);
          break;
        }

        case parquet::Type::INT32: {
          parquet::Int32Reader* int32Reader =
              static_cast<parquet::Int32Reader*>(columnReaders[i].get());
          tmpRows = int32Reader->ReadBatchSpaced(
              rowsToRead - rows, defLevel, repLevel, (int32_t*)buffersPtr[i] + rows,
              (uint8_t*)nullBitMap, 0, &levelsRead, &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::INT64: {
          parquet::Int64Reader* int64Reader =
              static_cast<parquet::Int64Reader*>(columnReaders[i].get());
          tmpRows = int64Reader->ReadBatchSpaced(
              rowsToRead - rows, defLevel, repLevel, (int64_t*)buffersPtr[i] + rows,
              (uint8_t*)nullBitMap, 0, &levelsRead, &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::INT96: {
          parquet::Int96Reader* int96Reader =
              static_cast<parquet::Int96Reader*>(columnReaders[i].get());
          tmpRows = int96Reader->ReadBatchSpaced(rowsToRead - rows, defLevel, repLevel,
                                                 (parquet::Int96*)buffersPtr[i] + rows,
                                                 (uint8_t*)nullBitMap, 0, &levelsRead,
                                                 &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::FLOAT: {
          parquet::FloatReader* floatReader =
              static_cast<parquet::FloatReader*>(columnReaders[i].get());
          tmpRows = floatReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel, repLevel, (float*)buffersPtr[i] + rows,
              (uint8_t*)nullBitMap, 0, &levelsRead, &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::DOUBLE: {
          parquet::DoubleReader* doubleReader =
              static_cast<parquet::DoubleReader*>(columnReaders[i].get());
          tmpRows = doubleReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel, repLevel, (double*)buffersPtr[i] + rows,
              (uint8_t*)nullBitMap, 0, &levelsRead, &valuesRead, &nullCount);
          break;
        }
        case parquet::Type::BYTE_ARRAY: {
          parquet::ByteArrayReader* byteArrayReader =
              static_cast<parquet::ByteArrayReader*>(columnReaders[i].get());
          tmpRows = byteArrayReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel, repLevel,
              (parquet::ByteArray*)buffersPtr[i] + rows, (uint8_t*)nullBitMap, 0,
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
          parquet::FixedLenByteArrayReader* fixedLenByteArrayReader =
              static_cast<parquet::FixedLenByteArrayReader*>(columnReaders[i].get());
          tmpRows = fixedLenByteArrayReader->ReadBatchSpaced(
              rowsToRead - rows, defLevel, repLevel,
              (parquet::FixedLenByteArray*)buffersPtr[i] + rows, (uint8_t*)nullBitMap, 0,
              &levelsRead, &valuesRead, &nullCount);
          break;
        }
        default:
          ARROW_LOG(WARNING) << "Unsupported Type!";
          break;
      }
      convertBitMap(nullBitMap, (uint8_t*)nullsPtr[i] + rows, tmpRows);
      rows += tmpRows;
    }
    assert(rowsToRead == rows);
    ARROW_LOG(DEBUG) << "columnReader read rows: " << rows;
  }
  totalRowsRead += rowsToRead;
  ARROW_LOG(DEBUG) << "total rows read yet: " << totalRowsRead;

  int rowsRet = rowsToRead;
  if (filterExpression) {
    auto start = std::chrono::steady_clock::now();
    rowsRet =
        filterExpression->ExecuteWithParam(rowsToRead, buffersPtr, nullsPtr, nullptr);
    time += std::chrono::steady_clock::now() - start;
  }

  if (aggExprs.size()) {
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < aggExprs.size(); i++) {
      auto agg = aggExprs[i];
      if (typeid(*agg) == typeid(RootAggExpression)) {
        rowsRet = agg->ExecuteWithParam(rowsToRead, buffersPtr, nullsPtr, nullptr);
        auto result = std::dynamic_pointer_cast<RootAggExpression>(agg)->getResult();
        if (result.size() == 1) {
          aggResults[i].push_back(result[0]);
        } else {
          ARROW_LOG(DEBUG) << "Oops... why return " << result.size() << " results";
        }
      } else if (typeid(*agg) == typeid(AttributeReferenceExpression)) {
        // TODO
      }
    }
    time += std::chrono::steady_clock::now() - start;
  }

  delete[] defLevel;
  delete[] repLevel;
  delete[] nullBitMap;

  if (filterBufferCount > 0 || aggBufferCount > 0) {
    delete[] buffersPtr;
    delete[] nullsPtr;
  }

  ARROW_LOG(DEBUG) << "ret rows " << rowsRet;
  return rowsRet;
}

bool Reader::hasNext() { return columnReaders[0]->HasNext(); }

bool Reader::skipNextRowGroup() {
  if (totalRowGroupsRead == totalRowGroups) {
    return false;
  }
  currentRowGroup++;
  totalRowGroupsRead++;
  return true;
}

void Reader::close() {
  ARROW_LOG(INFO) << "Filter takes " << time.count() * 1000 << " ms.";

  ARROW_LOG(INFO) << "close reader.";
  parquetReader->Close();
  file->Close();
  for (auto ptr : extraByteArrayBuffers) {
    delete[] ptr;
  }
  freeFilterBuffers();

  if (plasmaCacheManager) {
    plasmaCacheManager->close();
    plasmaCacheManager = nullptr;
  }

  // delete options;
}

void Reader::checkEndOfRowGroup() {
  if (totalRowsRead != totalRowsLoadedSoFar) return;
  // if a splitFile contains rowGroup [2,5], currentRowGroup is 2
  // rowGroupReaders index starts from 0
  rowGroupReader = rowGroupReaders[currentRowGroup - firstRowGroupIndex];
  currentRowGroup++;
  totalRowGroupsRead++;

  // relase objects in previous row group
  if (plasmaCacheManager) {
    plasmaCacheManager->release();
  }

  for (int i = 0; i < requiredColumnIndex.size(); i++) {
    columnReaders[i] = rowGroupReader->Column(requiredColumnIndex[i]);
  }

  totalRowsLoadedSoFar += rowGroupReader->metadata()->num_rows();
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
  schema.erase(schema.begin() + initRequiredColumnCount, schema.end());
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
      schema.push_back(Schema(
          columnName, fileMetaData->schema()->Column(columnIndex)->physical_type()));
      columnReaders.resize(requiredColumnIndex.size());
    }
  }

  filterExpression->setSchema(schema);
  filterReset = true;
  initPlusFilterRequiredColumnCount = requiredColumnIndex.size();
}

int Reader::allocateFilterBuffers(int batchSize) {
  if (!filterReset && batchSize <= currentBatchSize) {
    return 0;
  }
  filterReset = false;

  // free current filter buffers
  freeFilterBuffers();

  // allocate new filter buffers
  int extraBufferNum = 0;
  for (int i = initRequiredColumnCount; i < initPlusFilterRequiredColumnCount; i++) {
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

  ARROW_LOG(INFO) << "create extra filter buffers count: " << extraBufferNum;
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
  if (!aggReset && batchSize <= currentBatchSize) {
    return 0;
  }
  aggReset = false;

  // free current agg buffers
  freeAggBuffers();

  // allocate new agg buffers
  int extraBufferNum = 0;
  for (int i = initPlusFilterRequiredColumnCount; i < requiredColumnIndex.size(); i++) {
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
  // do nothing now
  groupByExprs = JsonConvertor::parseToGroupByExpressions(aggStr);
  aggExprs = JsonConvertor::parseToAggExpressions(aggStr);
  for (auto result : aggResults) {
    result.clear();
  }
  aggResults.clear();

  // get column names from expression
  aggColumnNames.clear();
  for (auto agg : aggExprs) {
    setAggColumnNames(agg);
  }

  // reset required columns to initial size
  requiredColumnIndex.resize(initPlusFilterRequiredColumnCount);
  requiredColumnNames.resize(initPlusFilterRequiredColumnCount);
  schema.erase(schema.begin() + initPlusFilterRequiredColumnCount, schema.end());
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
      schema.push_back(Schema(
          columnName, fileMetaData->schema()->Column(columnIndex)->physical_type()));
      columnReaders.resize(requiredColumnIndex.size());
    }
  }

  for (auto agg : aggExprs) {
    agg->setSchema(schema);
    aggResults.push_back(std::vector<ApeDecimal128Ptr>());
  }

  aggReset = true;
}

void Reader::setPlasmaCacheEnabled(bool isEnabled) { plasmaCacheEnabled = isEnabled; }

void Reader::setPlasmaCacheRedis(std::string host, int port, std::string password) {
  auto options = std::make_shared<sw::redis::ConnectionOptions>();
  options->host = host;
  options->port = port;
  if (password.size() > 0) {
    options->password = password;
  }
  redisConnectionOptions = options;

  if (plasmaCacheManager != nullptr) {
    plasmaCacheManager->setCacheRedis(options);
  }
}

}  // namespace ape
