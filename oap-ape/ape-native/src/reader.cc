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

#include "reader.h"

using namespace arrow::fs;
namespace ape {

Reader::Reader() {}

void Reader::init(std::string fileName, std::string hdfsHost, int hdfsPort,
                  std::string requiredSchema, long splitStart, long splitSize) {
  options = new HdfsOptions();
  ARROW_LOG(DEBUG) << "hdfsHost " << hdfsHost << " port " << hdfsPort;

  options->ConfigureEndPoint(hdfsHost, hdfsPort);
  // todo: if we delete `options`, it will core dump, seems like free twice.
  auto result = HadoopFileSystem::Make(*options);
  if (!result.ok()) {
    ARROW_LOG(WARNING) << "HadoopFileSystem Make failed! err msg:"
                       << result.status().ToString();
    exit(-1);
  }
  ARROW_LOG(DEBUG) << "HadoopFileSystem Make succeed. ";

  std::shared_ptr<FileSystem> fs = std::make_shared<SubTreeFileSystem>("", *result);
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

  fileMetaData = parquetReader->metadata();
  // getRequiredRowGroup(splitStart, splitSize, fileMetaData);
  totalColumns = fileMetaData->num_columns();

  ARROW_LOG(DEBUG) << "schema is " << fileMetaData->schema()->ToString();
  convertSchema(requiredSchema);

  // currentRowGroup = firstRowGroupIndex;
  // totalRowGroups = requiredRowGroupSize;

  getRequiredRowGroupId();
  currentRowGroup = *requiredRowGroupId.begin();
  totalRowGroups = requiredRowGroupId.size();

  rowGroupReaders.resize(totalRowGroups);
  for (int i = 0; i < totalRowGroups; i++) {
    rowGroupReaders[i] = parquetReader->RowGroup(firstRowGroupIndex + i);
    totalRows += rowGroupReaders[i]->metadata()->num_rows();
    ARROW_LOG(DEBUG) << "this rg have rows: "
                     << rowGroupReaders[i]->metadata()->num_rows();
  }
  columnReaders.resize(requiredColumnIndex.size());

  ARROW_LOG(INFO) << "init done, totalRows " << totalRows;
}

// a quick fix for parquet split, return all row groups
void Reader::getRequiredRowGroupId() {
  int totalRowGroupsInFile = fileMetaData->num_row_groups();
  requiredRowGroupId = std::vector<int>(totalRowGroupsInFile);
  std::iota(requiredRowGroupId.begin(), requiredRowGroupId.end(), 0);
}

// void Reader::getRequiredRowGroup(long splitStart, long splitSize,
//                                  std::shared_ptr<parquet::FileMetaData> fileMetaData) {
//   bool flag = false;
//   long currentOffSet = 0;
//   int PARQUET_MAGIC_NUMBER = 4;
//   currentOffSet += PARQUET_MAGIC_NUMBER;
//   int index = 0;
//   for (int i = 0; i < fileMetaData->num_row_groups(); i++) {
//     ARROW_LOG(DEBUG) << "rowgroup size " << i << " : "
//                      << parquetReader->RowGroup(i)->metadata()->total_byte_size();
//     if (splitStart <= currentOffSet && splitStart + splitSize >= currentOffSet) {
//       this->requiredRowGroupSize++;
//       if (flag == false) {
//         flag = true;
//         this->firstRowGroupIndex = index;
//       }
//     }

//     index++;
//     currentOffSet += parquetReader->RowGroup(i)->metadata()->total_byte_size();
//   }

//   ARROW_LOG(INFO) << "This splitStart is  " << splitStart << " splitSize is " <<
//   splitSize
//                   << " firstRowGroupIndex is " << this->firstRowGroupIndex
//                   << " requiredRowGroupSize is " << this->requiredRowGroupSize;
// }

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

int Reader::readBatch(int batchSize, long* buffersPtr, long* nullsPtr) {
  // this reader have read all rows
  if (totalRowsRead >= totalRows) {
    return -1;
  }
  checkEndOfRowGroup();

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
              (parquet::ByteArray*)buffersPtr[i] + rows, (uint8_t*)nullBitMap + rows, 0,
              &levelsRead, &valuesRead, &nullCount);
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

  delete[] defLevel;
  delete[] repLevel;
  delete[] nullBitMap;

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
  // delete options;
}

void Reader::checkEndOfRowGroup() {
  if (totalRowsRead != totalRowsLoadedSoFar) return;
  // if a splitFile contains rowGroup [2,5], currentRowGroup is 2
  // rowGroupReaders index starts from 0
  rowGroupReader = rowGroupReaders[currentRowGroup - firstRowGroupIndex];
  currentRowGroup++;
  totalRowGroupsRead++;
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
  filterExpression->setSchema(schema);
}

}  // namespace ape
