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

#include "reader.h"
#include <algorithm>

using namespace arrow::fs;

Reader::Reader() {}

void Reader::init(std::string fileName, std::string hdfsHost, int hdfsPort,
                  std::string requiredSchema) {
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
  ARROW_LOG(INFO) << "HadoopFileSystem Make succeed. ";

  std::shared_ptr<FileSystem> fs = std::make_shared<SubTreeFileSystem>("", *result);
  auto fileResult = fs->OpenInputFile(fileName);
  if (!fileResult.ok()) {
    ARROW_LOG(WARNING) << "Open hdfs file failed! err msg: "
                       << fileResult.status().ToString();
    exit(-1);
  }
  ARROW_LOG(INFO) << "Open hdfs file succeed. ";

  file = fileResult.ValueOrDie();

  parquet::ReaderProperties properties;
  parquetReader = parquet::ParquetFileReader::Open(file, properties, NULLPTR);
  fileMetaData = parquetReader->metadata();

  totalColumns = fileMetaData->num_columns();

  fileMetaData->schema();
  convertSchema("");

  getRequiredRowGroupId();
  currentRowGroup = *requiredRowGroupId.begin();

  totalRowGroups = requiredRowGroupId.size();
  rowGroupReaders.reserve(totalRowGroups);
  for (int i = 0; i < totalRowGroups; i++) {
    rowGroupReaders[i] = parquetReader->RowGroup(requiredRowGroupId[i]);
    totalRows += rowGroupReaders[i]->metadata()->num_rows();
    ARROW_LOG(DEBUG) << "this rg have rows: "
                     << rowGroupReaders[i]->metadata()->num_rows();
  }
  columnReaders.resize(requiredColumnIndex.size());

  ARROW_LOG(INFO) << "init done, totalRows " << totalRows;
}

// TODO: impl this method for file plit. For now it will return all row group ids
void Reader::getRequiredRowGroupId() {
  int totalRowGroupsInFile = fileMetaData->num_row_groups();
  requiredRowGroupId = std::vector<int>(totalRowGroupsInFile);
  std::iota(requiredRowGroupId.begin(), requiredRowGroupId.end(), 0);
}

void Reader::convertSchema(std::string requiredColumnName) {
  // std::string to std::vector<string> col_names
  // std::vector<string> to std::vector<int>
  // by call fileMetaData->schema()->ColumnIndex("col_name")

  // let's build a fake index vector for test.
  requiredColumnIndex = {1, 3, 5};
}

int Reader::readBatch(int batchSize, long* buffersPtr, long* nullsPtr) {
  ARROW_LOG(INFO) << "read batch size: " << batchSize;
  // this reader have read all rows
  if (totalRowsRead >= totalRows) {
    return -1;
  }
  checkEndOfRowGroup();

  int rowsToRead = std::min((int64_t)batchSize, totalRowsLoadedSoFar - totalRowsRead);
  int16_t* defLevel = new int16_t[rowsToRead];
  int16_t* repLevel = new int16_t[rowsToRead];
  ARROW_LOG(INFO) << "will read " << rowsToRead << " rows";
  for (int i = 0; i < columnReaders.size(); i++) {
    int64_t levels_read = 0, values_read = 0, null_count = 0;
    int rows = 0;
    // TODO: refactor. it's ugly, but didn't find some better way.
    switch (fileMetaData->schema()->Column(requiredColumnIndex[i])->physical_type()) {
      case parquet::Type::BOOLEAN: {
        parquet::BoolReader* bool_reader =
            static_cast<parquet::BoolReader*>(columnReaders[i].get());
        rows = bool_reader->ReadBatchSpaced(rowsToRead, defLevel, repLevel,
                                            (bool*)buffersPtr[i], (uint8_t*)nullsPtr[i],
                                            0, &levels_read, &values_read, &null_count);
        break;
      }

      case parquet::Type::INT32: {
        parquet::Int32Reader* int32_reader =
            static_cast<parquet::Int32Reader*>(columnReaders[i].get());
        rows = int32_reader->ReadBatchSpaced(
            rowsToRead, defLevel, repLevel, (int32_t*)buffersPtr[i], (uint8_t*)nullsPtr[i], 0,
            &levels_read, &values_read, &null_count);
        break;
      }
      case parquet::Type::INT64: {
        parquet::Int64Reader* int64_reader =
            static_cast<parquet::Int64Reader*>(columnReaders[i].get());
        rows = int64_reader->ReadBatchSpaced(
            rowsToRead, defLevel, repLevel, (int64_t*)buffersPtr[i], (uint8_t*)nullsPtr[i], 0,
            &levels_read, &values_read, &null_count);
        break;
      }
      case parquet::Type::INT96: {
        parquet::Int96Reader* int96_reader =
            static_cast<parquet::Int96Reader*>(columnReaders[i].get());
        rows = int96_reader->ReadBatchSpaced(
            rowsToRead, defLevel, repLevel, (parquet::Int96*)buffersPtr[i],
            (uint8_t*)nullsPtr[i], 0, &levels_read, &values_read, &null_count);
        break;
      }
      case parquet::Type::FLOAT: {
        parquet::FloatReader* float_reader =
            static_cast<parquet::FloatReader*>(columnReaders[i].get());
        rows = float_reader->ReadBatchSpaced(rowsToRead, defLevel, repLevel,
                                             (float*)buffersPtr[i], (uint8_t*)nullsPtr[i],
                                             0, &levels_read, &values_read, &null_count);
        break;
      }
      case parquet::Type::DOUBLE: {
        parquet::DoubleReader* double_reader =
            static_cast<parquet::DoubleReader*>(columnReaders[i].get());
        rows = double_reader->ReadBatchSpaced(
            rowsToRead, defLevel, repLevel, (double*)buffersPtr[i], (uint8_t*)nullsPtr[i], 0,
            &levels_read, &values_read, &null_count);
        break;
      }
      default:
        ARROW_LOG(WARNING) << "Unsupported Type!";
        break;
    }

    assert(rowsToRead == rows);
  }

  delete defLevel;
  delete repLevel;

  return rowsToRead;
}

bool Reader::hasNext() { return false; }

void Reader::skipNextRowGroup() { return; }

void Reader::close() {
  ARROW_LOG(INFO) << "close reader.";
  parquetReader->Close();
  file->Close();
  // delete options;
}

void Reader::checkEndOfRowGroup() {
  if (totalRowsRead != totalRowsLoadedSoFar) return;
  rowGroupReader = rowGroupReaders[currentRowGroup];
  currentRowGroup++;

  for (int i = 0; i < requiredColumnIndex.size(); i++) {
    // TODO: need to convert to type reader
    columnReaders[i] = rowGroupReader->Column(requiredColumnIndex[i]);
  }

  totalRowsLoadedSoFar += rowGroupReader->metadata()->num_rows();
}