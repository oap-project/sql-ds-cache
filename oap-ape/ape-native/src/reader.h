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
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>

#include "utils/FilterExpression.h"
#include "utils/jsonConvertor.h"
#include "utils/type.h"

using namespace arrow::fs;

namespace ape {
class Reader {
 public:
  Reader();

  void init(std::string fileName, std::string hdfsHost, int hdfsPort,
            std::string requiredSchema, int firstRowGroup, int rowGroupToRead);

  int readBatch(int batchSize, long* buffersPtr, long* nullsPtr);

  bool hasNext();

  bool skipNextRowGroup();

  void close();

  void setFilter(std::string filterJsonStr);

 private:
  void convertSchema(std::string requiredColumnName);

  void checkEndOfRowGroup();

  void setFilterColumnNames(std::shared_ptr<Expression> filter);
  int allocateFilterBuffers(int batchSize);
  void freeFilterBuffers();

  HdfsOptions* options;
  std::shared_ptr<FileSystem> fs;
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::unique_ptr<parquet::ParquetFileReader> parquetReader;
  std::shared_ptr<parquet::FileMetaData> fileMetaData;

  std::vector<std::shared_ptr<parquet::RowGroupReader>> rowGroupReaders;
  std::shared_ptr<parquet::RowGroupReader> rowGroupReader;

  std::vector<int> requiredColumnIndex;
  std::vector<std::string> requiredColumnNames;
  std::vector<Schema> schema;
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
  std::chrono::duration<double> time;

  std::vector<char*> extraByteArrayBuffers;

  bool filterReset = false;
  int currentBatchSize = 0;
  int initRequiredColumnCount = 0;
  std::vector<std::string> filterColumnNames;
  std::vector<char*> filterDataBuffers;
  std::vector<char*> filterNullBuffers;

};
}  // namespace ape
