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

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>

using namespace arrow::fs;

class Reader {
 public:
  Reader();

  void init(std::string fileName, std::string hdfsHost, int hdfsPort,
            std::string requiredSchema);

  void readBatch(int batchSize);

  bool hasNext();

  void skipNextRowGroup();

  void close();

 private:
  void convertSchema(std::string requiredColumnName);

  HdfsOptions *options;
  std::shared_ptr<FileSystem> fs;
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::unique_ptr<parquet::ParquetFileReader> parquetReader;
  std::shared_ptr<parquet::FileMetaData> fileMetaData;
  std::vector<int> requiredColumnIndex;

  int totalRowGroups;
  int totalColumns;
  int currentRowGroup = 0;
};