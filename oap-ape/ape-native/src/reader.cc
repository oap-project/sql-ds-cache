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
#include <iostream>

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

  fileMetaData->schema();
  ARROW_LOG(INFO) << "init done";
}

void Reader::convertSchema(std::string requiredColumnName) {
  // std::string to std::vector<string> col_names
  // std::vector<string> to std::vector<int>
  // by call fileMetaData->schema()->ColumnIndex("col_name")

  // let's build a fake index vector for test.
  requiredColumnIndex = {1, 3, 5};
}

void Reader::readBatch(int batchSize) { return; }

bool Reader::hasNext() { return false; }

void Reader::skipNextRowGroup() { return; }

void Reader::close() {
  ARROW_LOG(INFO) << "close reader.";
  parquetReader->Close();
  file->Close();
  // delete options;
}