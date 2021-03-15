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

#include <stdlib.h>

#include <iostream>
#include <memory>

#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <parquet/api/reader.h>

int main() {
  arrow::fs::HdfsOptions options_;

  std::string hdfs_host = "sr585";
  int hdfs_port = 9000;
  // std::string hdfs_user = "kunshang";

  options_.ConfigureEndPoint(hdfs_host, hdfs_port);
  // options_.ConfigureUser(hdfs_user);

  auto result = arrow::fs::HadoopFileSystem::Make(options_);
  if (!result.ok()) {
    std::cout << "HadoopFileSystem Make failed" << std::endl;
    return -1;
  }
  std::cout << "connect hdfs success" << std::endl;
  std::shared_ptr<arrow::fs::FileSystem> fs_ =
      std::make_shared<arrow::fs::SubTreeFileSystem>("", *result);

  std::string file_name =
      "/tpcds_10g/store_sales/"
      "part-00000-74feb3b4-1954-4be7-802d-a50912793bea-c000.snappy.parquet";

  auto file_result = fs_->OpenInputFile(file_name);
  if (!file_result.ok()) {
    std::cout << "Open hdfs file failed" << std::endl;
    return -1;
  }

  std::shared_ptr<arrow::io::RandomAccessFile> file = file_result.ValueOrDie();
  std::cout << "file size is " << file->GetSize().ValueOrDie() << std::endl;

  parquet::ReaderProperties properties;
  // std::shared_ptr<parquet::FileMetaData> metadata;
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::Open(file, properties, NULLPTR);

  // Get the File MetaData
  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

  // Get the number of RowGroups
  int num_row_groups = file_metadata->num_row_groups();
  std::cout << "this file have " << num_row_groups << " row groups" << std::endl;

  // Get the number of Columns
  int num_columns = file_metadata->num_columns();
  std::cout << "this file have " << num_columns << " columns" << std::endl;

  // Get schema
  std::cout << "schema is: " << std::endl
            << file_metadata->schema()->ToString() << std::endl;

  int index = file_metadata->schema()->ColumnIndex("ss_item_sk");
  std::cout << "ss_item_sk index is " << index << std::endl;
  // now we know index is 2
  auto col = file_metadata->schema()->Column(index);
  std::cout << "name " << col->name() << std::endl
            << "path" << col->path() << std::endl
            << "toString " << col->ToString() << std::endl;

  std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(0);
  std::shared_ptr<parquet::ColumnReader> column_reader;
  column_reader = row_group_reader->Column(1);
  parquet::Int32Reader* int32_reader =
      static_cast<parquet::Int32Reader*>(column_reader.get());
  int batch_size = 10000;
  int32_t* values_rb = (int32_t*)std::malloc(batch_size * sizeof(int32_t));
  int64_t values_read = 0;
  int64_t rows_read = 0;
  int16_t definition_level = 0;
  int16_t repetition_level = 0;

  // ReadBatch API will skip null values
  std::cout << std::endl << "test ReadBatch API" << std::endl;
  rows_read =
      int32_reader->ReadBatch(batch_size, NULLPTR, NULLPTR, values_rb, &values_read);
  // rows_read = int32_reader->ReadBatch(batch_size, &definition_level, &repetition_level,
  // values, &values_read);

  std::cout << "rows_read: " << rows_read << std::endl
            << "values_read: " << values_read << std::endl;
  // << "def level: " << definition_level << std::endl
  // << "rep level: " << repetition_level << std::endl;

  // ReadBatchSpaced will record a null bitmap.
  std::cout << std::endl << "test ReadBatchSpaced API" << std::endl;
  int32_t* values_rbs = (int32_t*)std::malloc(batch_size * sizeof(int32_t));
  int16_t* def_levels = (int16_t*)std::malloc(batch_size * sizeof(int16_t));
  int16_t* rep_levels = (int16_t*)std::malloc(batch_size * sizeof(int16_t));
  uint8_t* valid_bits = (uint8_t*)std::malloc(batch_size * sizeof(uint8_t));
  int64_t levels_read = 0;
  int64_t null_count = 0;

  // int32_reader->Skip(-1 * batch_size);  // seems there is no API like seek()
  rows_read = int32_reader->ReadBatchSpaced(batch_size, def_levels, rep_levels,
                                            values_rbs, valid_bits, 0, &levels_read,
                                            &values_read, &null_count);
  std::cout << "batch size is: " << batch_size << std::endl
            << "levels_read: " << levels_read << std::endl
            << "values read: " << values_read << std::endl
            << "null count: " << null_count << std::endl;

  // cannot comapre these two buffers.
  // for (int i = 0; i < 100; i++) {
  //   std::cout << values_rb[i] << "\t" << values_rbs[i] << std::endl;
  // }
  // std::cout << std::endl;

  std::free(values_rb);
  std::free(values_rbs);
  std::free(def_levels);
  std::free(rep_levels);
  std::free(valid_bits);

  return 0;
}
