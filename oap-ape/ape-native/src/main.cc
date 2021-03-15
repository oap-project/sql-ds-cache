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

#include <iostream>

#include <arrow/api.h>

#include "src/reader.h"

int main() {
  std::cout << "hello ape" << std::endl;

  std::shared_ptr<arrow::Table> table;

  ape::Reader reader;
  // just test hdfs connection.
  std::string file_name =
      "/tpcds_10g/catalog_sales/"
      "part-00000-be30656b-ae02-4015-a9be-b9f62c2d9159-c000.snappy.parquet";
  std::string schema =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"cs_sold_date_sk\",\"type\":"
      "\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_bill_cdemo_sk\","
      "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_item_sk\","
      "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_promo_sk\","
      "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_quantity\","
      "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_list_"
      "price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":"
      "\"cs_sales_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{"
      "\"name\":\"cs_coupon_amt\",\"type\":\"decimal(7,2)\",\"nullable\":true,"
      "\"metadata\":{}}]}";
  reader.init(file_name, "sr585", 9000, schema, 0, 1);
  int32_t batchSize = 1024;

  int32_t columnNum = 8;

  int64_t* buffers = new int64_t[columnNum];
  int64_t* nulls = new int64_t[columnNum];

  for (int32_t i = 0; i < columnNum; i++) {
    // TODO: some columns may be not long type, but doesn't matter for test
    buffers[i] = (int64_t)(std::malloc(sizeof(int64_t) * batchSize));
    nulls[i] = (int64_t)(std::malloc(sizeof(uint8_t) * batchSize));
    // std::cout << "buffer addr is: " << buffers[i] << " null addr is " << nulls[i]
    // << std::endl;
  }
  int32_t rowsRead = reader.readBatch(batchSize, buffers, nulls);
  std::cout << "read rows: " << rowsRead << std::endl;

  int64_t addr1 = buffers[0];
  int64_t addr2 = buffers[1];
  int64_t addr3 = buffers[2];
  int32_t* ptr1 = (int32_t*)addr1;
  int32_t* ptr2 = (int32_t*)addr2;
  int32_t* ptr3 = (int32_t*)addr3;
  for (int32_t i = 0; i < 10; i++) {
    std::cout << ptr1[i] << "\t" << ptr2[i] << "\t" << ptr3[i] << std::endl;
  }

  for (int32_t i = 0; i < columnNum; i++) {
    std::free((char*)buffers[i]);
    std::free((char*)nulls[i]);
  }

  delete buffers;
  delete nulls;

  reader.close();

  return 0;
}
