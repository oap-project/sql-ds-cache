#include <iostream>

#include <arrow/api.h>

#include "reader.h"

int main() {
  std::cout << "hello ape" << std::endl;

  std::shared_ptr<arrow::Table> table;

  Reader reader;
  // just test hdfs connection.
  std::string file_name =
      "/tpcds_10g/store_sales/"
      "part-00000-74feb3b4-1954-4be7-802d-a50912793bea-c000.snappy.parquet";
  reader.init(file_name, "sr585", 9000, "");
  int batchSize = 1024;

  long* buffers = new long[3];
  long* nulls = new long[3];

  for (int i = 0; i < 3; i++) {
    buffers[i] = (int64_t)(std::malloc(sizeof(long) * batchSize));
    nulls[i] = (int64_t)(std::malloc(sizeof(uint8_t) * batchSize));
    std::cout << "buffer addr is: " << buffers[i] << " null addr is " << nulls[i]
              << std::endl;
  }
  int rowsRead = reader.readBatch(batchSize, buffers, nulls);
  std::cout << "read rows: " << rowsRead << std::endl;

  long addr1 = buffers[0];
  long addr2 = buffers[1];
  long addr3 = buffers[2];
  int32_t* ptr1 = (int32_t*)addr1;
  int32_t* ptr2 = (int32_t*)addr2;
  int32_t* ptr3 = (int32_t*)addr3;
  for (int i = 0; i < 10; i++) {
    std::cout << ptr1[i] << "\t" << ptr2[i] << "\t" << ptr3[i] << std::endl;
  }

  long nullAddr1 = nulls[0];
  long nullAddr2 = nulls[1];
  long nullAddr3 = nulls[2];
  uint8_t* nullPtr1 = (uint8_t*)nullAddr1;
  uint8_t* nullPtr2 = (uint8_t*)nullAddr2;
  uint8_t* nullPtr3 = (uint8_t*)nullAddr3;
  // for (int i = 0; i < 10; i++) {
  //   std::cout << std::to_string(nullPtr1[i]) << "\t" << std::to_string(nullPtr2[i])
  //             << "\t" << std::to_string(nullPtr3[i]) << std::endl;
  // }

  reader.close();

  return 0;
}