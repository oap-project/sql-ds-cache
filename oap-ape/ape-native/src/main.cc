#include <iostream>

#include <arrow/api.h>

#include "reader.h"

int main() {
  std::cout << "hello ape" << std::endl;

  std::shared_ptr<arrow::Table> table;

  Reader reader;
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
  reader.init(file_name, "sr585", 9000, schema, 0 ,1);
  int batchSize = 1024;

  int columnNum = 8;

  long* buffers = new long[columnNum];
  long* nulls = new long[columnNum];

  for (int i = 0; i < columnNum; i++) {
    // TODO: some columns may be not long type, but doesn't matter for test
    buffers[i] = (int64_t)(std::malloc(sizeof(long) * batchSize));
    nulls[i] = (int64_t)(std::malloc(sizeof(uint8_t) * batchSize));
    // std::cout << "buffer addr is: " << buffers[i] << " null addr is " << nulls[i]
    // << std::endl;
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

  for (int i = 0; i < columnNum; i++) {
    std::free((char*)buffers[i]);
    std::free((char*)nulls[i]);
  }

  delete buffers;
  delete nulls;

  reader.close();

  return 0;
}
