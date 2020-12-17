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
  reader.close();

  return 0;
}