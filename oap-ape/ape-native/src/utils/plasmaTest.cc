#include <iostream>

#include "PlasmaCacheManager.h"

int main() {
  auto file =
      "hdfs://sr490:9000/tpch_1t_snappy/lineitem/"
      "part-00036-01f81a98-e2b0-4bd4-9134-62fd522bc891-c000.snappy.parquet";

  ape::PlasmaCacheManager cacheManager(file);
  auto range = arrow::io::ReadRange();
  range.offset = 587558587;
  range.length = 19723749;

  auto c = cacheManager.containsFileRange(range);

  std::cout << "contains: " << c << std::endl;

  auto g = cacheManager.getFileRange(range);
  std::cout << "is null: " << (g == nullptr) << std::endl;
}
