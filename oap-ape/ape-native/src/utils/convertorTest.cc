
#include <string>

#include "FilterExpression.h"
#include "jsonConvertor.h"

int main() {
  std::string s =
      "{\"FilterTypeName\":\"and\",\"LeftNode\":{\"FilterTypeName\":\"not\",\"child\":{"
      "\"FilterTypeName\":\"or\",\"LeftNode\":{\"FilterTypeName\":\"eq\",\"ColumnName\":"
      "\"a.b.c\",\"ColumnType\":\"Integer\",\"Value\":\"7\"},\"RightNode\":{"
      "\"FilterTypeName\":\"noteq\",\"ColumnName\":\"a.b.c\",\"ColumnType\":\""
      "Integer\",\"Value\":\"17\"}}},\"RightNode\":{\"FilterTypeName\":\"gt\","
      "\"ColumnName\":\"x.y.z\",\"ColumnType\":\"Double\",\"Value\":\"100.123\"}}";

  auto ex = ape::JsonConvertor::parseToFilterExpression(s);

  ex->Execute();

  return 0;
}