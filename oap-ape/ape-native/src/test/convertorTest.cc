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

#include <string>
#include <gtest/gtest.h>

#include "src/utils/FilterExpression.h"
#include "src/utils/JsonConvertor.h"

TEST(JasonConvertorTest, ConvertValidJson) {
  std::string s =
      "{\"FilterTypeName\":\"and\",\"LeftNode\":{\"FilterTypeName\":\"not\",\"Child\":{"
      "\"FilterTypeName\":\"or\",\"LeftNode\":{\"FilterTypeName\":\"eq\",\"ColumnName\":"
      "\"a.b.c\",\"ColumnType\":\"Integer\",\"Value\":\"7\"},\"RightNode\":{"
      "\"FilterTypeName\":\"noteq\",\"ColumnName\":\"a.b.c\",\"ColumnType\":\""
      "Integer\",\"Value\":\"17\"}}},\"RightNode\":{\"FilterTypeName\":\"gt\","
      "\"ColumnName\":\"x.y.z\",\"ColumnType\":\"Double\",\"Value\":\"100.123\"}}";

  auto ex = ape::JsonConvertor::parseToFilterExpression(s);

  EXPECT_TRUE(ex != nullptr);
  EXPECT_TRUE(ex->getType() == "and");

  auto left1 = std::dynamic_pointer_cast<ape::BinaryFilterExpression>(ex)->getLeftChild();
  auto right1 =
      std::dynamic_pointer_cast<ape::BinaryFilterExpression>(ex)->getRightChild();
  EXPECT_TRUE(left1->getType() == "not");
  EXPECT_TRUE(right1->getType() == "gt");

  auto not2 = std::dynamic_pointer_cast<ape::NotFilterExpression>(left1)->getChild();
  EXPECT_TRUE(not2->getType() == "or");

  auto left3 =
      std::dynamic_pointer_cast<ape::BinaryFilterExpression>(not2)->getLeftChild();
  auto right3 =
      std::dynamic_pointer_cast<ape::BinaryFilterExpression>(not2)->getRightChild();
  EXPECT_TRUE(left3->getType() == "eq");
  EXPECT_TRUE(right3->getType() == "noteq");
}
