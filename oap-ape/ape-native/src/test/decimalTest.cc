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

#include <vector>
#include <gtest/gtest.h>

#include <arrow/util/decimal.h>

#include "src/utils/ApeDecimal.h"
#include "src/utils/DecimalUtil.h"

using namespace ape;

class DecimalTest : public ::testing::Test {
 public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.

  void SetUp() { buffer = std::vector<uint8_t>(16); }

 protected:
  int32_t scale;
  int32_t precision;
  arrow::Decimal128 decimal;
  std::vector<uint8_t> buffer;
};

TEST_F(DecimalTest, Multiply) {
  // retsult from JAVA code
  // BigDecimal("1234567890.12345678").unscaledValue().toByteArray()
  std::vector<uint8_t> b1{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                          0x01, 0xb6, 0x9b, 0x4b, 0xa6, 0x30, 0xf3, 0x4e};
  arrow::Decimal128::FromString("1234567890.12345678", &decimal, &precision, &scale);
  ApeDecimal128 d1(decimal.high_bits(), decimal.low_bits(), precision, scale);
  d1.toBytes(buffer.data());
  EXPECT_TRUE(b1 == buffer);

  // retsult from JAVA code
  // BigDecimal("-9876543210.1234").unscaledValue().toByteArray()
  std::vector<uint8_t> b2{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                          0xff, 0xff, 0xa6, 0x2c, 0x61, 0x80, 0xe6, 0x8e};
  arrow::Decimal128::FromString("-9876543210.1234", &decimal, &precision, &scale);
  ApeDecimal128 d2(decimal.high_bits(), decimal.low_bits(), precision, scale);
  d2.toBytes(buffer.data());
  EXPECT_TRUE(b2 == buffer);

  // retsult from JAVA code
  // BigDecimal("-12193263112635198799.878698366652").unscaledValue().toByteArray()
  std::vector<uint8_t> b3{0xff, 0xff, 0xff, 0x66, 0x19, 0x71, 0x26, 0x43,
                          0xad, 0x7c, 0x25, 0x58, 0x5e, 0xbf, 0x09, 0x44};
  arrow::BasicDecimal128 d = DecimalUtil::Multiply(d1, d2, 18 + 14 + 1, 12);
  ApeDecimal128 d3(d, 38, 12);
  d3.toBytes(buffer.data());
  assert(b3 == buffer);
}
