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

#include "QplFilter.h"
#include <iostream>

#include <util/status_handler.hpp>

#include <arrow/util/logging.h>

namespace ape {

namespace {

inline const uint32_t* unpack32(const uint32_t* in, int8_t* out) {
  uint32_t inl = *in;
  *out = (inl >> 0) & 1;
  out++;
  *out = (inl >> 1) & 1;
  out++;
  *out = (inl >> 2) & 1;
  out++;
  *out = (inl >> 3) & 1;
  out++;
  *out = (inl >> 4) & 1;
  out++;
  *out = (inl >> 5) & 1;
  out++;
  *out = (inl >> 6) & 1;
  out++;
  *out = (inl >> 7) & 1;
  out++;
  *out = (inl >> 8) & 1;
  out++;
  *out = (inl >> 9) & 1;
  out++;
  *out = (inl >> 10) & 1;
  out++;
  *out = (inl >> 11) & 1;
  out++;
  *out = (inl >> 12) & 1;
  out++;
  *out = (inl >> 13) & 1;
  out++;
  *out = (inl >> 14) & 1;
  out++;
  *out = (inl >> 15) & 1;
  out++;
  *out = (inl >> 16) & 1;
  out++;
  *out = (inl >> 17) & 1;
  out++;
  *out = (inl >> 18) & 1;
  out++;
  *out = (inl >> 19) & 1;
  out++;
  *out = (inl >> 20) & 1;
  out++;
  *out = (inl >> 21) & 1;
  out++;
  *out = (inl >> 22) & 1;
  out++;
  *out = (inl >> 23) & 1;
  out++;
  *out = (inl >> 24) & 1;
  out++;
  *out = (inl >> 25) & 1;
  out++;
  *out = (inl >> 26) & 1;
  out++;
  *out = (inl >> 27) & 1;
  out++;
  *out = (inl >> 28) & 1;
  out++;
  *out = (inl >> 29) & 1;
  out++;
  *out = (inl >> 30) & 1;
  out++;
  *out = (inl >> 31);
  ++in;
  out++;

  return in;
}

void unpack(const uint32_t* in, int8_t* out, int batch_size) {
  int i = 0;
  int num_unpacked = batch_size / 32 * 32;
  int num_loops = num_unpacked / 32;
  for (int i = 0; i < num_loops; ++i) in = unpack32(in, out + i * 32);
  i += num_unpacked;
  uint32_t left = *in;
  for (; i < batch_size; ++i) {
    out[i] = left & 1;
    left >>= 1;
  }
}

}  // namespace

int32_t QplFilter(qpl::comparators comparator, bool inclusive, uint32_t value,
                  const uint8_t* in, std::vector<int8_t>& out, int32_t batch_size) {
  uint32_t result_status = 0;
  std::cout<<"qplfilter\n";
  std::vector<uint8_t> bitmask(batch_size + 7 / 8);
  auto scan_operation = qpl::scan_operation::builder(comparator, value)
                            .input_vector_width(32)
                            .output_vector_width(1)
                            .parser<qpl::parsers::little_endian_packed_array>(batch_size)
                            .is_inclusive(inclusive)
                            .build();
  //std::cout<<"2\n";
  const auto scan_result = qpl::execute<qpl::hardware>(
      scan_operation, in, in + batch_size * sizeof(uint32_t), std::begin(bitmask),
      std::end(bitmask));
  //std::cout<<"3\n";

  scan_result.handle(
      [&bitmask, &out](uint32_t scan_size) -> void {
        const auto* indices = reinterpret_cast<const uint32_t*>(bitmask.data());
        unpack(indices, out.data(), scan_size);
      },
      [&result_status](uint32_t status_code) -> void {
        try {
          result_status = status_code;
          qpl::util::handle_status(result_status);
        } catch (qpl::exception& e) {
          ARROW_LOG(ERROR) << "Failed to submit scan job: " << e.what();
        } catch (...) {
          ARROW_LOG(ERROR) << "Failed to submit scan job: "
                           << "Unknown Exception";
        }
      });

  return result_status;
}

}  // namespace ape
