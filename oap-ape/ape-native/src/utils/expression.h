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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "src/utils/type.h"

namespace ape {

class Expression {
 private:
 protected:
  std::string type;
  std::vector<Schema> schema;

 public:
  std::string getType() { return type; }
  virtual void Execute() = 0;
  virtual int ExecuteWithParam(int32_t batchSize, int64_t* dataBuffers,
                               int64_t* nullBuffers, char* outBuffers) = 0;
  Expression() {}
  virtual ~Expression() = default;
  virtual void setSchema(std::vector<Schema> schema_) {}
};
}  // namespace ape
