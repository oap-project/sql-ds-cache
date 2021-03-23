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

#include "src/utils/Type.h"

namespace ape {

class Expression {
 private:
 protected:
  std::string type;
  std::shared_ptr<std::vector<Schema>> schema;

 public:
  std::string getType() { return type; }
  virtual void Execute() = 0;
  virtual int ExecuteWithParam(int32_t batchSize, const std::vector<int64_t>& dataBuffers,
                               const std::vector<int64_t>& nullBuffers,
                               std::vector<int8_t>& outBuffers) = 0;
  Expression() {}
  virtual ~Expression() = default;
  // both filterExpression and aggExpression will use it, so make it a pointer.
  virtual void setSchema(std::shared_ptr<std::vector<Schema>> schema_) {}
};
}  // namespace ape
