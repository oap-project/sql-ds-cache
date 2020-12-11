# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.12)

project(APE)

set(CMAKE_CXX_STANDARD 11)

find_library(ARROW_LIB arrow)
find_library(PARQUET_LIB parquet)

find_package(JNI REQUIRED)
# add jni support
include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})

if(NOT ARROW_LIB)
  message(FATAL_ERROR "Arrow library not found")
endif()

if(NOT PARQUET_LIB)
  message(FATAL_ERROR "Parquet library not found")
endif()

set(APE_SRCS
    main.cc)

set(PARQUET_DEMO
    parquet-hdfs-test.cc)

add_executable(ape ${APE_SRCS})
add_executable(parquet_test ${PARQUET_DEMO})

add_compile_options("-I$ENV{JAVA_HOME}/include/")
add_compile_options("-I$ENV{JAVA_HOME}/include/linux")

target_link_libraries(ape ${ARROW_LIB})
target_link_libraries(parquet_test ${ARROW_LIB} ${PARQUET_LIB})