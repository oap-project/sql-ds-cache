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

find_package(nlohmann_json 3.2.0 REQUIRED)

find_package(JNI REQUIRED)
# add jni support
include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

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

set(PARSE_SRC utils/FilterExpression.cc utils/jsonConvertor.cc utils/type.cc)
set(PARSE_TEST utils/convertorTest.cc)

file(GLOB JNI_LIB_SRC
    com_intel_ape_ParquetReaderJNI.cc reader.cc)
# set(PARQUET_NATIVE_SRC reader.cc)

add_executable(ape ${APE_SRCS})
add_executable(parquet_test ${PARQUET_DEMO})
# add_library(parquet_native SHARED ${PARQUET_NATIVE_SRC})
add_library(parquet_jni SHARED ${JNI_LIB_SRC} ${PARSE_SRC})
add_library(parse SHARED ${PARSE_SRC})
add_executable(parse_test ${PARSE_SRC} ${PARSE_TEST})

add_compile_options("-I$ENV{JAVA_HOME}/include/")
add_compile_options("-I$ENV{JAVA_HOME}/include/linux")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3")

target_link_libraries(parquet_test ${ARROW_LIB} ${PARQUET_LIB})
# target_link_libraries(parquet_native ${ARROW_LIB} ${PARQUET_LIB} ${JNI} )
target_link_libraries(parquet_jni ${ARROW_LIB} ${PARQUET_LIB} ${JNI} nlohmann_json::nlohmann_json )
target_link_libraries(ape ${ARROW_LIB} ${PARQUET_LIB} parquet_jni)
target_link_libraries(parse nlohmann_json::nlohmann_json ${ARROW_LIB})
target_link_libraries(parse_test parse)