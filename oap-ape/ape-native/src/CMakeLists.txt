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

set(CMAKE_CXX_STANDARD 17)

find_library(ARROW_LIB arrow)
find_library(PARQUET_LIB parquet)

find_package(nlohmann_json 3.2.0 REQUIRED)

find_library(PLASMA_LIB plasma)
find_package(OpenSSL REQUIRED)

find_package(JNI REQUIRED)
# add jni support
include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

# redis
find_library(HIREDIS_LIB hiredis)
find_library(REDIS_PLUS_PLUS_LIB redis++)

if(NOT ARROW_LIB)
  message(FATAL_ERROR "Arrow library not found")
endif()

if(NOT PARQUET_LIB)
  message(FATAL_ERROR "Parquet library not found")
endif()

if(NOT PLASMA_LIB)
  message(FATAL_ERROR "Plasma library not found")
endif()

if(NOT HIREDIS_LIB)
  message(FATAL_ERROR "hiredis library not found")
endif()

if(NOT REDIS_PLUS_PLUS_LIB)
  message(FATAL_ERROR "redis++ library not found")
endif()

set(PARSE_SRC utils/FilterExpression.cc utils/JsonConvertor.cc utils/Type.cc utils/UnaryFilter.cc)
set(DECIMAL_SRC utils/DecimalUtil.cc utils/DecimalConvertor.cc)
set(AGG_SRC utils/AggExpression.cc)

file(GLOB JNI_LIB_SRC
    com_intel_ape_ParquetReaderJNI.cc reader.cc)
set(CACHE_SRC utils/PlasmaCacheManager.cc)

add_library(parquet_jni SHARED ${JNI_LIB_SRC} ${PARSE_SRC} ${CACHE_SRC} ${DECIMAL_SRC} ${AGG_SRC})
add_library(parse SHARED ${PARSE_SRC} ${DECIMAL_SRC} ${AGG_SRC})

add_compile_options("-I$ENV{JAVA_HOME}/include/")
add_compile_options("-I$ENV{JAVA_HOME}/include/linux")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3")

INSTALL(TARGETS parquet_jni LIBRARY DESTINATION lib)

LINK_DIRECTORIES(/lib /lib64 /usr/local/lib /usr )

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(Arrow REQUIRED)

target_link_libraries(parquet_jni
    -Wl,-Bstatic
    -Wl,--whole-archive
    -Wl,--start-group
    arrow_static
    libhiredis.a
    libredis++.a
    libplasma.a
    libparquet.a
    -Wl,--end-group
    -Wl,--no-whole-archive
    -Wl,-Bdynamic
    -Wl,--as-needed
    ${JNI} nlohmann_json::nlohmann_json OpenSSL::SSL Threads::Threads
    -Wl,--no-as-needed)

#target_link_libraries(parquet_jni ${ARROW_LIB} ${PARQUET_LIB} ${PLASMA_LIB} ${JNI} nlohmann_json::nlohmann_json OpenSSL::SSL ${HIREDIS_LIB} ${REDIS_PLUS_PLUS_LIB})
target_link_libraries(parse
    -Wl,-Bstatic
    -Wl,--whole-archive
    arrow_static
    -Wl,--no-whole-archive
    -Wl,-Bdynamic
    -Wl,--as-needed
    Threads::Threads nlohmann_json::nlohmann_json
    -Wl,--no-as-needed)
