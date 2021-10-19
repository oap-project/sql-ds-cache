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


set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(Arrow REQUIRED)

# Download and unpack googletest at configure time
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

set(CMAKE_CXX_STANDARD 17)

set(ARROW_STATIC -Wl,-Bstatic; -Wl,--whole-archive; arrow_static; -Wl,--no-whole-archive)
set(THREAD -Wl,-Bdynamic;-Wl,--as-needed;Threads::Threads;-Wl,--no-as-needed)

add_executable(convertorTest convertorTest.cc)
target_link_libraries(convertorTest gtest_main parse ${ARROW_STATIC} ${THREAD})
add_test(NAME convertorTest COMMAND ${EXECUTABLE_OUTPUT_PATH}/convertorTest)

add_executable(decimalTest decimalTest.cc)
target_link_libraries(decimalTest gtest_main parquet_jni ${ARROW_STATIC} ${THREAD})
add_test(NAME decimalTest COMMAND ${EXECUTABLE_OUTPUT_PATH}/decimalTest)

add_executable(aggParserTest aggParserTest.cc)
target_link_libraries(aggParserTest gtest_main parquet_jni ${ARROW_STATIC} ${THREAD})
add_test(NAME aggParserTest COMMAND ${EXECUTABLE_OUTPUT_PATH}/aggParserTest)

add_executable(apeHashMapTest apeHashMapTest.cc)
target_link_libraries(apeHashMapTest gtest_main parquet_jni ${ARROW_STATIC} ${THREAD})
add_test(NAME apeHashMapTest COMMAND ${EXECUTABLE_OUTPUT_PATH}/apeHashMapTest)

# these test works well on my dev machine, but it will fail on ci env
# because it have hdfs dependency, so I will disable it. You can enable 
# it in your local machine to test.

add_executable(parquetHdfsTest parquetHdfsTest.cc)
target_link_libraries(parquetHdfsTest gtest_main parquet_jni ${ARROW_STATIC} ${THREAD})

add_executable(plasmaTest plasmaTest.cc)
target_link_libraries(plasmaTest gtest_main parquet_jni ${ARROW_STATIC} ${THREAD})

if(APE_CI)
  # Will not run these test
else()
  add_test(NAME parquetHdfsTest COMMAND ${EXECUTABLE_OUTPUT_PATH}/parquetHdfsTest)
  add_test(NAME plasmaTest COMMAND ${EXECUTABLE_OUTPUT_PATH}/plasmaTest)
endif()
