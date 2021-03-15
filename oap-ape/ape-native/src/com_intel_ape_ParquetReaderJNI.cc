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

#include "src/com_intel_ape_ParquetReaderJNI.h"
#include "src/reader.h"

JNIEXPORT jlong JNICALL Java_com_intel_ape_ParquetReaderJNI_init(
    JNIEnv* env, jclass cls, jstring fileName, jstring hdfsHost, jint hdfsPort,
    jstring requiredSchema, jint firstRowGroup, jint rowGroupToRead,
    jboolean plasmaCacheEnabled) {
  int i = 0;
  ape::Reader* reader = new ape::Reader();

  reader->setPlasmaCacheEnabled(plasmaCacheEnabled);

  std::string schema_ = env->GetStringUTFChars(requiredSchema, nullptr);
  std::string fileName_ = env->GetStringUTFChars(fileName, nullptr);
  std::string hdfsHost_ = env->GetStringUTFChars(hdfsHost, nullptr);

  reader->init(fileName_, hdfsHost_, hdfsPort, schema_, firstRowGroup, rowGroupToRead);
  return reinterpret_cast<int64_t>(reader);
}

JNIEXPORT jboolean JNICALL Java_com_intel_ape_ParquetReaderJNI_hasNext(JNIEnv* env,
                                                                       jclass cls,
                                                                       jlong readerPtr) {
  ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
  return reader->hasNext();
}

JNIEXPORT jint JNICALL Java_com_intel_ape_ParquetReaderJNI_readBatch(
    JNIEnv* env, jclass cls, jlong readerPtr, jint batchSize, jlongArray buffers,
    jlongArray nulls) {
  ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
  jsize buffersLen = env->GetArrayLength(buffers);
  jsize nullsLen = env->GetArrayLength(nulls);
  assert(buffersLen == nullsLen);

  jlong* buffersPtr = env->GetLongArrayElements(buffers, 0);
  jlong* nullsPtr = env->GetLongArrayElements(nulls, 0);

  int ret = reader->readBatch(batchSize, buffersPtr, nullsPtr);

  env->ReleaseLongArrayElements(buffers, buffersPtr, 0);
  env->ReleaseLongArrayElements(nulls, nullsPtr, 0);
  return ret;
}

JNIEXPORT jboolean JNICALL Java_com_intel_ape_ParquetReaderJNI_skipNextRowGroup(
    JNIEnv* env, jclass cls, jlong readerPtr) {
  ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
  return reader->skipNextRowGroup();
}

JNIEXPORT void JNICALL Java_com_intel_ape_ParquetReaderJNI_close(JNIEnv* env, jclass cls,
                                                                 jlong readerPtr) {
  ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
  reader->close();
  delete reader;
}

JNIEXPORT void JNICALL Java_com_intel_ape_ParquetReaderJNI_setFilterStr(
    JNIEnv* env, jclass cls, jlong readerPtr, jstring filterJsonStr) {
  ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
  std::string filterJsonStr_ = env->GetStringUTFChars(filterJsonStr, nullptr);
  reader->setFilter(filterJsonStr_);
}

JNIEXPORT void JNICALL Java_com_intel_ape_ParquetReaderJNI_setAggStr(JNIEnv* env,
                                                                     jclass cls,
                                                                     jlong readerPtr,
                                                                     jstring aggStr) {
  ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
  std::string aggStr_ = env->GetStringUTFChars(aggStr, nullptr);
  ARROW_LOG(INFO) << "agg str is: " << aggStr_;
  reader->setAgg(aggStr_);
}

JNIEXPORT void JNICALL Java_com_intel_ape_ParquetReaderJNI_setPlasmaCacheRedis(
    JNIEnv* env, jclass cls, jlong readerPtr, jstring host, jint port, jstring password) {
  ape::Reader* reader = reinterpret_cast<ape::Reader*>(readerPtr);
  std::string host_ = env->GetStringUTFChars(host, nullptr);
  std::string password_ = env->GetStringUTFChars(password, nullptr);
  reader->setPlasmaCacheRedis(host_, port, password_);
}
