/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.ape;

import com.intel.oap.common.util.NativeLibraryLoader;

public class ParquetReaderJNI {
  public static final String LibraryName = "parquet_jni";

  static {
    NativeLibraryLoader.load(LibraryName);
  }

  public static long init(String fileName, String hdfsHost, int hdfsPort, String requiredSchema,
                          int firstRowGroupIndex, int totalGroupToRead) {
    return init(fileName, hdfsHost, hdfsPort, requiredSchema,
            firstRowGroupIndex, totalGroupToRead, false);
  }

  // return a reader pointer
  public static native long init(String fileName, String hdfsHost, int hdfsPort,
                                 String requiredSchema, int firstRowGroupIndex,
                                 int totalGroupToRead, boolean plasmaCacheEnabled);

  public static native int readBatch(long reader, int batchSize, long[] buffers, long[] nulls);

  public static native boolean hasNext(long reader);

  // This is for flink seek()
  // Do not support usage like
  // reader.readBatch(4096); reader.skipNextRowGroup(); reader.readBatch(4096);
  public static native boolean skipNextRowGroup(long reader);

  public static native void close(long reader);

  public static native void setFilterStr(long reader, String filterStr);

  public static native void setAggStr(long reader, String aggStr);

  public static native void setPlasmaCacheRedis(long reader, String host,
                                                int port, String password);
}
