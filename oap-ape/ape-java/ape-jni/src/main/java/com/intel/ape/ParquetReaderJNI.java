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

  // return a reader pointer
  public static native long init(String fileName, String hdfsNN, int hdfsPort, String requiredSchema);

  public static native int readBatch(long reader, int batchSize);

  public static native boolean hasNext(long reader);

  public static native boolean skipNextRowGroup(long reader);

  public static native void close(long reader);

}