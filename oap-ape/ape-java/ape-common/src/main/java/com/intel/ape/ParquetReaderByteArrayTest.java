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

import java.nio.charset.StandardCharsets;

import com.intel.ape.util.Platform;

class ParquetReaderByteArrayTest {
  public static void main(String[] args) {
    String fileName = "/tpcds_10g/item/" +
            "part-00000-a48097a6-757d-4896-a5c5-0ed1db82fcbd-c000.snappy.parquet";
    String hdfsHost = "sr585";
    int hdfsPort = 9000;
    String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"i_item_id\",\"type\":" +
            "\"string\",\"nullable\":true,\"metadata\":{}}]}";

    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema, 0, 1);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    // cpp use struct ByteArray to store string type data, contains a u32 length and a uint8_t* ptr
    // total size is 4(u32) + 4(padding) + 8(ptr) = 16 bytes
    int batchSize = 4096;
    long[] buffers = new long[8];
    for (int i = 0; i < 8; i++) {
      buffers[i] = Platform.allocateMemory(batchSize * 16);
    }

    long[] nulls = new long[8];
    for (int i = 0; i < 8; i++) {
      nulls[i] = Platform.allocateMemory(batchSize);
    }

    int rows = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);

    System.out.println("read rows: " + rows);


    for(int i =0; i < 10; i++) {
      int size = Platform.getInt(null, buffers[0] + i * 16);
      long addr = Platform.getLong(null, buffers[0] + i * 16 + 8);
      byte[] str = new byte[size];
      System.out.println("debug log, size " + size + " addr " + addr);
      Platform.copyMemory(null, addr, str, Platform.BYTE_ARRAY_OFFSET, size);
      System.out.println("String is " + new String(str, StandardCharsets.UTF_8));

    }

    ParquetReaderJNI.close(reader);
    for (int i = 0; i < 8; i++) {
      Platform.freeMemory(buffers[i]);
      Platform.freeMemory(nulls[i]);
    }
  }
}
