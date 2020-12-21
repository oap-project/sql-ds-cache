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

import com.intel.ape.ParquetReaderJNI;

import org.apache.spark.unsafe.Platform;

class ParquetReaderTest {
  public static void main(String[] args) {
    String fileName = "/tpcds_10g/store_sales/part-00000-74feb3b4-1954-4be7-802d-a50912793bea-c000.snappy.parquet";
    String hdfsHost = "sr585";
    int hdfsPort = 9000;
    String schema = "balabala";
    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    int batchSize = 4096;
    long[] buffers = new long[3];
    for (int i = 0; i < 3; i++)
      buffers[i] = Platform.allocateMemory(batchSize * 4);

    long[] nulls = new long[3];
    for (int i = 0; i < 3; i++)
      nulls[i] = Platform.allocateMemory(batchSize);

    int rows = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);

    System.out.println("read rows: " + rows);

    ParquetReaderJNI.close(reader);
    for (int i = 0; i < 3; i++) {
      Platform.freeMemory(buffers[i]);
      Platform.freeMemory(nulls[i]);
    }
  }
}