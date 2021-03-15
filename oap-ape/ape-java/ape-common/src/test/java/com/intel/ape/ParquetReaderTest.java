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

import com.intel.ape.util.Platform;

class ParquetReaderTest {

  static String fileName = "/tpcds_10g/catalog_sales/" +
          "part-00000-be30656b-ae02-4015-a9be-b9f62c2d9159-c000.snappy.parquet";
  static String hdfsHost = "sr585";
  static int hdfsPort = 9000;
  static String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"cs_sold_date_sk\"," +
    "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_bill_cdemo_sk\"," +
    "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_item_sk\"," +
    "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_promo_sk\"," +
    "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_quantity\"," +
    "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_list_price\"," +
    "\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_sales_price\"," +
    "\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_coupon_amt\"," +
    "\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}}]}";

  /*
      rowgroup1size: 134789765
      rowgroup2size: 4964302
      rowgroup3size: 134711558
      rowgroup4size: 4998366
      rowgroup5size: 73488114
   */
  public static void main(String[] args) {
    readBatchTest();
    skipNextRowGroupTest();
    splitAbleReaderTest();
    splitAbleReadNoRowGroupToReadTest();
  }

  public static void splitAbleReadNoRowGroupToReadTest() {
    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema, 0, 0);
    System.out.println("reader address " + reader);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    int batchSize = 4096;
    long[] buffers = new long[8];
    for (int i = 0; i < 8; i++) {
      buffers[i] = Platform.allocateMemory(batchSize * 8);
    }

    long[] nulls = new long[8];
    for (int i = 0; i < 8; i++) {
      nulls[i] = Platform.allocateMemory(batchSize);
    }

    int rows = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);

    System.out.println("read rows: " + rows);

    ParquetReaderJNI.close(reader);
    for (int i = 0; i < 8; i++) {
      Platform.freeMemory(buffers[i]);
      Platform.freeMemory(nulls[i]);
    }
  }

  public static void readBatchTest() {
    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema, 1, 1);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    int batchSize = 4096;
    long[] buffers = new long[8];
    for (int i = 0; i < 8; i++) {
      buffers[i] = Platform.allocateMemory(batchSize * 8);
    }

    long[] nulls = new long[8];
    for (int i = 0; i < 8; i++) {
      nulls[i] = Platform.allocateMemory(batchSize);
    }

    int rows = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);

    System.out.println("read rows: " + rows);

    ParquetReaderJNI.close(reader);
    for (int i = 0; i < 8; i++) {
      Platform.freeMemory(buffers[i]);
      Platform.freeMemory(nulls[i]);
    }
  }

  public static void splitAbleReaderTest() {
    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema, 2, 1);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    int batchSize = 4096;
    long[] buffers = new long[8];
    for (int i = 0; i < 8; i++) {
      buffers[i] = Platform.allocateMemory(batchSize * 8);
    }

    long[] nulls = new long[8];
    for (int i = 0; i < 8; i++){
      nulls[i] = Platform.allocateMemory(batchSize);
    }

    int rows = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);

    System.out.println("read rows: " + rows);

    ParquetReaderJNI.close(reader);
    for (int i = 0; i < 8; i++) {
      Platform.freeMemory(buffers[i]);
      Platform.freeMemory(nulls[i]);
    }
  }

  public static void skipNextRowGroupTest() {
    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema, 1, 1);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    int batchSize = 4096;
    long[] buffers = new long[8];
    for (int i = 0; i < 8; i++) {
      buffers[i] = Platform.allocateMemory(batchSize * 8);
    }

    long[] nulls = new long[8];
    for (int i = 0; i < 8; i++) {
      nulls[i] = Platform.allocateMemory(batchSize);
    }

    ParquetReaderJNI.skipNextRowGroup(reader);
    ParquetReaderJNI.skipNextRowGroup(reader);

    int rows = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);

    System.out.println("read rows: " + rows);

    ParquetReaderJNI.close(reader);
    for (int i = 0; i < 8; i++) {
      Platform.freeMemory(buffers[i]);
      Platform.freeMemory(nulls[i]);
    }
  }
}
