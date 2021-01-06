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
  /*
      rowgroup1size: 134789765
      rowgroup2size: 4964302
      rowgroup3size: 134711558
      rowgroup4size: 4998366
      rowgroup5size: 73488114
   */
  public static void main(String[] args) {
    String fileName = "/tpcds_10g/catalog_sales/part-00000-be30656b-ae02-4015-a9be-b9f62c2d9159-c000.snappy.parquet";
    String hdfsHost = "sr585";
    int hdfsPort = 9000;
    String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"cs_sold_date_sk\",\"type\":" +
            "\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_bill_cdemo_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_item_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_promo_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_quantity\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_list_" +
            "price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":" +
            "\"cs_sales_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{" +
            "\"name\":\"cs_coupon_amt\",\"type\":\"decimal(7,2)\",\"nullable\":true," +
            "\"metadata\":{}}]}";

    skipNextRowGroupTest();
    splitAbleReaderTest();

    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema, 0l, 360000000l);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    int batchSize = 4096;
    long[] buffers = new long[8];
    for (int i = 0; i < 8; i++)
      buffers[i] = Platform.allocateMemory(batchSize * 8);

    long[] nulls = new long[8];
    for (int i = 0; i < 8; i++)
      nulls[i] = Platform.allocateMemory(batchSize);

    int rows = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);

    System.out.println("read rows: " + rows);

    ParquetReaderJNI.close(reader);
    for (int i = 0; i < 8; i++) {
      Platform.freeMemory(buffers[i]);
      Platform.freeMemory(nulls[i]);
    }
  }

  public static void splitAbleReaderTest() {
    String fileName = "/tpcds_10g/catalog_sales/part-00000-be30656b-ae02-4015-a9be-b9f62c2d9159-c000.snappy.parquet";
    String hdfsHost = "sr585";
    int hdfsPort = 9000;
    String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"cs_sold_date_sk\",\"type\":" +
            "\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_bill_cdemo_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_item_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_promo_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_quantity\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_list_" +
            "price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":" +
            "\"cs_sales_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{" +
            "\"name\":\"cs_coupon_amt\",\"type\":\"decimal(7,2)\",\"nullable\":true," +
            "\"metadata\":{}}]}";
    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema, 134789790l, 140000000);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    int batchSize = 4096;
    long[] buffers = new long[8];
    for (int i = 0; i < 8; i++)
      buffers[i] = Platform.allocateMemory(batchSize * 8);

    long[] nulls = new long[8];
    for (int i = 0; i < 8; i++)
      nulls[i] = Platform.allocateMemory(batchSize);

    int rows = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);

    System.out.println("read rows: " + rows);

    ParquetReaderJNI.close(reader);
    for (int i = 0; i < 8; i++) {
      Platform.freeMemory(buffers[i]);
      Platform.freeMemory(nulls[i]);
    }
  }

  public static void skipNextRowGroupTest() {
    String fileName = "/tpcds_10g/catalog_sales/part-00000-be30656b-ae02-4015-a9be-b9f62c2d9159-c000.snappy.parquet";
    String hdfsHost = "sr585";
    int hdfsPort = 9000;
    String schema = "{\"type\":\"struct\",\"fields\":[{\"name\":\"cs_sold_date_sk\",\"type\":" +
            "\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_bill_cdemo_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_item_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_promo_sk\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_quantity\"," +
            "\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"cs_list_" +
            "price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{\"name\":" +
            "\"cs_sales_price\",\"type\":\"decimal(7,2)\",\"nullable\":true,\"metadata\":{}},{" +
            "\"name\":\"cs_coupon_amt\",\"type\":\"decimal(7,2)\",\"nullable\":true," +
            "\"metadata\":{}}]}";
    long reader = ParquetReaderJNI.init(fileName, hdfsHost, hdfsPort, schema, 134789765l, 360000000l);
    if (reader == 0) {
      System.out.println("reader init failed");
      return;
    }

    int batchSize = 4096;
    long[] buffers = new long[8];
    for (int i = 0; i < 8; i++)
      buffers[i] = Platform.allocateMemory(batchSize * 8);

    long[] nulls = new long[8];
    for (int i = 0; i < 8; i++)
      nulls[i] = Platform.allocateMemory(batchSize);

    int rows1 = ParquetReaderJNI.readBatch(reader, batchSize, buffers, nulls);
    System.out.println("read rows1: " + rows1);

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