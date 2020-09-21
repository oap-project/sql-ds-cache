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

package org.apache.spark.sql.execution.vectorized

import java.nio.ByteBuffer

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.cacheUtil.{CacheDumper, OapFiberCache}
import org.apache.spark.sql.execution.datasources.parquet.VectorizedCacheReader
import org.apache.spark.sql.types.ByteType

class ReadOnlyColumnVectorSuite extends SparkFunSuite with BeforeAndAfterEach with Logging {

  test("write a OnHeapColumnVector to FiberCache and read it") {
    val num = 100
    val column = new OnHeapColumnVector(num, ByteType)
    for ( i <- 0 until num) {
      column.putByte(i, i.toByte)
    }

    val len = CacheDumper.calculateLength(ByteType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new OapFiberCache(buffer)
    fiber.setTotalRow(num)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new VectorizedCacheReader(ByteType, fiber)
    val readonlyColumn = reader.readBatch(num)

    for( i <- 0 until num) {
      val val1 = column.getByte(i)
      val val2 = readonlyColumn.getByte(i)
      assert(val1 == val2)
    }
  }

  test("write multi OnHeapColumnVector to FiberCache and read them") {
    val num = 100
    val column1 = new OnHeapColumnVector(num, ByteType)
    val column2 = new OnHeapColumnVector(num, ByteType)

    for ( i <- 0 until num) {
      column1.putByte(i, i.toByte)
      column2.putByte(i, (i + 100).toByte)
    }

    val len = CacheDumper.calculateLength(ByteType, num * 2)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new OapFiberCache(buffer)
    fiber.setTotalRow(num * 2)

    CacheDumper.syncDumpToCache(column1, fiber, num)

    CacheDumper.syncDumpToCache(column2, fiber, num)

    val reader = new VectorizedCacheReader(ByteType, fiber)
    val readonlyColumn1 = reader.readBatch(num)

    for( i <- 0 until num) {
      val val1 = column1.getByte(i)
      val val2 = readonlyColumn1.getByte(i)
      assert(val1 == val2)
    }

    val readonlyColumn2 = reader.readBatch(num)
    for( i <- 0 until num) {
      val val1 = column2.getByte(i)
      val val2 = readonlyColumn2.getByte(i)
      assert(val1 == val2)
    }
  }

  test("OnheapColumnVector with null") {
    val num = 100
    val null_index = Array[Int](10, 20, 30)

    val column = new OnHeapColumnVector(num, ByteType)
    for ( i <- 0 until num) {
      if (null_index.contains(i)) {
        column.putNull(i)
      } else {
        column.putByte(i, i.toByte)
      }
    }

    val len = CacheDumper.calculateLength(ByteType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new OapFiberCache(buffer)
    fiber.setTotalRow(num)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new VectorizedCacheReader(ByteType, fiber)
    val readonlyColumn = reader.readBatch(num)

    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        assert(readonlyColumn.isNullAt(i))
      } else {
        assert(!readonlyColumn.isNullAt(i))
        val val1 = column.getByte(i)
        val val2 = readonlyColumn.getByte(i)
        assert(val1 == val2)
      }
    }
  }
}
