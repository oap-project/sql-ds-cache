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

import com.intel.oap.vectorized.ArrowWritableColumnVector
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.cacheUtil.{ArrowFiberCache, CacheDumper}
import org.apache.spark.sql.execution.datasources.parquet.ArrowVectorizedCacheReader
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType};

class CacheArrowWritableCoumnVectorSuite extends SparkFunSuite with BeforeAndAfterEach with Logging {

  test("Boolean type test") {
    val num = 100
    val null_index = Array[Int](10, 20, 30)

    val column = new ArrowWritableColumnVector(num, BooleanType)
    for ( i <- 0 until num) {
      if (null_index.contains(i)) {
        column.putNull(i)
      } else {
        column.putBoolean(i, true)
      }
    }

    val len = CacheDumper.calculateArrowLength(BooleanType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new ArrowFiberCache(buffer)
    fiber.setTotalRow(num)
    fiber.setDataType(BooleanType)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new ArrowVectorizedCacheReader(BooleanType, fiber)
    val arrowColumn = reader.readBatch(num)

    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        assert(arrowColumn.isNullAt(i))
      } else {
        assert(!arrowColumn.isNullAt(i))
        assert(column.getBoolean(i) == arrowColumn.getBoolean(i))
      }
    }
  }

  test("Short type test") {
    val num = 100
    val null_index = Array[Int](10, 20, 30)

    val column = new ArrowWritableColumnVector(num, ShortType)
    for ( i <- 0 until num) {
      if (null_index.contains(i)) {
        column.putNull(i)
      } else {
        column.putShort(i, i.toShort)
      }
    }

    val len = CacheDumper.calculateArrowLength(ShortType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new ArrowFiberCache(buffer)
    fiber.setTotalRow(num)
    fiber.setDataType(ShortType)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new ArrowVectorizedCacheReader(ShortType, fiber)
    val arrowColumn = reader.readBatch(num)

    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        assert(arrowColumn.isNullAt(i))
      } else {
        assert(!arrowColumn.isNullAt(i))
        assert(column.getShort(i) == arrowColumn.getShort(i))
      }
    }
  }

  test("Int type test") {
    val num = 100
    val null_index = Array[Int](10, 20, 30)

    val column = new ArrowWritableColumnVector(num, IntegerType)
    for ( i <- 0 until num) {
      if (null_index.contains(i)) {
        column.putNull(i)
      } else {
        column.putInt(i, i.toInt)
      }
    }

    val len = CacheDumper.calculateArrowLength(IntegerType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new ArrowFiberCache(buffer)
    fiber.setTotalRow(num)
    fiber.setDataType(IntegerType)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new ArrowVectorizedCacheReader(IntegerType, fiber)
    val arrowColumn = reader.readBatch(num)

    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        assert(arrowColumn.isNullAt(i))
      } else {
        assert(!arrowColumn.isNullAt(i))
        assert(column.getInt(i) == arrowColumn.getInt(i))
      }
    }
  }

  test("Long type test") {
    val num = 100
    val null_index = Array[Int](10, 20, 30)

    val column = new ArrowWritableColumnVector(num, LongType)
    for ( i <- 0 until num) {
      if (null_index.contains(i)) {
        column.putNull(i)
      } else {
        column.putLong(i, i.toLong)
      }
    }

    val len = CacheDumper.calculateArrowLength(LongType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new ArrowFiberCache(buffer)
    fiber.setTotalRow(num)
    fiber.setDataType(LongType)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new ArrowVectorizedCacheReader(LongType, fiber)
    val arrowColumn = reader.readBatch(num)

    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        assert(arrowColumn.isNullAt(i))
      } else {
        assert(!arrowColumn.isNullAt(i))
        assert(column.getLong(i) == arrowColumn.getLong(i))
      }
    }
  }

  test("Float type test") {
    val num = 100
    val null_index = Array[Int](10, 20, 30)

    val column = new ArrowWritableColumnVector(num, FloatType)
    for ( i <- 0 until num) {
      if (null_index.contains(i)) {
        column.putNull(i)
      } else {
        column.putFloat(i, i.toFloat)
      }
    }

    val len = CacheDumper.calculateArrowLength(FloatType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new ArrowFiberCache(buffer)
    fiber.setTotalRow(num)
    fiber.setDataType(FloatType)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new ArrowVectorizedCacheReader(FloatType, fiber)
    val arrowColumn = reader.readBatch(num)

    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        assert(arrowColumn.isNullAt(i))
      } else {
        assert(!arrowColumn.isNullAt(i))
        val val1 = column.getFloat(i)
        val val2 = arrowColumn.getFloat(i)
        assert(column.getFloat(i) == arrowColumn.getFloat(i))
      }
    }
  }

  test("Double type test") {
    val num = 100
    val null_index = Array[Int](10, 20, 30)

    val column = new ArrowWritableColumnVector(num, DoubleType)
    for ( i <- 0 until num) {
      if (null_index.contains(i)) {
        column.putNull(i)
      } else {
        column.putDouble(i, i.toDouble)
      }
    }

    val len = CacheDumper.calculateArrowLength(DoubleType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new ArrowFiberCache(buffer)
    fiber.setTotalRow(num)
    fiber.setDataType(DoubleType)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new ArrowVectorizedCacheReader(DoubleType, fiber)
    val arrowColumn = reader.readBatch(num)

    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        assert(arrowColumn.isNullAt(i))
      } else {
        assert(!arrowColumn.isNullAt(i))
        assert(column.getDouble(i) == arrowColumn.getDouble(i))
      }
    }
  }

  test("String type test") {
    val num = 10
    val null_index = Array[Int](1, 2, 5)
    val column = new ArrowWritableColumnVector(num, StringType)

    val s = "abcd"
    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        column.putNull(i)
      } else {
        var tmps = s
        for (m <- 0 until i) {
          tmps += m
        }
        column.putByteArray(i, tmps.getBytes())
      }
    }

    val len = CacheDumper.calculateArrowLength(StringType, num)
    val buffer = ByteBuffer.allocateDirect(len.toInt)
    val fiber = new ArrowFiberCache(buffer)
    fiber.setTotalRow(num)
    fiber.setDataType(StringType)
    CacheDumper.syncDumpToCache(column, fiber, num)

    val reader = new ArrowVectorizedCacheReader(StringType, fiber)
    val arrowColumn = reader.readBatch(num)

    for (i <- 0 until num) {
      if (null_index.contains(i)) {
        assert(arrowColumn.isNullAt(i))
      } else {
        assert(!arrowColumn.isNullAt(i))
        assert(column.getUTF8String(i) == arrowColumn.getUTF8String(i))
      }
    }
  }
}
