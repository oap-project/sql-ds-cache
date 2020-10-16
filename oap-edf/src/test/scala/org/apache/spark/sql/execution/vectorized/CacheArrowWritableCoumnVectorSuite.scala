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
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, ShortType};

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
        val val1 = column.getBoolean(i)
        val val2 = arrowColumn.getBoolean(i)
        assert(val1 == val2)
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
        val val1 = column.getShort(i)
        val val2 = arrowColumn.getShort(i)
        assert(val1 == val2)
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
        val val1 = column.getInt(i)
        val val2 = arrowColumn.getInt(i)
        assert(val1 == val2)
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
        val val1 = column.getLong(i)
        val val2 = arrowColumn.getLong(i)
        assert(val1 == val2)
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
        assert(val1 == val2)
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
        val val1 = column.getDouble(i)
        val val2 = arrowColumn.getDouble(i)
        assert(val1 == val2)
      }
    }
  }

}
