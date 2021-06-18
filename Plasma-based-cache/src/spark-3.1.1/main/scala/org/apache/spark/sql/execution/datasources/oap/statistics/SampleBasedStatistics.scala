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
package org.apache.spark.sql.execution.datasources.oap.statistics

import java.io.{ByteArrayOutputStream, OutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index._
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.types.StructType

private[oap] class SampleBasedStatisticsReader(
    schema: StructType) extends StatisticsReader(schema) {
  override val id: Int = StatisticsType.TYPE_SAMPLE_BASE

  @transient private lazy val ordering = GenerateOrdering.create(schema)

  protected var sampleArray: Array[Key] = _

  override def read(fiberCache: FiberCache, offset: Int): Int = {
    var readOffset = super.read(fiberCache, offset) + offset

    val size = fiberCache.getInt(readOffset)
    readOffset += 4

    // TODO use unsafe way to interact with sample array
    sampleArray = new Array[Key](size)

    var rowOffset = 0
    for (i <- 0 until size) {
      sampleArray(i) = nnkr.readKey(
        fiberCache, readOffset + size * IndexUtils.INT_SIZE + rowOffset)._1
      rowOffset = fiberCache.getInt(readOffset + i * IndexUtils.INT_SIZE)
    }
    readOffset += (rowOffset + size * IndexUtils.INT_SIZE)
    readOffset - offset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): StatsAnalysisResult = {
    if (sampleArray == null || sampleArray.isEmpty) {
      StatsAnalysisResult.USE_INDEX
    } else {
      var hitCnt = 0
      val partialOrder = GenerateOrdering.create(StructType(schema.dropRight(1)))
      for (row <- sampleArray) {
        if (Statistics.rowInIntervalArray(row, intervalArray, ordering, partialOrder)) {
          hitCnt += 1
        }
      }
      StatsAnalysisResult(hitCnt * 1.0 / sampleArray.length)
    }
  }
}

private[oap] class SampleBasedStatisticsWriter(schema: StructType, conf: Configuration)
  extends StatisticsWriter(schema, conf) {
  override val id: Int = StatisticsType.TYPE_SAMPLE_BASE

  lazy val sampleRate: Double = conf.getDouble(
    OapConf.OAP_STATISTICS_SAMPLE_RATE.key, OapConf.OAP_STATISTICS_SAMPLE_RATE.defaultValue.get)

  private val minSampleSize = conf.getInt(
    OapConf.OAP_STATISTICS_SAMPLE_MIN_SIZE.key,
    OapConf.OAP_STATISTICS_SAMPLE_MIN_SIZE.defaultValue.get)

  protected var sampleArray: Array[Key] = _

  // this is the common part used by write and write2
  private def internalWrite(writer: OutputStream, offsetP: Int, sizeP: Int): Int = {
    var offset = offsetP
    val size = sizeP
    IndexUtils.writeInt(writer, size)
    offset += IndexUtils.INT_SIZE
    val tempWriter = new ByteArrayOutputStream()
    sampleArray.foreach(key => {
      nnkw.writeKey(tempWriter, key)
      IndexUtils.writeInt(writer, tempWriter.size())
      offset += IndexUtils.INT_SIZE
    })
    offset += tempWriter.size()
    writer.write(tempWriter.toByteArray)
    offset
  }

  // SampleBasedStatistics file structure
  // statistics_id        4 Bytes, Int, specify the [[Statistic]] type
  // sample_size          4 Bytes, Int, number of UnsafeRow
  //
  // | unsafeRow-1 sizeInBytes | unsafeRow-1 content |   (4 + u1_sizeInBytes) Bytes, unsafeRow-1
  // | unsafeRow-2 sizeInBytes | unsafeRow-2 content |   (4 + u2_sizeInBytes) Bytes, unsafeRow-2
  // | unsafeRow-3 sizeInBytes | unsafeRow-3 content |   (4 + u3_sizeInBytes) Bytes, unsafeRow-3
  // ...
  // | unsafeRow-(sample_size) sizeInBytes | unsafeRow-(sample_size) content |
  override def write(writer: OutputStream, sortedKeys: ArrayBuffer[Key]): Int = {
    val offset = super.write(writer, sortedKeys)
    val size = math.max(
      (sortedKeys.size * sampleRate).toInt, math.min(minSampleSize, sortedKeys.size))
    sampleArray = takeSample(sortedKeys, size)

    internalWrite(writer, offset, size)
  }

  protected def takeSample(keys: ArrayBuffer[InternalRow], size: Int): Array[InternalRow] =
    Random.shuffle(keys.indices.toList).take(size).map(keys(_)).toArray

  protected var randomHashSet: mutable.HashSet[Int] = mutable.HashSet.empty[Int]
  protected var sampleArrayBuffer: ArrayBuffer[Key] = ArrayBuffer.empty[Key]
  private var keyCount: Int = 0

  override def write2(writer: OutputStream): Int = {
    val offset = super.write2(writer)
    sampleArray = sampleArrayBuffer.toArray
    sampleArrayBuffer = null
    internalWrite(writer, offset, sampleArray.size)
  }

  def initParams(totalSorterRecordSize: Int): Unit = {
    val size = math.max(
      (totalSorterRecordSize * sampleRate).toInt, math.min(minSampleSize, totalSorterRecordSize))
    if (size == (totalSorterRecordSize * sampleRate).toInt) {
      while (randomHashSet.size < size) {
        randomHashSet.add(Random.nextInt((totalSorterRecordSize - 1)))
      }
    } else {
      Random.shuffle((0 to totalSorterRecordSize).indices.toList).take(size)
        .foreach(randomHashSet.add(_))
    }
  }

  def buildSampleArray(keyArray: Array[Product2[Key, ArrayBuffer[Int]]], isLast: Boolean): Unit = {
    keyArray.foreach(
      value => {
        value._2.foreach(
          _ => {
            if (randomHashSet.contains(keyCount)) {
              sampleArrayBuffer += value._1
            }
            keyCount += 1
          }
        )
      }
    )
  }
}
