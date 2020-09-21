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

package org.apache.spark.sql.execution.cacheUtil

import java.nio.ByteBuffer

import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform

class OapFiberCache(buffer: ByteBuffer) extends FiberCache {
  def getBuffer(): DirectBuffer = buffer.asInstanceOf[DirectBuffer]

  private var index: Int = 0
  private var totalRow: Int = _
  private val headerLen = 6

  def writeByte(b: Byte): Unit = {
    // TODO: should impl a writeBytes() method for better performance.
    Platform.putByte(null, getBuffer().address() + headerLen + totalRow + index, b)
    index += 1
  }

  def writeNull(): Unit = {
    Platform.putBoolean(null, getBuffer().address() + headerLen + index, true)
    index += 1
  }

  def setTotalRow(num: Int): Unit = {
    totalRow = num
    Platform.putInt(null, getBuffer().address(), totalRow)
  }

}

object CacheDumper {
  def asyncDumpToCache(columnVector: WritableColumnVector, fiberCache: FiberCache, num: Int): Unit = {

  }

  def syncDumpToCache(columnVector: WritableColumnVector, fiberCache: OapFiberCache, num: Int): Unit = {
    columnVector.dataType() match {
      case ByteType =>
        for (i <- 0 until num) {
          if (columnVector.isNullAt(i)) {
            fiberCache.writeNull()
          } else {
            fiberCache.writeByte(columnVector.getByte(i))
          }
        }
      case other => throw new UnsupportedOperationException(s"$other data type is not support data cache.")
    }
  }

  def calculateLength(dataType: DataType, totalRow: Long): Long = {
    // TODO: what if unfixed size type?
    // header + null + data
    6 + totalRow + dataType.defaultSize * totalRow
  }

  def canCache(dataType: DataType): Boolean = {
    dataType match {
      case ByteType => true
      case other => false
    }
  }
}
