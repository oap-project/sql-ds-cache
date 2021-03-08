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
import org.apache.spark.unsafe.types.UTF8String

class OapFiberCache(buffer: ByteBuffer) extends FiberCache {
  def getBuffer(): DirectBuffer = buffer.asInstanceOf[DirectBuffer]

  protected var index: Int = 0
  protected var totalRow: Int = _
  protected var nullArrayLen: Int = _
  protected var headerLen = 6

  private var currentDataAddrCursor: Int = 0

  def writeByte(b: Byte): Unit = {
    // TODO: should impl a writeBytes() method for better performance.
    Platform.putByte(null,
      getBuffer().address() + headerLen + nullArrayLen + index * ByteType.defaultSize, b)
    index += 1
  }

  def writeBoolean(b: Boolean): Unit = {
    Platform.putBoolean(null,
      getBuffer().address() + headerLen + nullArrayLen + index * BooleanType.defaultSize, b)
    index += 1
  }

  def writeShort(s: Short): Unit = {
    Platform.putShort(null,
      getBuffer().address() + headerLen + nullArrayLen + index * ShortType.defaultSize, s)
    index += 1
  }

  def writeInt(i: Int): Unit = {
    Platform.putInt(null,
      getBuffer().address() + headerLen + nullArrayLen + index * IntegerType.defaultSize, i)
    index += 1
  }

  def writeLong(l: Long): Unit = {
    Platform.putLong(null,
      getBuffer().address() + headerLen + nullArrayLen + index * LongType.defaultSize, l)
    index += 1
  }

  def writeFloat(f: Float): Unit = {
    Platform.putFloat(null,
      getBuffer().address() + headerLen + nullArrayLen + index * FloatType.defaultSize, f)
    index += 1
  }

  def writeDouble(d: Double): Unit = {
    Platform.putDouble(null,
      getBuffer().address() + headerLen + nullArrayLen + index * DoubleType.defaultSize, d)
    index += 1
  }

  def writeNull(): Unit = {
    Platform.putBoolean(null, getBuffer().address() + headerLen + index, true)
    index += 1
  }

  def writeString(s: UTF8String): Unit = {
    val length = s.getBytes.length

    val offsetArrayAddr = getBuffer().address() + headerLen + nullArrayLen + index * 4
    val lengthArrayAddr = getBuffer().address() + headerLen + nullArrayLen + totalRow * 4 + index * 4

    // write this string's offset
    Platform.putInt(null, offsetArrayAddr, headerLen + nullArrayLen + totalRow * 4 * 2 + currentDataAddrCursor)

    // write this string's length
    Platform.putInt(null, lengthArrayAddr, length)

    var dataOffSet = getBuffer().address() + Platform.getInt(null, offsetArrayAddr)

    // write data
    for (byte <- s.getBytes) {
      Platform.putByte(null, dataOffSet, byte)
      dataOffSet += ByteType.defaultSize
    }

    currentDataAddrCursor += length
    index += 1
  }

  def setTotalRow(num: Int): Unit = {
    totalRow = num
    nullArrayLen = totalRow
    Platform.putInt(null, getBuffer().address(), totalRow)
    (0 until num).foreach(i => Platform.putBoolean(null, getBuffer().address() + headerLen + i, false))
  }

}

class ArrowFiberCache(buffer: ByteBuffer) extends OapFiberCache(buffer) {

  private var nullCount: Long = 0
  headerLen = 32

  // TODO: use a bitmap rather than byte map for null array
  override def writeNull() {
    var s: Byte = Platform.getByte(null, getBuffer().address() + headerLen + index / 8)
    s = (s | (0x01 << (index % 8))).toByte
    Platform.putByte(null, getBuffer().address() + headerLen + index / 8, s)
    nullCount += 1
    index += 1
  }

  override def setTotalRow(num: Int): Unit = {
    totalRow = num
    // align to 16 Bytes
    nullArrayLen = (totalRow / 8 + 0x10) & (~0x0F)
    Platform.putLong(null, getBuffer().address(), totalRow.toLong)
    (0 until nullArrayLen).foreach(i => Platform.putByte(null,
      getBuffer().address() + headerLen + i, 0.toByte))
  }

  def setDataType(dataType: DataType): Unit = {
    val enum = dataType match {
      case ByteType => 0
      case BooleanType => 1
      case ShortType => 2
      case IntegerType => 3
      case LongType => 4
      case FloatType => 5
      case DoubleType => 6
      case StringType => 7
      case _ => throw new UnsupportedOperationException("Type not support.")
    }
    Platform.putByte(null, getBuffer().address() + 16, enum.toByte)
  }

}

object CacheDumper {
  def asyncDumpToCache(columnVector: WritableColumnVector, fiberCache: FiberCache, num: Int): Unit = {

  }

  def syncDumpToCache(columnVector: WritableColumnVector, fiberCache: OapFiberCache, num: Int): Unit = {
    columnVector.dataType() match {
      case ByteType =>
        (0 until num).foreach(i =>
          if (columnVector.isNullAt(i)) fiberCache.writeNull()
          else fiberCache.writeByte(columnVector.getByte(i)))
      case BooleanType =>
        (0 until num).foreach(i =>
          if (columnVector.isNullAt(i)) fiberCache.writeNull()
          else fiberCache.writeBoolean(columnVector.getBoolean(i)))
      case ShortType =>
        (0 until num).foreach(i =>
          if (columnVector.isNullAt(i)) fiberCache.writeNull()
          else fiberCache.writeShort(columnVector.getShort(i)))
      case IntegerType =>
        (0 until num).foreach(i =>
          if (columnVector.isNullAt(i)) fiberCache.writeNull()
          else fiberCache.writeInt(columnVector.getInt(i)))
      case LongType =>
        (0 until num).foreach(i =>
          if (columnVector.isNullAt(i)) fiberCache.writeNull()
          else fiberCache.writeLong(columnVector.getLong(i)))
      case FloatType =>
        (0 until num).foreach(i =>
          if (columnVector.isNullAt(i)) fiberCache.writeNull()
          else fiberCache.writeFloat(columnVector.getFloat(i)))
      case DoubleType =>
        (0 until num).foreach(i =>
          if (columnVector.isNullAt(i)) fiberCache.writeNull()
          else fiberCache.writeDouble(columnVector.getDouble(i)))
      case StringType =>
        (0 until num).foreach(i =>
          if (columnVector.isNullAt(i)) fiberCache.writeNull()
          else fiberCache.writeString(columnVector.getUTF8String(i))
        )
      case other => throw new UnsupportedOperationException(s"$other data type is not support data cache.")
    }
  }

  def calculateLength(dataType: DataType, totalRow: Long): Long = {
    // TODO: what if unfixed size type?
    // header + null + data
    6 + totalRow + dataType.defaultSize * totalRow
  }

  def calculateArrowLength(dataType: DataType, totalRow: Long): Long = {
    dataType match {
      case StringType =>
        32 + ((totalRow / 8 + 0x10) & (~0x0F)) + totalRow * 4 + totalRow * 4 + dataType.defaultSize * totalRow
      case _ =>
        32 + ((totalRow / 8 + 0x10) & (~0x0F)) + dataType.defaultSize * totalRow
    }
    // header + null bit array + data
  }

  def canCache(dataType: DataType): Boolean = {
    dataType match {
      case ByteType | BooleanType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType => true
      case other => false
    }
  }
}
