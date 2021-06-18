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

package org.apache.spark.sql.execution.datasources.oap.index

import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.io.ChunkedByteBuffer

private[oap] object CurrentKey {
  val INVALID_KEY_INDEX = -1
}

// B+ tree values in the leaf node, in long term, a single value should be associated
// with a single key, however, in order to eliminate the duplicated key in the B+ tree,
// we simply take out the values for the identical keys, and keep only a single key in the
// B+ tree leaf node
private[oap] trait IndexNodeValue {
  def length: Int
  def apply(idx: Int): Long
}

// B+ Tree Node
private[oap] trait IndexNode {
  def length: Int
  def keyAt(idx: Int): Key
  def childAt(idx: Int): IndexNode
  def valueAt(idx: Int): IndexNodeValue
  def next: IndexNode
  def isLeaf: Boolean
}

trait UnsafeIndexTree {
  def buffer: ChunkedByteBuffer
  def offset: Long
  def baseObj: Object = buffer.chunks.head match {
    case _: DirectBuffer => null
    case _ => buffer.toArray
  }
  def baseOffset: Long = buffer.chunks.head match {
    case buf: DirectBuffer => buf.address()
    case _ => Platform.BYTE_ARRAY_OFFSET
  }
  def length: Int = Platform.getInt(baseObj, baseOffset + offset)
}

private[oap] case class UnsafeIndexNodeValue(
    buffer: ChunkedByteBuffer,
    offset: Long,
    dataEnd: Long) extends IndexNodeValue with UnsafeIndexTree {
  // 4 <- value1, 8 <- value2
  override def apply(idx: Int): Long = Platform.getLong(baseObj, baseOffset + offset + 4 + idx * 8)

  // for debug
  private def values: Seq[Long] = (0 until length).map(apply)
  override def toString: String = "ValuesNode(" + values.mkString(",") + ")"
}

private[oap] case class UnsafeIndexNode(
    buffer: ChunkedByteBuffer,
    offset: Long,
    dataEnd: Long,
    schema: StructType) extends IndexNode with UnsafeIndexTree {
  override def keyAt(idx: Int): Key = {
    // 16 <- value5, 12(4 + 8) <- value3 + value4
    val keyOffset = Platform.getLong(baseObj, baseOffset + offset + 12 + idx * 16)
    val len = Platform.getInt(baseObj, baseOffset + keyOffset)
//  val row = new UnsafeRow(schema.length) // this is for debug use
    val row = UnsafeIndexNode.getUnsafeRow(schema.length, baseObj, baseOffset + keyOffset + 4, len)
    row
  }

  private def treeChildAt(idx: Int): UnsafeIndexTree = {
    // 16 <- value5, 20(4 + 8 + 8) <- value3 + value4 + value5/2
    val childOffset = Platform.getLong(baseObj, baseOffset + offset + 16 * idx + 20)
    if (isLeaf) {
      UnsafeIndexNodeValue(buffer, childOffset, dataEnd)
    } else {
      UnsafeIndexNode(buffer, childOffset, dataEnd, schema)
    }
  }

  override def childAt(idx: Int): UnsafeIndexNode =
    treeChildAt(idx).asInstanceOf[UnsafeIndexNode]
  override def valueAt(idx: Int): UnsafeIndexNodeValue =
    treeChildAt(idx).asInstanceOf[UnsafeIndexNodeValue]
  // if the first child offset is in data segment (treeChildAt(0)), 20 <- 16 * 0 + 20
  override def isLeaf: Boolean = Platform.getLong(baseObj, baseOffset + offset + 20) < dataEnd
  override def next: UnsafeIndexNode = {
    // 4 <- value3
    val nextOffset = Platform.getLong(baseObj, baseOffset + offset + 4)
    if (nextOffset == -1L) {
      null
    } else {
      UnsafeIndexNode(buffer, nextOffset, dataEnd, schema)
    }
  }

  // for debug
  private def children: Seq[UnsafeIndexTree] = (0 until length).map(treeChildAt)
  private def keys: Seq[Key] = (0 until length).map(keyAt)
  override def toString: String =
    s"[Signs(${keys.map(_.toSeq(schema).mkString("(", ",", ")")).mkString(",")}) " +
      children.mkString(" ") + "]"
}

private[oap] object UnsafeIndexNode {
  lazy val row = new ThreadLocal[UnsafeRow] {
    override def initialValue = new UnsafeRow
  }

  private def getCorrectUnsafeRow(fieldNum : Int) : UnsafeRow = {
    val curRow = row.get
    if (curRow.numFields() != fieldNum) {
      val newRow = new UnsafeRow(fieldNum)
      UnsafeIndexNode.row.set(newRow)
      newRow
    } else {
      curRow
    }
  }

  def getUnsafeRow(
      schemaLen: Int,
      baseObj : Object,
      baseOffset: Long,
      sizeInBytes : Int) : UnsafeRow = {
    val curRow = getCorrectUnsafeRow(schemaLen)
    curRow.pointTo(baseObj, baseOffset, sizeInBytes)
    curRow
  }
}

private[oap] class CurrentKey(node: IndexNode, keyIdx: Int, valueIdx: Int, indexLimit: Int = 0) {
  assert(node.isLeaf, "Should be Leaf Node")

  private var currentNode: IndexNode = node
  // currentKeyIdx is the flag that we check if we are in the end of the tree traversal
  private var currentKeyIdx: Int = if (node.length > keyIdx) {
    keyIdx
  } else {
    CurrentKey.INVALID_KEY_INDEX
  }

  private val limitScanNum: Int = indexLimit

  private var currentScanNum: Int = 1

  private var currentValueIdx: Int = valueIdx

  private var currentValues: IndexNodeValue = if (currentKeyIdx != CurrentKey.INVALID_KEY_INDEX) {
    currentNode.valueAt(currentKeyIdx)
  } else {
    null
  }

  def currentKey: Key = if (currentKeyIdx == CurrentKey.INVALID_KEY_INDEX) {
    IndexScanner.DUMMY_KEY_END
  } else {
    currentNode.keyAt(currentKeyIdx)
  }

  def currentRowId: Long = currentValues(currentValueIdx)

  def moveNextValue: Unit = {
    if (currentValueIdx < currentValues.length - 1 && limitScanNum == 0) {
      currentValueIdx += 1
    } else if (currentValueIdx < currentValues.length - 1 &&
      limitScanNum > 0 && currentScanNum < limitScanNum) {
      currentValueIdx += 1
      currentScanNum += 1
    } else {
      moveNextKey
    }
  }

  def moveNextKey: Unit = {
    currentScanNum = 1
    if (currentKeyIdx < currentNode.length - 1) {
      currentKeyIdx += 1
      currentValueIdx = 0
      currentValues = currentNode.valueAt(currentKeyIdx)
    } else {
      currentNode = currentNode.next
      if (currentNode != null) {
        currentKeyIdx = 0
        currentValueIdx = 0
        currentValues = currentNode.valueAt(currentKeyIdx)
      } else {
        currentKeyIdx = CurrentKey.INVALID_KEY_INDEX
      }
    }
  }

  def isEnd: Boolean = currentNode == null || currentKey == IndexScanner.DUMMY_KEY_END
}

private[oap] class RangeInterval(
    s: Key,
    e: Key,
    includeStart: Boolean,
    includeEnd: Boolean,
    ignoreTail: Boolean = false,
    isNull: Boolean = false) extends Serializable {
  var start = s
  var end = e
  var startInclude = includeStart
  var endInclude = includeEnd
  val isNullPredicate = isNull
  var isPrefixMatch = ignoreTail
}

private[oap] object RangeInterval{
  def apply(
      s: Key,
      e: Key,
      includeStart: Boolean,
      includeEnd: Boolean,
      ignoreTail: Boolean = false,
      isNull: Boolean = false): RangeInterval = {
    new RangeInterval(s, e, includeStart, includeEnd, ignoreTail, isNull)
  }
}
