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

package org.apache.spark.sql.execution.datasources.oap.io

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.hadoop.utils.Collections3
import org.apache.parquet.schema.{MessageType, Type}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, RecordReader}
import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, FiberId, VectorDataFiberId}
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupportWrapper
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.oap.OapRuntime
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class VectorizedCacheReader(
    configuration: Configuration,
    footer: ParquetMetadata,
    dataFile: ParquetDataFile,
    requiredColumnIds: Array[Int],
    file: PartitionedFile = null)
  extends RecordReader[AnyRef] with Logging {

  protected val defaultCapacity: Int =
    OapRuntime.getOrCreate.fiberCacheManager.dataCacheCompressionSize

  protected var batchIdx = 0

  protected var numBatched = 0

  protected var rowsReturned = 0L

  protected var totalCountLoadedSoFar = 0

  protected var missingColumns: Array[Boolean] = _

  protected var columnarBatch: ColumnarBatch = _

  protected var columnVectors: Array[ColumnVector] = _

  protected var returnColumnarBatch = false

  private var fileSchema: MessageType = _

  private var requestedSchema: MessageType = _

  private var sparkSchema: StructType = _

  protected var totalRowCount = 0L

  protected var fiberReaders: Array[ParquetDataFiberReader] = _

  protected var rowGroupMetaIter: Iterator[BlockMetaData] = _

  protected var currentRowGroup: BlockMetaData = _

  protected var currentRowGroupRowsReturned: Int = 0

  // To record if current row group has failed memory block
  protected var hasFailedMemoryBlock = false

  protected var failedMemoryBlockList: util.LinkedList[FiberId] = new util.LinkedList[FiberId]()

  override def initialize(): Unit = {
    initializeMetas()
    initializeInternal()
  }

  override def nextKeyValue(): Boolean = {
    resultBatch

    if (returnColumnarBatch) {
      return nextBatch
    }

    if (batchIdx >= numBatched) {
      if (!nextBatch) {
        return false
      }
    }
    batchIdx += 1
    true
  }

  override def getCurrentValue: AnyRef = {
    if (returnColumnarBatch) {
      return columnarBatch
    }
    columnarBatch.getRow(batchIdx - 1)
  }

  override def close(): Unit = {
    clearFailedCache()
    columnarBatch.close()
  }

  def nextBatch: Boolean = {
    if (rowsReturned >= totalRowCount) {
      return false
    }

    checkEndOfRowGroup()
    nextBatchInternal()
    true
  }

  def initBatch(): Unit = {
    initBatch(MemoryMode.ON_HEAP, null, null)
  }

  def initBatch(partitionColumns: StructType, partitionValues: InternalRow): Unit =
    initBatch(MemoryMode.ON_HEAP, partitionColumns, partitionValues)

  def resultBatch: ColumnarBatch = {
    if (columnarBatch == null) initBatch()
    columnarBatch
  }

  def enableReturningBatches(): Unit = returnColumnarBatch = true

  protected def checkEndOfRowGroup(): Unit = {
    if (rowsReturned != totalCountLoadedSoFar) {
      return
    }
    readNextRowGroup()
  }

  protected def readNextRowGroup(): Unit = {
    assert(rowGroupMetaIter.hasNext)
    hasFailedMemoryBlock = false

    currentRowGroup = rowGroupMetaIter.next()
    val groupId = currentRowGroup.asInstanceOf[OrderedBlockMetaData].getRowGroupId

    var loadFiberTime = 0L
    var loadDicTime = 0L
    val rowCount = currentRowGroup.getRowCount.toInt

    OapRuntime.getOrCreate.fiberCacheManager.getCacheGuardian().getGuardianLock().lock()
    while (OapRuntime.getOrCreate.fiberCacheManager.isNeedWaitForFree) {
      logWarning(s"${TaskContext.get().taskAttemptId()} start to wait for cache free, " +
        s"current pending occupied size: " +
        s"${OapRuntime.getOrCreate.fiberCacheManager.pendingOccupiedSize}")
      OapRuntime.getOrCreate.fiberCacheManager
        .getCacheGuardian().getGuardianLockCondition().await()
      logWarning(s"${TaskContext.get().taskAttemptId()} leave wait")
    }
    OapRuntime.getOrCreate.fiberCacheManager.getCacheGuardian().getGuardianLock().unlock()

    fiberReaders = requiredColumnIds.zipWithIndex.map {
      case (id, order) =>
        if (missingColumns(order)) {
          null
        } else {
          val start = System.nanoTime()
          val fiberId = VectorDataFiberId(dataFile, id, groupId)
          val fiberCache: FiberCache =
            OapRuntime.getOrCreate.fiberCacheManager.get(fiberId)
          val end = System.nanoTime()
          loadFiberTime += (end - start)
          dataFile.update(id, fiberCache)
          val start2 = System.nanoTime()

          if (fiberCache.isFailedMemoryBlock()) {
            failedMemoryBlockList.offer(fiberId)
            hasFailedMemoryBlock = true
          }

          val reader: ParquetDataFiberReader = if (fiberCache.fiberCompressed) {
            ParquetDataFiberCompressedReader(fiberCache.getBaseOffset,
              columnarBatch.column(order).dataType(), rowCount, fiberCache)
          } else {
            if (!fiberCache.isFailedMemoryBlock()) {
              ParquetDataFiberReader(fiberCache.getBaseOffset,
                columnarBatch.column(order).dataType(), rowCount)
            } else {
              ParquetDataFaultFiberReader(fiberCache,
                columnarBatch.column(order).dataType(), rowCount)
            }
          }
          val end2 = System.nanoTime()
          loadDicTime += (end2 - start2)
          reader
        }
    }
    logDebug(s"load row group with cols = ${columnarBatch.numCols}, " +
      s"loadFiberTime = $loadFiberTime, loadDicTime = $loadDicTime")

    totalCountLoadedSoFar += rowCount
    currentRowGroupRowsReturned = 0
  }

  protected def initializeMetas(): Unit = {
    this.fileSchema = footer.getFileMetaData.getSchema
    val fileMetadata = footer.getFileMetaData.getKeyValueMetaData
    val readContext = new ParquetReadSupportWrapper()
      .init(new InitContext(configuration, Collections3.toSetMultiMap(fileMetadata), fileSchema))
    this.requestedSchema = readContext.getRequestedSchema
    val sparkRequestedSchemaString =
      configuration.get(ParquetReadSupportWrapper.SPARK_ROW_REQUESTED_SCHEMA)
    this.sparkSchema = StructType.fromString(sparkRequestedSchemaString)
    // we should not get all row Groups here
    val rowGroupMetas = footer.getBlocks.asScala
    if (file == null) {
      this.rowGroupMetaIter = rowGroupMetas.iterator
      for (block <- rowGroupMetas) {
        this.totalRowCount += block.getRowCount
      }
      return
    }

    var rowGourpList = List[BlockMetaData]()
    var currentOffset : Long = 0
    // refer parquet file format: https://github.com/apache/parquet-format#file-format
    val PARQUET_MAGIC_NUMBER = 4
    currentOffset += PARQUET_MAGIC_NUMBER
    // if a split file include a row group's head, this reader will read this row group.
    for (block <- rowGroupMetas) {
      if (file.start <= currentOffset && file.start + file.length >= currentOffset) {
        rowGourpList +:= block
        this.totalRowCount += block.getRowCount
      }
      currentOffset += block.getTotalByteSize
    }
    this.rowGroupMetaIter = rowGourpList.iterator
  }

  protected def initializeInternal(): Unit = {
    missingColumns = new Array[Boolean](requestedSchema.getFieldCount)
    (0 until requestedSchema.getFieldCount).foreach { i =>
      val t = requestedSchema.getFields.get(i)
      if (!t.isPrimitive || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new UnsupportedOperationException(s"Complex types ${t.getName} not supported.")
      }

      val colPath = requestedSchema.getPaths.get(i)
      if (fileSchema.containsPath(colPath)) {
        val fd = fileSchema.getColumnDescription(colPath)
        if (!(fd == requestedSchema.getColumns.get(i))) {
          throw new UnsupportedOperationException("Schema evolution not supported.")
        }
        missingColumns(i) = false
      }
      else {
        if (requestedSchema.getColumns.get(i).getMaxDefinitionLevel == 0) {
          // Column is missing in data but the required data is non-nullable.
          // This file is invalid.
          throw new IOException(s"Required column is missing in data file.Col: ${colPath.mkString}")
        }
        missingColumns(i) = true
      }
    }
  }

  def initBatch(memMode: MemoryMode, partitionColumns: StructType,
      partitionValues: InternalRow): Unit = {
    var batchSchema = new StructType
    for (f <- sparkSchema.fields) {
      batchSchema = batchSchema.add(f)
    }

    if (partitionColumns != null) for (f <- partitionColumns.fields) {
      batchSchema = batchSchema.add(f)
    }

    columnVectors = ColumnVectorAllocator.allocateColumns(memMode, defaultCapacity, batchSchema)

    columnarBatch = new ColumnarBatch(columnVectors)

    if (partitionColumns != null) {
      val partitionIdx = sparkSchema.fields.length
      for (i <- partitionColumns.fields.indices) {
        val writable = columnVectors(i + partitionIdx).asInstanceOf[WritableColumnVector]
        ColumnVectorUtils.populate(writable, partitionValues, i)
        writable.setIsConstant()
      }
    }

    for (i <- missingColumns.indices) {
      if (missingColumns(i)) {
        val writable = columnVectors(i).asInstanceOf[WritableColumnVector]
        writable.putNulls(0, defaultCapacity)
        writable.setIsConstant()
      }
    }
  }

  protected def nextBatchInternal(): Unit = {
    columnVectors.foreach(cv => cv.asInstanceOf[WritableColumnVector].reset())
    columnarBatch.setNumRows(0)

    val num = Math.min(defaultCapacity.toLong, totalCountLoadedSoFar - rowsReturned).toInt
    val start = System.nanoTime()

    for (i <- fiberReaders.indices) {
      if (fiberReaders(i) != null) {
          fiberReaders(i).readBatch(currentRowGroupRowsReturned, num, columnVectors(i)
            .asInstanceOf[OapOnHeapColumnVector])
      }
    }

    val end = System.nanoTime()
    logDebug(s"load batch with cols = ${columnarBatch.numCols()}, time = ${end -start}")
    rowsReturned += num
    columnarBatch.setNumRows(num)
    numBatched = num
    batchIdx = 0
    currentRowGroupRowsReturned += num

    if (rowsReturned == totalCountLoadedSoFar) {
      dataFile.releaseAll()
      clearFailedCache()
    }
  }

  protected def clearFailedCache(): Unit = {
    while (!failedMemoryBlockList.isEmpty) {
      val tempFiberId = failedMemoryBlockList.poll()
      OapRuntime.getOrCreate.fiberCacheManager.releaseFiber(tempFiberId)
    }
  }
}
