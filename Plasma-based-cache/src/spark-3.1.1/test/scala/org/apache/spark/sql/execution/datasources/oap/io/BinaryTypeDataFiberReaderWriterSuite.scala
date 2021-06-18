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

import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.execution.datasources.oap.filecache.{FiberCache, MemoryBlockHolder, SourceEnum}
import org.apache.spark.sql.execution.datasources.parquet.ParquetDictionaryWrapper
import org.apache.spark.sql.execution.vectorized.{Dictionary, OapOnHeapColumnVector}
import org.apache.spark.sql.types.BinaryType


class BinaryTypeDataFiberReaderWriterSuite extends DataFiberReaderWriterSuite {



  // binary type use BinaryDictionary
  protected val dictionary: Dictionary = new ParquetDictionaryWrapper(
    BinaryDictionary(Array(Binary.fromString("oap"),
    Binary.fromString("parquet"),
    Binary.fromString("orc"))))

  test("no dic no nulls in ParquetDataFaultFiberReader") {
    // write data
    val column = new OapOnHeapColumnVector(total, BinaryType)
    (0 until total).foreach(i => column.putByteArray(i, i.toString.getBytes))
    val fc : FiberCache = ParquetDataFiberWriter.dumpToCache(column, total)
    fiberCache = FiberCache(fc.fiberType, MemoryBlockHolder(
      null, 0L, 0L, 0L, SourceEnum.DRAM))
    fiberCache.column = column

    // init reader
    val address = fiberCache.getBaseOffset
    val faultReader = ParquetDataFaultFiberReader(fiberCache, BinaryType, total)

    // read use batch api
    val ret1 = new OapOnHeapColumnVector(total, BinaryType)
    faultReader.readBatch(start, num, ret1)
    (0 until num).foreach(i => {
      assert(ret1.getBinary(i).sameElements((i + start).toString.getBytes))
    })

    /* To be improved for batch read with row id list for fallback reader
    // read use random access api
    val ret2 = new OnHeapColumnVector(total, BinaryType)
    faultReader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => {
      assert(ret2.getBinary(i).sameElements(ints(i).toString.getBytes))
    })
    */
  }

  test("no dic no nulls") {
    // write data
    val column = new OapOnHeapColumnVector(total, BinaryType)
    (0 until total).foreach(i => column.putByteArray(i, i.toString.getBytes))
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, BinaryType, total)

    // read use batch api
    val ret1 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => {
      assert(ret1.getBinary(i).sameElements((i + start).toString.getBytes))
    })

    // read use random access api
    val ret2 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => {
      assert(ret2.getBinary(i).sameElements(ints(i).toString.getBytes))
    })
  }

  test("with dic no nulls") {
    // write data
    val column = new OapOnHeapColumnVector(total, BinaryType)
    column.reserveDictionaryIds(total)
    val dictionaryIds = column.getDictionaryIds.asInstanceOf[OapOnHeapColumnVector]
    column.setDictionary(dictionary)
    (0 until total).foreach(i => dictionaryIds.putInt(i, i % column.dictionaryLength ))
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, BinaryType, total)

    // read use batch api
    val ret1 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => {
      val idx = (i + start) % column.dictionaryLength
      assert(ret1.getBinary(i).sameElements(dictionary.decodeToBinary(idx)))
    })

    // read use random access api
    val ret2 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => {
      val idx = ints(i) % column.dictionaryLength
      assert(ret2.getBinary(i).sameElements(dictionary.decodeToBinary(idx)))
    })
  }

  test("no dic all nulls") {
    // write data
    val column = new OapOnHeapColumnVector(total, BinaryType)
    column.putNulls(0, total)
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, BinaryType, total)

    // read use batch api
    val ret1 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => assert(ret1.isNullAt(i)))

    // read use random access api
    val ret2 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => assert(ret2.isNullAt(i)))
  }

  test("with dic all nulls") {
    // write data
    val column = new OapOnHeapColumnVector(total, BinaryType)
    column.reserveDictionaryIds(total)
    column.setDictionary(dictionary)
    column.putNulls(0, total)
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, BinaryType, total)

    // read use batch api
    val ret1 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => assert(ret1.isNullAt(i)))

    // read use random access api
    val ret2 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => assert(ret2.isNullAt(i)))
  }

  test("no dic") {
    // write data
    val column = new OapOnHeapColumnVector(total, BinaryType)
    (0 until total).foreach(i => {
      if (i % 3 == 0) column.putNull(i)
      else column.putByteArray(i, i.toString.getBytes)
    })
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, BinaryType, total)

    // read use batch api
    val ret1 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => {
      if ((i + start) % 3 == 0) assert(ret1.isNullAt(i))
      else assert(ret1.getBinary(i).sameElements((i + start).toString.getBytes))
    })

    // read use random access api
    val ret2 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => {
      if ((i + start) % 3 == 0) assert(ret2.isNullAt(i))
      else assert(ret2.getBinary(i).sameElements(ints(i).toString.getBytes))
    })
  }

  test("with dic") {
    // write data
    val column = new OapOnHeapColumnVector(total, BinaryType)
    column.reserveDictionaryIds(total)
    val dictionaryIds = column.getDictionaryIds.asInstanceOf[OapOnHeapColumnVector]
    column.setDictionary(dictionary)
    (0 until total).foreach(i => {
      if (i % 3 == 0) column.putNull(i)
      else dictionaryIds.putInt(i, i % column.dictionaryLength)
    })
    fiberCache = ParquetDataFiberWriter.dumpToCache(column, total)

    // init reader
    val address = fiberCache.getBaseOffset
    val reader = ParquetDataFiberReader(address, BinaryType, total)

    // read use batch api
    val ret1 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(start, num, ret1)
    (0 until num).foreach(i => {
      if ((i + start) % 3 == 0) assert(ret1.isNullAt(i))
      else {
        val idx = (i + start) % column.dictionaryLength
        assert(ret1.getBinary(i).sameElements(dictionary.decodeToBinary(idx)))
      }
    })

    // read use random access api
    val ret2 = new OapOnHeapColumnVector(total, BinaryType)
    reader.readBatch(rowIdList, ret2)
    ints.indices.foreach(i => {
      if ((i + start) % 3 == 0) assert(ret2.isNullAt(i))
      else {
        val idx = ints(i) % column.dictionaryLength
        assert(ret2.getBinary(i).sameElements(dictionary.decodeToBinary(idx)))
      }
    })
  }
}
