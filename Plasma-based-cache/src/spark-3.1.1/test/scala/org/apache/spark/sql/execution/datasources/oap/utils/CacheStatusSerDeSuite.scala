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

package org.apache.spark.sql.execution.datasources.oap.utils

import scala.collection.mutable.ArrayBuffer

import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheStatus
import org.apache.spark.util.collection.OapBitSet

class CacheStatusSerDeSuite extends SparkFunSuite {
  private def assertStringEquals(json1: String, json2: String) {
    val formatJsonString = (json: String) => json.replaceAll("[\\s|]", "")
    assert(formatJsonString(json1) === formatJsonString(json2),
      s"input ${formatJsonString(json1)} got ${formatJsonString(json2)}")
  }

  test("test BitSet Json") {
    val bitSet = new OapBitSet(100)
    bitSet.set(3)
    bitSet.set(8)
    val bitSetStr = compact(render(CacheStatusSerDe.bitSetToJson(bitSet)))
    assertStringEquals(bitSetStr, CacheStatusSerDeTestStrs.bitSetString)
    val newBitSet = CacheStatusSerDe.bitSetFromJson(parse(bitSetStr))
    assertBitSetEquals(bitSet, newBitSet)
  }

  test("test status raw data") {
    val path = "file1"
    val bitSet = new OapBitSet(90)
    bitSet.set(3)
    bitSet.set(8)
    val groupCount = 30
    val fieldCount = 3
    val rawData = FiberCacheStatus(path, bitSet, groupCount, fieldCount)
    val newRawData =
      CacheStatusSerDe.statusRawDataFromJson(CacheStatusSerDe.statusRawDataToJson(rawData))
    assertStatusRawDataEquals(rawData, newRawData)
  }

  test("test ser and deser") {
    val rawDataArray = new ArrayBuffer[FiberCacheStatus]()
    val path1 = "file1"
    val path2 = "file2"
    val bitSet1 = new OapBitSet(90)
    val bitSet2 = new OapBitSet(150)
    bitSet1.set(3)
    bitSet1.set(8)
    bitSet2.set(5)
    bitSet2.set(6)
    val groupCount1 = 30
    val groupCount2 = 50
    val fieldCount1 = 3
    val fieldCount2 = 3
    rawDataArray += FiberCacheStatus(path1, bitSet1, groupCount1, fieldCount1)
    rawDataArray += FiberCacheStatus(path2, bitSet2, groupCount2, fieldCount2)
    val statusRawDataArrayStr = CacheStatusSerDe.serialize(rawDataArray)
    assertStringEquals(statusRawDataArrayStr, CacheStatusSerDeTestStrs.statusRawDataArrayString)
    val deserRawDataArr = CacheStatusSerDe.deserialize(statusRawDataArrayStr)
    assert(deserRawDataArr.length === rawDataArray.length)
    var i = 0
    while (i < deserRawDataArr.length) {
      assertStatusRawDataEquals(deserRawDataArr(i), rawDataArray(i))
      i += 1
    }
  }

  private def assertBitSetEquals(bitSet1: OapBitSet, bitSet2: OapBitSet) {
    assert(bitSet1.cardinality() === bitSet2.cardinality())
    assert(bitSet1.nextSetBit(0) === bitSet2.nextSetBit(0))
    assert(bitSet1.nextSetBit(5) === bitSet2.nextSetBit(5))
    val longArray1 = bitSet1.toLongArray()
    val longArray2 = bitSet2.toLongArray()
    assert(longArray1.length === longArray2.length)
    var i = 0
    while(i < longArray1.length) {
      assert(longArray1(i) === longArray2(i))
      i += 1
    }
  }

  private def assertStatusRawDataEquals(data1: FiberCacheStatus, data2: FiberCacheStatus): Unit = {
    assert(data1.file === data2.file)
    assertBitSetEquals(data1.bitmask, data2.bitmask)
    assert(data1.groupCount === data2.groupCount)
    assert(data1.fieldCount === data2.fieldCount)
  }

}

private[oap] object CacheStatusSerDeTestStrs {
  val bitSetString =
    s"""
       |{
       |  "bitSet" : [
       |    {
       |      "word" : 264
       |    },
       |    {
       |      "word" : 0
       |    }
       |  ]
       |}
     """

  val statusRawDataArrayString =
    s"""
       |{
       |  "statusRawDataArray" : [
       |    {
       |      "fiberFilePath" : "file1",
       |      "bitSetJValue" : {
       |        "bitSet" : [
       |          {
       |            "word" : 264
       |          },
       |          {
       |            "word" : 0
       |          }
       |        ]
       |      },
       |      "groupCount" : 30,
       |      "fieldCount" : 3
       |    },
       |    {
       |      "fiberFilePath" : "file2",
       |      "bitSetJValue" : {
       |        "bitSet" : [
       |          {
       |            "word" : 96
       |          },
       |          {
       |            "word" : 0
       |          },
       |          {
       |            "word" : 0
       |          }
       |        ]
       |      },
       |      "groupCount" : 50,
       |      "fieldCount" : 3
       |    }
       |  ]
       |}
     """
}
