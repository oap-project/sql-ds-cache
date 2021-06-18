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

import org.json4s.{DefaultFormats, StringInput}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheStatus
import org.apache.spark.util.collection.OapBitSet

/**
 * This is user defined Json protocol for SerDe, here the format of Json output should like
 * following:
 *   {"statusRawDataArray" :
 *     ["fiberFilePath" : ""
 *      "bitSetJValue" :
 *        {"bitSet" :
 *          ["word" : Long,
 *           "word" : Long,
 *           "word" : Long, ...]}
 *      "dataFileMetaJValue" : {
 *        "rowCountInEachGroup" : Int
 *        "rowCountInLastGroup" : Int
 *        "groupCount" : Int
 *        "fieldCount" : Int
 *      }]
 *     []...[]}
 */
private[oap] object CacheStatusSerDe extends SerDe[String, Seq[FiberCacheStatus]] {
  import org.json4s.jackson.JsonMethods._

  override def serialize(statusRawDataArray: Seq[FiberCacheStatus]): String = {
    val statusJArray = JArray(statusRawDataArray.map(statusRawDataToJson).toList)
    compact(render("statusRawDataArray" -> statusJArray))
  }

  private implicit val format = DefaultFormats

  override def deserialize(json: String): Seq[FiberCacheStatus] = {
    (parse(StringInput(json), false) \ "statusRawDataArray")
      .extract[List[JValue]].map(statusRawDataFromJson)
  }

  private[oap] def bitSetToJson(bitSet: OapBitSet): JValue = {
    val words: Array[Long] = bitSet.toLongArray()
    val bitSetJson = JArray(words.map(word => ("word" -> word): JValue).toList)
    ("bitSet" -> bitSetJson)
  }

  private[oap] def bitSetFromJson(json: JValue): OapBitSet = {
    val words: Array[Long] = (json \ "bitSet").extract[List[JValue]].map { word =>
      (word \ "word").extract[Long]
    }.toArray[Long]
    new OapBitSet(words)
  }

  private[oap] def statusRawDataToJson(statusRawData: FiberCacheStatus): JValue = {
    ("fiberFilePath" -> statusRawData.file) ~
      ("bitSetJValue" -> bitSetToJson(statusRawData.bitmask)) ~
      ("groupCount" -> statusRawData.groupCount) ~
      ("fieldCount" -> statusRawData.fieldCount)
  }

  private[oap] def statusRawDataFromJson(json: JValue): FiberCacheStatus = {
    val path = (json \ "fiberFilePath").extract[String]
    val bitSet = bitSetFromJson(json \ "bitSetJValue")
    val groupCount = (json \ "groupCount").extract[Int]
    val fieldCount = (json \ "fieldCount").extract[Int]
    FiberCacheStatus(path, bitSet, groupCount, fieldCount)
  }
}
