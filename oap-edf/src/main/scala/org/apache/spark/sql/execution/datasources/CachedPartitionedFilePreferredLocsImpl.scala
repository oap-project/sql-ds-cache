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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Partition, SparkEnv}
import org.apache.spark.sql.internal.edf.EdfConf
import org.apache.spark.util.Utils

object CachedPartitionedFilePreferredLocsImpl extends PartitionedFilePreferredLocs {

  private var externalDBClient: ExternalDBClient = null

  private def init() = synchronized {
    if (null == externalDBClient) {
      externalDBClient = Utils
        .classForName(SparkEnv.get.conf.get(EdfConf.EDF_EXTERNAL_DB))
        .getConstructor()
        .newInstance()
        .asInstanceOf[ExternalDBClient]
      externalDBClient.init(SparkEnv.get)
    }
  }

  override def getPreferredLocs(split: Partition): Seq[String] = {
    if (null == externalDBClient) {
      init
    }

    val files = split.asInstanceOf[FilePartition].files
    var preferredLocs = new ArrayBuffer[String]

    // total cachedBytes on one host
    val hostToCachedBytes = mutable.HashMap.empty[String, Long]

    files.foreach { file =>
      val cacheMetaInfoValueArr = externalDBClient.get(file.filePath, file.start, file.length)
      if (cacheMetaInfoValueArr.size > 0) {
        // host<->cachedBytes
        cacheMetaInfoValueArr.foreach(x => {
          hostToCachedBytes.put(x._host, hostToCachedBytes.getOrElse(x._host, 0L) + x._length)
        })
      }
    }

    // TODO if cachedBytes <<< hdfsPreferLocBytes
    hostToCachedBytes.toSeq.sortWith(_._2 > _._2).take(3).foreach(x => preferredLocs.+=(x._1))
    val hdfsPreferLoc = hdfsPreferredLocs(split)

    hdfsPreferLoc.foreach(x => preferredLocs.+=(x))
    preferredLocs.take(3)
  }

  def hdfsPreferredLocs(split: Partition): Seq[String] = {
    val files = split.asInstanceOf[FilePartition].files
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }
  }
}
