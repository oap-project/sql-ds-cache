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

import java.nio.charset.Charset

import scala.collection.mutable.ArrayBuffer

import io.etcd.jetcd.{ByteSequence, Client, KV}
import io.etcd.jetcd.options.GetOption
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.internal.edf.EdfConf

class EtcdClient extends ExternalDBClient {

  private var kvCLient: KV = null

  private implicit val formats = DefaultFormats

  override def init(sparkEnv: SparkEnv): Unit = {
    kvCLient = Client.builder()
      .endpoints(sparkEnv.conf.get(EdfConf.EDF_EXTERNAL_DB_SERVER))
      .build()
      .getKVClient
  }

  override def get(fileName: String, start: Long, length: Long): ArrayBuffer[CacheMetaInfoValue] = {
    val cacheMetaInfoArrayBuffer: ArrayBuffer[CacheMetaInfoValue] =
      new ArrayBuffer[CacheMetaInfoValue](0)

    val getOption: GetOption = GetOption.newBuilder()
      .withPrefix(ByteSequence.from(fileName.getBytes()))
      .withSortOrder(GetOption.SortOrder.ASCEND)
      .build()

    val getResponse = kvCLient.get(ByteSequence.from(fileName.getBytes()), getOption).get
    val kvs = getResponse.getKvs
    // TODO search the startpoint more efficiently like binary search
    for (i <- 0 to kvs.size()) {
      val key = kvs.get(i).getKey.toString(Charset.defaultCharset())
      val index = key.indexOf("offset") + 6
      val offset = key.substring(index).asInstanceOf[Long]
      if (offset > length) return cacheMetaInfoArrayBuffer
      if (offset >= start && offset <= length) {
        cacheMetaInfoArrayBuffer.+=(parse(kvs.get(i).getValue.toString(Charset.defaultCharset())).
          extract[CacheMetaInfoValue])
      }
    }
    cacheMetaInfoArrayBuffer
  }

  override def upsert(cacheMetaInfo: CacheMetaInfo): Boolean = {
    cacheMetaInfo match {
      case storeInfo: StoreCacheMetaInfo =>
        val value = storeInfo._value
        val cacheMetaInfoJson = ("offSet" -> value._offSet) ~
          ("length" -> value._length) ~
          ("host" -> value._host)
        kvCLient.put(ByteSequence.from((storeInfo._key + "offset" + value._offSet)
          .getBytes()), ByteSequence.from(compact(render(cacheMetaInfoJson)).getBytes())).isDone
      case evictInfo: EvictCacheMetaInfo =>
        val value = evictInfo._value
        kvCLient.delete(ByteSequence.from((evictInfo._key + "offset" + value._offSet)
          .getBytes())).isDone
    }
  }

  override def stop(): Unit = {
    kvCLient.close()
  }
}
