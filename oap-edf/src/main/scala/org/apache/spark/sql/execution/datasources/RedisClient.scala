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

import scala.collection.mutable.ArrayBuffer

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.edf.EdfConf

class RedisClient extends ExternalDBClient with Logging {

  private var redisClient: Jedis = null

  private implicit val formats = DefaultFormats

  override def init(sparkEnv: SparkEnv): Unit = {
    logInfo("Initing RedisClient, server address is : " +
      sparkEnv.conf.get(EdfConf.EDF_EXTERNAL_DB_SERVER))
    redisClient = new Jedis(sparkEnv.conf.get(EdfConf.EDF_EXTERNAL_DB_SERVER))
  }

  override def get(fileName: String, start: Long,
                   length: Long): ArrayBuffer[CacheMetaInfoValue] = {
    val cacheMetaInfoArrayBuffer: ArrayBuffer[CacheMetaInfoValue] =
      new ArrayBuffer[CacheMetaInfoValue](0)
    // start - 1 because zrange is (start, length]
    val cacheMetaInfoValueSet = redisClient.zrange(fileName, start - 1, length)
    for (x <- cacheMetaInfoValueSet.asInstanceOf[Set[String]]) {
      cacheMetaInfoArrayBuffer.+=(parse(x.asInstanceOf[String]).extract[CacheMetaInfoValue])
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
        redisClient
          .zadd(storeInfo._key.asInstanceOf[String], value._offSet, compact(render(cacheMetaInfoJson)))
          .equals(1L)
      case evictInfo: EvictCacheMetaInfo =>
        val value = evictInfo._value
        val cacheMetaInfoJson = ("offSet" -> value._offSet) ~
          ("length" -> value._length) ~
          ("host" -> value._host)
        redisClient.zrem(evictInfo._key.asInstanceOf[String], compact(render(cacheMetaInfoJson)))
          .equals(1L)
    }
  }

  override def stop(): Unit = {
    redisClient.close()
  }
}
