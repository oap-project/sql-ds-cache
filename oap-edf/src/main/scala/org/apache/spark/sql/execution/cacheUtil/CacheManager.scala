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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.internal.SQLConf

trait CacheManager {

  def init(): Unit

  def put(id: ObjectId): Unit

  def get(id: ObjectId): FiberCache

  def contains(id: ObjectId): Boolean

  def delete(id: ObjectId): Unit

  def status(): Unit

  def create(id: ObjectId, length: Long): FiberCache

  def seal(id: ObjectId): Unit

  def release(id: ObjectId): Unit

}

private[sql] object CacheManagerFactory {

  private val lock = new Object()
  private var manager: CacheManager = _

  def getOrCreate(conf: SQLConf): CacheManager = {
    lock.synchronized {
      if (manager == null) {
        manager = createCacheManager(conf)
      }
      manager
    }
  }

  private def createCacheManager(conf: SQLConf): CacheManager = {
    // TODO: will use reflection to construct a new instance. For now, Let's just
    //  new a PlasmaCacheManager.
    new PlasmaCacheManager(conf)
  }

}
