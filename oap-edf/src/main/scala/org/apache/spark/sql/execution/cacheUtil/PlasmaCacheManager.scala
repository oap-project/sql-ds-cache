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

import org.apache.arrow.plasma.PlasmaClient
import org.apache.arrow.plasma.exceptions.{DuplicateObjectException, PlasmaClientException, PlasmaGetException}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

class PlasmaCacheManager(conf: SQLConf) extends CacheManager with Logging {

  // TODO: do we need a pool here? If we need maintain a pool, we have to record a
  //  client-object map since one object have to be sealed by the client which create it.
  //  And multi clients will not bring perf gain.
  private var client: PlasmaClient = _

  init()

  override def init(): Unit = {
    // TODO: get socket via SQLConf
    // val socketName = sqlConf.getConf("socket")
    client = new PlasmaClient("/tmp/plasmaStore", "", 0)
  }

  override def put(id: ObjectId): Unit = {
    throw new UnsupportedOperationException("Not support yet")
  }

  override def get(id: ObjectId): FiberCache = {
    // TODO: what if get an unsealed object? Let's throw an exception here,
    //  higher level should catch this exception and do some fall back.
    try {
      new OapFiberCache(client.getObjAsByteBuffer(id.toByteArray(), -1, false));
    } catch {
      case e: PlasmaGetException =>
        logWarning("Plasma get exception: " + e.getMessage + ". Please catch a  exception" +
          " and provide a fallback.")
        throw new CacheManagerException("Plasma exception:" + e.getMessage)
    }
  }

  override def contains(id: ObjectId): Boolean = {
    client.contains(id.toByteArray())
  }

  override def delete(id: ObjectId): Unit = {
    throw new UnsupportedOperationException("Not support yet")
  }

  override def status(): Unit = {
    throw new UnsupportedOperationException("Not support yet")
  }

  override def create(id: ObjectId, length: Int): FiberCache = {
    try {
      new OapFiberCache(client.create(id.toByteArray(), length))
    } catch {
      case e: DuplicateObjectException =>
        // TODO: since we only have one client maybe we should not call get here?
        logWarning("Plasma object duplicate: " + e.getMessage +
        ". Will try to get this object.")
        get(id)
    }
  }

  override def seal(id: ObjectId): Unit = {
    try {
      client.seal(id.toByteArray())
    } catch {
      case e: PlasmaClientException =>
        logWarning(" Plasma Seal object error: " + e.getMessage)
    }
  }
}
