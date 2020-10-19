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
import org.apache.arrow.plasma.exceptions.{DuplicateObjectException, PlasmaClientException, PlasmaGetException, PlasmaOutOfMemoryException}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

class PlasmaCacheManager(sparkEnv: SparkEnv) extends CacheManager with Logging {

  // TODO: do we need a pool here? If we need maintain a pool, we have to record a
  //  client-object map since one object have to be sealed by the client which create it.
  //  And multi clients will not bring perf gain.
  private var client: PlasmaClient = _

  init()

  override def init(): Unit = {
    try {
      System.loadLibrary("plasma_java")
    } catch {
      case e: Exception => logError(s"load plasma jni lib failed " + e.getMessage)
    }
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
      // TODO: should not return a ArrowFiberCache directly
      new ArrowFiberCache(client.getObjAsByteBuffer(id.toByteArray(), -1, false));
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

  override def create(id: ObjectId, length: Long): FiberCache = {
    try {
      // TODO: We should extend plasma.create to support larger size object.
      if (length > Int.MaxValue) {
        throw new ArithmeticException(s"Can't create $length bytes Object")
      }
      new ArrowFiberCache(client.create(id.toByteArray(), length.toInt))
    } catch {
      case e: DuplicateObjectException =>
        // TODO: since we only have one client maybe we should not call get here?
        logWarning("Plasma object duplicate: " + e.getMessage +
        ". Please fallback.")
        throw new CacheManagerException("Plasma exception:" + e.getMessage)
      case e: PlasmaOutOfMemoryException =>
        logWarning("Plasma Server is OutOfMemory! " + e.getMessage)
        throw new CacheManagerException("Plasma exception:" + e.getMessage)
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

  override def release(id: ObjectId): Unit = {
    client.release(id.toByteArray())
  }
}
