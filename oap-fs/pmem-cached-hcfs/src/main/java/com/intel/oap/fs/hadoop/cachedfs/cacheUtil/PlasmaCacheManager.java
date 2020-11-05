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

package com.intel.oap.fs.hadoop.cachedfs.cacheUtil;

import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.*;


public class PlasmaCacheManager implements CacheManager {
  private PlasmaClient client;

  public void init() {
    try {
      System.loadLibrary("plasma_java");
    } catch(Exception e) {

    }
    // TODO: get socket via conf(hadoopConf/SparkConf)
    client = new PlasmaClient("/tmp/plasmaStore", "", 0);
  }

  public void put(ObjectId id) {
    throw new UnsupportedOperationException("Not support yet");
  }

  public FiberCache get(ObjectId id) {
    // TODO: what if get an unsealed object? Let's throw an exception here,
    //  higher level should catch this exception and do some fall back.
    try {
      // TODO: should not return a ArrowFiberCache directly
      return new SimpleFiberCache(client.getObjAsByteBuffer(id.toByteArray(), -1, false));
    } catch(PlasmaGetException e) {
        throw new CacheManagerException("Plasma exception:" + e.getMessage());
    }
  }

  public Boolean contains(ObjectId id) {
    return client.contains(id.toByteArray());
  }

  public void delete(ObjectId id) {
    client.delete(id.toByteArray());
  }

  public void status() {
    throw new UnsupportedOperationException("Not support yet");
  }

  public FiberCache create(ObjectId id, Long length) {
    try {
      // TODO: We should extend plasma.create to support larger size object.
      if (length > Integer.MAX_VALUE) {
        throw new ArithmeticException("Can't create $length bytes Object");
      }
      return new SimpleFiberCache(client.create(id.toByteArray(), length.intValue()));
    } catch (DuplicateObjectException | PlasmaOutOfMemoryException e) {
        throw new CacheManagerException("Plasma exception:" + e.getMessage());
    }

  }

  public void seal(ObjectId id) {
    try {
      client.seal(id.toByteArray());
    } catch (PlasmaClientException e) {
      // TODO: print some log
    }
  }

  public void release(ObjectId id) {
    client.release(id.toByteArray());
  }

  public FiberCache reCreate(ObjectId id, Long length) {
    seal(id);
    release(id);
    delete(id);
    return create(id, length);
  }
}
