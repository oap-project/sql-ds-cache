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

package com.intel.oap.fs.hadoop.cachedfs.cacheutil;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public interface CacheManager {

  void init();

  void put(ObjectId id);

  FiberCache get(ObjectId id);

  Boolean contains(ObjectId id);

  void delete(ObjectId id);

  void status();

  FiberCache create(ObjectId id, Long length);

  void seal(ObjectId id);

  void release(ObjectId id);

  FiberCache reCreate(ObjectId id, Long length);

}

class HashHelper {
  static HashFunction hf = Hashing.murmur3_128();

  public static byte[] hash(byte[] key) {
    byte[] ret = new byte[20];
    hf.newHasher().putBytes(key).hash().writeBytesTo(ret, 0, 20);
    return ret;
  }

  public static byte[] hash(String key) {
    return hash(key.getBytes());
  }
}

