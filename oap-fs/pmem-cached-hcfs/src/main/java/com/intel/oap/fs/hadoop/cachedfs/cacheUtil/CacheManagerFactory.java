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

public class CacheManagerFactory {
  private static final Object lock = new Object();
  private static CacheManager manager;

  public static CacheManager getOrCreate() {
    synchronized (lock) {
      if(manager == null) {
        manager = createCacheManager();
        manager.init();
      }
      return manager;
    }
  }

  private static CacheManager createCacheManager() {
    // TODO: will use reflection to construct a new instance. For now, Let's just
    //  new a PlasmaCacheManager.
    return new PlasmaCacheManager();
  }
}
