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

package com.intel.oap.fs.hadoop.cachedfs.redis;

import com.intel.oap.fs.hadoop.cachedfs.Constants;
import com.intel.oap.fs.hadoop.cachedfs.PMemCacheStatisticsStore;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisGlobalPMemCacheStatisticsStore implements PMemCacheStatisticsStore {
    private static final Logger LOG =
            LoggerFactory.getLogger(RedisGlobalPMemCacheStatisticsStore.class);

    private final Configuration conf;

    public RedisGlobalPMemCacheStatisticsStore(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void reset() {
        RedisUtils
                .getRedisClient(this.conf)
                .set(Constants.REDIS_KEY_PMEM_CACHE_GLOBAL_STATISTICS_CACHE_HIT, "0");

        RedisUtils
                .getRedisClient(this.conf)
                .set(Constants.REDIS_KEY_PMEM_CACHE_GLOBAL_STATISTICS_CACHE_MISSED, "0");
    }

    @Override
    public void incrementCacheHit(int count) {
        RedisUtils
                .getRedisClient(this.conf)
                .incrBy(Constants.REDIS_KEY_PMEM_CACHE_GLOBAL_STATISTICS_CACHE_HIT, count);
    }

    @Override
    public void incrementCacheMissed(int count) {
        RedisUtils
                .getRedisClient(this.conf)
                .incrBy(Constants.REDIS_KEY_PMEM_CACHE_GLOBAL_STATISTICS_CACHE_MISSED, count);
    }

    @Override
    public long getCacheHit() {
        String result = RedisUtils
                .getRedisClient(this.conf)
                .get(Constants.REDIS_KEY_PMEM_CACHE_GLOBAL_STATISTICS_CACHE_HIT);

        if (result != null) {
            try {
                return Long.parseLong(result);
            } catch (Exception ex) {
                LOG.error("exception when parse cache hit count: {}", ex.toString());
                throw ex;
            }
        }

        return 0;
    }

    @Override
    public long getCacheMissed() {
        String result = RedisUtils
                .getRedisClient(this.conf)
                .get(Constants.REDIS_KEY_PMEM_CACHE_GLOBAL_STATISTICS_CACHE_MISSED);

        if (result != null) {
            try {
                return Long.parseLong(result);
            } catch (Exception ex) {
                LOG.error("exception when parse cache missed count: {}", ex.toString());
                throw ex;
            }
        }

        return 0;
    }
}
