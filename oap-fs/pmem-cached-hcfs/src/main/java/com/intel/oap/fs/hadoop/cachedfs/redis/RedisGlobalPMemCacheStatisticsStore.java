package com.intel.oap.fs.hadoop.cachedfs.redis;

import com.intel.oap.fs.hadoop.cachedfs.Constants;
import com.intel.oap.fs.hadoop.cachedfs.PMemCacheStatisticsStore;
import org.apache.hadoop.conf.Configuration;

public class RedisGlobalPMemCacheStatisticsStore implements PMemCacheStatisticsStore {
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
                ex.printStackTrace();
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
                ex.printStackTrace();
            }
        }

        return 0;
    }
}
