package com.intel.oap.fs.hadoop.cachedfs;

public interface PMemCacheStatisticsStore {
    void reset();

    void incrementCacheHit(int count);

    void incrementCacheMissed(int count);

    long getCacheHit();

    long getCacheMissed();
}
