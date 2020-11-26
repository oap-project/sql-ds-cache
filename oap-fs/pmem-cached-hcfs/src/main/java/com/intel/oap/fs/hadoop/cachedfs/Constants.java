package com.intel.oap.fs.hadoop.cachedfs;

public class Constants {
    public static final String HDFS_SCHEME = "hdfs";

    public static final String CACHED_FS_SCHEME = "cachedFs";

    public static final long DEFAULT_CACHED_BLOCK_SIZE = 1024 * 1024 * 16;

    public static final String DEFAULT_REDIS_HOST = "localhost";

    public static final int DEFAULT_REDIS_PORT = 6379;

    public static final String DEFAULT_REDIS_AUTH = "";

    public static final int DEFAULT_REDIS_POOL_MAX_TOTAL = 100;

    public static final int DEFAULT_REDIS_POOL_MAX_IDLE = 1000;

    public static final String CONF_KEY_CACHED_FS_BLOCK_SIZE = "fs.cachedFs.block.size";

    public static final String CONF_KEY_CACHED_FS_REDIS_HOST = "fs.cachedFs.redis.host";

    public static final String CONF_KEY_CACHED_FS_REDIS_PORT = "fs.cachedFs.redis.port";

    public static final String CONF_KEY_CACHED_FS_REDIS_AUTH = "fs.cachedFs.redis.auth";

    public static final String CONF_KEY_CACHED_FS_REDIS_MAX_TOTAL = "fs.cachedFs.redis.maxTotal";

    public static final String CONF_KEY_CACHED_FS_REDIS_MAX_IDLE = "fs.cachedFs.redis.maxIdle";

    public static final String REDIS_KEY_PMEM_CACHE_GLOBAL_STATISTICS_CACHE_HIT = "pmem_cache_global_cache_hit";

    public static final String REDIS_KEY_PMEM_CACHE_GLOBAL_STATISTICS_CACHE_MISSED = "pmem_cache_global_cache_missed";

    public static final long UNSAFE_COPY_MEMORY_STEP_LENGTH = 1024 * 1024;

    public static final String CONF_KEY_CACHED_FS_BLOCK_LOCATION_POLICY = "fs.cachedFs.blockLocation.policy";

    // default policy. file block locations consist of cached blocks and hdfs blocks (if cached blocks are incomplete)
    public static final String CACHE_LOCATION_POLICY_DEFAULT = "default";
    public static final String CACHE_LOCATION_POLICY_MERGING_HDFS = "cache_merging_hdfs";

    // use cached block location only if all requested content is cached, otherwise use HDFS block locations.
    public static final String CACHE_LOCATION_POLICY_OVER_HDFS = "cache_over_hdfs";

    // use HDFS file block locations directly. ignoring cached blocks when finding file block locations
    public static final String CACHE_LOCATION_POLICY_HDFS_ONLY = "hdfs_only";
}
