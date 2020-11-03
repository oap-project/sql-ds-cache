package com.intel.oap.fs.hadoop.cachedfs;

public class Constants {
    public static final String HDFS_SCHEME = "hdfs";

    public static final String CACHED_FS_SCHEME = "cachedFs";

    public static final long DEFAULT_CACHED_BLOCK_SIZE = 1024 * 1024 * 20;

    public static final String DEFAULT_REDIS_HOST = "localhost";

    public static final int DEFAULT_REDIS_PORT = 6379;

    public static final String DEFAULT_REDIS_AUTH = "";

    public static final int DEFAULT_REDIS_POOL_MAX_TOTAL = 100;

    public static final int DEFAULT_REDIS_POOL_MAX_IDLE = 1000;

    public static final String CONF_KEY_CACHED_FS_BLOCK_SIZE = "fs.cachedFs.block.size";

    public static final String CONF_KEY_CACHED_FS_REDIS_HOST = "fs.cachedFs.redis.host";

    public static final String CONF_KEY_CACHED_FS_REDIS_PORT = "fs.cachedFs.redis.port";

    public static final String CONF_KEY_CACHED_FS_REDIS_AUTH = "fs.cachedFs.redis.auth";

    public static final String CONF_KEY_CACHED_FS_REDIS_MAX_TOTAL = "fs.cachedFs.redis.max_total";

    public static final String CONF_KEY_CACHED_FS_REDIS_MAX_IDLE = "fs.cachedFs.redis.max_idle";
}
