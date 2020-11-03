package com.intel.oap.fs.hadoop.cachedfs.redis;


import com.intel.oap.fs.hadoop.cachedfs.Constants;
import org.apache.hadoop.conf.Configuration;

/**
 * redis utils
 */
public class RedisUtils {

    /**
     * get redis client based on config
     * @param configuration Configuration
     * @return RedisClient
     */
    public static RedisClient getRedisClient(Configuration configuration) {
        String host = configuration.get(Constants.CONF_KEY_CACHED_FS_REDIS_HOST, Constants.DEFAULT_REDIS_HOST);
        int port = configuration.getInt(Constants.CONF_KEY_CACHED_FS_REDIS_PORT, Constants.DEFAULT_REDIS_PORT);
        String auth = configuration.get(Constants.CONF_KEY_CACHED_FS_REDIS_AUTH, Constants.DEFAULT_REDIS_AUTH);
        int maxTotal = configuration.getInt(Constants.CONF_KEY_CACHED_FS_REDIS_MAX_TOTAL, Constants.DEFAULT_REDIS_POOL_MAX_TOTAL);
        int maxIdle = configuration.getInt(Constants.CONF_KEY_CACHED_FS_REDIS_MAX_IDLE, Constants.DEFAULT_REDIS_POOL_MAX_IDLE);

        return RedisClient.getInstance(host, port, auth, maxTotal, maxIdle);
    }
}
