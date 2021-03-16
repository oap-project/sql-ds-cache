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

package com.intel.oap.fs.hadoop.ape.hcfs.redis;


import com.intel.oap.fs.hadoop.ape.hcfs.Constants;
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
        String host = configuration.get(Constants.CONF_KEY_FS_APE_HCFS_REDIS_HOST,
                Constants.DEFAULT_REDIS_HOST);
        int port = configuration.getInt(Constants.CONF_KEY_FS_APE_HCFS_REDIS_PORT,
                Constants.DEFAULT_REDIS_PORT);
        String auth = configuration.get(Constants.CONF_KEY_FS_APE_HCFS_REDIS_AUTH,
                Constants.DEFAULT_REDIS_AUTH);
        int maxTotal = configuration.getInt(Constants.CONF_KEY_FS_APE_HCFS_REDIS_MAX_TOTAL,
                Constants.DEFAULT_REDIS_POOL_MAX_TOTAL);
        int maxIdle = configuration.getInt(Constants.CONF_KEY_FS_APE_HCFS_REDIS_MAX_IDLE,
                Constants.DEFAULT_REDIS_POOL_MAX_IDLE);

        return RedisClient.getInstance(host, port, auth, maxTotal, maxIdle);
    }
}
