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

package com.intel.oap.fs.hadoop.ape.hcfs;

public class Constants {

    public static final String CONF_KEY_FS_APE_HCFS_BLOCK_LOCATION_POLICY =
            "fs.ape.hcfs.blockLocation.policy";

    // default policy. file block locations consist of cached blocks
    // and hdfs blocks (if cached blocks are incomplete)
    public static final String CACHE_LOCATION_POLICY_DEFAULT = "default";
    public static final String CACHE_LOCATION_POLICY_MERGING_HDFS = "cache_merging_hdfs";

    // use cached block location only if all requested content is cached,
    // otherwise use HDFS block locations.
    public static final String CACHE_LOCATION_POLICY_OVER_HDFS = "cache_over_hdfs";

    // use HDFS file block locations directly.
    // ignoring cached blocks when finding file block locations
    public static final String CACHE_LOCATION_POLICY_HDFS_ONLY = "hdfs_only";

    public static final String DEFAULT_REDIS_HOST = "localhost";

    public static final int DEFAULT_REDIS_PORT = 6379;

    public static final String DEFAULT_REDIS_AUTH = "";

    public static final int DEFAULT_REDIS_POOL_MAX_TOTAL = 100;

    public static final int DEFAULT_REDIS_POOL_MAX_IDLE = 1000;

    public static final String CONF_KEY_FS_APE_HCFS_REDIS_HOST = "fs.ape.hcfs.redis.host";

    public static final String CONF_KEY_FS_APE_HCFS_REDIS_PORT = "fs.ape.hcfs.redis.port";

    public static final String CONF_KEY_FS_APE_HCFS_REDIS_AUTH = "fs.ape.hcfs.redis.auth";

    public static final String CONF_KEY_FS_APE_HCFS_REDIS_MAX_TOTAL = "fs.ape.hcfs.redis.maxTotal";

    public static final String CONF_KEY_FS_APE_HCFS_REDIS_MAX_IDLE = "fs.ape.hcfs.redis.maxIdle";

    // configuration name of cache key prefix in redis for column chunk cache.
    public static final String CONF_KEY_REDIS_KEY_PREFIX_COLUMN_CHUNK =
            "fs.ape.hcfs.redis.keyPrefix.columnChunk";

    // default value of cache key prefix in redis for column chunk cache.
    public static final String DEFAULT_REDIS_KEY_PREFIX_COLUMN_CHUNK = "";

}
