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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.intel.oap.fs.hadoop.ape.hcfs.CacheLocation;
import com.intel.oap.fs.hadoop.ape.hcfs.CacheLocationStore;
import com.intel.oap.fs.hadoop.ape.hcfs.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import redis.clients.jedis.Tuple;

/**
 * Class for storage of block locations in redis.
 */
public class RedisCacheLocationStore implements CacheLocationStore {
    private static final String REDIS_ZSET_VALUE_DELIM = "_";

    private final Configuration conf;

    public RedisCacheLocationStore(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public CacheLocation[] getSortedColumnChunkLocations(Path path, long start, long len) {
        List<CacheLocation> result = new ArrayList<>();

        // redis key prefix for column chunk locations
        String redisKeyPrefix = conf.get(
                        Constants.CONF_KEY_REDIS_KEY_PREFIX_COLUMN_CHUNK,
                        Constants.DEFAULT_REDIS_KEY_PREFIX_COLUMN_CHUNK);

        // get locations with range
        Set<Tuple> locationStrings = RedisUtils
                .getRedisClient(this.conf)
                .zrangeByScoreWithScores(redisKeyPrefix + path.toString(), start, start + len - 1);

        // parse and get location info
        List<CacheLocation> merged = mergeCacheLocationInfo(locationStrings);
        if (merged != null) {
            result.addAll(merged);
        }

        // sort by offset and length
        result.sort((o1, o2) -> {
            if (o1.getOffset() < o2.getOffset()) {
                return -1;
            } else if (o1.getOffset() > o2.getOffset()) {
                return 1;
            } else {
                return Long.compare(o1.getLength(), o2.getLength());
            }
        });

        return result.toArray(new CacheLocation[0]);
    }

    private List<CacheLocation> mergeCacheLocationInfo(Set<Tuple> locationStrings) {
        if (locationStrings == null || locationStrings.size() == 0) {
            return null;
        }

        // parse and merge location info
        Map<String, Set<String>> rangeHosts = new HashMap<>();
        locationStrings.forEach(t -> {
            String[] parts = t.getElement().split(REDIS_ZSET_VALUE_DELIM);

            if (parts.length >= 3) {
                long offset = Long.parseLong(parts[0]);
                long length = Long.parseLong(parts[1]);
                String range = String.format("%d%s%d", offset, REDIS_ZSET_VALUE_DELIM, length);

                // hostname may contains delimiter. So we don't use parts[2] as hostname.
                String host = t.getElement().substring(
                    parts[0].length() + parts[1].length() + 2 * REDIS_ZSET_VALUE_DELIM.length());

                // merge hosts of the same range
                if (rangeHosts.containsKey(range)) {
                    rangeHosts.get(range).add(host);
                } else {
                    Set<String> hosts = new HashSet<>();
                    hosts.add(host);
                    rangeHosts.put(range, hosts);
                }
            }
        });

        // get results
        List<CacheLocation> result = new ArrayList<>();
        rangeHosts.forEach((range, hosts) -> {
            String[] parts = range.split(REDIS_ZSET_VALUE_DELIM);
            long offset = Long.parseLong(parts[0]);
            long length = Long.parseLong(parts[1]);
            result.add(new CacheLocation(hosts.toArray(new String[0]), offset, length));
        });

        return result;
    }
}
