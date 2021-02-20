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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.intel.oap.fs.hadoop.cachedfs.PMemBlock;
import com.intel.oap.fs.hadoop.cachedfs.PMemBlockLocation;
import com.intel.oap.fs.hadoop.cachedfs.PMemBlockLocationStore;
import org.apache.hadoop.conf.Configuration;
import redis.clients.jedis.Tuple;

/**
 * Class for storage of block locations.
 */
public class RedisPMemBlockLocationStore implements PMemBlockLocationStore {
    private static final String REDIS_ZSET_VALUE_DELIM = "_";

    private final Configuration conf;

    public RedisPMemBlockLocationStore(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void addBlockLocation(PMemBlock block, String host) {
        if (block == null || host == null || host.trim().isEmpty()) {
            return;
        }

        RedisUtils
                .getRedisClient(this.conf)
                .zadd(
                        block.getPath().toString(),
                        block.getOffset(),
                        String.format(
                                "%d%s%d%s%s",
                                block.getOffset(),
                                REDIS_ZSET_VALUE_DELIM,
                                block.getLength(),
                                REDIS_ZSET_VALUE_DELIM,
                                host)
                );

    }


    @Override
    public void addBlockLocations(List<PMemBlock> blocks, String host) {
        if (blocks == null || blocks.isEmpty() || host == null || host.trim().isEmpty()) {
            return;
        }

        String path = blocks.get(0).getPath().toString();

        Map<String, Double> scoreMembers = new HashMap<>();
        blocks.forEach(block -> {
            scoreMembers.put(
                String.format(
                    "%d%s%d%s%s",
                    block.getOffset(),
                    REDIS_ZSET_VALUE_DELIM,
                    block.getLength(),
                    REDIS_ZSET_VALUE_DELIM,
                    host
                ),
                (double) block.getOffset()
            );
        });

        RedisUtils.getRedisClient(this.conf).zadd(path, scoreMembers);
    }

    @Override
    public PMemBlockLocation getBlockLocation(PMemBlock block) {
        if (block == null) {
            return null;
        }

        PMemBlockLocation[] locations = this.getBlockLocations(new PMemBlock[]{block}, false);

        if (locations.length > 0) {
            return locations[0];
        } else {
            return null;
        }
    }

    /**
     * Get locations from redis.
     * @param blocks PMemBlock[]
     * @param consecutive boolean
     * @return PMemBlockLocation[]
     */
    @Override
    public PMemBlockLocation[] getBlockLocations(PMemBlock[] blocks, boolean consecutive) {
        PMemBlockLocation[] ret = new PMemBlockLocation[0];

        if (blocks == null || blocks.length == 0) {
            return ret;
        }

        // consecutive blocks or not
        if (consecutive) {
            return this.getConsecutiveBlockLocations(blocks);
        } else {
            return this.getDiscreteBlockLocations(blocks);
        }
    }

    /**
     * Get locations from redis for consecutive blocks.
     * @param blocks PMemBlock[]
     * @return PMemBlockLocation[]
     */
    private PMemBlockLocation[] getConsecutiveBlockLocations(PMemBlock[] blocks) {
        List<PMemBlockLocation> result = new ArrayList<>();

        long minOffset = blocks[0].getOffset();
        long maxOffset = blocks[blocks.length - 1].getOffset();

        // get locations with the right offset range
        Set<Tuple> locationStrings = RedisUtils
                .getRedisClient(this.conf)
                .zrangeByScoreWithScores(blocks[0].getPath().toString(), minOffset, maxOffset);

        // parse and get location info
        if (locationStrings != null && locationStrings.size() > 0) {
            for (PMemBlock block : blocks) {

                PMemBlockLocation location = this.filterLocationInfo(block,locationStrings);

                if (location != null) {
                    result.add(location);
                }
            }
        }

        return result.toArray(new PMemBlockLocation[0]);
    }

    /**
     * Get locations from redis for discrete blocks.
     * Discrete blocks will result a for-loop of redis look-up.
     * @param blocks PMemBlock[]
     * @return PMemBlockLocation[]
     */
    private PMemBlockLocation[] getDiscreteBlockLocations(PMemBlock[] blocks) {
        List<PMemBlockLocation> result = new ArrayList<>();

        for (PMemBlock block : blocks) {

            // get locations with the right offset range
            Set<Tuple> locationStrings = RedisUtils
                    .getRedisClient(this.conf)
                    .zrangeByScoreWithScores(blocks[0].getPath().toString(),
                                             block.getOffset(), block.getOffset());

            PMemBlockLocation location = this.filterLocationInfo(block,locationStrings);

            if (location != null) {
                result.add(location);
            }
        }

        return result.toArray(new PMemBlockLocation[0]);
    }

    private PMemBlockLocation filterLocationInfo(PMemBlock block, Set<Tuple> locationStrings) {
        // hosts for current block
        Set<String> hosts = new HashSet<>();

        // parse and get location info
        if (locationStrings != null && locationStrings.size() > 0) {

            // get locations for current block
            locationStrings.forEach(t -> {
                String[] parts = t.getElement().split(REDIS_ZSET_VALUE_DELIM);

                if (parts.length >= 3) {
                    long offset = Long.parseLong(parts[0]);
                    long length = Long.parseLong(parts[1]);
                    String host = parts[2];

                    // check cached block's offset and length
                    if (offset == block.getOffset() && length == block.getLength()) {
                        hosts.add(host);
                    }
                }
            });
        }

        if (hosts.size() > 0) {
            return new PMemBlockLocation(hosts.toArray(new String[0]), block);
        } else {
            return null;
        }
    }
}
