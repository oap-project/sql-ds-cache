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

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;

/**
 * read and write data with redis.
 * a singleton instance mechanism is needed.
 */
public class RedisClient {
    private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);

    private JedisPool jedisPool;

    private String password = "";

    private static volatile RedisClient instance;

    private RedisClient(String host, int port, String auth, int maxTotal, int maxIdle) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        jedisPool = new JedisPool(config, host, port);

        password = auth;
    }

    public static RedisClient getInstance(String host, int port, String auth,
                                          int maxTotal, int maxIdle) {
        if (instance == null) {
            synchronized (RedisClient.class) {
                if (instance == null) {
                    instance = new RedisClient(host, port, auth, maxTotal, maxIdle);
                }
            }
        }

        return instance;
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    /**
     * @return Jedis
     */
    public Jedis getJedis() {
        Jedis jedis = jedisPool.getResource();
        if (!password.isEmpty()) {
            jedis.auth(password);
        }
        return jedis;
    }

    /**
     * jedis release
     *
     * @param jedis Jedis
     */
    public void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    /**
     * get
     *
     * @param key String
     * @return String
     */
    public String get(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.get(key);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "get", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * set
     *
     * @param key   String
     * @param value String
     */
    public void set(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.set(key, value);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "set", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * @param key String
     * @return Long
     */
    public Long incr(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.incr(key);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "incr", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * @param key String
     * @param count long
     * @return Long
     */
    public Long incrBy(String key, long count) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.incrBy(key, count);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "incrBy", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * hset
     *
     * @param key   String
     * @param field String
     * @param value String
     */
    public void hset(String key, String field, String value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.hset(key, field, value);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "hset", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * hget
     *
     * @param key   String
     * @param field String
     * @return String
     */
    public String hget(String key, String field) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hget(key, field);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "hget", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * hgetAll
     *
     * @param key String
     * @return Map<String, String>
     */
    public Map<String, String> hgetAll(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.hgetAll(key);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "hgetAll", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    public void lpush(String key, String... value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            jedis.lpush(key, value);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "lpush", key);
            throw new JedisException(e.getMessage(), e);
        }
    }

    /**
     * @param key   String
     * @param value String...
     * @return Long
     */
    public Long sadd(String key, String... value) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.sadd(key, value);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "sadd", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * @param key String
     * @return Set<String>
     */
    public Set<String> smembers(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.smembers(key);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "smembers", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * @param key    String
     * @param score  double
     * @param member String
     * @return Long
     */
    public Long zadd(String key, double score, String member) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zadd(key, score, member);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "zadd", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    public Long zadd(String key, Map<String, Double> scoreMembers) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zadd(key, scoreMembers);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "zadd", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    public Long zrem(String key, String... members) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrem(key, members);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "zrem", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * @param key   String
     * @param start long
     * @param stop  long
     * @return Set<String>
     */
    public Set<String> zrange(String key, long start, long stop) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrange(key, start, stop);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "zrange", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * @param key   String
     * @param min double
     * @param max  double
     * @return Set<Tuple>
     */
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.zrangeByScoreWithScores(key, min, max);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}",
                    e.toString(), "zrangeByScoreWithScores", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }
    /**
     * @param key     String
     * @param seconds int
     * @return Long
     */
    public Long expire(String key, int seconds) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "expire", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

    /**
     * timeout for key
     *
     * @param key String
     * @return long
     */
    public long ttl(String key) {
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.ttl(key);
        } catch (Exception e) {
            LOG.error("redis exception: {}, when: {}, key: {}", e.toString(), "ttl", key);
            throw new JedisException(e.getMessage(), e);
        } finally {
            close(jedis);
        }
    }

}
