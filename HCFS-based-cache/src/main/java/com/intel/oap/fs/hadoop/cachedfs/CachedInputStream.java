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

package com.intel.oap.fs.hadoop.cachedfs;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.intel.oap.fs.hadoop.cachedfs.cacheutil.CacheManager;
import com.intel.oap.fs.hadoop.cachedfs.cacheutil.CacheManagerFactory;
import com.intel.oap.fs.hadoop.cachedfs.cacheutil.FiberCache;
import com.intel.oap.fs.hadoop.cachedfs.cacheutil.ObjectId;
import com.intel.oap.fs.hadoop.cachedfs.cacheutil.SimpleFiberCache;
import com.intel.oap.fs.hadoop.cachedfs.redis.RedisGlobalPMemCacheStatisticsStore;
import com.intel.oap.fs.hadoop.cachedfs.redis.RedisPMemBlockLocationStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedInputStream extends FSInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(CachedInputStream.class);

  private final FSDataInputStream hdfsInputStream;
  private final Configuration conf;
  private final Path path;
  private final int bufferSize;
  private final long contentLength;

  private long pos;
  private long currentCachePos;
  private boolean closed;

  private byte[] oneByte;

  private PMemBlock currentBlock;
  private CacheManager cacheManager;
  private PMemBlockLocationStore locationStore;
  private PMemCacheStatisticsStore statisticsStore;

  private final long pmemCachedBlockSize;
  private ObjectId[] ids;

  private int cacheMissCount = 0;
  private int cacheHitCount = 0;
  private List<PMemBlock> cachedBlocks = new ArrayList<>();

  // white list and black list regular expressions that decide whether to cache or not
  private String cacheWhiteListRegexp;
  private String cacheBlackListRegexp;
  private boolean fileShouldBeCached;

  public CachedInputStream(FSDataInputStream hdfsInputStream, Configuration conf,
                           Path path, int bufferSize, long contentLength) {
    this.hdfsInputStream = hdfsInputStream;
    this.conf = conf;
    this.path = path;
    this.bufferSize = bufferSize;
    this.contentLength = contentLength;

    this.pos = 0;
    this.currentCachePos = 0;
    this.closed = false;

    this.currentBlock = null;
    this.cacheManager = CacheManagerFactory.getOrCreate();
    this.locationStore = new RedisPMemBlockLocationStore(conf);
    this.pmemCachedBlockSize = conf.getLong(Constants.CONF_KEY_CACHED_FS_BLOCK_SIZE,
                                            Constants.DEFAULT_CACHED_BLOCK_SIZE);
    this.statisticsStore = new RedisGlobalPMemCacheStatisticsStore(conf);
    this.ids = new ObjectId[(int)((contentLength + pmemCachedBlockSize - 1) / pmemCachedBlockSize)];

    cacheWhiteListRegexp = conf.get(Constants.CONF_KEY_CACHE_WHITE_LIST_REGEXP,
            Constants.DEFAULT_CACHE_WHITE_LIST_REGEXP);

    cacheBlackListRegexp = conf.get(Constants.CONF_KEY_CACHE_BLACK_LIST_REGEXP,
            Constants.DEFAULT_CACHE_BLACK_LIST_REGEXP);

    fileShouldBeCached = checkFileShouldBeCached();

    LOG.info("Opening file: {} for reading. fileShouldBeCached: {}", path, fileShouldBeCached);
  }

  private boolean checkFileShouldBeCached() {
    return (cacheWhiteListRegexp.isEmpty()
            || Pattern.compile(cacheWhiteListRegexp).matcher(path.toString()).find())
            && (cacheBlackListRegexp.isEmpty()
            || !Pattern.compile(cacheBlackListRegexp).matcher(path.toString()).find());
  }

  private void advanceCachePosition(long pos) {
    if ((pos >= currentCachePos) && (pos < currentCachePos + pmemCachedBlockSize)) {
      return;
    }
    currentCachePos = pos / pmemCachedBlockSize * pmemCachedBlockSize;
    currentBlock = null;
  }

  @Override
  public synchronized void seek(long pos) throws IOException {
    checkNotClosed();

    if (this.pos == pos) {
      return;
    }

    if (pos < 0) {
      throw new EOFException("Cannot seek to negative offset");
    }

    if (contentLength > 0 && pos > contentLength - 1) {
      throw new EOFException("Cannot seek after EOF");
    }

    this.pos = pos;

    advanceCachePosition(pos);

    LOG.debug("Seeking file: {} to pos: {}.", path, pos);
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    boolean newNode = hdfsInputStream.seekToNewSource(targetPos);
    if (!newNode) {
      seek(targetPos);
    }
    return newNode;
  }

  @Override
  public int read() throws IOException {
    if (oneByte == null) {
      oneByte = new byte[1];
    }
    if (read(oneByte, 0, 1) <= 0) {
      return -1;
    }
    return oneByte[0] & 0xFF;
  }

  private boolean dataAvailableInCache() {
    return currentBlock != null && pos >= currentCachePos
            && pos < currentCachePos + currentBlock.getLength();
  }

  private boolean ensureDataInCache() throws IOException {
    if (dataAvailableInCache()) {
      return true;
    }
    advanceCachePosition(pos);
    if (currentCachePos >= contentLength) {
      return false;
    }
    final long bytesToRead = Math.min(pmemCachedBlockSize, contentLength - currentCachePos);
    currentBlock = new PMemBlock(path, currentCachePos, bytesToRead);
    ObjectId id = new ObjectId(currentBlock.getCacheKey());

    boolean cacheHit = false;
    boolean cacheValid = false;
    ByteBuffer cachedByteBuffer = null;
    if (fileShouldBeCached) {
      cacheHit = cacheManager.contains(id);
    }

    // read block from cache
    if (cacheHit) {
      LOG.debug("read block from cache: {}", currentBlock);

      // get cache
      try {
        cachedByteBuffer = ((SimpleFiberCache)cacheManager.get(id)).getBuffer();
        ids[(int)(currentCachePos / pmemCachedBlockSize)] = id;
        cacheHitCount += 1;
        cacheValid = true;
      } catch (Exception ex) {
        // fail
        LOG.warn("exception when get cache: {}, block: {}", ex.toString(), currentBlock);
        cacheValid = false;

        // remove cache
        try {
          cacheManager.delete(id);
          LOG.info("block cache removed, block: {}", currentBlock);
        } catch (Exception ex1) {
          // ignore
          LOG.warn("exception when removing block cache: {}, block: {}",
                   ex1.toString(), currentBlock);
        }
      }
    }

    // read block from HDFS
    if (!cacheHit || !cacheValid){
      LOG.info("read block from hdfs: {}", currentBlock);
      if (fileShouldBeCached) {
        cacheMissCount += 1;
      }
      hdfsInputStream.seek(currentCachePos);
      byte[] cacheBytes = new byte[(int)bytesToRead];
      hdfsInputStream.readFully(cacheBytes);
      cachedByteBuffer = ByteBuffer.wrap(cacheBytes);
      hdfsInputStream.seek(pos);

      // save to cache
      if (fileShouldBeCached && !cacheManager.contains(id)) {
        try {
          FiberCache fiberCache = cacheManager.create(id, bytesToRead);
          ((SimpleFiberCache)fiberCache).getBuffer().put(cacheBytes);
          cacheManager.seal(id);
          ids[(int)(currentCachePos / pmemCachedBlockSize)] = id;
          LOG.info("data cached to pmem for block: {}", currentBlock);
          cacheValid = true;
        } catch (Exception ex) {
          cacheValid = false;
          LOG.warn("exception, data not cached to pmem for block: {}", currentBlock);
        }
      } else {
        LOG.debug("data will not be cached since it's in blacklist or it's already cached: {}",
                  currentBlock);
      }
    }

    if (fileShouldBeCached && cacheValid) {
      cachedBlocks.add(new PMemBlock(currentBlock.getPath(),
                                     currentBlock.getOffset(),
                                     currentBlock.getLength()));
    }

    currentBlock.setData(cachedByteBuffer);
    return true;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    checkNotClosed();

    int totalBytesRead = 0;
    while (len > 0 && pos < contentLength) {
      if (!ensureDataInCache()) {
        return totalBytesRead;
      }
      int currentOffsetInCache = (int)(pos - currentCachePos);
      int bytesRemainingInCurrentCache = (int)(currentBlock.getLength() - currentOffsetInCache);
      int bytesToRead = Math.min(len, bytesRemainingInCurrentCache);

      currentBlock.getData().position(currentOffsetInCache);
      currentBlock.getData().get(buf, off, bytesToRead);

      totalBytesRead += bytesToRead;
      pos += bytesToRead;
      off += bytesToRead;
      len -= bytesToRead;
    }

    if (len > 0 && totalBytesRead == 0) {
      return -1;
    }

    return totalBytesRead;
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = this.contentLength - this.pos;
    return (int)remaining;
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      super.close();
      hdfsInputStream.close();
      closed = true;

      // set cache locations to redis
      String host = "";
      try {
        host = InetAddress.getLocalHost().getHostName();
        locationStore.addBlockLocations(cachedBlocks, host);
        LOG.debug("block locations saved. path: {}, host: {}", path.toString(), host);
      } catch (Exception ex) {
        // ignore
        LOG.warn("block locations failed to be saved. path: {}, host: {}", path.toString(), host);
      }

      // set cache hit/miss count
      statisticsStore.incrementCacheHit(cacheHitCount);
      statisticsStore.incrementCacheMissed(cacheMissCount);

      for (int i = 0; i < ids.length; i++) {
        if (ids[i] != null) {
          cacheManager.release(ids[i]);
          LOG.debug("release id: {}", ids[i]);
        }
      }
    }
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed!");
    }
  }
}
