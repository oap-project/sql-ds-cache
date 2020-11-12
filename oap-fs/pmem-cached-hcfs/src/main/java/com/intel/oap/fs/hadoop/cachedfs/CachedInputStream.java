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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.CacheManager;
import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.CacheManagerFactory;
import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.ObjectId;
import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.FiberCache;
import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.SimpleFiberCache;
import com.intel.oap.fs.hadoop.cachedfs.redis.RedisPMemBlockLocationStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.EOFException;
import java.net.InetAddress;

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
  private final long pmemCachedBlockSize;

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

    LOG.info("Opening file: {} for reading.", path);
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

    LOG.info("Seeking file: {} to pos: {}.", path, pos);
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
    if (read(oneByte, 0, 1) == 0) {
      return -1;
    }
    return oneByte[0] & 0xFF;
  }

  private boolean dataAvailableInCache() {
    return currentBlock != null && pos >= currentCachePos && pos < currentCachePos + currentBlock.getLength();
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
    byte[] cacheBytes = new byte[(int)bytesToRead];
    currentBlock = new PMemBlock(path, currentCachePos, bytesToRead);
    ObjectId id = new ObjectId(currentBlock.getCacheKey());
    boolean hit = cacheManager.contains(id);
    if (hit) {
      LOG.info("read block {} from cache", currentBlock);
      ((SimpleFiberCache)cacheManager.get(id)).getBuffer().get(cacheBytes);
    } else {
      LOG.info("read block {} from hdfs", currentBlock);
      hdfsInputStream.seek(currentCachePos);
      hdfsInputStream.readFully(cacheBytes);
      hdfsInputStream.seek(pos);
      if (!cacheManager.contains(id)) {
        try {
          FiberCache fiberCache = cacheManager.create(id, bytesToRead);
          ((SimpleFiberCache)fiberCache).getBuffer().put(cacheBytes);
          cacheManager.seal(id);
          LOG.info("data cached to pmem for block: {}", currentBlock);
          try {
            String host = InetAddress.getLocalHost().getHostName();
            locationStore.addBlockLocation(currentBlock, host);
            LOG.info("block location saved for block: {}, host: {}", currentBlock, host);
          } catch (Exception ex) {
            // ignore
          }
        } catch (Exception ex) {
          LOG.warn("exception, data not cached to pmem for block: {}", currentBlock);
        }
      } else {
        LOG.info("data already cached to pmem by others for block: {}", currentBlock);
      }
    }
    currentBlock.setData(cacheBytes);
    return true;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    checkNotClosed();

    int totalBytesRead = 0;
    while (len > 0) {
      if (!ensureDataInCache()) {
        return totalBytesRead;
      }
      int currentOffsetInCache = (int)(pos - currentCachePos);
      int bytesRemainingInCurrentCache = (int)(currentBlock.getLength() - currentOffsetInCache);
      int bytesToRead = Math.min(len, bytesRemainingInCurrentCache);
      System.arraycopy(currentBlock.getData(), currentOffsetInCache, buf, off, bytesToRead);
      totalBytesRead += bytesToRead;
      pos += bytesToRead;
      off += bytesToRead;
      len -= bytesToRead;
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
    super.close();
    hdfsInputStream.close();
    closed = true;
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed!");
    }
  }
}
