package com.intel.oap.fs.hadoop.cachedfs;

import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.CacheManager;
import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.CacheManagerFactory;
import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.FiberCache;
import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.ObjectId;
import com.intel.oap.fs.hadoop.cachedfs.cacheUtil.SimpleFiberCache;
import com.intel.oap.fs.hadoop.cachedfs.redis.RedisPMemBlockLocationStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;

public class SimpleCachedInputStream extends FSInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleCachedInputStream.class);

    private FSDataInputStream hdfsInputStream;

    private Configuration conf;
    private Path path;

    private int bufferSize;

    private long contentLength;
    private long position;
    private boolean closed;
    private long partRemaining;

    private long expectNextPos;
    private long lastByteStart;

    private long pmemCachedBlockSize = Constants.DEFAULT_CACHED_BLOCK_SIZE;

    private PMemBlock currentBlock;

    private CacheManager cacheManager;
    private PMemBlockLocationStore locationStore;

    public SimpleCachedInputStream(FSDataInputStream hdfsInputStream, Configuration conf, Path path, int bufferSize, Long contentLength) {
        this.hdfsInputStream = hdfsInputStream;

        this.conf = conf;
        this.path = path;
        this.bufferSize = bufferSize;

        this.contentLength = contentLength;
        this.expectNextPos = 0L;
        this.lastByteStart = -1L;
        this.closed = false;

        this.pmemCachedBlockSize = conf.getLong(Constants.CONF_KEY_CACHED_FS_BLOCK_SIZE, Constants.DEFAULT_CACHED_BLOCK_SIZE);

        this.cacheManager = CacheManagerFactory.getOrCreate();
        this.locationStore = new RedisPMemBlockLocationStore(conf);
    }

    public synchronized void seek(long pos) throws IOException {
        LOG.info("seek, path: {}, pos: {}", path, pos);

        if (this.position == pos) {
            return;
        }

        // compute cache block
        PMemBlock block = CachedFileSystemUtils.computePossiblePMemBlocks(path, pos, 1, pmemCachedBlockSize)[0];

        // create new block
        if (currentBlock == null || currentBlock.getOffset() != block.getOffset()) {
            this.fetchBlockDataAndCache(block);
            this.currentBlock = block;
            this.partRemaining = block.getLength();

            LOG.info("new block created to seek, path: {}, pos: {}", path, pos);
        }

        // seek in current block
        long len = pos - this.currentBlock.getOffset();
        this.partRemaining = this.currentBlock.getLength() - len;
        this.position = pos;

        LOG.info("seek in current block, path: {}, pos: {}", path, pos);

        // seek in backend stream
        this.hdfsInputStream.seek(pos);
    }

    private void fetchBlockDataAndCache(PMemBlock block) throws IOException {
        LOG.info("fetch block: {}", block);

        // read data from backend stream
        long len = block.getOffset() + block.getLength() > this.contentLength ?
                this.contentLength - block.getOffset() : block.getLength();
        block.setLength(len);
        byte[] buffer = new byte[(int)len];

        LOG.info("init block buffer with length: {}", len);

        // check pmem cache for new block
        ObjectId objectId = new ObjectId(block.getCacheKey());

        if (cacheManager.contains(objectId)) {
            LOG.info("pmem cache found for block: {}", block);

            // read data from local pmem cache
            FiberCache cacheObject = cacheManager.get(objectId);
            ((SimpleFiberCache)cacheObject).getBuffer().get(buffer);
            block.setData(buffer);

            LOG.info("data read from pmem for block: {}", block);
        } else {
            LOG.info("pmem cache NOT found for block: {}", block);

            // read data from backend stream
            this.hdfsInputStream.seek(block.getOffset());
            this.hdfsInputStream.readFully(buffer, 0, (int)len);
            block.setData(buffer);

            LOG.info("data read from HDFS for block: {}", block);

            // reset backend stream position
            this.hdfsInputStream.seek(this.position);

            // cache data to pmem
            // double check
            if (!cacheManager.contains(objectId)) {
                try {
                    FiberCache cacheObject = cacheManager.create(objectId, block.getLength());
                    ((SimpleFiberCache)cacheObject).getBuffer().put(buffer);
                    cacheManager.seal(objectId);
                    LOG.info("data cached to pmem for block: {}", block);
                } catch (Exception exception) {
                    LOG.warn("exception, data not cached to pmem for block: {}", block);
                }
            } else {
                LOG.info("data already cached to pmem by others for block: {}", block);
            }

            // save location info to redis
            String host = "";
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (Exception ex) {
                // ignore
            }
            this.locationStore.addBlockLocation(block, host);

            LOG.info("block location saved for block: {}, host: {}", block, host);
        }
    }

    public synchronized long getPos() throws IOException {
        return this.position;
    }

    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        LOG.info("seekToNewSource, path: {}, : {}", path, targetPos);

        boolean ret = this.hdfsInputStream.seekToNewSource(targetPos);
        if (ret) {
            this.seek(targetPos);
        }
        return ret;
    }

    public synchronized int read() throws IOException {
        checkNotClosed();

        // create new block
        if ((currentBlock == null || partRemaining <= 0) && this.position < this.contentLength) {
            // compute cache block
            PMemBlock block = CachedFileSystemUtils.computePossiblePMemBlocks(path, this.position, 1, pmemCachedBlockSize)[0];
            this.fetchBlockDataAndCache(block);
            this.currentBlock = block;
            this.partRemaining = block.getLength();
            LOG.info("read new block, remaining, {}", this.partRemaining);
        }

        // seek in current block
        long len = this.position - this.currentBlock.getOffset();
        this.partRemaining = this.currentBlock.getLength() - len;

        // read byte
        int byteRead = -1;
        if (this.partRemaining != 0L) {
            byteRead = this.currentBlock.getData()[(int)this.currentBlock.getLength() - (int)this.partRemaining] & 255;
        }

        if (byteRead >= 0) {
            ++this.position;
            --this.partRemaining;
        }

        return byteRead;
    }

    @Override
    public synchronized int available() throws IOException {
        return hdfsInputStream.available();
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.hdfsInputStream.close();
        this.closed = true;
    }

    private void checkNotClosed() throws IOException {
        if (this.closed) {
            throw new IOException("Stream is closed!");
        }
    }
}
