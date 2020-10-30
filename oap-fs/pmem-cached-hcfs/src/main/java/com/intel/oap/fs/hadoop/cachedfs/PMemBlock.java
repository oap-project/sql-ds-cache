package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.fs.Path;

/**
 * pmem cache block
 */
public class PMemBlock {

    private Path path;

    private long offset;

    private long length;

    private String cacheKey;

    private byte[] data;


    public PMemBlock(Path path, long offset, long length) {
        this(path, offset, length, null);
    }

    public PMemBlock(Path path, long offset, long length, byte[] data) {
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.cacheKey = "pmem_hcfs_blk:" + path.toUri().toString() + ":" + offset + "_" + length;
        this.data = data;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public void setCacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
