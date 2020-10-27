package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class CachedInputStream extends FSInputStream {
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

    public CachedInputStream(FSDataInputStream hdfsInputStream, Configuration conf, Path path, int bufferSize, Long contentLength) {
        this.hdfsInputStream = hdfsInputStream;

        this.conf = conf;
        this.path = path;
        this.bufferSize = bufferSize;

        this.contentLength = contentLength;
        this.expectNextPos = 0L;
        this.lastByteStart = -1L;
        this.closed = false;
    }

    public void seek(long pos) throws IOException {
        // TODO seek with cache fetching

        this.hdfsInputStream.seek(pos);
    }

    public long getPos() throws IOException {
        return this.hdfsInputStream.getPos();
    }

    public boolean seekToNewSource(long targetPos) throws IOException {
        return this.hdfsInputStream.seekToNewSource(targetPos);
    }

    public int read() throws IOException {
        // TODO read with cache functionality and cached blocks

        return this.hdfsInputStream.read();
    }

    @Override
    public int available() throws IOException {
        return hdfsInputStream.available();
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.hdfsInputStream.close();

        // TODO unregistering cache tool
    }

    private void checkNotClosed() throws IOException {
        if (this.closed) {
            throw new IOException("Stream is closed!");
        }
    }
}
