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

import java.nio.ByteBuffer;

import org.apache.hadoop.fs.Path;

/**
 * pmem cache block
 */
public class PMemBlock {

    private Path path;

    private long offset;

    private long length;

    private String cacheKey;

    private ByteBuffer data;

    public PMemBlock(Path path, long offset, long length) {
        this(path, offset, length, null);
    }

    public PMemBlock(Path path, long offset, long length, ByteBuffer data) {
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

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "path: " + path.toString() + ", offset: " + offset + ", length: " + length;
    }
}
