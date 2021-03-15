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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.intel.oap.fs.hadoop.ape.hcfs.redis.RedisCacheLocationStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A hadoop file system implementation that wraps HDFS data accessing
 * and provides locations of cached data.
 *
 * Locations of cached data can be loaded from redis.
 *
 * Usage: set fs.hdfs.impl to com.intel.oap.fs.hadoop.ape.hcfs.CacheAwareFileSystem
 * in hdfs configuration, for example, in hdfs-site.xml.
 *
 */
public class CacheAwareFileSystem extends FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CacheAwareFileSystem.class);

    private static final String SCHEME = "hdfs";

    private URI uri;

    /** The wrapped Hadoop File System. */
    private org.apache.hadoop.fs.FileSystem hdfs;

    private String locationPolicy;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        uri = name;
        hdfs = new DistributedFileSystem();
        hdfs.initialize(name, conf);
        LOG.info("cache aware scheduler is initialized.");
        locationPolicy = getConf().get(
                Constants.CONF_KEY_FS_APE_HCFS_BLOCK_LOCATION_POLICY,
                Constants.CACHE_LOCATION_POLICY_MERGING_HDFS);
        LOG.info("block location policy: {}", locationPolicy);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
                                                 long len) throws IOException {
        if (file == null) {
            throw new NullPointerException();
        }

        return getFileBlockLocations(file.getPath(), start, len);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path path, long start,
                                                 long len) throws IOException {
        if (path == null) {
            throw new NullPointerException();
        }
        LOG.debug("getFileBlockLocations with: {}, start: {}, len: {}",
                path.toString(), start, len);

        List<BlockLocation> result = new ArrayList<>();

        if (start < 0 || len <= 0) {
            return result.toArray(new BlockLocation[0]);
        }

        switch (locationPolicy) {
            case Constants.CACHE_LOCATION_POLICY_HDFS_ONLY:
                // get HDFS block locations
                LOG.debug("getFileBlockLocations with native HDFS, start: {}, len: {}",
                        start, len);
                BlockLocation[] hdfsBlockLocations = hdfs.getFileBlockLocations(path, start, len);
                result.addAll(Arrays.asList(hdfsBlockLocations));
                break;
            case Constants.CACHE_LOCATION_POLICY_OVER_HDFS:
                result.addAll(getFileBlockLocationsWithCacheChecking(path, start, len, false));
                break;
            case Constants.CACHE_LOCATION_POLICY_DEFAULT:
            case Constants.CACHE_LOCATION_POLICY_MERGING_HDFS:
            default:
                result.addAll(getFileBlockLocationsWithCacheChecking(path, start, len, true));
        }

        return result.toArray(new BlockLocation[0]);
    }

    // Get block locations taking account of cache locations
    private List<BlockLocation> getFileBlockLocationsWithCacheChecking(Path path, long start,
                                                                       long len, boolean merging)
            throws IOException {
        CacheLocation[] cachedBlockLocations;
        BlockLocation[] hdfsBlockLocations;
        RedisCacheLocationStore locationStore;

        List<BlockLocation> result = new ArrayList<>();

        // get block locations of cached data
        locationStore = new RedisCacheLocationStore(getConf());
        cachedBlockLocations = locationStore.getSortedColumnChunkLocations(path, start, len);

        if (!checkLocationsCompleted(cachedBlockLocations, start, len)) {
            // get HDFS block locations
            LOG.debug("getFileBlockLocations fell back to native HDFS, start: {}, len: {}",
                    start, len);
            hdfsBlockLocations = hdfs.getFileBlockLocations(path, start, len);

            if (merging) {
                result.addAll(mergeBlockLocations(cachedBlockLocations, hdfsBlockLocations,
                        start, len));
            } else {
                result.addAll(Arrays.asList(hdfsBlockLocations));
            }
        } else {
            result.addAll(Arrays.asList(cachedBlockLocations));
        }

        return result;
    }

    // Check if the range is covered by block locations
    private boolean checkLocationsCompleted(BlockLocation[] blockLocations, long start, long len) {
        if (len == 0) {
            return true;
        } else if (blockLocations == null || blockLocations.length == 0) {
            return false;
        }

        // check start
        BlockLocation firstBlock = blockLocations[0];
        if (firstBlock.getOffset() > start) {
            return false;
        }

        // check if locations cover the range fully
        long currentEnd = firstBlock.getOffset() + firstBlock.getLength();
        for (BlockLocation blockLocation : blockLocations) {
            if (blockLocation.getOffset() > currentEnd) {
                // there is a gap
                return false;
            }

            long blockEnd = blockLocation.getOffset() + blockLocation.getLength();
            if (blockEnd > currentEnd) {
                currentEnd = blockEnd;
            }
        }

        // check end
        if (currentEnd < start + len) {
            return false;
        }

        return false;
    }

    // Merge cached block locations and HDFS block locations.
    // Cached block locations hold higher priority.
    private List<BlockLocation> mergeBlockLocations(CacheLocation[] cacheBlockLocations,
                                                    BlockLocation[] hdfsBlockLocations,
                                                    long start, long len) {

        List<BlockLocation> result = new ArrayList<>();

        if (cacheBlockLocations.length == 0) {
            result.addAll(Arrays.asList(hdfsBlockLocations));
            return result;
        }

        long currentOffset = start;
        int cacheIndex = 0;
        int hdfsIndex = 0;
        while (currentOffset < start + len) {

            long cacheOffset = cacheIndex >= cacheBlockLocations.length ?
                    Long.MAX_VALUE : cacheBlockLocations[cacheIndex].getOffset();
            long hdfsOffset = hdfsIndex >= hdfsBlockLocations.length ?
                    Long.MAX_VALUE : hdfsBlockLocations[hdfsIndex].getOffset();

            if (cacheOffset <= currentOffset) {

                result.add(cacheBlockLocations[cacheIndex]);
                currentOffset = cacheBlockLocations[cacheIndex].getOffset() +
                        cacheBlockLocations[cacheIndex].getLength();
                cacheIndex ++;

            } else if (hdfsOffset <= currentOffset) {

                if (hdfsOffset + hdfsBlockLocations[hdfsIndex].getLength() > currentOffset) {
                    // copy block location data. keep no changes to hdfsBlockLocations[hdfsIndex]
                    BlockLocation temp = new BlockLocation(hdfsBlockLocations[hdfsIndex]);

                    temp.setOffset(currentOffset);
                    temp.setLength(Math.min(
                            hdfsOffset + temp.getLength() - currentOffset,
                            cacheOffset - currentOffset
                    ));

                    result.add(temp);
                    currentOffset = temp.getOffset() + temp.getLength();
                } else {
                    hdfsIndex ++;
                }

            } else {
                break;
            }
        }

        return result;
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        LOG.debug("open: {}", path.toString());
        return hdfs.open(path, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite,
                                     int bufferSize, short replication, long blockSize,
                                     Progressable progressable) throws IOException {
        LOG.debug("create: {}", path.toString());
        return hdfs.create(path, fsPermission, overwrite, bufferSize,
                replication, blockSize, progressable);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize,
                                     Progressable progressable) throws IOException {
        return hdfs.append(path, bufferSize, progressable);
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        return hdfs.rename(path, path1);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return hdfs.delete(path, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        return hdfs.listStatus(path);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        hdfs.setWorkingDirectory(path);
    }

    @Override
    public Path getWorkingDirectory() {
        return hdfs.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return hdfs.mkdirs(path, fsPermission);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return hdfs.getFileStatus(path);
    }
}
