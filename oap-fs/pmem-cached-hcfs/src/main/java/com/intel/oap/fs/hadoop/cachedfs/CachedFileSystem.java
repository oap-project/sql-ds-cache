package com.intel.oap.fs.hadoop.cachedfs;

import com.intel.oap.fs.hadoop.cachedfs.redis.RedisPMemBlockLocationStore;
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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CachedFileSystem extends FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CachedFileSystem.class);

    /** The wrapped Hadoop File System. */
    private org.apache.hadoop.fs.FileSystem hdfs;

    private URI uri;

    private String scheme;

    private long pmemCachedBlockSize = Constants.DEFAULT_CACHED_BLOCK_SIZE;

    private String locationPolicy;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        LOG.info("initialize cachedFs with uri: {}", name.toString());
        super.initialize(name, conf);
        this.setConf(conf);
        this.uri = name;
        this.scheme = name.getScheme();
        this.pmemCachedBlockSize = conf.getLong(Constants.CONF_KEY_CACHED_FS_BLOCK_SIZE, Constants.DEFAULT_CACHED_BLOCK_SIZE);

        URI hdfsName = URIConverter.toHDFSScheme(name);
        LOG.info("backend hdfs uri: {}", hdfsName.toString());

        // to prevent stackoverflow from use of: new Path(hdfsName).getFileSystem(conf)
        // when fs.hdfs.impl is configured as CachedFileSystem itself
        this.hdfs = new DistributedFileSystem();
        this.hdfs.initialize(hdfsName, conf);

        this.locationPolicy = this.getConf().get(
                Constants.CONF_KEY_CACHED_FS_BLOCK_LOCATION_POLICY,
                Constants.CONF_VALUE_CACHED_FS_BLOCK_LOCATION_POLICY_CACHE_MERGING_HDFS);
        LOG.info("block location policy: {}", this.locationPolicy);
    }

    @Override
    public String getScheme() {
        return Constants.CACHED_FS_SCHEME;
    }

    public URI getUri() {
        return this.uri;
    }

    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        FileStatus fileStatus = this.getFileStatus(path);
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can't open " + path + " because it is a directory");
        } else {
            FSDataInputStream hdfsInputStream = this.hdfs.open(PathConverter.toHDFSScheme(path), bufferSize);
            return new FSDataInputStream(new CachedInputStream(hdfsInputStream, this.getConf(), path, bufferSize, fileStatus.getLen()));
        }
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        if (file == null) {
            throw new NullPointerException();
        }

        return this.getFileBlockLocations(file.getPath(), start, len);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws IOException {
        if (path == null) {
            throw new NullPointerException();
        }
        LOG.debug("getFileBlockLocations with: {}, start: {}, len: {}", path.toString(), start, len);

        List<BlockLocation> result = new ArrayList<>();

        if (start >= 0 && len > 0) {
            PMemBlock[] blocks;
            PMemBlockLocation[] pmemBlockLocations;
            BlockLocation[] hdfsBlockLocations;
            PMemBlockLocationStore locationStore;

            switch (this.locationPolicy) {
                case Constants.CONF_VALUE_CACHED_FS_BLOCK_LOCATION_POLICY_HDFS_ONLY:
                    // get HDFS block locations
                    LOG.debug("getFileBlockLocations with native HDFS, start: {}, len: {}", start, len);
                    hdfsBlockLocations = this.hdfs.getFileBlockLocations(PathConverter.toHDFSScheme(path), start, len);
                    result.addAll(Arrays.asList(hdfsBlockLocations));
                    break;
                case Constants.CONF_VALUE_CACHED_FS_BLOCK_LOCATION_POLICY_CACHE_OVER_HDFS:
                    // return block locations based on cache checking result
                    blocks = CachedFileSystemUtils.computePossiblePMemBlocks(path, start, len, this.pmemCachedBlockSize);
                    locationStore = new RedisPMemBlockLocationStore(this.getConf());
                    pmemBlockLocations = locationStore.getBlockLocations(blocks, true);

                    if (pmemBlockLocations.length < blocks.length) {
                        // get HDFS block locations
                        LOG.debug("getFileBlockLocations fell back to native HDFS, start: {}, len: {}", start, len);
                        hdfsBlockLocations = this.hdfs.getFileBlockLocations(PathConverter.toHDFSScheme(path), start, len);
                        result.addAll(Arrays.asList(hdfsBlockLocations));
                    } else {
                        result.addAll(Arrays.asList(pmemBlockLocations));
                    }
                    break;
                default:
                    // return block locations based on cache checking result
                    blocks = CachedFileSystemUtils.computePossiblePMemBlocks(path, start, len, this.pmemCachedBlockSize);
                    locationStore = new RedisPMemBlockLocationStore(this.getConf());
                    pmemBlockLocations = locationStore.getBlockLocations(blocks, true);

                    if (pmemBlockLocations.length < blocks.length) {
                        // get HDFS block locations
                        LOG.debug("getFileBlockLocations fell back to native HDFS, start: {}, len: {}", start, len);
                        hdfsBlockLocations = this.hdfs.getFileBlockLocations(PathConverter.toHDFSScheme(path), start, len);
                        result.addAll(mergeBlockLocations(pmemBlockLocations, hdfsBlockLocations, start, len));
                    } else {
                        result.addAll(Arrays.asList(pmemBlockLocations));
                    }
            }

        }

        return result.toArray(new BlockLocation[0]);
    }

    // Merge cached block locations and HDFS block locations.
    // Cached block locations hold higher priority.
    private List<BlockLocation> mergeBlockLocations(
            PMemBlockLocation[] pmemBlockLocations, BlockLocation[] hdfsBlockLocations, long start, long len) {

        List<BlockLocation> result = new ArrayList<>();

        if (pmemBlockLocations.length == 0) {
            result.addAll(Arrays.asList(hdfsBlockLocations));
        } else {
            long currentOffset = start;
            int pmemIndex = 0;
            int hdfsIndex = 0;
            while (currentOffset < start + len) {

                long pmemOffset = pmemIndex >= pmemBlockLocations.length ?
                        Long.MAX_VALUE : pmemBlockLocations[pmemIndex].getOffset();
                long hdfsOffset = hdfsIndex >= hdfsBlockLocations.length ?
                        Long.MAX_VALUE : hdfsBlockLocations[hdfsIndex].getOffset();

                if (pmemOffset <= currentOffset) {

                    result.add(pmemBlockLocations[pmemIndex]);
                    currentOffset = pmemBlockLocations[pmemIndex].getOffset() + pmemBlockLocations[pmemIndex].getLength();
                    pmemIndex ++;

                } else if (hdfsOffset <= currentOffset) {

                    if (hdfsOffset + hdfsBlockLocations[hdfsIndex].getLength() > currentOffset) {
                        // copy block location data. keep no changes to hdfsBlockLocations[hdfsIndex]
                        BlockLocation temp = new BlockLocation(hdfsBlockLocations[hdfsIndex]);

                        temp.setOffset(currentOffset);
                        temp.setLength(temp.getLength() - (currentOffset - hdfsOffset));

                        if (temp.getOffset() + temp.getLength() > pmemOffset) {

                            temp.setLength(pmemOffset - temp.getOffset());
                        }

                        result.add(temp);
                        currentOffset = temp.getOffset() + temp.getLength();
                    } else {
                        hdfsIndex ++;
                    }

                } else {
                    break;
                }
            }

        }

        return result;
    }

    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progressable) throws IOException {
        return this.hdfs.create(PathConverter.toHDFSScheme(path), fsPermission, overwrite, bufferSize, replication, blockSize, progressable);
    }

    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable) throws IOException {
        return this.hdfs.append(PathConverter.toHDFSScheme(path), bufferSize, progressable);
    }

    public boolean rename(Path srcPath, Path dstPath) throws IOException {
        return this.hdfs.rename(PathConverter.toHDFSScheme(srcPath), PathConverter.toHDFSScheme(dstPath));
    }

    public boolean delete(Path path, boolean recursive) throws IOException {
        return this.hdfs.delete(PathConverter.toHDFSScheme(path), recursive);
    }

    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        FileStatus[] result = this.hdfs.listStatus(PathConverter.toHDFSScheme(path));
        for (FileStatus status : result) {
            // convert scheme back
            status.setPath(PathConverter.toScheme(status.getPath(), path.toUri().getScheme()));
        }
        return result;
    }

    public void setWorkingDirectory(Path path) {
        this.hdfs.setWorkingDirectory(PathConverter.toHDFSScheme(path));
    }

    public Path getWorkingDirectory() {
        Path result = this.hdfs.getWorkingDirectory();
        // convert scheme back
        return PathConverter.toScheme(result, this.scheme);
    }

    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return this.hdfs.mkdirs(PathConverter.toHDFSScheme(path), fsPermission);
    }

    public FileStatus getFileStatus(Path path) throws IOException {
        FileStatus result = this.hdfs.getFileStatus(PathConverter.toHDFSScheme(path));
        // convert scheme back
        result.setPath(PathConverter.toScheme(result.getPath(), path.toUri().getScheme()));
        return result;
    }
}
