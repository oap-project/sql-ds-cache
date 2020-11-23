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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CachedFileSystem extends FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CachedFileSystem.class);

    /** The wrapped Hadoop File System. */
    private org.apache.hadoop.fs.FileSystem hdfs;

    private URI uri;

    private String scheme;

    private long pmemCachedBlockSize = Constants.DEFAULT_CACHED_BLOCK_SIZE;

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
        LOG.info("getFileBlockLocations with: {}, start: {}, len: {}", path.toString(), start, len);

        List<BlockLocation> result = new ArrayList<>();

        // return block locations based on cache checking result
        PMemBlock[] blocks = CachedFileSystemUtils.computePossiblePMemBlocks(path, start, len, this.pmemCachedBlockSize);

        if (blocks.length > 0) {
            PMemBlockLocationStore locationStore = new RedisPMemBlockLocationStore(this.getConf());

            PMemBlockLocation[] pmemBlockLocations = locationStore.getBlockLocations(blocks, true);

            if (pmemBlockLocations.length < blocks.length) {
                // get HDFS block locations
                LOG.info("getFileBlockLocations fell back to HDFS native, start: {}, len: {}", start, len);

                BlockLocation[] hdfsBlockLocations = this.hdfs.getFileBlockLocations(
                        PathConverter.toHDFSScheme(path), start, len);

                result.addAll(Arrays.asList(hdfsBlockLocations));
            } else {
                result.addAll(Arrays.asList(pmemBlockLocations));
            }

        }

        return result.toArray(new BlockLocation[0]);
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
