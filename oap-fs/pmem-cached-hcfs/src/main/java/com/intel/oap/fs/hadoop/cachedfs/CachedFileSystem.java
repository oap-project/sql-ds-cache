package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;

public class CachedFileSystem extends FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(CachedFileSystem.class);

    /** The wrapped Hadoop File System. */
    private org.apache.hadoop.fs.FileSystem hdfs;

    private URI uri;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.setConf(conf);
        this.uri = name;

        URI hdfsName = URIConverter.toHDFSScheme(name);
        LOG.info("hdfs name: {}", hdfsName.toString());
        this.hdfs = new Path(hdfsName).getFileSystem(conf);
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
        // TODO return based on cache checking result

        file.setPath(PathConverter.toHDFSScheme(file.getPath()));
        return this.hdfs.getFileBlockLocations(file, start, len);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws IOException {
        // TODO return based on cache checking result

        return this.hdfs.getFileBlockLocations(PathConverter.toHDFSScheme(path), start, len);
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
            status.setPath(PathConverter.toCachedFSScheme(status.getPath()));
        }
        return result;
    }

    public void setWorkingDirectory(Path path) {
        this.hdfs.setWorkingDirectory(PathConverter.toHDFSScheme(path));
    }

    public Path getWorkingDirectory() {
        Path result = this.hdfs.getWorkingDirectory();
        return PathConverter.toCachedFSScheme(result);
    }

    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return this.hdfs.mkdirs(PathConverter.toHDFSScheme(path), fsPermission);
    }

    public FileStatus getFileStatus(Path path) throws IOException {
        FileStatus result = this.hdfs.getFileStatus(PathConverter.toHDFSScheme(path));
        result.setPath(PathConverter.toCachedFSScheme(result.getPath()));
        return result;
    }
}
