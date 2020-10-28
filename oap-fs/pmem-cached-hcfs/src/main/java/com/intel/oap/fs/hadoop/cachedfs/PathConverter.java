package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.fs.Path;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class PathConverter {
    public static Path toHDFSScheme(Path path) {
        if (path == null || path.toUri().getScheme().length() == 0) {
            return path;
        }

        URI newURI = UriBuilder.fromUri(path.toUri()).scheme(Constants.HDFS_SCHEME).build();
        return new Path(newURI);
    }

    public static Path toCachedFSScheme(Path path) {
        if (path == null || path.toUri().getScheme().length() == 0) {
            return path;
        }

        URI newURI = UriBuilder.fromUri(path.toUri()).scheme(Constants.CACHED_FS_SCHEME).build();
        return new Path(newURI);
    }
}
