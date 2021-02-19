package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.fs.Path;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class PathConverter {
    public static Path toHDFSScheme(Path path) {
        if (path == null || path.toUri() == null
                || path.toUri().getScheme() == null
                || path.toUri().getScheme().length() == 0) {
            return path;
        }

        return new Path(URIConverter.toHDFSScheme(path.toUri()));
    }

    public static Path toCachedFSScheme(Path path) {
        if (path == null || path.toUri() == null
                || path.toUri().getScheme() == null
                || path.toUri().getScheme().length() == 0) {
            return path;
        }

        return new Path(URIConverter.toCachedFSScheme(path.toUri()));
    }
    public static Path toScheme(Path path, String newScheme) {
        if (path == null || path.toUri() == null
                || path.toUri().getScheme() == null
                || path.toUri().getScheme().length() == 0) {
            return path;
        }

        return new Path(URIConverter.toScheme(path.toUri(), newScheme));
    }
}
