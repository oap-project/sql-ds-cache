package com.intel.oap.fs.hadoop.cachedfs;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class URIConverter {
    public static URI toHDFSScheme(URI name) {
        if (name == null || name.getScheme() == null || name.getScheme().length() == 0) {
            return name;
        }
        return UriBuilder.fromUri(name).scheme(Constants.HDFS_SCHEME).build();
    }

    public static URI toCachedFSScheme(URI name) {
        if (name == null || name.getScheme() == null || name.getScheme().length() == 0) {
            return name;
        }
        return UriBuilder.fromUri(name).scheme(Constants.CACHED_FS_SCHEME).build();
    }

    public static URI toScheme(URI name, String newScheme) {
        if (name == null || name.getScheme() == null || name.getScheme().length() == 0) {
            return name;
        }
        return UriBuilder.fromUri(name).scheme(newScheme).build();
    }
}
