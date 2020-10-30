package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.fs.Path;

public class CachedFileSystemUtils {

    public static PMemBlock[] computePossiblePMemBlocks(Path path, long start, long len, long blockSize) {
        PMemBlock[] ret = new PMemBlock[0];

        if (path == null || start < 0 || len <= 0 || blockSize <= 0) {
            return ret;
        }

        long blkStart = start - (start % blockSize);
        long blkEnd = ((start + len) % blockSize == 0) ? start + len : start + len - ((start + len) % blockSize) + blockSize;
        long blkNum = (blkEnd - blkStart) / blockSize;

        ret = new PMemBlock[(int)blkNum];

        for (int i = 0; i < blkNum; i++) {
            ret[i] = new PMemBlock(path, blkStart + (i * blockSize), blockSize);
        }

        return ret;
    }
}
