package com.intel.oap.fs.hadoop.cachedfs;

public interface PMemBlockLocationStore {
    void addBlockLocation(PMemBlock block, String host);

    PMemBlockLocation getBlockLocation(PMemBlock block);

    PMemBlockLocation[] getBlockLocations(PMemBlock[] blocks, boolean consecutive);
}
