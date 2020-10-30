package com.intel.oap.fs.hadoop.cachedfs;

public interface PMemBlockLocationStore {
    void addBlockLocation(PMemBlock block, String host);

    void setBlockLocations(PMemBlock block, String[] hosts);

    PMemBlockLocation getBlockLocation(PMemBlock block);
}
