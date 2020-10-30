package com.intel.oap.fs.hadoop.cachedfs.redis;

import com.intel.oap.fs.hadoop.cachedfs.PMemBlock;
import com.intel.oap.fs.hadoop.cachedfs.PMemBlockLocation;
import com.intel.oap.fs.hadoop.cachedfs.PMemBlockLocationStore;

public class RedisPMemBlockLocationStore implements PMemBlockLocationStore {
    public void addBlockLocation(PMemBlock block, String host) {

    }

    public void setBlockLocations(PMemBlock block, String[] hosts) {

    }

    public PMemBlockLocation getBlockLocation(PMemBlock block) {
        return null;
    }
}
