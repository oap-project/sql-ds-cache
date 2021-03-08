package com.intel.oap.fs.hadoop.cachedfs;

import java.util.List;

public interface PMemBlockLocationStore {
    void addBlockLocation(PMemBlock block, String host);

    void addBlockLocations(List<PMemBlock> blocks, String host);

    PMemBlockLocation getBlockLocation(PMemBlock block);

    PMemBlockLocation[] getBlockLocations(PMemBlock[] blocks, boolean consecutive);
}
