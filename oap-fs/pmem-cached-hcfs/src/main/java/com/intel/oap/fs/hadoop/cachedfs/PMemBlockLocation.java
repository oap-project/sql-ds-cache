package com.intel.oap.fs.hadoop.cachedfs;

import org.apache.hadoop.fs.BlockLocation;

/**
 * pmem cache block location
 */
public class PMemBlockLocation extends BlockLocation {
    private PMemBlock cachedBlock;

    public PMemBlockLocation(String[] hosts, PMemBlock cachedBlock) {
        super(null, hosts, cachedBlock.getOffset(), cachedBlock.getLength());
        this.cachedBlock = cachedBlock;
    }

    public PMemBlock getCachedBlock() {
        return cachedBlock;
    }

    public void setCachedBlock(PMemBlock cachedBlock) {
        this.cachedBlock = cachedBlock;
    }
}
