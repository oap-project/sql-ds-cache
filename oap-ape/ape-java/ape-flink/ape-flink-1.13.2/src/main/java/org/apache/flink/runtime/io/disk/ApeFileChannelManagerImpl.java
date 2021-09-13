/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.disk;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkState;

/** A FileChannelManager implementation respecting the order of numa binding paths. */
public class ApeFileChannelManagerImpl extends FileChannelManagerImpl {

    /** A random number generator for the anonymous Channel IDs. */
    private final Random random;

    /**
     * Flag to signal that the file channel manager has been shutdown already. The flag should
     * support concurrent access for cases like multiple shutdown hooks.
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private final ApeDirectorySelector apeDirectorySelector;

    public ApeFileChannelManagerImpl(
            String[] tempDirs, String prefix, String ordering, int maxPercent) {
        super(tempDirs, prefix);

        random = new Random();
        apeDirectorySelector = new ApeDirectorySelector(tempDirs, ordering, maxPercent);
    }

    /**
     * Create a channel for shuffle IO, etc.
     *
     * @return FileIOChannel.ID
     */
    @Override
    public FileIOChannel.ID createChannel() {
        checkState(!isShutdown.get(), "File channel manager has shutdown.");

        int num = apeDirectorySelector.getNextPathNum();

        return new FileIOChannel.ID(getPaths()[num], num, random);
    }

    @Override
    public void close() throws Exception {
        // Marks shutdown and exits if it has already shutdown.
        if (!isShutdown.compareAndSet(false, true)) {
            return;
        }

        super.close();
    }
}
