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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.runtime.io.disk.ApeFileChannelManagerImpl;
import org.apache.flink.runtime.io.disk.FileChannelManager;

import java.io.File;

/** To override IOManager for numa binding paths. */
public class ApeIOManagerAsync extends IOManagerAsync {

    private static final String DIR_NAME_PREFIX = "io-ape";

    private final FileChannelManager fileChannelManager;

    public ApeIOManagerAsync(String[] tempDirs, String ordering, int maxPercent) {
        super(tempDirs);

        fileChannelManager =
                new ApeFileChannelManagerImpl(tempDirs, DIR_NAME_PREFIX, ordering, maxPercent);
    }

    @Override
    public FileIOChannel.ID createChannel() {
        return fileChannelManager.createChannel();
    }

    @Override
    public FileIOChannel.Enumerator createChannelEnumerator() {
        return fileChannelManager.createChannelEnumerator();
    }

    @Override
    public File[] getSpillingDirectories() {
        return fileChannelManager.getPaths();
    }

    @Override
    public String[] getSpillingDirectoriesPaths() {
        File[] paths = fileChannelManager.getPaths();
        String[] strings = new String[paths.length];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = paths[i].getAbsolutePath();
        }
        return strings;
    }

    @Override
    public void close() throws Exception {
        super.close();
        fileChannelManager.close();
    }
}
