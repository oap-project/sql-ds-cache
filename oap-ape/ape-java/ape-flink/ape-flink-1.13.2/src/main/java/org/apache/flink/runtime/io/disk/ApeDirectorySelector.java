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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;

/** A path selector based on configured ordering. */
public class ApeDirectorySelector {

    private static final Logger LOG = LoggerFactory.getLogger(ApeDirectorySelector.class);

    private final String[] dirs;
    private final String ordering;
    private final int maxPercent;

    private volatile int current = 0;

    private static final String ORDERING_FAIR = "fair";
    private static final String ORDERING_PRIOR = "prior";

    public ApeDirectorySelector(String[] dirs, String ordering, int maxPercent) {
        this.dirs = dirs;
        this.ordering = ordering;
        this.maxPercent = maxPercent;
    }

    public int getNextPathNum() {
        if (ordering.equals(ORDERING_PRIOR)) {
            return getPriorNext();
        } else {
            return getFairNext();
        }
    }

    private int getPriorNext() {
        for (int i = 0; i < dirs.length; i++) {
            if (checkStorageSpace(i)) {
                return i;
            }
        }

        LOG.warn("All directories have no enough space.");
        return 0;
    }

    private int getFairNext() {
        int next = current;

        while (!checkStorageSpace(next)) {
            next += 1;
            next = next >= dirs.length ? 0 : next;

            if (next == current) {
                LOG.warn("All directories have no enough space.");
                break;
            }
        }

        int newNext = next + 1;
        current = newNext >= dirs.length ? 0 : newNext;

        return next;
    }

    private boolean checkStorageSpace(int index) {
        try {
            FileStore fs = Files.getFileStore(Paths.get(dirs[index]));
            long left = fs.getUnallocatedSpace();
            long total = fs.getTotalSpace();

            return !(left * 1.0 / total * 100 <= (100 - maxPercent));
        } catch (IOException ex) {
            LOG.warn("Exception when checking storage space, {}", ex.toString());
            return false;
        }
    }
}
