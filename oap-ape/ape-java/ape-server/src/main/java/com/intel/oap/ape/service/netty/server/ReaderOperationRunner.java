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

package com.intel.oap.ape.service.netty.server;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread pools to run time-consuming tasks, and to limit the number of alive native readers.
 */
public class ReaderOperationRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ReaderOperationRunner.class);

    private final int maxAliveReaders;
    private int currentAliveReaders;
    private final Queue<Runnable> waitingInitReaderTasks;

    private final ExecutorService initReaderThreadPool;
    private final ExecutorService readBatchThreadPool;

    public ReaderOperationRunner(int maxAliveReaders, int initPoolSize, int readPoolSize) {
        if (maxAliveReaders <= 0 || initPoolSize <= 0 || readPoolSize <= 0) {
            throw new IllegalArgumentException(
                    "Invalid arguments when creating runner for reader operations.");
        }

        this.maxAliveReaders = maxAliveReaders;
        currentAliveReaders = 0;
        waitingInitReaderTasks = new LinkedList<>();
        initReaderThreadPool = Executors.newFixedThreadPool(initPoolSize);
        readBatchThreadPool = Executors.newFixedThreadPool(readPoolSize);

        LOG.info("Initialized, max readers: {}, init pool: {}, read pool: {}",
                maxAliveReaders, initPoolSize, readPoolSize);
    }

    public synchronized void addInitReaderTask(Runnable task) {
        if (currentAliveReaders < maxAliveReaders) {
            currentAliveReaders++;
            initReaderThreadPool.submit(task);
        } else {
            waitingInitReaderTasks.add(task);
        }
    }

    public synchronized void notifyReaderClosed() {
        currentAliveReaders--;

        if (!waitingInitReaderTasks.isEmpty()) {
            currentAliveReaders++;
            initReaderThreadPool.submit(waitingInitReaderTasks.poll());
        }
    }

    public void addReadBatchTask(Runnable task) {
        readBatchThreadPool.submit(task);
    }
}
