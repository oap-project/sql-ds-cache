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

package com.intel.oap.ape.service;

import java.net.InetSocketAddress;

import com.intel.oap.ape.service.netty.client.NettyClient;
import com.intel.oap.ape.service.netty.server.NettyServer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public class NettyClientTestBase {
    protected final String defaultAddress;
    protected int port = 0;
    protected final NettyClient nettyClient;

    private NettyServerRunner serverRunner;

    public NettyClientTestBase() {
        defaultAddress = new InetSocketAddress(0).getAddress().getHostAddress();
        nettyClient = new NettyClient(120, 1);
    }

    static class NettyServerRunner implements Runnable {
        private NettyServer nettyServer;

        private final String address;

        private final Object lock = new Object();
        private boolean needWakeUp = false;

        NettyServerRunner(String address) {
            this.address = address;
        }

        @Override
        public void run() {
            nettyServer = new NettyServer(0, address);
            try {
                nettyServer.run();
            } catch (Exception exception) {
                exception.printStackTrace();
                nettyServer.shutdown();
                nettyServer = null;
            } finally {
                synchronized (lock) {
                    needWakeUp = true;
                    lock.notify();
                }
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        // start server
        serverRunner = new NettyServerRunner(defaultAddress);
        Thread serverThread = new Thread(serverRunner);
        serverThread.start();

        // wait server ready
        synchronized (serverRunner.lock) {
            while (!serverRunner.needWakeUp) {
                serverRunner.lock.wait();
            }
        }
        Assert.assertNotNull(serverRunner.nettyServer);

        port = serverRunner.nettyServer.getPort();
    }

    @After
    public void tearDown() throws Exception {
        Thread.sleep(1000);

        // shutdown server
        if (serverRunner.nettyServer != null) {
            serverRunner.nettyServer.shutdown();
        }

        // shutdown client
        nettyClient.shutdown();
    }
}
