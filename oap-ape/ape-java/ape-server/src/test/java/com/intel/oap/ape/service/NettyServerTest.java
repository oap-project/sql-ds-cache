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

import com.intel.oap.ape.service.netty.server.NettyServer;

import org.junit.Assert;
import org.junit.Test;

public class NettyServerTest {

    @Test
    public void testServerBind() throws Exception {
        // start server
        String defaultAddress = new InetSocketAddress(0).getAddress().getHostAddress();
        NettyServer nettyServer = new NettyServer(0, defaultAddress);
        nettyServer.run();
        Assert.assertTrue(nettyServer.getPort() > 0);

        // shutdown
        Thread.sleep(1000);
        nettyServer.shutdown();
    }
}
