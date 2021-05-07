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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import com.intel.ape.service.netty.NettyMessage;
import com.intel.ape.service.params.ParquetReaderInitParams;
import com.intel.oap.ape.service.netty.client.NettyClient;
import com.intel.oap.ape.service.netty.client.ParquetDataRequestClient;
import com.intel.oap.ape.service.netty.client.ParquetDataRequestClientFactory;
import com.intel.oap.ape.service.netty.server.NettyServer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NettyClientTest {
    private final String defaultAddress;
    private int port = 0;

    private NettyServerRunner serverRunner;

    public NettyClientTest() {
        defaultAddress = new InetSocketAddress(0).getAddress().getHostAddress();
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
        serverRunner.nettyServer.shutdown();
    }

    private ParquetDataRequestClient createRequestClient()
            throws IOException, InterruptedException {
        Assert.assertTrue(port > 0);

        // start client connection
        ParquetDataRequestClientFactory factory =
                new ParquetDataRequestClientFactory(new NettyClient(120));
        ParquetDataRequestClient requestClient =
                factory.createParquetDataRequestClient(defaultAddress, port);
        Assert.assertTrue(requestClient.isWritable());

        return requestClient;
    }

    private ParquetReaderInitParams initParamsForFixedColumns() throws UnknownHostException {
        String localhost = InetAddress.getLocalHost().getHostAddress();
        int port = 9001;
        String path =
                "/tpch_1g_snappy/lineitem" +
                "/part-00051-42846104-06f4-4eeb-b92d-1e947eead2d3-c000.snappy.parquet";
        String jsonSchema =
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"l_orderkey\"}," +
                "{\"name\":\"l_linenumber\"}]}";
        int firstRowGroupIndex = 0;
        int totalGroupsToRead = 1;

        List<Integer> typeSizes = Arrays.asList(8, 4);
        List<Boolean> variableLengthFlags = Arrays.asList(false, false);
        int batchSize = 2048;

        return new ParquetReaderInitParams(
                path,
                localhost,
                port,
                jsonSchema,
                firstRowGroupIndex,
                totalGroupsToRead,
                typeSizes,
                variableLengthFlags,
                batchSize,
                false,
                false,
                false
        );
    }

    private ParquetReaderInitParams initParamsForVariableColumns() throws UnknownHostException {
        String localhost = InetAddress.getLocalHost().getHostAddress();
        int port = 9001;
        String path =
                "/tpch_1g_snappy/lineitem" +
                        "/part-00051-42846104-06f4-4eeb-b92d-1e947eead2d3-c000.snappy.parquet";
        String jsonSchema =
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"l_shipmode\"}," +
                        "{\"name\":\"l_comment\"}]}";
        int firstRowGroupIndex = 0;
        int totalGroupsToRead = 1;

        List<Integer> typeSizes = Arrays.asList(16, 16);
        List<Boolean> variableLengthFlags = Arrays.asList(true, true);
        int batchSize = 2048;

        return new ParquetReaderInitParams(
                path,
                localhost,
                port,
                jsonSchema,
                firstRowGroupIndex,
                totalGroupsToRead,
                typeSizes,
                variableLengthFlags,
                batchSize,
                false,
                false,
                false
        );
    }

    @Test
    public void testConnection() throws Exception {
        ParquetDataRequestClient requestClient = createRequestClient();

        // request close
        requestClient.close();
    }

    @Test
    public void testParquetReaderInit() throws IOException, InterruptedException {
        ParquetDataRequestClient requestClient = createRequestClient();

        // send for initialization of remote parquet reader
        ParquetReaderInitParams params = initParamsForFixedColumns();
        requestClient.initRemoteParquetReader(params);

        try {
            requestClient.waitForReaderInitResult();
            Assert.assertTrue(requestClient.isReaderInitialized());
        } finally {
            requestClient.close();
        }
    }

    @Test
    public void testReadBatch() throws IOException, InterruptedException {
        ParquetDataRequestClient requestClient = createRequestClient();

        // send for initialization of remote parquet reader
        ParquetReaderInitParams params = initParamsForFixedColumns();
        requestClient.initRemoteParquetReader(params);

        try {
            // the first batch
            NettyMessage.ReadBatchResponse response = requestClient.nextBatch();
            Assert.assertEquals(0, response.getSequenceId());
            Assert.assertEquals(2, response.getColumnCount());
            Assert.assertEquals(2048, response.getRowCount());
            Assert.assertTrue(response.hasNextBatch());

            response.releaseBuffers();

            // the second batch
            response = requestClient.nextBatch();
            Assert.assertEquals(1, response.getSequenceId());

            response.releaseBuffers();
        } finally {
            requestClient.close();
        }
    }

    @Test
    public void testReadBatchOnVariableColumns() throws IOException, InterruptedException {
        ParquetDataRequestClient requestClient = createRequestClient();

        // send for initialization of remote parquet reader
        ParquetReaderInitParams params = initParamsForVariableColumns();
        requestClient.initRemoteParquetReader(params);

        try {
            // the first batch
            NettyMessage.ReadBatchResponse response = requestClient.nextBatch();
            Assert.assertEquals(0, response.getSequenceId());
            Assert.assertEquals(2, response.getColumnCount());
            Assert.assertEquals(2048, response.getRowCount());
            Assert.assertTrue(response.hasNextBatch());

            response.releaseBuffers();

            // the second batch
            response = requestClient.nextBatch();
            Assert.assertEquals(1, response.getSequenceId());

            response.releaseBuffers();
        } finally {
            requestClient.close();
        }
    }
}
