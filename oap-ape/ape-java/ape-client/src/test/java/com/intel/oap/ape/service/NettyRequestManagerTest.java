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
import java.net.UnknownHostException;

import com.intel.oap.ape.service.netty.NettyParquetRequestHelper;
import com.intel.oap.ape.service.netty.client.ParquetDataRequestClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class NettyRequestManagerTest extends NettyClientTestBase {

    @Test
    public void testRequestManager() throws UnknownHostException {
        Assert.assertTrue(port > 0);
        String localhost = InetAddress.getLocalHost().getHostAddress();

        Configuration conf = new Configuration();
        conf.set(NettyParquetRequestHelper.CONF_KEY_SERVER_LIST,
                localhost + ":" + port);

        int hdfsPort = 9001;
        String fileName = "/test.parquet";
        Path path = new Path(
                "hdfs://"
                + localhost
                + ":"
                + hdfsPort
                + fileName
                );

        try {
            NettyParquetRequestHelper helper = new NettyParquetRequestHelper();
            ParquetDataRequestClient requestClient =
                    helper.createFileSplitRequestClient(
                            conf,
                            path,
                            0,
                            100
                    );

            Assert.assertNotNull(requestClient);

            Assert.assertTrue(requestClient.isWritable());

            requestClient.close();

            helper.shutdownNettyClient();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
