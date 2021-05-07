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

package com.intel.oap.ape.service.netty.client;

import java.io.IOException;

import io.netty.channel.Channel;

/**
 * Factory to create {@link ParquetDataRequestClient}.
 */
public class ParquetDataRequestClientFactory {
    private final NettyClient nettyClient;

    public ParquetDataRequestClientFactory(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }

    public ParquetDataRequestClient createParquetDataRequestClient(String address, int port)
            throws IOException, InterruptedException {

        Channel channel = nettyClient.connect(address, port);
        ResponseHandler responseHandler = channel.pipeline().get(ResponseHandler.class);
        return new ParquetDataRequestClient(channel, responseHandler);
    }
}
