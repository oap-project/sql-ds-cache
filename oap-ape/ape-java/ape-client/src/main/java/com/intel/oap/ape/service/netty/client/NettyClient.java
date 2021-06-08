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

import com.intel.ape.service.netty.NettyMessage;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty client to communicate with remote server.
 */
public class NettyClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    private Bootstrap bootstrap;

    public NettyClient(int timeoutSeconds, int nThreads) {

        final long start = System.nanoTime();

        EventLoopGroup workerGroup = new NioEventLoopGroup(nThreads);
        ChannelHandler encoder = new NettyMessage.NettyMessageEncoder();

        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeoutSeconds * 1000);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(
                        encoder,
                        new ReadTimeoutHandler(timeoutSeconds),
                        new NettyMessage.NettyMessageDecoder(),
                        new ResponseHandler());
            }
        });

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful initialization (took {} ms).", duration);
    }

    public Channel connect(String remoteHost, int remotePort)
            throws InterruptedException, IOException {
        if (bootstrap == null) {
            throw new IllegalStateException("Client has not been initialized yet.");
        }

        // Start the client.
        ChannelFuture f = bootstrap.connect(remoteHost, remotePort).await();
        if (!f.isSuccess()) {
            throw new IOException(f.cause());
        }
        return f.channel();
    }

    public void shutdown() {
        final long start = System.nanoTime();

        if (bootstrap != null) {
            bootstrap.config().group().shutdownGracefully();
            bootstrap = null;
        }

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful shutdown (took {} ms).", duration);
    }
}
