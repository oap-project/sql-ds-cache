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


import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.intel.ape.service.netty.NettyMessage;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.PlatformDependent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty server to handle requests from clients.
 * This server will run standalone.
 */
public class NettyServer {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

    private int port;
    private final String address;

    private ServerBootstrap bootstrap;
    private ChannelFuture bindFuture;

    public NettyServer(int port, String address) {
        this.port = port;
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public String getAddress() {
        return address;
    }

    public void run() throws Exception {
        final long start = System.nanoTime();

        // preload some classes
        Class.forName(Unpooled.class.getCanonicalName());
        Class.forName(CompositeByteBuf.class.getCanonicalName());
        Class.forName(UnpooledUnsafeDirectByteBuf.class.getCanonicalName());
        Class.forName(PlatformDependent.class.getCanonicalName());

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ChannelHandler encoder = new NettyMessage.NettyMessageEncoder();

        bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                encoder,
                                new NettyMessage.NettyMessageDecoder(),
                                new RequestHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        // Bind and start to accept incoming connections.
        bootstrap.localAddress(InetAddress.getByName(address), port);
        bindFuture = bootstrap.bind().sync();

        InetSocketAddress localAddress = (InetSocketAddress) bindFuture.channel().localAddress();
        port = localAddress.getPort();
        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful initialization (took {} ms). Listening on SocketAddress {}.",
                duration, localAddress);
    }

    public void shutdown() {
        final long start = System.nanoTime();
        if (bindFuture != null) {
            bindFuture.channel().close().awaitUninterruptibly();
            bindFuture = null;
        }

        if (bootstrap != null) {
            bootstrap.config().group().shutdownGracefully();
            bootstrap.config().childGroup().shutdownGracefully();
            bootstrap = null;
        }
        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful shutdown (took {} ms).", duration);
    }

    private static String getDefaultBindAddress() {
        return new InetSocketAddress(0).getAddress().getHostAddress();
    }

    public static void main(String[] args) throws Exception {
        int port = 0;
        String address = getDefaultBindAddress();
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            address = args[1].trim();
        }

        NettyServer server = new NettyServer(port, address);
        server.run();
    }
}
