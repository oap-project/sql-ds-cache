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

import com.intel.ape.service.netty.NettyMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for client requests.
 */
public class RequestHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Received connection from: {}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();
            LOG.info("Received request, message type: {}", msgClazz.getName());

            if (msgClazz == NettyMessage.ParquetReaderInitRequest.class) {
                NettyMessage.ParquetReaderInitRequest request =
                        (NettyMessage.ParquetReaderInitRequest)msg;
                LOG.info("Received request: {}", request.toString());
            } else if (msgClazz == NettyMessage.CloseReaderRequest.class) {
                LOG.info("Received request: close reader.");
                ctx.channel().close();
            } else {
                LOG.warn("Received unexpected request type: {}", msgClazz.getName());

                respondWithError(ctx,
                        new IllegalStateException("Unknown message type: " +  msgClazz.getName()));
            }
        } catch (Throwable t) {
            respondWithError(ctx, t);
        }
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
        ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
    }
}
