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
import java.net.SocketAddress;

import com.intel.ape.service.netty.NettyMessage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler to process responses from remote server.
 */
public class ResponseHandler extends SimpleChannelInboundHandler<NettyMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(ResponseHandler.class);

    // Some operations often wait a response from server before continuing.
    // Below variables are used to deliver bool responses with notifications to waiting threads.
    private boolean needListenersWakeup;
    private final Object boolResponseLock = new Object();
    private NettyMessage.BooleanResponse recentBoolResponse;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();
            LOG.info("Received response, message type: {}", msgClazz.getName());

            if (msgClazz == NettyMessage.ErrorResponse.class) {
                NettyMessage.ErrorResponse response = (NettyMessage.ErrorResponse)msg;
                LOG.info("Response: {}", response.toString());

                SocketAddress remoteAddr = ctx.channel().remoteAddress();
                throw new IOException("Error on remote server: " + remoteAddr, response.getCause());

            } else if (msgClazz == NettyMessage.BooleanResponse.class) {
                synchronized (boolResponseLock) {
                    needListenersWakeup = true;
                    recentBoolResponse = (NettyMessage.BooleanResponse)msg;

                    boolResponseLock.notifyAll();
                }
            } else {
                throw new IllegalStateException("Received unknown message from server: "
                        + ctx.channel().remoteAddress());
            }
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    public Object getBoolResponseLock() {
        return boolResponseLock;
    }

    public NettyMessage.BooleanResponse getRecentBoolResponse() {
        return recentBoolResponse;
    }

    public boolean needListenersWakeup() {
        return needListenersWakeup;
    }

    public void resetBoolResponse() {
        needListenersWakeup = false;
        recentBoolResponse = null;
    }
}
