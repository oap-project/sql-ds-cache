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
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    private boolean hasNextBatch = true;
    private int sequenceId = 0;
    private final PriorityQueue<NettyMessage.ReadBatchResponse> receivedBatches;
    private final LinkedBlockingQueue<NettyMessage.ReadBatchResponse> availableBatches;

    private boolean closed = false;

    private Throwable error = null;

    public ResponseHandler() {
        receivedBatches = new PriorityQueue<>(
                Comparator.comparingInt(NettyMessage.ReadBatchResponse::getSequenceId));
        availableBatches = new LinkedBlockingQueue<>();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();
            LOG.debug("Received response: {}", msg.toString());

            if (msgClazz == NettyMessage.ReadBatchResponse.class) {
                NettyMessage.ReadBatchResponse response = (NettyMessage.ReadBatchResponse)msg;

                handleReadBatchResponse(ctx, response);
            } else if (msgClazz == NettyMessage.BooleanResponse.class) {
                notifyBooleanResponseListeners((NettyMessage.BooleanResponse)msg);
            } else if (msgClazz == NettyMessage.ErrorResponse.class) {
                NettyMessage.ErrorResponse response = (NettyMessage.ErrorResponse)msg;

                SocketAddress remoteAddr = ctx.channel().remoteAddress();
                handleErrorResponse(remoteAddr.toString(), response.getCause());
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

    public NettyMessage.BooleanResponse getRecentBoolResponse() throws IOException {
        // error handling
        if (error != null) {
            throw new IOException(error);
        }

        return recentBoolResponse;
    }

    public boolean needListenersWakeup() {
        return needListenersWakeup;
    }

    public void resetBoolResponse() {
        needListenersWakeup = false;
        recentBoolResponse = null;
    }

    private void notifyBooleanResponseListeners(NettyMessage.BooleanResponse msg) {
        synchronized (boolResponseLock) {
            needListenersWakeup = true;
            recentBoolResponse = msg;

            boolResponseLock.notifyAll();
        }
    }

    private void notifyReadBatchResponseListeners(NettyMessage.ReadBatchResponse res) {
        availableBatches.add(res);
    }

    private void handleErrorResponse(String remoteServer, Throwable cause) {
        error = cause;

        if (closed) {
            LOG.warn("Error on remote server: {}, {}", remoteServer, cause);
            return;
        }

        // wake up waiting threads
        notifyBooleanResponseListeners(new NettyMessage.BooleanResponse(false, (byte)-1));
        notifyReadBatchResponseListeners(NettyMessage.ReadBatchResponse.newEmptyResponse());
    }

    private void handleReadBatchResponse(
            ChannelHandlerContext ctx, NettyMessage.ReadBatchResponse response) {
        if (closed) {
            return;
        }

        synchronized (this) {
            if (closed) {
                return;
            }

            // send receipt to server, let server send next batch if having pending batches
            ctx.writeAndFlush(new NettyMessage.BatchResponseReceipt());

            // offer the new batch to consumer
            receivedBatches.add(response);

            while (receivedBatches.peek() != null
                    && receivedBatches.peek().getSequenceId() == sequenceId) {
                notifyReadBatchResponseListeners(receivedBatches.poll());
                sequenceId++;
            }
        }

    }

    public boolean hasNextBatch() {
        return hasNextBatch;
    }

    /**
     * Wait for and return the next batch response from remote server.
     * @return NettyMessage.ReadBatchResponse
     */
    public NettyMessage.ReadBatchResponse nextBatch() throws InterruptedException, IOException {
        NettyMessage.ReadBatchResponse response = availableBatches.take();

        // error handling
        if (error != null) {
            response.releaseBuffers();
            throw new IOException(error);
        }

        hasNextBatch = response.hasNextBatch();

        return response;
    }

    public void close() {
        synchronized (this) {
            closed = true;

            for (NettyMessage.ReadBatchResponse response : availableBatches) {
                response.releaseBuffers();
            }
            availableBatches.clear();

            for (NettyMessage.ReadBatchResponse response : receivedBatches) {
                response.releaseBuffers();
            }
            receivedBatches.clear();
        }
    }

}
