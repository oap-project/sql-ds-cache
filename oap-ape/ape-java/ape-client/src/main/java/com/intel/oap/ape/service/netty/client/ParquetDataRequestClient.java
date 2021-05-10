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
import java.security.InvalidParameterException;

import com.intel.ape.service.netty.NettyMessage;
import com.intel.ape.service.params.ParquetReaderInitParams;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to send parquet data request with netty channel.
 * Data responses are saved in {@link ResponseHandler}.
 *
 */
public class ParquetDataRequestClient {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetDataRequestClient.class);

    private final Channel tcpChannel;
    private final ResponseHandler responseHandler;

    private final ChannelFutureListener throwErrorListener;

    private boolean readerInitRequested = false;
    private boolean readerInitialized = false;

    private int requestedBatchCount = 0;

    public boolean isWritable() {
        return tcpChannel != null && tcpChannel.isWritable();
    }

    public ParquetDataRequestClient(Channel tcpChannel, ResponseHandler responseHandler) {
        this.tcpChannel = tcpChannel;
        this.responseHandler = responseHandler;
        throwErrorListener = future -> {
            if (!future.isSuccess()) {
                SocketAddress remoteAddr = future.channel().remoteAddress();

                LOG.error("Sending request to '{}' failed.", remoteAddr);

                throw new IOException(future.cause());
            }
        };
        LOG.info("Connected to remote server: {} with address: {}",
                tcpChannel.remoteAddress(), tcpChannel.localAddress());
    }

    public void initRemoteParquetReader(ParquetReaderInitParams params) {
        tcpChannel.writeAndFlush(new NettyMessage.ParquetReaderInitRequest(params))
                .addListener(throwErrorListener);

        readerInitRequested = true;
    }

    /**
     * Optional request to notify remote server to preload 1 batch of data.
     * This will wait for finish of reader initialization before sending out request.
     */
    public void sendReadBatchRequest() throws IOException, InterruptedException {
        sendReadBatchRequest(1);
    }

    /**
     * Optional request to notify remote server to preload specified batches of data.
     * This will wait for finish of reader initialization before sending out request.
     * @param batchCount number of batches that need to be retrieved from remote server.
     */
    public void sendReadBatchRequest(int batchCount) throws IOException, InterruptedException {
        if (batchCount <= 0) {
            throw new InvalidParameterException("batchCount should be greater than 0.");
        }
        if (!readerInitRequested) {
            throw new IllegalStateException(
                    "readBatch should not be called before initialization of reader.");
        }

        if (!readerInitialized) {
            waitForReaderInitResult();
        }

        requestedBatchCount += batchCount;

        tcpChannel.writeAndFlush(new NettyMessage.ReadBatchRequest(batchCount))
                .addListener(throwErrorListener);
    }

    /**
     * Wait and return next batch response from remote server.
     * This will send `readBatch` request to remote server if needed.
     * @return NettyMessage.ReadBatchResponse
     * Note: {@link NettyMessage.ReadBatchResponse#releaseBuffers()} should be called after
     * it's no longer used.
     */
    public NettyMessage.ReadBatchResponse nextBatch() throws InterruptedException, IOException {
        if (requestedBatchCount == 0) {
            sendReadBatchRequest();
        }

        NettyMessage.ReadBatchResponse response = responseHandler.nextBatch();

        requestedBatchCount--;

        return response;
    }

    public void skipNextRowGroup() throws IOException, InterruptedException {
        if (!readerInitRequested) {
            throw new IllegalStateException(
                    "skipNextRowGroup should not be called before initialization of reader.");
        }

        if (!readerInitialized) {
            waitForReaderInitResult();
        }

        tcpChannel.writeAndFlush(new NettyMessage.SkipNextRowGroupRequest())
                .addListener(throwErrorListener);

        // wait for result
        boolean ret = waitForBoolResponse(NettyMessage.SkipNextRowGroupRequest.ID);
        if (!ret) {
            throw new IOException(
                    "skipNextRowGroup failed on remote server: " + tcpChannel.remoteAddress());
        }
    }

    public boolean hasNextBatch() {
        return responseHandler.hasNextBatch();
    }

    public void waitForReaderInitResult() throws IOException, InterruptedException {
        boolean initResult = waitForBoolResponse(NettyMessage.ParquetReaderInitRequest.ID);
        if (initResult) {
            readerInitialized = true;
        } else {
            throw new IOException(
                    "Failed to initialize parquet reader on server: " + tcpChannel.remoteAddress());
        }
    }

    public boolean isReaderInitialized() {
        return readerInitialized;
    }

    private boolean waitForBoolResponse(byte messageID) throws IOException, InterruptedException {
        synchronized (responseHandler.getBoolResponseLock()) {
            // wait for notification of init response
            while(!(responseHandler.needListenersWakeup()
                    && responseHandler.getRecentBoolResponse().getRespondingTo() == messageID)) {
                responseHandler.getBoolResponseLock().wait();
            }

            boolean ret = responseHandler.getRecentBoolResponse().getResult();
            responseHandler.resetBoolResponse();
            return ret;
        }
    }

    public void close() throws IOException, InterruptedException {
        responseHandler.close();

        tcpChannel.writeAndFlush(new NettyMessage.CloseReaderRequest())
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            SocketAddress remoteAddr = future.channel().remoteAddress();

                            LOG.error("Sending request to '{}' failed.", remoteAddr);
                        }

                        future.channel().close();
                    }
                });
    }

}
