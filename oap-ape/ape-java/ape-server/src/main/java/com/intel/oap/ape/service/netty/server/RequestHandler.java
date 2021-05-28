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

import java.security.InvalidParameterException;
import java.util.List;

import com.intel.ape.ParquetReaderJNI;
import com.intel.ape.service.netty.NettyMessage;
import com.intel.ape.service.params.ParquetReaderInitParams;
import com.intel.ape.util.Platform;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for client requests.
 */
public class RequestHandler extends SimpleChannelInboundHandler<NettyMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

    private long reader = 0;

    private int sequenceId = 0;
    private int columnCount;
    private int batchSize;
    private List<Integer> typeSizes;
    private List<Boolean> variableTypeFlags; // to indicate types having variable data lengths.
    private long[] nativeDataBuffers;
    private long[] nativeNullBuffers;

    private boolean hasNextBatch = true;
    private int pendingBatchCount = 0;
    private boolean waitingReceipt = false;

    private int rowGroupsToRead = 0;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Received connection from: {}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
        try {
            Class<?> msgClazz = msg.getClass();
            LOG.debug("Received request: {}", msg.toString());

            if (msgClazz == NettyMessage.ReadBatchRequest.class) {
                NettyMessage.ReadBatchRequest request =
                        (NettyMessage.ReadBatchRequest)msg;

                int batchCount = request.getBatchCount();
                if (batchCount > 0) {
                    pendingBatchCount += batchCount;

                    // start `sendNextBatch` when all before pending batches were sent.
                    // otherwise, previous call of `sendNextBatch` will handle all pending batches.
                    if (!waitingReceipt) {
                        sendNextBatch(ctx.channel());
                    }
                }
            } else if (msgClazz == NettyMessage.BatchResponseReceipt.class) {
                waitingReceipt = false;
                sendNextBatch(ctx.channel());
            } else if (msgClazz == NettyMessage.ParquetReaderInitRequest.class) {
                NettyMessage.ParquetReaderInitRequest request =
                        (NettyMessage.ParquetReaderInitRequest)msg;

                handleParquetReaderInitRequest(request);

                NettyMessage.BooleanResponse response =
                        new NettyMessage.BooleanResponse(
                                true, NettyMessage.ParquetReaderInitRequest.ID);
                ctx.writeAndFlush(response);
                LOG.debug("Sent reader init response: {}", response.toString());
            } else if (msgClazz == NettyMessage.CloseReaderRequest.class) {
                handleCloseReader();

                ctx.channel().close();
            } else if (msgClazz == NettyMessage.SkipNextRowGroupRequest.class) {
                NettyMessage.SkipNextRowGroupRequest request =
                        (NettyMessage.SkipNextRowGroupRequest)msg;

                boolean result = handleSkipNextRowGroup();

                ctx.writeAndFlush(
                        new NettyMessage.BooleanResponse(
                                result, NettyMessage.SkipNextRowGroupRequest.ID));
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
        handleException(ctx.channel(), error);
    }

    private void handleException(Channel channel, Throwable cause) {
        handleCloseReader();

        if (channel.isActive()) {
            channel.writeAndFlush(new NettyMessage.ErrorResponse(cause))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void handleParquetReaderInitRequest(NettyMessage.ParquetReaderInitRequest request) {
        ParquetReaderInitParams params = request.getParams();
        rowGroupsToRead = params.getTotalGroupsToRead();

        // init reader
        reader = ParquetReaderJNI.init(
                    params.getFileName(),
                    params.getHdfsHost(),
                    params.getHdfsPort(),
                    params.getJsonSchema(),
                    params.getFirstRowGroupIndex(),
                    params.getTotalGroupsToRead(),
                    params.isPlasmaCacheEnabled(),
                    params.isPreBufferEnabled(),
                    params.isPlasmaCacheAsync()
        );

        // set storage of cache locality
        ParquetReaderInitParams.CacheLocalityStorage cacheStore = params.getCacheLocalityStorage();
        if (cacheStore != null) {
            ParquetReaderJNI.setPlasmaCacheRedis(
                    reader,
                    cacheStore.getRedisHost(),
                    cacheStore.getRedisPort(),
                    cacheStore.getRedisPassword()
            );
        }

        // set filters
        if (params.getFilterPredicate() != null) {
            ParquetReaderJNI.setFilterStr(reader, params.getFilterPredicate());
        }

        // set aggregates
        if (params.getAggregateExpression() != null) {
            ParquetReaderJNI.setAggStr(reader, params.getAggregateExpression());
        }

        // init batch buffers
        batchSize = params.getBatchSize();
        if (batchSize <= 0) {
            throw new InvalidParameterException("Batch size should be greater than 0.");
        }
        columnCount = params.getTypeSizes().size();

        variableTypeFlags = params.getVariableLengthFlags();
        typeSizes = params.getTypeSizes();
        nativeDataBuffers = new long[columnCount];
        nativeNullBuffers = new long[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int typeSize = params.getTypeSizes().get(i);
            if (typeSize <= 0) {
                throw new InvalidParameterException("Type size should be greater than 0.");
            }
            nativeDataBuffers[i] = Platform.allocateMemory(batchSize * typeSize);
            nativeNullBuffers[i] = Platform.allocateMemory(batchSize);
        }
    }

    private NettyMessage.ReadBatchResponse readNextBatch(Channel channel) {
        final long start = System.nanoTime();

        int rowCount = 0;
        hasNextBatch = false;

        // read batch
        if (rowGroupsToRead > 0) {
            rowCount = ParquetReaderJNI.readBatch(
                    reader, batchSize, nativeDataBuffers, nativeNullBuffers);

            if (rowCount < 0) {
                rowCount = 0;
            }

            // check next batch
            hasNextBatch = ParquetReaderJNI.hasNext(reader);
        }

        // header info
        boolean[] compositeFlags = new boolean[columnCount];
        int[] dataBufferLengths = new int[columnCount];
        int compositedElementCount = 0;
        ByteBuf compositedElementLengths = channel.alloc().ioBuffer(4 * rowCount);
        ByteBuf[] dataBuffers = new ByteBuf[columnCount];
        ByteBuf[] nullBuffers = new ByteBuf[columnCount];

        // column data
        for (int i = 0; i < columnCount; i++) {
            nullBuffers[i] = Unpooled.wrappedBuffer(
                    nativeNullBuffers[i], rowCount, false);

            if (!variableTypeFlags.get(i)) {
                // data already in the native buffers
                compositeFlags[i] = false;
                dataBufferLengths[i] = typeSizes.get(i) * rowCount;
                dataBuffers[i] = Unpooled.wrappedBuffer(
                        nativeDataBuffers[i], typeSizes.get(i) * rowCount, false);
            } else {
                // column data is address of the real data
                compositeFlags[i] = true;
                compositedElementCount += rowCount;

                int dataBufferLength = 0;
                ByteBuf compositedDataBuffer = channel.alloc().ioBuffer(4 * rowCount);
                for (int j = 0; j < rowCount; j ++) {
                    // check null
                    boolean isNull = !(nullBuffers[i].getBoolean(j));
                    if (isNull) {
                        compositedElementLengths.writeInt(0);
                        continue;
                    }

                    // get address and size of real data
                    long elementBaseAddr = nativeDataBuffers[i] + j * 16;
                    int dataSize = Platform.getInt(null, elementBaseAddr);
                    long dataAddr = Platform.getLong(null, elementBaseAddr + 8);

                    // accumulate total length of column data
                    dataBufferLength += dataSize;

                    // save lengths of column elements
                    ByteBuf buf = Unpooled.wrappedBuffer(elementBaseAddr, 4, false);
                    compositedElementLengths.writeBytes(buf);
                    buf.release();

                    // save real data
                    ByteBuf dataBuf = Unpooled.wrappedBuffer(dataAddr, dataSize, false);
                    compositedDataBuffer.writeBytes(dataBuf);
                    dataBuf.release();
                }
                dataBufferLengths[i] = dataBufferLength;

                dataBuffers[i] = compositedDataBuffer;

            }
        }

        final long duration = (System.nanoTime() - start) / 1_000;
        LOG.debug("Composing batch response takes: {} us.", duration);

        return new NettyMessage.ReadBatchResponse(
                sequenceId++,
                hasNextBatch,
                columnCount,
                rowCount,
                dataBufferLengths,
                compositeFlags,
                compositedElementCount,
                compositedElementLengths,
                dataBuffers,
                nullBuffers
        );

    }

    private void handleCloseReader() {
        // close native reader
        if (reader != 0) {
            ParquetReaderJNI.close(reader);
            reader = 0;
        }

        // release memory
        if (nativeDataBuffers != null) {
            for (long addr : nativeDataBuffers) {
                Platform.freeMemory(addr);
            }
            nativeDataBuffers = null;
        }
        if (nativeNullBuffers != null) {
            for (long addr : nativeNullBuffers) {
                Platform.freeMemory(addr);
            }
            nativeNullBuffers = null;
        }
    }

    private boolean handleSkipNextRowGroup() {
        return ParquetReaderJNI.skipNextRowGroup(reader);
    }

    private void sendNextBatch(final Channel channel) {
        if (pendingBatchCount > 0) {
            pendingBatchCount--;

            NettyMessage.ReadBatchResponse response = readNextBatch(channel);
            channel.writeAndFlush(response);

            if (!hasNextBatch) {
                pendingBatchCount = 0;
            }

            // a receipt is required before sending next batch.
            // this is to avoid overwriting native buffers.
            waitingReceipt = true;
        }
    }
}
