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
import com.intel.compression.util.Platform;

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
    private boolean compressEnabled;

    private ParquetReaderInitParams readerInitParams = null;
    private boolean hasNextBatch = true;
    private int pendingBatchCount = 0;
    private boolean waitingReceipt = false;

    private int rowGroupsToRead = 0;

    private ChannelHandlerContext ctx;
    private final ReaderOperationRunner readerOperationRunner;

    public RequestHandler(ReaderOperationRunner readerOperationRunner) {
        this.readerOperationRunner = readerOperationRunner;
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelRegistered(ctx);
    }

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

                    // start next batch when all before pending batches were sent.
                    // otherwise, previous call of `startNextBatch` will handle all pending batches.
                    if (!waitingReceipt) {
                        startNextBatch();
                    }
                }
            } else if (msgClazz == NettyMessage.BatchResponseReceipt.class) {
                waitingReceipt = false;
                startNextBatch();
            } else if (msgClazz == NettyMessage.ParquetReaderInitRequest.class) {
                NettyMessage.ParquetReaderInitRequest request =
                        (NettyMessage.ParquetReaderInitRequest)msg;

                handleParquetReaderInitRequest(request);
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        respondWithError(ctx, cause);
    }

    private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
        handleException(ctx.channel(), error);
    }

    private void handleException(Channel channel, Throwable cause) {
        LOG.warn("Exception caught on channel: {}, {}", channel.remoteAddress(), cause.toString());

        handleCloseReader();

        if (channel.isActive()) {
            channel.writeAndFlush(new NettyMessage.ErrorResponse(cause))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void handleParquetReaderInitRequest(NettyMessage.ParquetReaderInitRequest request) {
        if (readerInitParams != null) {
            throw new IllegalStateException("Request to init parquet reader again.");
        }
        readerInitParams = request.getParams();

        // async init
        readerOperationRunner.addInitReaderTask(() -> {
            try {
                initParquetReader();
                notifyReaderOperationResult(reader);
            } catch (Exception ex) {
                notifyReaderOperationResult(ex);
            }
        });
    }

    private void initParquetReader() {
        ParquetReaderInitParams params = readerInitParams;
        rowGroupsToRead = params.getTotalGroupsToRead();
        compressEnabled = params.isCompressEnabled();

        long plasmaClientPoolPtr = 0L;
        if (params.isPlasmaCacheEnabled()) {
            plasmaClientPoolPtr = PlasmaClientPoolInitializer.getClientPoolPtr();
        }

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
                    params.isPlasmaCacheAsync(),
                    plasmaClientPoolPtr
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

    private void sendReaderInitResponse() {
        NettyMessage.BooleanResponse response =
                new NettyMessage.BooleanResponse(
                        true, NettyMessage.ParquetReaderInitRequest.ID);
        ctx.writeAndFlush(response);
        LOG.debug("Sent reader init response: {}", response.toString());
    }

    private void startNextBatch() {
        if (pendingBatchCount > 0) {
            pendingBatchCount--;

            // async reading
            readerOperationRunner.addReadBatchTask(() -> {
                try {
                    NettyMessage.ReadBatchResponse response = readNextBatch();
                    notifyReaderOperationResult(response);
                } catch (Exception ex) {
                    notifyReaderOperationResult(ex);
                }

            });

            // a receipt is required before sending next batch.
            // this is to avoid overwriting native buffers.
            waitingReceipt = true;
        }
    }

    NettyMessage.ReadBatchResponse readNextBatch() {
        final long start = System.nanoTime();

        int rowCount = 0;
        hasNextBatch = false;

        // read batch
        if (rowGroupsToRead > 0) { // avoid error when no data to read
            synchronized (this) {
                if (reader != 0) { // avoid error when native reader has been closed
                    rowCount = ParquetReaderJNI.readBatch(
                            reader, batchSize, nativeDataBuffers, nativeNullBuffers);
                    // check next batch
                    hasNextBatch = ParquetReaderJNI.hasNext(reader);
                }
            }
        }

        // adjust returned row count
        if (rowCount < 0) {
            rowCount = 0;
        }

        // header info
        boolean[] compositeFlags = new boolean[columnCount];
        int[] dataBufferLengths = new int[columnCount];
        int compositedElementCount = 0;
        ByteBuf compositedElementLengths = ctx.alloc().ioBuffer(Integer.BYTES * rowCount);
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
                ByteBuf compositedDataBuffer = ctx.alloc().ioBuffer(Integer.BYTES * rowCount);
                for (int j = 0; j < rowCount; j ++) {
                    // check null
                    boolean isNull = !(nullBuffers[i].getBoolean(j));
                    if (isNull) {
                        compositedElementLengths.writeInt(0);
                        continue;
                    }

                    // get address and size of real data
                    long elementBaseAddr = nativeDataBuffers[i] + j * (Long.BYTES + Long.BYTES);
                    int dataSize = Platform.getInt(null, elementBaseAddr);
                    long dataAddr = Platform.getLong(null, elementBaseAddr + Long.BYTES);

                    // accumulate total length of column data
                    dataBufferLength += dataSize;

                    // save lengths of column elements
                    ByteBuf buf = Unpooled.wrappedBuffer(elementBaseAddr, Integer.BYTES, false);
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
                nullBuffers,
                compressEnabled
        );

    }

    private void sendNextBatch(NettyMessage.ReadBatchResponse response) {
        ctx.channel().writeAndFlush(response);

        if (!hasNextBatch) {
            pendingBatchCount = 0;
        }
    }

    void notifyReaderOperationResult(Object result) {
        ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(result));
    }

    private synchronized void handleCloseReader() {
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

        // notify runner
        readerOperationRunner.notifyReaderClosed();
    }

    private boolean handleSkipNextRowGroup() {
        return ParquetReaderJNI.skipNextRowGroup(reader);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NettyMessage.ReadBatchResponse) {
            sendNextBatch((NettyMessage.ReadBatchResponse) msg);
        } else if (msg instanceof Long) {
            sendReaderInitResponse();
        } else if (msg instanceof Throwable) {
            respondWithError(ctx, (Throwable)msg);
        } else {
            super.userEventTriggered(ctx, msg);
        }
    }

    static class PlasmaClientPoolInitializer {
        // native pointer of the pool for plasma clients.
        // this can save virtual memory consumption of server processes.
        private static long plasmaClientPoolPtr = 0L;

        public static long getClientPoolPtr() {
            if (plasmaClientPoolPtr != 0L) {
                return plasmaClientPoolPtr;
            }

            synchronized (PlasmaClientPoolInitializer.class) {
                if (plasmaClientPoolPtr != 0L) {
                    return plasmaClientPoolPtr;
                }

                plasmaClientPoolPtr =
                    ParquetReaderJNI.createPlasmaClientPool(
                            NettyServer.DEFAULT_PLASMA_CLIENT_POOL_CAPACITY);
            }

            return plasmaClientPoolPtr;
        }

        public static void closeClientPool() {
            if (plasmaClientPoolPtr != 0L) {
                ParquetReaderJNI.closePlasmaClientPool(plasmaClientPoolPtr);
                plasmaClientPoolPtr = 0L;
            }
        }
    }

}
