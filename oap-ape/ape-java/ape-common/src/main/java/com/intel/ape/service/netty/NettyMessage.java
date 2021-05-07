/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.ape.service.netty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ProtocolException;
import java.util.function.Consumer;

import com.intel.ape.service.params.ParquetReaderInitParams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A simple and generic interface to serialize messages to Netty's buffer space.
 */
public abstract class NettyMessage {
    // frame length (4), magic number (4), msg ID (1)
    static final int FRAME_HEADER_LENGTH = 4 + 4 + 1;

    static final int MAGIC_NUMBER = 0x3DEBF19F;

    abstract void write(ChannelOutboundInvoker out, ChannelPromise promise,
                        ByteBufAllocator allocator) throws IOException;

    // ------------------------------------------------------------------------

    /**
     * Allocates a new (header and contents) buffer and adds some header information for the frame
     * decoder.
     *
     * <p>Before sending the buffer, you must write the actual length after adding the contents as
     * an integer to position <tt>0</tt>!
     *
     * @param allocator
     *         byte buffer allocator to use
     * @param id
     *         {@link NettyMessage} subclass ID
     *
     * @return a newly allocated direct buffer with header data written for {@link
     * NettyMessageEncoder}
     */
    private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
        return allocateBuffer(allocator, id, -1);
    }

    /**
     * Allocates a new (header and contents) buffer and adds some header information for the frame
     * decoder.
     *
     * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
     * the contents as an integer to position <tt>0</tt>!
     *
     * @param allocator
     *         byte buffer allocator to use
     * @param id
     *         {@link NettyMessage} subclass ID
     * @param contentLength
     *         content length (or <tt>-1</tt> if unknown)
     *
     * @return a newly allocated direct buffer with header data written for {@link
     * NettyMessageEncoder}
     */
    private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int contentLength) {
        return allocateBuffer(allocator, id, 0, contentLength, true);
    }

    /**
     * Allocates a new buffer and adds some header information for the frame decoder.
     *
     * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
     * the contents as an integer to position <tt>0</tt>!
     *
     * @param allocator
     *         byte buffer allocator to use
     * @param id
     *         {@link NettyMessage} subclass ID
     * @param messageHeaderLength
     *         additional header length that should be part of the allocated buffer and is written
     *         outside of this method
     * @param contentLength
     *         content length (or <tt>-1</tt> if unknown)
     * @param allocateForContent
     *         whether to make room for the actual content in the buffer (<tt>true</tt>) or whether
     *         to only return a buffer with the header information (<tt>false</tt>)
     *
     * @return a newly allocated direct buffer with header data written for {@link
     * NettyMessageEncoder}
     */
    private static ByteBuf allocateBuffer(
            ByteBufAllocator allocator,
            byte id,
            int messageHeaderLength,
            int contentLength,
            boolean allocateForContent) {
        checkArgument(contentLength <= Integer.MAX_VALUE - FRAME_HEADER_LENGTH);

        final ByteBuf buffer;
        if (!allocateForContent) {
            buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + messageHeaderLength);
        } else if (contentLength != -1) {
            buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + messageHeaderLength
                    + contentLength);
        } else {
            // content length unknown -> start with the default initial size
            // (rather than FRAME_HEADER_LENGTH only):
            buffer = allocator.directBuffer();
        }
        // may be updated later, e.g. if contentLength == -1
        buffer.writeInt(FRAME_HEADER_LENGTH + messageHeaderLength + contentLength);
        buffer.writeInt(MAGIC_NUMBER);
        buffer.writeByte(id);

        return buffer;
    }

    // ------------------------------------------------------------------------
    // Generic NettyMessage encoder and decoder
    // ------------------------------------------------------------------------

    @ChannelHandler.Sharable
    public static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                throws IOException {
            if (msg instanceof NettyMessage) {
                ((NettyMessage) msg).write(ctx, promise, ctx.alloc());
            }
            else {
                ctx.write(msg, promise);
            }
        }
    }

    /**
     * Message decoder based on netty's {@link LengthFieldBasedFrameDecoder} but avoiding the
     * additional memory copy inside {@link #extractFrame(ChannelHandlerContext, ByteBuf, int, int)}
     * since we completely decode the {@link ByteBuf} inside {@link #decode(ChannelHandlerContext,
     * ByteBuf)} and will not re-use it afterwards.
     *
     * <p>The frame-length encoder will be based on this transmission scheme created by
     * {@link NettyMessage#allocateBuffer(ByteBufAllocator, byte, int)}:
     * <pre>
     * +------------------+------------------+--------++----------------+
     * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
     * +------------------+------------------+--------++----------------+
     * </pre>
     */
    public static class NettyMessageDecoder extends LengthFieldBasedFrameDecoder {
        /**
         * Creates a new message decoded with the required frame properties.
         */
        public NettyMessageDecoder() {
            super(Integer.MAX_VALUE, 0, 4, -4, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf msg = (ByteBuf) super.decode(ctx, in);
            if (msg == null) {
                return null;
            }

            try {
                int magicNumber = msg.readInt();

                if (magicNumber != MAGIC_NUMBER) {
                    throw new IllegalStateException(
                            "Network stream corrupted: received incorrect magic number.");
                }

                byte msgId = msg.readByte();

                final NettyMessage decodedMsg;
                switch (msgId) {
                    case ParquetReaderInitRequest.ID:
                        decodedMsg = ParquetReaderInitRequest.readFrom(msg);
                        break;
                    case ReadBatchRequest.ID:
                        decodedMsg = ReadBatchRequest.readFrom(msg);
                        break;
                    case HasNextRequest.ID:
                        decodedMsg = new HasNextRequest();
                        break;
                    case SkipNextRowGroupRequest.ID:
                        decodedMsg = new SkipNextRowGroupRequest();
                        break;
                    case CloseReaderRequest.ID:
                        decodedMsg = new CloseReaderRequest();
                        break;
                    case ErrorResponse.ID:
                        decodedMsg = ErrorResponse.readFrom(msg);
                        break;
                    case BooleanResponse.ID:
                        decodedMsg = BooleanResponse.readFrom(msg);
                        break;
                    default:
                        throw new ProtocolException(
                                "Received unknown message from producer: "
                                        + ctx.channel().remoteAddress());
                }

                return decodedMsg;
            } finally {
                // ByteToMessageDecoder cleanup (only the ReadBatchResponse holds on to the decoded
                // msg but already retain()s the buffer once)
                msg.release();
            }
        }
    }

    void writeToChannel(
            ChannelOutboundInvoker out,
            ChannelPromise promise,
            ByteBufAllocator allocator,
            Consumer<ByteBuf> consumer,
            byte id,
            int length) throws IOException {

        ByteBuf byteBuf = null;
        try {
            byteBuf = allocateBuffer(allocator, id, length);
            consumer.accept(byteBuf);
            out.write(byteBuf, promise);
        }
        catch (Throwable t) {
            handleException(byteBuf, t);
        }
    }

    void handleException(ByteBuf byteBuf, Throwable t) throws IOException {
        if (byteBuf != null) {
            byteBuf.release();
        }
        throw new IOException(t);
    }

    // ------------------------------------------------------------------------
    // Client requests
    // ------------------------------------------------------------------------
    public static class ParquetReaderInitRequest extends NettyMessage {
        public static final byte ID = 0;

        private ParquetReaderInitParams params;

        public ParquetReaderInitRequest(ParquetReaderInitParams params) {
            this.params = params;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            final ByteBuf result = allocateBuffer(allocator, ID);

            try (ObjectOutputStream oos = new ObjectOutputStream(new ByteBufOutputStream(result))) {
                oos.writeObject(params);

                // Update frame length...
                result.setInt(0, result.readableBytes());
                out.write(result, promise);
            }
            catch (Throwable t) {
                handleException(result, t);
            }
        }

        static ParquetReaderInitRequest readFrom(ByteBuf buffer) throws Exception {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
                Object obj = ois.readObject();

                if (!(obj instanceof ParquetReaderInitParams)) {
                    throw new ClassCastException(
                            "Read object expected to be of type ReaderInitParams, " +
                            "actual type is " + obj.getClass() + ".");
                } else {
                    return new ParquetReaderInitRequest((ParquetReaderInitParams) obj);
                }
            }
        }

        @Override
        public String toString() {
            return "ReaderInitRequest{" +
                    "params=" + params +
                    '}';
        }
    }

    public static class ReadBatchRequest extends NettyMessage {
        public static final byte ID = 1;

        private final int batchCount;

        public ReadBatchRequest(int batchCount) {
            this.batchCount = batchCount;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            Consumer<ByteBuf> consumer = (bb) -> {
                bb.writeInt(batchCount);
            };

            writeToChannel(out, promise, allocator, consumer, ID, 4);
        }

        static ReadBatchRequest readFrom(ByteBuf buffer) throws IOException {
            return new ReadBatchRequest(buffer.readInt());
        }

        public int getBatchCount() {
            return batchCount;
        }
    }

    public static class HasNextRequest extends NettyMessage {
        public static final byte ID = 2;

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            final ByteBuf result = allocateBuffer(allocator, ID, 0, 0, false);
            out.write(result, promise);
        }
    }

    public static class SkipNextRowGroupRequest extends NettyMessage {
        public static final byte ID = 3;

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            final ByteBuf result = allocateBuffer(allocator, ID, 0, 0, false);
            out.write(result, promise);
        }
    }

    public static class CloseReaderRequest extends NettyMessage {
        public static final byte ID = 4;

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            final ByteBuf result = allocateBuffer(allocator, ID, 0, 0, false);
            out.write(result, promise);
        }
    }
    // ------------------------------------------------------------------------
    // Server responses
    // ------------------------------------------------------------------------

    public static class ErrorResponse extends NettyMessage {
        public static final byte ID = 5;

        final Throwable cause;

        public ErrorResponse(Throwable cause) {
            if (cause == null) {
                throw new NullPointerException();
            }

            this.cause = cause;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            final ByteBuf result = allocateBuffer(allocator, ID);

            try (ObjectOutputStream oos = new ObjectOutputStream(new ByteBufOutputStream(result))) {
                oos.writeObject(cause);

                // Update frame length...
                result.setInt(0, result.readableBytes());
                out.write(result, promise);
            }
            catch (Throwable t) {
                handleException(result, t);
            }
        }

        static ErrorResponse readFrom(ByteBuf buffer) throws Exception {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
                Object obj = ois.readObject();

                if (!(obj instanceof Throwable)) {
                    throw new ClassCastException("Read object expected to be of type Throwable, " +
                            "actual type is " + obj.getClass() + ".");
                } else {
                    return new ErrorResponse((Throwable) obj);
                }
            }
        }

        public Throwable getCause() {
            return cause;
        }

        @Override
        public String toString() {
            return "ErrorResponse{" +
                    "cause=" + cause +
                    '}';
        }
    }

    public static class BooleanResponse extends NettyMessage {
        public static final byte ID = 6;

        private final boolean result;

        // Indicator of type of request that the result responding to.
        private final byte respondingTo;

        BooleanResponse(boolean result, byte respondingTo) {
            this.result = result;
            this.respondingTo = respondingTo;
        }


        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            Consumer<ByteBuf> consumer = (bb) -> {
                bb.writeBoolean(result);
                bb.writeByte(respondingTo);
            };

            writeToChannel(out, promise, allocator, consumer, ID, 1);
        }

        static BooleanResponse readFrom(ByteBuf buffer) throws IOException {
            return new BooleanResponse(buffer.readBoolean(), buffer.readByte());
        }

        public boolean getResult() {
            return result;
        }

        public byte getRespondingTo() {
            return respondingTo;
        }

        @Override
        public String toString() {
            return "BooleanResponse{" +
                    "result=" + result +
                    ", respondTo=" + respondingTo +
                    '}';
        }
    }

    public static class ReadBatchResponse extends NettyMessage {
        public static final byte ID = 7;

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {

        }
    }

}
