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

package com.intel.ape.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.intel.compression.spark.IntelCompressionCodecBlockInputStream;
import com.intel.compression.spark.IntelCompressionCodecBlockOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

public final class ICLCompressionUtils {

  public static ByteBuf compress(CompositeByteBuf compositeByteBuf, String codec)
          throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    IntelCompressionCodecBlockOutputStream zstdOutputStream = new
            IntelCompressionCodecBlockOutputStream(outputStream, codec, 1,
            4 * 1024 * 1024, false);
    for (ByteBuf byteBuf : compositeByteBuf) {
      if (byteBuf.hasArray()) {
        zstdOutputStream.write(byteBuf.array());
      }
      if (byteBuf.hasMemoryAddress()) {
        zstdOutputStream.write(byteBuf, 0, byteBuf.readableBytes());
      }
    }
    zstdOutputStream.close();
    byte[] data = outputStream.toByteArray();
    return Unpooled.wrappedBuffer(data);
  }

  public static ByteBuf decompress(ByteBuf byteBuf, int originLength) throws IOException {
    ByteBufInputStream inputStream = new ByteBufInputStream(byteBuf);
    IntelCompressionCodecBlockInputStream zstdInputStream =
            new IntelCompressionCodecBlockInputStream(inputStream, 4 * 1024 * 1024, false);
    byte[] originData = new byte[originLength];
    zstdInputStream.read(originData);
    byteBuf.release();
    return Unpooled.wrappedBuffer(originData);
  }
}
