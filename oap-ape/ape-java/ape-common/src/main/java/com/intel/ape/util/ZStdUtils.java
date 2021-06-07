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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;

public class ZStdUtils {

  public static ByteBuf compress(CompositeByteBuf compositeByteBuf) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ZstdOutputStream zstdOutputStream = new ZstdOutputStream(outputStream);
    for (ByteBuf byteBuf : compositeByteBuf) {
      if (byteBuf.hasArray()) {
        zstdOutputStream.write(byteBuf.array());
      }
      if (byteBuf.hasMemoryAddress()) {
        byte[] data = new byte[byteBuf.readableBytes()];
        PlatformDependent.copyMemory(byteBuf.memoryAddress(), data, 0, byteBuf.readableBytes());
        zstdOutputStream.write(data);
      }
    }
    zstdOutputStream.close();
    byte[] data = outputStream.toByteArray();
    return Unpooled.wrappedBuffer(data);

  }

  public static ByteBuf decompress(ByteBuf byteBuf, int originLength) throws IOException{
    byte[] compressedData = new byte[byteBuf.readableBytes()];
    byteBuf.readBytes(compressedData);
    // release ByteBuf compressed from buffer.retainedSlice()
    byteBuf.release();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(compressedData);
    ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream);
    byte[] originData = new byte[originLength];
    zstdInputStream.read(originData);
    return Unpooled.wrappedBuffer(originData);

  }
}
