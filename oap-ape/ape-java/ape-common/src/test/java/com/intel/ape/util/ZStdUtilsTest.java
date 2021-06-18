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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.intel.compression.util.Platform;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import sun.nio.ch.DirectBuffer;

public class ZStdUtilsTest {
  public static void main(String[] args) {
    byte[] a1 = new byte[]{'a', 'b', 'c'};
    byte[] a2 = new byte[]{'d', 'e', 'f'};
    byte[] a3 = new byte[]{'h', 'i', 'j'};
    ByteBuffer bd1 = ByteBuffer.allocateDirect(1024*1024);
    Platform.copyMemory(a1, Platform.BYTE_ARRAY_OFFSET, null, ((DirectBuffer)bd1).address(), 3);
    ByteBuffer bd2 = ByteBuffer.allocateDirect(300);
    Platform.copyMemory(a2, Platform.BYTE_ARRAY_OFFSET, null, ((DirectBuffer)bd2).address(), 3);
    ByteBuffer bd3 = ByteBuffer.allocateDirect(300);
    Platform.copyMemory(a3, Platform.BYTE_ARRAY_OFFSET, null, ((DirectBuffer)bd3).address(), 3);
    ByteBuf d1 = Unpooled.wrappedBuffer(bd1);
    ByteBuf d2 = Unpooled.wrappedBuffer(bd2);
    ByteBuf d3 = Unpooled.wrappedBuffer(bd3);
    ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{'a', 'b', 'c'});
    ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{'d', 'e', 'f'});
    ByteBuf b3 = Unpooled.wrappedBuffer(new byte[]{'h', 'i', 'j'});
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer(6);
    compositeByteBuf.addComponent(d1);
    compositeByteBuf.addComponent(d2);
    compositeByteBuf.addComponent(d3);
    compositeByteBuf.addComponent(b1);
    compositeByteBuf.addComponent(b2);
    compositeByteBuf.addComponent(b3);
    try {
      ByteBuf compressedData = ICLCompressionUtils.compress(compositeByteBuf, "zstd");

      ByteBuf data = ICLCompressionUtils.decompress(compressedData, 1024*1024 + 609);
      for (int i = 0; i < 18; i++) {
        System.out.println(data.getByte(i));
      }
    } catch (IOException exception) {
      exception.printStackTrace();
    }
  }
}
