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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

public class ZStdUtilsTest {
  public static void main(String[] args) {
    ByteBuf b1 = Unpooled.wrappedBuffer(new byte[]{'a', 'b', 'c'});
    ByteBuf b2 = Unpooled.wrappedBuffer(new byte[]{'d', 'e', 'f'});
    ByteBuf b3 = Unpooled.wrappedBuffer(new byte[]{'h', 'i', 'j'});
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer(3);
    compositeByteBuf.addComponent(b1);
    compositeByteBuf.addComponent(b2);
    compositeByteBuf.addComponent(b3);
    try {
      ByteBuf compressedData = ZStdUtils.compress(compositeByteBuf);

      ByteBuf data = ZStdUtils.decompress(compressedData, 9);
      for (int i = 0; i < 9; i++) {
        System.out.println(data.getByte(i));
      }
    } catch (IOException exception) {
      exception.printStackTrace();
    }
  }
}
