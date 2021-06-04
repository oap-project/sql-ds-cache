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
