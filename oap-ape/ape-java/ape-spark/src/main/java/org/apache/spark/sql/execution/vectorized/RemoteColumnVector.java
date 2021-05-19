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

package org.apache.spark.sql.execution.vectorized;

import io.netty.buffer.ByteBuf;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;

public class RemoteColumnVector extends ColumnVector {

  private int trackingId = 0; // tracking batch ID for resource recycling.
  private final int capacity;
  protected ByteBuf dataBuf;
  protected ByteBuf nullBuf;

  protected ByteBuf elementLengthBuf;
  private final int[] sizes;
  private final int[] offsets;

  public RemoteColumnVector(int capacity, DataType type) {
    super(type);
    this.capacity = capacity;
    sizes = new int[capacity];
    offsets = new int[capacity];
  }

  public void setBuffers(ByteBuf dataBuf, ByteBuf nullBuf, ByteBuf elementLengthBuf) {
    this.dataBuf = dataBuf;
    this.nullBuf = nullBuf;
    this.elementLengthBuf = elementLengthBuf;
    if (elementLengthBuf != null) {
      int i = 0;
      int cur = 0;
      while (elementLengthBuf.isReadable()) {
        int size = elementLengthBuf.readIntLE();
        sizes[i] = size;
        offsets[i] = cur;
        cur += size;
        i++;
      }
    }
  }
  public void setTrackingId(int id) {
    trackingId = id;
  }

  public int getTrackingId() {
    return trackingId;
  }

  @Override
  public boolean hasNull() {
    throw new UnsupportedOperationException("Not support yet");
  }

  @Override
  public int numNulls() {
    throw new UnsupportedOperationException("Not support yet");
  }

  @Override
  public boolean isNullAt(int i) {
    return !(nullBuf.getBoolean(i));
  }


  @Override
  public boolean getBoolean(int i) {
    return dataBuf.getBoolean(i);
  }

  @Override
  public byte getByte(int i) {
    throw new UnsupportedOperationException("Not sure about how to convert yet");
  }

  @Override
  public short getShort(int i) {
    throw new UnsupportedOperationException("Not sure about how to convert yet");
  }

  @Override
  public int getInt(int i) {
    return dataBuf.getIntLE(i * type.defaultSize());
  }

  @Override
  public long getLong(int i) {
    return dataBuf.getLongLE(i * type.defaultSize());
  }

  @Override
  public float getFloat(int i) {
    return dataBuf.getFloatLE(i * type.defaultSize());
  }

  @Override
  public double getDouble(int i) {
    return dataBuf.getDoubleLE(i * type.defaultSize());
  }

  @Override
  public ColumnarArray getArray(int i) {
    throw new UnsupportedOperationException("Not support yet");
  }

  @Override
  public ColumnarMap getMap(int i) {
    throw new UnsupportedOperationException("Not support yet");
  }

  @Override
  public Decimal getDecimal(int i, int precision, int scale) {
    if (isNullAt(i)) return null;
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(get32BitInt(i), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(i), precision, scale);
    } else {
      // convert from parquet fix length byte array to spark decimal
      byte[] bytes = getFixedLenBinary(i);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  public byte[] getFixedLenBinary(int i) {
    byte[] str = new byte[16];
    dataBuf.getBytes(i * 16, str);
    return str;
  }

  private int get32BitInt(int i) {
    return dataBuf.getIntLE(i * 4);
  }

  @Override
  public UTF8String getUTF8String(int i) {
    byte[] str = new byte[sizes[i]];
    dataBuf.getBytes(offsets[i], str);
    return UTF8String.fromBytes(str);
  }

  @Override
  public byte[] getBinary(int i) {
    byte[] str = new byte[sizes[i]];
    dataBuf.getBytes(offsets[i], str);
    return str;
  }

  @Override
  public ColumnVector getChild(int i) {
    throw new UnsupportedOperationException("Not support yet");
  }

  public void reset() {
    // Only element length buffer is sliced from batch response.
    // Data buffer and null buffer will be released by response itself.
    if (elementLengthBuf != null) {
      elementLengthBuf.release();
      elementLengthBuf = null;
    }
  }

  @Override
  public void close()  {
    reset();
  }
}
