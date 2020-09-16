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
package org.apache.spark.sql.execution.vectorized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import sun.nio.ch.DirectBuffer;

import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 *  This class maintains a ByteBuffer(could be a directBuffer), provide offheap access.
 *  Any put method is not supported
 * */

/**
 * In memory fiber cache structure
 *
 * *****************************************************************
 * *          *           *            *       *            *      *
 * * No nulls * All nulls * Dic Length * nulls * dictionary * data *
 * *****************************************************************
 * *  Boolean *  Boolean  *   Int      * total * dic length * total*
 * *****************************************************************
 *
 * ReadOnlyColumnVectorV1 structure: don't consider dictionary and nested type.
 * ***********************************************
 * *       *          *           *       *      *
 * * total * No nulls * All nulls * nulls * data *
 * ***********************************************
 * *  Int  *  Boolean *  Boolean  * total * total*
 * ***********************************************
 *
 * ReadOnlyColumnVectorV2 structure
 * *************************************************************************
 * *       *          *           *            *       *            *      *
 * * total * no nulls * all nulls * dic Length * nulls * dictionary * data *
 * *************************************************************************
 * *  Int  *  Boolean *  Boolean  *   Int      * total * dic length * total*
 * *************************************************************************
 *
 * */

public class ReadOnlyColumnVectorV1 extends ColumnVector {

  private ByteBuffer buffer;
  private DirectBuffer directBuffer;
  private boolean isDirect;

  private int total;

  private boolean allNull;
  private boolean noNull;
  private int nullsNum;

  private Dictionary dictionary = null;

  private int TOTAL_OFFSET = 0;
  private int NONULL_OFFSET = TOTAL_OFFSET + 4;
  private int ALLNULL_OFFSET = NONULL_OFFSET + 1;
  private int NULLS_OFFSET = ALLNULL_OFFSET + 1;
  private int DATA_OFFSET = NULLS_OFFSET;

  private long nulls = 0;
  private long data = 0;

  private void readDictionary(int dicLength, Long dicNativeAddress) {
    dictionary = null;
  }

  void getNulls(long nativeAddress, int len) {
  }

  private void initWithDirectBuffer() {
    long nativeAddress = directBuffer.address();
    total = Platform.getInt(null, nativeAddress + TOTAL_OFFSET);

    noNull = Platform.getBoolean(null, nativeAddress + NONULL_OFFSET);
    allNull = Platform.getBoolean(null, nativeAddress + ALLNULL_OFFSET);
    if(!noNull && !allNull) {
      nulls = nativeAddress + NULLS_OFFSET;
      getNumNulls();
      DATA_OFFSET = NULLS_OFFSET + total;
    }

    dictionary = null;

    data = nativeAddress + DATA_OFFSET;
  }

  //  public ReadOnlyColumnVectorV1(ByteBuffer buffer, DataType type) {
  //    super(type);
  //    if(isArray() || type instanceof StructType ||
  //        type instanceof MapType || type instanceof CalendarIntervalType) {
  //      throw new UnsupportedOperationException("Unsupported type: " + type.typeName());
  //    }
  //
  //    this.buffer = buffer;
  //    if(buffer.isDirect()) {
  //      directBuffer = (DirectBuffer) buffer;
  //      isDirect = true;
  //      initWithDirectBuffer();
  //    } else {
  //      directBuffer = null;
  //      isDirect = false;
  //    }
  //  }

  public ReadOnlyColumnVectorV1(DataType type, long nullAddr, long dataAddr, int num) {
    super(type);
    nulls = nullAddr;
    data = dataAddr;
    total = num;
    getNumNulls();
  }

  @Override
  public void close() {

  }

  @Override
  public boolean hasNull() {
    return !noNull;
  }

  private void getNumNulls() {
    for (int i = 0; i < total; i++) {
      if(isNullAt(i)) {
        nullsNum++;
      }
    }
    noNull = nullsNum == 0 ? false : true;
    allNull = nullsNum == total ? true : false;
  }

  @Override
  public int numNulls() {
    return nullsNum;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return Platform.getByte(null, nulls + rowId) == 1;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return Platform.getByte(null, data + rowId) == 1;
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    assert(dictionary == null);
    boolean[] array = new boolean[count];
    for (int i = 0; i < count; ++i) {
      array[i] = (Platform.getByte(null, data + rowId + i) == 1);
    }
    return array;
  }

  @Override
  public byte getByte(int rowId) {
    if (dictionary == null) {
      return Platform.getByte(null, data + rowId);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    assert(dictionary == null);
    byte[] array = new byte[count];
    Platform.copyMemory(null, data + rowId, array, Platform.BYTE_ARRAY_OFFSET, count);
    return array;
  }

  @Override
  public short getShort(int rowId) {
    if (dictionary == null) {
      return Platform.getShort(null, data + 2L * rowId);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    assert(dictionary == null);
    short[] array = new short[count];
    Platform.copyMemory(null, data + rowId * 2L, array, Platform.SHORT_ARRAY_OFFSET, count * 2L);
    return array;
  }

  @Override
  public int getInt(int rowId) {
    if (dictionary == null) {
      return Platform.getInt(null, data + 4L * rowId);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public int[] getInts(int rowId, int count) {
    assert(dictionary == null);
    int[] array = new int[count];
    Platform.copyMemory(null, data + rowId * 4L, array, Platform.INT_ARRAY_OFFSET, count * 4L);
    return array;
  }

  public int getDictId(int rowId) {
    assert(dictionary == null)
            : "A ColumnVector dictionary should not have a dictionary for itself.";
    return Platform.getInt(null, data + 4L * rowId);
  }

  @Override
  public long getLong(int rowId) {
    if (dictionary == null) {
      return Platform.getLong(null, data + 8L * rowId);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    assert(dictionary == null);
    long[] array = new long[count];
    Platform.copyMemory(null, data + rowId * 8L, array, Platform.LONG_ARRAY_OFFSET, count * 8L);
    return array;
  }

  @Override
  public float getFloat(int rowId) {
    if (dictionary == null) {
      return Platform.getFloat(null, data + rowId * 4L);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    assert(dictionary == null);
    float[] array = new float[count];
    Platform.copyMemory(null, data + rowId * 4L, array, Platform.FLOAT_ARRAY_OFFSET, count * 4L);
    return array;
  }

  @Override
  public double getDouble(int rowId) {
    if (dictionary == null) {
      return Platform.getDouble(null, data + rowId * 8L);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    assert(dictionary == null);
    double[] array = new double[count];
    Platform.copyMemory(null, data + rowId * 8L, array, Platform.DOUBLE_ARRAY_OFFSET, count * 8L);
    return array;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(getInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      // TODO: best perf?
      byte[] bytes = getBinary(rowId);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return Decimal.apply(javaDecimal, precision, scale);
    }
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException();
  }

  protected boolean isArray() {
    return type instanceof ArrayType || type instanceof BinaryType || type instanceof StringType ||
            DecimalType.isByteArrayDecimalType(type);
  }

}
