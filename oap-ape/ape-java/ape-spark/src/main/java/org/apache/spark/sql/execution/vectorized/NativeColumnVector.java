package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

public class NativeColumnVector extends ColumnVector {
  private long bufferPtr = 0;
  private long nullPtr = 0;
  private int capacity = 0;

  public NativeColumnVector(int capacity_, DataType type) {
    super(type);
  }

  public NativeColumnVector(DataType type) {
    super(type);
  }

  public void reset() {
    bufferPtr = 0;
    nullPtr = 0;
    capacity = 0;
  }

  public void set(long bufferPtr_, long nullPtr_, int size_) {
    bufferPtr = bufferPtr_;
    nullPtr = nullPtr_;
    capacity = size_;
  }

  @Override
  public void close() {
    reset();
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
  public boolean isNullAt(int rowId) {
    byte b = Platform.getByte(null, nullPtr + rowId);
    return (b == 0);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return Platform.getBoolean(null, bufferPtr + rowId * type.defaultSize());
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("Not sure about how to convert yet");
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("Not sure about how to convert yet");
  }

  @Override
  public int getInt(int rowId) {
    return Platform.getInt(null, bufferPtr + rowId * type.defaultSize());
  }

  @Override
  public long getLong(int rowId) {
    return Platform.getLong(null, bufferPtr + rowId * type.defaultSize());
  }

  @Override
  public float getFloat(int rowId) {
    return Platform.getFloat(null, bufferPtr + rowId * type.defaultSize());
  }

  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(null, bufferPtr + rowId * type.defaultSize());
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("Not support yet");
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException("Not support yet");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    if (precision <= Decimal.MAX_INT_DIGITS()) {
      return Decimal.createUnsafe(get32BitInt(rowId), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return Decimal.createUnsafe(getLong(rowId), precision, scale);
    } else {
      throw new UnsupportedOperationException("Not support yet");
    }
  }

  private int get32BitInt(int rowId) {
    return Platform.getInt(null, bufferPtr + rowId * 4);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    int size = Platform.getInt(null, bufferPtr + rowId * 16);
    byte[] str = new byte[size];
    long addr = Platform.getLong(null, bufferPtr + rowId * 16 + 8);
    Platform.copyMemory(null, addr, str, Platform.BYTE_ARRAY_OFFSET, size);
    return UTF8String.fromBytes(str);
    // is it possible?
    // return  UTF8String.fromAddress(null, bufferPtr + rowId * 16 + 8, size);
  }

  @Override
  public byte[] getBinary(int rowId) {
    int size = Platform.getInt(null, bufferPtr + rowId * 16);
    byte[] str = new byte[size];
    long addr = Platform.getLong(null, bufferPtr + rowId * 16 + 8);
    Platform.copyMemory(null, addr, str, Platform.BYTE_ARRAY_OFFSET, size);
    return str;
  }

  @Override
  public ColumnVector getChild(int rowId) {
    throw new UnsupportedOperationException("Not support yet");
  }
}
