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

package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

public class SkippableVectorizedRleValuesReader extends ValuesReader
        implements SkippableVectorizedValuesReader {

  protected enum MODE {
    RLE,
    PACKED
  }

  // Encoded data.
  private ByteBufferInputStream in;

  // bit/byte width of decoded data and utility to batch unpack them.
  private int bitWidth;
  private int bytesWidth;
  private BytePacker packer;

  // Current decoding mode and values
  protected SkippableVectorizedRleValuesReader.MODE mode;
  protected int currentCount;
  protected int currentValue;

  // Buffer of decoded values if the values are PACKED.
  protected int[] currentBuffer = new int[16];
  protected int currentBufferIdx = 0;

  // If true, the bit width is fixed. This decoder is used in different places and this also
  // controls if we need to read the bitwidth from the beginning of the data stream.
  private final boolean fixedWidth;
  private final boolean readLength;

  private static final String UNSUPPORTED_OP_MSG = "only skipIntegers is valid.";

  public SkippableVectorizedRleValuesReader() {
    this.fixedWidth = false;
    this.readLength = false;
  }

  public SkippableVectorizedRleValuesReader(int bitWidth) {
    this.fixedWidth = true;
    this.readLength = bitWidth != 0;
    init(bitWidth);
  }

  public SkippableVectorizedRleValuesReader(int bitWidth, boolean readLength) {
    this.fixedWidth = true;
    this.readLength = readLength;
    init(bitWidth);
  }


  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.in = in;
    if (fixedWidth) {
      // initialize for repetition and definition levels
      if (readLength) {
        int length = readIntLittleEndian();
        this.in = in.sliceStream(length);
      }
    } else {
      // initialize for values
      if (in.available() > 0) {
        init(in.read());
      }
    }
    if (bitWidth == 0) {
      // 0 bit width, treat this as an RLE run of valueCount number of 0's.
      this.mode = SkippableVectorizedRleValuesReader.MODE.RLE;
      this.currentCount = valueCount;
      this.currentValue = 0;
    } else {
      this.currentCount = 0;
    }
  }

  /**
   * Initializes the internal state for decoding ints of `bitWidth`.
   */
  private void init(int bitWidth) {
    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
  }


  @Override
  public boolean readBoolean() {
    return this.readInteger() != 0;
  }

  @Override
  public void skip() {
    this.readInteger();
  }

  @Override
  public int readValueDictionaryId() {
    return readInteger();
  }

  @Override
  public int readInteger() {
    if (this.currentCount == 0) { this.readNextGroup(); }

    this.currentCount--;
    switch (mode) {
      case RLE:
        return this.currentValue;
      case PACKED:
        return this.currentBuffer[currentBufferIdx++];
    }
    throw new RuntimeException("Unreachable");
  }

  /**
   * Reads `total` ints into `c` filling them in starting at `c[rowId]`. This reader
   * reads the definition levels and then will read from `data` for the non-null values.
   * If the value is null, c will be populated with `nullValue`. Note that `nullValue` is only
   * necessary for readIntegers because we also use it to decode dictionaryIds and want to make
   * sure it always has a value in range.
   *
   * This is a batched version of this logic:
   *  if (this.readInt() == level) {
   *    c[rowId] = data.readInteger();
   *  } else {
   *    c[rowId] = null;
   *  }
   */
  public void readIntegers(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readIntegers(n, c, rowId);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              c.putInt(rowId + i, data.readInteger());
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  // A fork of `readIntegers`, which rebases the date int value (days) before filling
  // the Spark column vector.
  public void readIntegersWithRebase(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data,
          final boolean failIfRebase) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readIntegersWithRebase(n, c, rowId, failIfRebase);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              int julianDays = data.readInteger();
              c.putInt(rowId + i, VectorizedColumnReader.rebaseDays(julianDays, failIfRebase));
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  // TODO: can this code duplication be removed without a perf penalty?
  public void readBooleans(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readBooleans(n, c, rowId);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              c.putBoolean(rowId + i, data.readBoolean());
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  public void readBytes(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readBytes(n, c, rowId);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              c.putByte(rowId + i, data.readByte());
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  public void readShorts(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            for (int i = 0; i < n; i++) {
              c.putShort(rowId + i, (short)data.readInteger());
            }
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              c.putShort(rowId + i, (short)data.readInteger());
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  public void readLongs(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readLongs(n, c, rowId);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              c.putLong(rowId + i, data.readLong());
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  // A fork of `readLongs`, which rebases the timestamp long value (microseconds) before filling
  // the Spark column vector.
  public void readLongsWithRebase(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data,
          final boolean failIfRebase) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readLongsWithRebase(n, c, rowId, failIfRebase);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              long julianMicros = data.readLong();
              c.putLong(rowId + i, VectorizedColumnReader.rebaseMicros(julianMicros, failIfRebase));
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  public void readFloats(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readFloats(n, c, rowId);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              c.putFloat(rowId + i, data.readFloat());
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  public void readDoubles(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readDoubles(n, c, rowId);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              c.putDouble(rowId + i, data.readDouble());
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  public void readBinarys(
          int total,
          WritableColumnVector c,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readBinary(n, c, rowId);
          } else {
            c.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.readBinary(1, c, rowId + i);
            } else {
              c.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  /**
   * Decoding for dictionary ids. The IDs are populated into `values` and the nullability is
   * populated into `nulls`.
   */
  public void readIntegers(
          int total,
          WritableColumnVector values,
          WritableColumnVector nulls,
          int rowId,
          int level,
          VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.readIntegers(n, values, rowId);
          } else {
            nulls.putNulls(rowId, n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              values.putInt(rowId + i, data.readInteger());
            } else {
              nulls.putNull(rowId + i);
            }
          }
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }


  // The RLE reader implements the vectorized decoding interface when used to decode dictionary
  // IDs. This is different than the above APIs that decodes definitions levels along with values.
  // Since this is only used to decode dictionary IDs, only decoding integers is supported.
  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          c.putInts(rowId, n, currentValue);
          break;
        case PACKED:
          c.putInts(rowId, n, currentBuffer, currentBufferIdx);
          currentBufferIdx += n;
          break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  @Override
  public void readIntegersWithRebase(
          int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBytes(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readLongsWithRebase(
          int total, WritableColumnVector c, int rowId, boolean failIfRebase) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBooleans(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readFloats(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readDoubles(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public Binary readBinary(int len) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  /**
   * Reads the next varint encoded int.
   */
  private int readUnsignedVarInt() throws IOException {
    int value = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      value |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return value;
  }

  /**
   * Reads the next 4 byte little endian int.
   */
  private int readIntLittleEndian() throws IOException {
    int ch4 = in.read();
    int ch3 = in.read();
    int ch2 = in.read();
    int ch1 = in.read();
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  /**
   * Reads the next byteWidth little endian int.
   */
  private int readIntLittleEndianPaddedOnBitWidth() throws IOException {
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return in.read();
      case 2: {
        int ch2 = in.read();
        int ch1 = in.read();
        return (ch1 << 8) + ch2;
      }
      case 3: {
        int ch3 = in.read();
        int ch2 = in.read();
        int ch1 = in.read();
        return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
      }
      case 4: {
        return readIntLittleEndian();
      }
    }
    throw new RuntimeException("Unreachable");
  }

  private int ceil8(int value) {
    return (value + 7) / 8;
  }

  /**
   * Reads the next group.
   */
  protected void readNextGroup() {
    try {
      int header = readUnsignedVarInt();
      this.mode = (header & 1) == 0 ? SkippableVectorizedRleValuesReader.MODE.RLE :
              SkippableVectorizedRleValuesReader.MODE.PACKED;
      switch (mode) {
        case RLE:
          this.currentCount = header >>> 1;
          this.currentValue = readIntLittleEndianPaddedOnBitWidth();
          return;
        case PACKED:
          int numGroups = header >>> 1;
          this.currentCount = numGroups * 8;

          if (this.currentBuffer.length < this.currentCount) {
            this.currentBuffer = new int[this.currentCount];
          }
          currentBufferIdx = 0;
          int valueIndex = 0;
          while (valueIndex < this.currentCount) {
            // values are bit packed 8 at a time, so reading bitWidth will always work
            ByteBuffer buffer = in.slice(bitWidth);
            this.packer.unpack8Values(buffer, buffer.position(), this.currentBuffer, valueIndex);
            valueIndex += 8;
          }
          return;
        default:
          throw new ParquetDecodingException("not a valid mode " + this.mode);
      }
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read from input stream", e);
    }
  }

  public void skipIntegers(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipIntegers(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipInteger();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipBooleans(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipBooleans(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipBoolean();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipBytes(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipBytes(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipByte();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipShorts(int total, int level, SkippableVectorizedValuesReader data) {
    skipIntegers(total, level, data);
  }

  public void skipLongs(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipLongs(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipLong();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipFloats(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipFloats(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipFloat();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void skipDoubles(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipDoubles(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipDouble();
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }


  public void skipBinarys(int total, int level, SkippableVectorizedValuesReader data) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == level) {
            data.skipBinary(n);
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == level) {
              data.skipBinary(1);
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  @Override
  public void skipBoolean() {
    this.skipInteger();
  }

  @Override
  public void skipInteger() {
    if (this.currentCount == 0) { this.readNextGroup(); }

    this.currentCount--;
    switch (mode) {
      case RLE:
        break;
      case PACKED:
        currentBufferIdx++;
        break;
      default:
        throw new RuntimeException("Unreachable");
    }
  }

  @Override
  public void skipIntegers(int total) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0) this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          break;
        case PACKED:
          currentBufferIdx += n;
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  @Override
  public void skipByte() {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipBooleans(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipBytes(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipLongs(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipFloats(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipDoubles(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipBinary(int total) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipLong() {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipFloat() {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipDouble() {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }

  @Override
  public void skipBinaryByLen(int len) {
    throw new UnsupportedOperationException(UNSUPPORTED_OP_MSG);
  }
}
