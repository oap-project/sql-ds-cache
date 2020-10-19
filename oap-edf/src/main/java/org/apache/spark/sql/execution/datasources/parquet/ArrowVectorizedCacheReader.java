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

import com.intel.oap.vectorized.ArrowWritableColumnVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;

import org.apache.spark.sql.execution.cacheUtil.ArrowFiberCache;
import org.apache.spark.sql.execution.cacheUtil.FiberCache;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.unsafe.Platform;


public class ArrowVectorizedCacheReader extends VectorizedCacheReader {

  long total;
  static BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public ArrowVectorizedCacheReader(DataType type, FiberCache fiberCache) {
    super(type, fiberCache);
    this.total = Platform.getLong(null, ((ArrowFiberCache)fiberCache).getBuffer().address());
  }

  public ColumnVector readBatch(int num) {
    if(fiberCache instanceof ArrowFiberCache) {
      long addr = ((ArrowFiberCache) fiberCache).getBuffer().address();
      long header = 32;
      long nullOffset = addr + header + index / 8;
      long dataOffset = addr + header + ((total / 8 + 0x0F) & (~0x0F)) + index * typeSize;
      // TODO: construct a ValueVector via Offset
      ValueVector vector = buildVector(nullOffset, dataOffset, num);
      ColumnVector column = new ArrowWritableColumnVector(vector, null, 0, num, false);
      index += num;
      return column;
    } else {
      throw new UnsupportedOperationException("Only support ArrowFiberCache Now");
    }
  }

  private ValueVector buildVector(long nullOffset, long dataOffset, int num) {
    if (type instanceof BooleanType) {
      return buildBitVector(nullOffset, dataOffset, num);
    } else if (type instanceof ByteType) {
      return buildTinyIntVector(nullOffset, dataOffset, num);
    } else if (type instanceof ShortType) {
      return buildSmallIntVector(nullOffset, dataOffset, num);
    } else if (type instanceof IntegerType) {
      return buildIntVector(nullOffset, dataOffset, num);
    } else if (type instanceof LongType) {
      return buildBigIntVector(nullOffset, dataOffset, num);
    } else if (type instanceof FloatType) {
      return buildFloat4Vector(nullOffset, dataOffset, num);
    } else if (type instanceof DoubleType) {
      return buildFloat8Vector(nullOffset, dataOffset, num);
    } else if (type instanceof StringType) {
      return buildVarCharVector(nullOffset, dataOffset, num);
    } else {
      // TODO: Decimal type, TimeStamp type.
      throw new UnsupportedOperationException("type not support!");
    }
  }

  private boolean isNullAt(long nullOffset, int index) {
    Byte b = Platform.getByte(null, nullOffset + index / 8);
    return (b & (1 << index % 8)) != 0;
  }

  // we are using byte type in cache while vector use bit.
  private ValueVector buildBitVector(long nullOffset, long dataOffset, int num) {
    BitVector vector = new BitVector("bit vector", allocator);
    vector.allocateNew(num);
    for (int i = 0; i < num; i++) {
      if(isNullAt(nullOffset, i)) vector.setNull(i);
      else vector.set(i, Platform.getByte(null, dataOffset + i * typeSize));
    }
    return vector;
  }

  private ValueVector buildTinyIntVector(long nullOffset, long dataOffset, int num) {
    TinyIntVector vector = new TinyIntVector("tinyint vector", allocator);
    vector.allocateNew(num);
    for (int i = 0; i < num; i++) {
      if(isNullAt(nullOffset, i)) vector.setNull(i);
      else vector.set(i, Platform.getByte(null, dataOffset + i * typeSize));
    }
    return vector;
  }

  private ValueVector buildSmallIntVector(long nullOffset, long dataOffset, int num) {
    SmallIntVector vector = new SmallIntVector("smallint vector", allocator);
    vector.allocateNew(num);
    for (int i = 0; i < num; i++) {
      if(isNullAt(nullOffset, i)) vector.setNull(i);
      else vector.set(i, Platform.getShort(null, dataOffset + i * typeSize));
    }
    return vector;
  }

  private ValueVector buildIntVector(long nullOffset, long dataOffset, int num) {
    IntVector vector = new IntVector("int vector", allocator);
    vector.allocateNew(num);
    for (int i = 0; i < num; i++) {
      if(isNullAt(nullOffset, i)) vector.setNull(i);
      else vector.set(i, Platform.getInt(null, dataOffset + i * typeSize));
    }
    return vector;
  }

  private ValueVector buildBigIntVector(long nullOffset, long dataOffset, int num) {
    BigIntVector vector = new BigIntVector("bigint vector", allocator);
    vector.allocateNew(num);
    for (int i = 0; i < num; i++) {
      if(isNullAt(nullOffset, i)) vector.setNull(i);
      else vector.set(i, Platform.getLong(null, dataOffset + i * typeSize));
    }
    return vector;
  }

  private ValueVector buildFloat4Vector(long nullOffset, long dataOffset, int num) {
    Float4Vector vector = new Float4Vector("float4 vector", allocator);
    vector.allocateNew(num);
    for (int i = 0; i < num; i++) {
      if(isNullAt(nullOffset, i)) vector.setNull(i);
      else vector.set(i, Platform.getFloat(null, dataOffset + i * typeSize));
    }
    return vector;
  }

  private ValueVector buildFloat8Vector(long nullOffset, long dataOffset, int num) {
    Float8Vector vector = new Float8Vector("float8 vector", allocator);
    vector.allocateNew(num);
    for (int i = 0; i < num; i++) {
      if(isNullAt(nullOffset, i)) vector.setNull(i);
      else vector.set(i, Platform.getDouble(null, dataOffset + i * typeSize));
    }
    return vector;
  }

  private ValueVector buildVarCharVector(long nullOffset, long dataOffset, int num) {
    VarCharVector vector = new VarCharVector("VarChar vector", allocator);
    // TODO: impl
    return vector;
  }
}
